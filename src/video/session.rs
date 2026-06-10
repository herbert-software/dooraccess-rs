//! session：video session 生命周期 Manager（移植 Go `internal/video/session.go` 全部：
//! Outdoor/Caller/SessionInfo/Manager/Start/Stop/Shutdown/RefreshTTL/stopInternal/
//! shutdown teardown/TTL monitor/newUUIDv4/newRandomSSRC/IsValidUUID）。
//!
//! 等价纪律（spec「session 生命周期与 TTL 等价」/ design D2/D4/D5）：
//!   - 单 active session；**Start 持 Manager 锁跨 bind + preview 信令全程**
//!     （Go `session.go:228` defer unlock，最长 ~10s）——互斥对 in-flight Start
//!     也成立，禁止信令期间放锁（否则并发 Start 双双过 preview 造双 session）
//!   - 同 outdoor 幂等复用：**仅 `latest_idr` 三件套齐**才刷 TTL（死 session
//!     不得被 client retry 反复续命，钉死唯一 active slot）
//!   - Start 顺序 = bind UDP（同步，先于信令）→ preview start（失败回滚**仅关
//!     socket**）→ UUID v4 + 随机 SSRC（`/dev/urandom`，读失败返错不 panic；
//!     失败发生在 preview ack 之后，回滚 = 关 socket + **best-effort StopPreview
//!     （3s）**，否则外机持续推流无人接）→ 起 RTP/RTCP/TTL 三线程
//!     （per-session `Arc<AtomicBool>` stop flag）
//!   - TTL 默认 60s；deadline 存 **`Mutex<Instant>`**（D4：MIPS32 禁 64-bit 原子；
//!     `Instant` 单调，对 Go wall-clock 的有意实现差异）；1s monitor 轮询
//!   - TTL 过期 → **detached cleanup**（monitor 禁自 join——Go 死锁同坑：monitor
//!     只发起 detached 线程跑 stop_internal，自己继续等 stop 信号 drain RTP/RTCP
//!     后发 done）；JoinHandle 收进 `Mutex<Vec<JoinHandle>>` + retain 模式
//!     （复刻 daemon.rs PushTracker 套路）
//!   - 双驱动竞态（显式 Stop vs TTL 过期）用「摘 current 比对」CAS 守卫
//!     （等价 Go `stopInternal` 的 `m.current != sess` 早返）保证 teardown 单方执行
//!   - teardown 序列：preview stop（best-effort，按剩余预算裁短/跳过）→ RTCP BYE
//!     （best-effort，照发）→ ~50ms RTP 尾等待（observations §3.4，按剩余预算裁短）→
//!     置位 stop + 等三线程退出（2s 兜底按剩余预算裁短，超时放弃并 log）→
//!     `FrameBuffer::close()`（**不受预算约束，永远执行**——资源释放不可跳）
//!   - teardown 全程受 deadline 总预算约束（锚 Go 三入口真相）：显式 stop 8s
//!     （video_handlers.go handler 8s ctx 包整个 shutdown）/ TTL 过期 detached
//!     cleanup 5s（stopInternal 自建 5s ctx）/ daemon shutdown 收 caller deadline
//!     （main stopCtx 5s），shutdown 内部 teardown 共享同一预算不叠加
//!   - `Manager::shutdown(deadline)` 摘 current 跑 teardown + 对账全部 detached
//!     cleanup（deadline 内等完，超时放弃 + log——等价 Go cleanupWG.Wait 受 ctx 约束）
//!
//! 测试注入口（D8）：`ttl` / `rtp_listen_addr`（等价 Go Manager.TTL/RTPListenAddr
//! struct 字段）+ `ttl_monitor_interval` / `teardown_tail_wait` / `stop_budget` /
//! `ttl_cleanup_budget` / `urandom_path`
//! 均为 per-Manager 字段（无全局 static，无原子，MIPS32 天然合规）。

use std::io::{self, Read};
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use super::frame_buffer::FrameBuffer;
use super::preview::{PreviewClient, PreviewError, PREVIEW_DIAL_TIMEOUT};
use super::rtcp::RtcpSender;
use super::rtp::{bind_udp, RtpReceiver};
use super::{LogFn, SharedLogFn};
use crate::codec::{self, BcdError, UriError};

/// active session 在无 keepalive 续期时的最大存活时间。锚 Go `DefaultSessionTTL`。
pub const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(60);

/// RTP receiver 默认监听地址（Go 默认 ":9880"；Rust `UdpSocket::bind` 需显式 host）。
const DEFAULT_RTP_LISTEN_ADDR: &str = "0.0.0.0:9880";

/// TTL monitor 默认轮询周期（锚 Go 1s ticker）。
const DEFAULT_TTL_MONITOR_INTERVAL: Duration = Duration::from_secs(1);

/// teardown 第 3 步残余 RTP 尾等待默认值（observations §3.4 停止尾实测均值 ~52ms）。
const DEFAULT_TEARDOWN_TAIL_WAIT: Duration = Duration::from_millis(50);

/// UUID/SSRC 生成失败回滚时 best-effort StopPreview 的超时（锚 Go
/// `context.WithTimeout(3s)`，session.go UUID/SSRC 错误分支）。
const ROLLBACK_STOP_PREVIEW_TIMEOUT: Duration = Duration::from_secs(3);

/// teardown 第 4 步等三线程退出的兜底上限（锚 Go `<-sess.done` 的 2s abandoning；
/// 实际等待按剩余预算裁短）。
const SESSION_THREADS_EXIT_WAIT: Duration = Duration::from_secs(2);

/// 显式 stop 的 teardown 总预算默认值（锚 Go `handleVideoStop` 的 8s handler ctx
/// ——video_handlers.go：8s ctx 包住 `Manager.Stop` 整个 shutdown 序列，StopPreview
/// 受 ctx 裁短、50ms 尾等是 `select{After(50ms), ctx.Done()}`）。
const DEFAULT_STOP_BUDGET: Duration = Duration::from_secs(8);

/// TTL 过期 detached cleanup 的 teardown 总预算默认值（锚 Go `stopInternal`
/// 自建 `context.WithTimeout(5s)`）。
const DEFAULT_TTL_CLEANUP_BUDGET: Duration = Duration::from_secs(5);

/// monitor 线程 stop flag 分片轮询片长（援引 self_unlock HANGUP_POLL_SLICE 惯用法）。
const MONITOR_POLL_SLICE: Duration = Duration::from_millis(20);

/// 随机源设备节点（design D5：禁 rand/uuid/getrandom crate，`std::fs` 读零新 FFI 面）。
const URANDOM_PATH: &str = "/dev/urandom";

// ---------------------------------------------------------------------------
// Outdoor / Caller（锚 Go ParseOutdoor / ParseCaller，复用 codec）
// ---------------------------------------------------------------------------

/// Outdoor/Caller URI 解析错误（包装 codec 两类错误；HTTP 400 body 字面由
/// 组 F 经 `control::format_uri_err` 等价路径生成，本 Display 仅诊断用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// SIP URI 拆分失败（缺 @ / 名长 / IPv4 / port）。
    Uri(UriError),
    /// 号码串伪 BCD 编码失败。
    Bcd(BcdError),
}

impl core::fmt::Display for ParseError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ParseError::Uri(e) => write!(f, "video: parse uri: {e:?}"),
            ParseError::Bcd(e) => write!(f, "video: encode bcd: {e:?}"),
        }
    }
}

impl std::error::Error for ParseError {}

/// 一个外机的连接参数（从 `cfg.Stations[*].SIP` 解析出的 BCD + IP + port）。
/// 锚 Go `Outdoor`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outdoor {
    /// 完整 SIP URI 形如 `06020000@172.16.106.152:18022`（idempotent 复用判断 key）。
    pub uri: String,
    /// 8 char 号码（BCD 编码源）。
    pub name: String,
    /// IPv4 字符串。
    pub ip: String,
    /// TCP 18022 端口。
    pub port: u16,
    /// 号码的 4 字节伪 BCD。
    pub bcd_name: [u8; 4],
}

impl Outdoor {
    /// 18022 TCP 端点。锚 Go `AddrTCP`。
    pub fn addr_tcp(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }

    /// RTCP 反馈的 outdoor:6671 地址（端口非标，specs/anjubao-video-stream
    /// /messages/udp9881-rtcp/format.md：违反 RFC 5761 但稳定行为）。锚 Go `AddrRTCP`。
    pub fn addr_rtcp(&self) -> String {
        format!("{}:6671", self.ip)
    }
}

/// 从 SIP URI 解析 [`Outdoor`]。锚 Go `ParseOutdoor`。
pub fn parse_outdoor(uri: &str) -> Result<Outdoor, ParseError> {
    let (name, ip, port) = codec::parse_uri(uri).map_err(ParseError::Uri)?;
    let bcd = codec::encode_bcd(&name).map_err(ParseError::Bcd)?;
    Ok(Outdoor {
        uri: uri.to_string(),
        name,
        ip,
        port,
        bcd_name: bcd,
    })
}

/// 本机室内机参数（从 cfg.SIP 解析）。preview 帧 BCD caller 字段必须填它，
/// 外机据此决定把 RTP 推到哪台室内机（不查源 IP）。锚 Go `Caller`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    /// 8 char 号码。
    pub name: String,
    /// 号码的 4 字节伪 BCD。
    pub bcd_name: [u8; 4],
}

/// 从 cfg.SIP 解析 [`Caller`]。锚 Go `ParseCaller`。
pub fn parse_caller(uri: &str) -> Result<Caller, ParseError> {
    let (name, _, _) = codec::parse_uri(uri).map_err(ParseError::Uri)?;
    let bcd = codec::encode_bcd(&name).map_err(ParseError::Bcd)?;
    Ok(Caller {
        name,
        bcd_name: bcd,
    })
}

// ---------------------------------------------------------------------------
// SessionInfo（锚 Go SessionInfo；组 F 序列化成 /video/start 200 JSON）
// ---------------------------------------------------------------------------

/// `Manager::start` 返回给 HTTP handler 的轻量描述（HACS 端用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionInfo {
    /// UUID v4。
    pub id: String,
    /// 外机 URI（idempotent 复用判断 key）。
    pub outdoor_uri: String,
    /// `/video/<id>/stream.flv`。
    pub stream_url: String,
    /// 秒级粒度暴露给 HACS（生产 60）。
    pub ttl_secs: u64,
}

// ---------------------------------------------------------------------------
// StartError（组 F 错误映射面：Conflict→409 / Preview timeout→503 / 其余→503）
// ---------------------------------------------------------------------------

/// `Manager::start` 错误。Display 字面对齐 Go 各错误路径的 wrap 格式
/// （errorJSON 内嵌错误字串是契约，组 F golden 逐字对）。
#[derive(Debug)]
pub enum StartError {
    /// 已有不同 outdoor 的 active session（并发限制 1）→ HTTP 409。
    /// 锚 Go `ErrConflict` + `fmt.Errorf("%w: active=%s, requested=%s", ...)`。
    Conflict {
        /// 现 active session 的 outdoor URI。
        active: String,
        /// 本次请求的 outdoor URI。
        requested: String,
    },
    /// UDP 9880 bind 失败（端口被占等）→ HTTP 503（其它失败档）。
    Bind {
        /// 尝试 bind 的地址。
        addr: String,
        /// 底层错误。
        source: io::Error,
    },
    /// preview 信令失败（dial/ack 5s 内未完成或 ack 校验失败）→ HTTP 503。
    /// 锚 Go `ErrPreviewTimeout` wrap（`%w: %v`）。
    Preview(PreviewError),
    /// UUID 生成失败（/dev/urandom 读失败）→ HTTP 503。锚 Go "video: gen UUID"。
    Uuid(io::Error),
    /// SSRC 生成失败 → HTTP 503。锚 Go "video: gen SSRC"。
    Ssrc(io::Error),
}

impl core::fmt::Display for StartError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            StartError::Conflict { active, requested } => write!(
                f,
                "video: another outdoor session is active: active={active}, requested={requested}"
            ),
            StartError::Bind { addr, source } => write!(f, "video: rtp bind {addr}: {source}"),
            StartError::Preview(e) => write!(f, "video: outdoor preview signal failed: {e}"),
            StartError::Uuid(e) => write!(f, "video: gen UUID: {e}"),
            StartError::Ssrc(e) => write!(f, "video: gen SSRC: {e}"),
        }
    }
}

impl std::error::Error for StartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StartError::Bind { source, .. } => Some(source),
            StartError::Preview(e) => Some(e),
            StartError::Uuid(e) | StartError::Ssrc(e) => Some(e),
            StartError::Conflict { .. } => None,
        }
    }
}

impl StartError {
    /// 是否 conflict（组 F 映射 409）。
    pub fn is_conflict(&self) -> bool {
        matches!(self, StartError::Conflict { .. })
    }

    /// 是否 preview 超时（组 F 503 preview-timeout 档与「其它失败」档的判别）。
    pub fn is_preview_timeout(&self) -> bool {
        matches!(self, StartError::Preview(e) if e.is_timeout())
    }
}

// ---------------------------------------------------------------------------
// 依赖注入接口（锚 Go previewClient / rtpReceiver / rtcpSender interface）
// ---------------------------------------------------------------------------

/// Manager 与 preview 信令之间的依赖注入接口（便于五件套单测注入 fake）。
/// 锚 Go `previewClient`。
pub trait PreviewPort: Send + Sync {
    /// 发 req=704 + 等 req=705 ack。
    fn start_preview(&self, outdoor: &Outdoor, caller: &Caller) -> Result<(), PreviewError>;
    /// 发 req=708 + 等 req=709 ack。`timeout` `None` → 默认 5s；回滚路径传 3s
    /// （Go ctx 裁短的等价）。
    fn stop_preview(
        &self,
        outdoor: &Outdoor,
        caller: &Caller,
        timeout: Option<Duration>,
    ) -> Result<(), PreviewError>;
}

/// Manager 与 RTP receiver 之间的依赖注入接口。锚 Go `rtpReceiver`
/// （仅 RunWithConn 路径——bind 在 Start 内同步执行，conn 经此接口移交）。
///
/// `None`（默认）时 Manager 在 session 线程内 lazy 构造真 [`RtpReceiver`]
/// （绑 session 的 FrameBuffer 与 streamSSRC sink，等价 Go `m.RTP == nil` 分支）。
pub trait RtpPort: Send + Sync {
    /// 接管已绑定 socket 跑 read loop 直到 stop 置位。
    fn run_with_conn(
        &self,
        conn: UdpSocket,
        expect_src_ip: &str,
        stop: &AtomicBool,
    ) -> io::Result<()>;
}

/// Manager 与 RTCP sender 之间的依赖注入接口。锚 Go `rtcpSender`。
pub trait RtcpPort: Send + Sync {
    /// 周期保活直到 stop 置位。
    fn run(
        &self,
        dst: &str,
        ssrc_source: &dyn Fn() -> Option<u32>,
        reporter_ssrc: u32,
        stop: &AtomicBool,
    ) -> io::Result<()>;
    /// 一次性 BYE 复合包（best-effort）。
    fn send_bye(&self, dst: &str, reporter_ssrc: u32) -> io::Result<()>;
}

/// 把 [`SharedLogFn`]（Arc）适配成 [`LogFn`]（Box）——PreviewClient/RtcpSender
/// 持 Box 形态。
fn boxed_logf(logf: &Option<SharedLogFn>) -> Option<LogFn> {
    logf.as_ref().map(|a| {
        let a = Arc::clone(a);
        Box::new(move |m: &str| a(m)) as LogFn
    })
}

/// 默认 preview 实现：每次调用按 timeout 构造 [`PreviewClient`]（短连接直拨）。
struct WirePreview {
    logf: Option<SharedLogFn>,
}

impl PreviewPort for WirePreview {
    fn start_preview(&self, outdoor: &Outdoor, caller: &Caller) -> Result<(), PreviewError> {
        let client = PreviewClient {
            logf: boxed_logf(&self.logf),
            timeout: None, // 默认 5s（dial + 连接 deadline 两段）。
        };
        client.start_preview(&outdoor.ip, outdoor.port, outdoor.bcd_name, caller.bcd_name)
    }

    fn stop_preview(
        &self,
        outdoor: &Outdoor,
        caller: &Caller,
        timeout: Option<Duration>,
    ) -> Result<(), PreviewError> {
        let client = PreviewClient {
            logf: boxed_logf(&self.logf),
            timeout,
        };
        client.stop_preview(&outdoor.ip, outdoor.port, outdoor.bcd_name, caller.bcd_name)
    }
}

/// 默认 RTCP 实现：包装 [`RtcpSender`]。
struct WireRtcp {
    logf: Option<SharedLogFn>,
}

impl RtcpPort for WireRtcp {
    fn run(
        &self,
        dst: &str,
        ssrc_source: &dyn Fn() -> Option<u32>,
        reporter_ssrc: u32,
        stop: &AtomicBool,
    ) -> io::Result<()> {
        let sender = RtcpSender {
            logf: boxed_logf(&self.logf),
        };
        sender.run(dst, ssrc_source, reporter_ssrc, stop)
    }

    fn send_bye(&self, dst: &str, reporter_ssrc: u32) -> io::Result<()> {
        let sender = RtcpSender {
            logf: boxed_logf(&self.logf),
        };
        sender.send_bye(dst, reporter_ssrc)
    }
}

// ---------------------------------------------------------------------------
// Session（锚 Go Session）
// ---------------------------------------------------------------------------

/// 单个 active session 的状态。锚 Go `Session`。
///
/// 经 `Arc` 暴露给 http 层（组 F）持引用读 [`Session::frame_buf`]；
/// 其余字段由 Manager 内部管理。
pub struct Session {
    id: String,
    outdoor: Outdoor,

    /// TTL 截止时间。D4：**`Mutex<Instant>`**（MIPS32 禁 64-bit 原子；单调时钟，
    /// 对 Go `atomic.Int64` UnixNano 的有意实现差异）。Start 同 outdoor 且
    /// `latest_idr` 三件套齐（healthy）时才刷新；monitor 检查时刻 > 此值则自动 stop。
    ttl_deadline: Mutex<Instant>,

    /// 本端发 RTCP 时的 reporter SSRC（随机生成；与 session 同生命周期）。
    reporter_ssrc: u32,

    /// 从 RTP 流解析出的外机端 SSRC（RTCP RR report block 用）。
    /// 0 表示尚未收到 RTP（哨兵——SSRC 生成已排除 0）；RTP 首包 sink 写入。
    stream_ssrc: AtomicU32,

    /// RTP 收到的 NAL 单元缓冲区（种子 + 实时 fan-out）。RTP receiver 写入；
    /// transmux StreamWriter（组 F handler）读出。
    pub frame_buf: Arc<FrameBuffer>,

    /// per-session stop flag：终结本 session 的 RTP/RTCP/monitor 三线程
    /// （Go sess.cancel 的等价物）。
    stop: Arc<AtomicBool>,

    /// 三线程 graceful 退出信号接收端（Go sess.done 的等价物）：monitor 线程
    /// drain RTP/RTCP 后发 `()` 并 drop 发送端。teardown 单方执行（CAS 守卫），
    /// 故 take 一次即可。
    done_rx: Mutex<Option<mpsc::Receiver<()>>>,
}

impl Session {
    /// session UUID（http 层做 RefreshTTL key）。锚 Go `Session.ID`。
    pub fn id(&self) -> &str {
        &self.id
    }

    /// 外机 URI（Stop 比对 key）。
    pub fn outdoor_uri(&self) -> &str {
        &self.outdoor.uri
    }

    /// 返回 [`SessionInfo`]（HTTP handler 用）。锚 Go `Session.Info`。
    pub fn info(&self, ttl: Duration) -> SessionInfo {
        SessionInfo {
            id: self.id.clone(),
            outdoor_uri: self.outdoor.uri.clone(),
            stream_url: format!("/video/{}/stream.flv", self.id),
            ttl_secs: ttl.as_secs(),
        }
    }
}

// ---------------------------------------------------------------------------
// Manager（锚 Go Manager）
// ---------------------------------------------------------------------------

/// 协调单 active video session 生命周期的 Manager。锚 Go `Manager`。
///
/// daemon 全进程共享 single instance（main.rs 在 `cfg.video.forward=true` 时
/// 构造，包 `Arc` 注入 http 层）；状态机仅 1 个槽位，并发限制由内部 mutex 保证。
///
/// 配置字段在 `Arc::new` 前设置（测试注入口）：
/// `ttl` / `rtp_listen_addr` / `ttl_monitor_interval` / `teardown_tail_wait` /
/// `stop_budget` / `ttl_cleanup_budget` / `urandom_path` / `preview` / `rtp` / `rtcp`。
pub struct Manager {
    /// 本机室内机参数（preview 帧 caller 字段）。
    pub caller: Caller,
    /// 可选 log hook。
    pub logf: Option<SharedLogFn>,

    /// session TTL（`Duration::ZERO` → [`DEFAULT_SESSION_TTL`]；测试可调小）。
    /// 锚 Go `Manager.TTL`。
    pub ttl: Duration,
    /// RTP receiver 监听的本地 UDP 地址（空 → `0.0.0.0:9880`；测试填
    /// `127.0.0.1:0` 避免端口冲突）。锚 Go `Manager.RTPListenAddr`。
    pub rtp_listen_addr: String,
    /// TTL monitor 轮询周期（`Duration::ZERO` → 1s；测试压缩用）。
    pub ttl_monitor_interval: Duration,
    /// teardown 第 3 步残余 RTP 尾等待（默认 ~50ms；测试压缩用）。
    pub teardown_tail_wait: Duration,
    /// 显式 [`Manager::stop`] 的 teardown 总预算（`Duration::ZERO` →
    /// [`DEFAULT_STOP_BUDGET`] 8s，锚 Go handler 8s ctx；测试压缩用）。
    pub stop_budget: Duration,
    /// TTL 过期 detached cleanup 的 teardown 总预算（`Duration::ZERO` →
    /// [`DEFAULT_TTL_CLEANUP_BUDGET`] 5s，锚 Go stopInternal 5s ctx；测试压缩用）。
    pub ttl_cleanup_budget: Duration,
    /// 随机源路径（默认 `/dev/urandom`；测试指向不存在路径以演练
    /// UUID/SSRC 失败回滚链）。
    pub urandom_path: String,

    /// preview 信令依赖（默认真 [`PreviewClient`] 包装）。
    pub preview: Arc<dyn PreviewPort>,
    /// RTP receiver 依赖（`None` → session 线程内 lazy 构造真 [`RtpReceiver`]）。
    pub rtp: Option<Arc<dyn RtpPort>>,
    /// RTCP sender 依赖（默认真 [`RtcpSender`] 包装）。
    pub rtcp: Arc<dyn RtcpPort>,

    /// 内部状态：当前 active session（Go `mu + current`）。Start 持本锁跨
    /// bind + preview 信令全程。
    current: Mutex<Option<Arc<Session>>>,

    /// TTL monitor 异步发起的 detached cleanup 线程 handle（Go `cleanupWG` 的
    /// std-native 等价：`Mutex<Vec<JoinHandle>>` + register 时 retain 回收）。
    /// `Manager::shutdown` 在清完 current 后于 deadline 内对账排空——否则
    /// SIGTERM 落在 TTL-expired 后、stop_internal 仍在 StopPreview/BYE/50ms
    /// drain 之间时，daemon 退出会把 cleanup 杀在半路（外机持续推流 /
    /// BYE 没发出 / consumer 卡 incomplete close）。
    cleanups: Mutex<Vec<JoinHandle<()>>>,
}

impl Manager {
    /// 构造 Manager（注入默认运行时依赖）。锚 Go `New`。
    pub fn new(caller: Caller, logf: Option<SharedLogFn>) -> Manager {
        Manager {
            preview: Arc::new(WirePreview { logf: logf.clone() }),
            rtp: None,
            rtcp: Arc::new(WireRtcp { logf: logf.clone() }),
            caller,
            logf,
            ttl: DEFAULT_SESSION_TTL,
            rtp_listen_addr: String::new(),
            ttl_monitor_interval: DEFAULT_TTL_MONITOR_INTERVAL,
            teardown_tail_wait: DEFAULT_TEARDOWN_TAIL_WAIT,
            stop_budget: DEFAULT_STOP_BUDGET,
            ttl_cleanup_budget: DEFAULT_TTL_CLEANUP_BUDGET,
            urandom_path: URANDOM_PATH.to_string(),
            current: Mutex::new(None),
            cleanups: Mutex::new(Vec::new()),
        }
    }

    fn logf(&self, msg: &str) {
        if let Some(f) = &self.logf {
            f(msg);
        }
    }

    /// 生效 TTL（锚 Go `m.ttl()`：非正值回退默认）。
    fn ttl_value(&self) -> Duration {
        if self.ttl.is_zero() {
            DEFAULT_SESSION_TTL
        } else {
            self.ttl
        }
    }

    fn monitor_interval(&self) -> Duration {
        if self.ttl_monitor_interval.is_zero() {
            DEFAULT_TTL_MONITOR_INTERVAL
        } else {
            self.ttl_monitor_interval
        }
    }

    /// 生效显式 stop 总预算（ZERO 回退默认 8s）。
    fn stop_budget_value(&self) -> Duration {
        if self.stop_budget.is_zero() {
            DEFAULT_STOP_BUDGET
        } else {
            self.stop_budget
        }
    }

    /// 生效 TTL cleanup 总预算（ZERO 回退默认 5s）。
    fn ttl_cleanup_budget_value(&self) -> Duration {
        if self.ttl_cleanup_budget.is_zero() {
            DEFAULT_TTL_CLEANUP_BUDGET
        } else {
            self.ttl_cleanup_budget
        }
    }

    /// 起 session（或 idempotent 复用）。锚 Go `Manager.Start`。
    ///
    /// 行为：
    ///   - 无 active session → bind UDP 9880 + dial 18022 等 ack + 起三线程
    ///   - 有 active session 同 outdoor → 复用（仅 healthy 才刷 TTL）
    ///   - 有 active session 不同 outdoor → [`StartError::Conflict`]
    ///
    /// **持 `current` 锁跨 bind + preview 信令全程**（最长 ~10s）；返回后
    /// session 生命周期由 Manager 内部维持。
    pub fn start(self: &Arc<Self>, outdoor: Outdoor) -> Result<SessionInfo, StartError> {
        let mut cur = self.current.lock().unwrap_or_else(|e| e.into_inner());

        if let Some(sess) = cur.as_ref() {
            if sess.outdoor.uri == outdoor.uri {
                // idempotent 复用：返已有 SessionInfo。
                // 仅在已收到 IDR 的 healthy session 才刷 TTL（死 session 不续命——
                // 外机 ack 但实际未推 RTP 时，client retry 不得钉死唯一 slot；
                // 健康流由 stream handler post-wait_idr refresh + 30s refresh 循环维持）。
                if sess.frame_buf.latest_idr().is_some() {
                    *sess.ttl_deadline.lock().unwrap_or_else(|e| e.into_inner()) =
                        Instant::now() + self.ttl_value();
                    self.logf(&format!(
                        "video: reuse existing session id={} outdoor={} (TTL refreshed)",
                        sess.id, outdoor.uri
                    ));
                } else {
                    self.logf(&format!(
                        "video: reuse existing session id={} outdoor={} (TTL not refreshed; no IDR yet)",
                        sess.id, outdoor.uri
                    ));
                }
                return Ok(sess.info(self.ttl_value()));
            }
            return Err(StartError::Conflict {
                active: sess.outdoor.uri.clone(),
                requested: outdoor.uri,
            });
        }

        // 1. 同步绑定 UDP 9880：必须在告知外机推流前确认 socket 已绑，
        //    否则外机推 RTP 到端口未就绪状态，HACS 端 ffmpeg 卡等首帧。
        let rtp_addr = if self.rtp_listen_addr.is_empty() {
            DEFAULT_RTP_LISTEN_ADDR
        } else {
            self.rtp_listen_addr.as_str()
        };
        let rtp_conn = bind_udp(rtp_addr).map_err(|e| StartError::Bind {
            addr: rtp_addr.to_string(),
            source: e,
        })?;

        // 2. dial 18022 + 等 req=705 ack（回滚链第一段：preview 失败仅关 socket）。
        if let Err(e) = self.preview.start_preview(&outdoor, &self.caller) {
            drop(rtp_conn);
            return Err(StartError::Preview(e));
        }

        // 3. UUID + reporter SSRC（回滚链第二段：preview 已 ack，失败必须
        //    关 socket + best-effort StopPreview（3s）——否则外机持续推流无人接）。
        let id = match uuid_v4_from(&self.urandom_path) {
            Ok(v) => v,
            Err(e) => {
                drop(rtp_conn);
                let _ = self.preview.stop_preview(
                    &outdoor,
                    &self.caller,
                    Some(ROLLBACK_STOP_PREVIEW_TIMEOUT),
                );
                return Err(StartError::Uuid(e));
            }
        };
        let reporter_ssrc = match random_ssrc_from(&self.urandom_path) {
            Ok(v) => v,
            Err(e) => {
                drop(rtp_conn);
                let _ = self.preview.stop_preview(
                    &outdoor,
                    &self.caller,
                    Some(ROLLBACK_STOP_PREVIEW_TIMEOUT),
                );
                return Err(StartError::Ssrc(e));
            }
        };

        let (done_tx, done_rx) = mpsc::channel::<()>();
        let sess = Arc::new(Session {
            id,
            outdoor: outdoor.clone(),
            ttl_deadline: Mutex::new(Instant::now() + self.ttl_value()),
            reporter_ssrc,
            stream_ssrc: AtomicU32::new(0),
            frame_buf: Arc::new(FrameBuffer::new()),
            stop: Arc::new(AtomicBool::new(false)),
            done_rx: Mutex::new(Some(done_rx)),
        });

        // 4a. RTP receiver 线程（已绑 conn 移交；ctx → per-session stop flag）。
        let h_rtp = {
            let mgr = Arc::clone(self);
            let sess = Arc::clone(&sess);
            let expect_ip = outdoor.ip.clone();
            thread::spawn(move || {
                let res = if let Some(port) = &mgr.rtp {
                    port.run_with_conn(rtp_conn, &expect_ip, &sess.stop)
                } else {
                    // lazy 构造真 receiver（绑 session FrameBuffer + SSRC sink，
                    // 等价 Go m.RTP == nil 分支）。
                    let sink_sess = Arc::clone(&sess);
                    let recv = RtpReceiver {
                        logf: mgr.logf.clone(),
                        buf: Arc::clone(&sess.frame_buf),
                        ssrc_sink: Some(Box::new(move |s| {
                            sink_sess.stream_ssrc.store(s, Ordering::SeqCst)
                        })),
                        packet_hook: None,
                    };
                    recv.run_with_conn(rtp_conn, &expect_ip, &sess.stop)
                };
                if let Err(e) = res {
                    mgr.logf(&format!("video: rtp receiver exited: {e}"));
                }
            })
        };

        // 4b. RTCP keepalive 线程。
        let h_rtcp = {
            let mgr = Arc::clone(self);
            let sess = Arc::clone(&sess);
            let dst = outdoor.addr_rtcp();
            thread::spawn(move || {
                let src_sess = Arc::clone(&sess);
                let ssrc_source = move || {
                    let s = src_sess.stream_ssrc.load(Ordering::SeqCst);
                    if s == 0 {
                        None
                    } else {
                        Some(s)
                    }
                };
                if let Err(e) = mgr
                    .rtcp
                    .run(&dst, &ssrc_source, sess.reporter_ssrc, &sess.stop)
                {
                    mgr.logf(&format!("video: rtcp sender exited: {e}"));
                }
            })
        };

        // 4c. TTL monitor 线程：每 monitor_interval 检查；过期则**异步**发起 stop。
        //
        // TTL 过期路径必须 detached（monitor 禁自 join——Go 死锁同坑）：若 monitor
        // 同步跑 stop_internal → teardown 内等 done，而 done 只有本 monitor 才会发
        // （在 stop 置位分支里）——自己等自己，只能卡到 2s abandoning 兜底。
        // 修复形态（复刻 Go 最终版）：过期 → 新线程跑 stop_internal（handle 收进
        // cleanups 对账）；本 monitor 继续走 stop 置位分支 drain RTP/RTCP 后发 done，
        // teardown 的 done-wait 立刻完成。done 由本线程唯一负责发（不变量）。
        {
            let mgr = Arc::clone(self);
            let sess = Arc::clone(&sess);
            let interval = self.monitor_interval();
            // monitor 自身 detached（handle 丢弃）：退出由 stop flag 驱动，
            // 退出耗时被 teardown 的 done-wait 2s 兜底覆盖。
            let _ = thread::spawn(move || {
                let mut fired = false;
                let mut next_tick = Instant::now() + interval;
                loop {
                    if sess.stop.load(Ordering::SeqCst) {
                        // session 取消：drain RTP/RTCP（Go bgWG.Wait）后发 done
                        // （Go close(sess.done)）。
                        let _ = h_rtp.join();
                        let _ = h_rtcp.join();
                        let _ = done_tx.send(());
                        return;
                    }
                    let now = Instant::now();
                    if now >= next_tick {
                        next_tick = now + interval;
                        if !fired
                            && now > *sess.ttl_deadline.lock().unwrap_or_else(|e| e.into_inner())
                        {
                            mgr.logf(&format!(
                                "video: session id={} outdoor={} TTL expired; auto-stop",
                                sess.id, sess.outdoor.uri
                            ));
                            fired = true;
                            let cleanup = thread::spawn({
                                let mgr = Arc::clone(&mgr);
                                let sess = Arc::clone(&sess);
                                move || mgr.stop_internal(&sess, "ttl-expired")
                            });
                            mgr.register_cleanup(cleanup);
                            // 不 return：继续等 stop 置位把 RTP/RTCP drain 掉再发
                            // done——否则 done 永远不来 → teardown 卡 2s 兜底。
                        }
                    }
                    thread::park_timeout(
                        MONITOR_POLL_SLICE.min(next_tick.saturating_duration_since(Instant::now())),
                    );
                }
            });
        }

        *cur = Some(Arc::clone(&sess));
        self.logf(&format!(
            "video: started session id={} outdoor={} ssrc=0x{:08x}",
            sess.id, outdoor.uri, reporter_ssrc
        ));
        Ok(sess.info(self.ttl_value()))
    }

    /// 终结指定 outdoor 的 active session（idempotent：无匹配 session 时静默返回，
    /// 上层照样 200）。锚 Go `Manager.Stop`（Go 返 error 但 shutdown 恒 nil；
    /// teardown 内部失败仅 log——Rust 直接返 `()`，组 F 禁把 Stop 失败映射 503）。
    ///
    /// teardown 受 [`Manager::stop_budget`]（默认 8s）总预算约束——锚 Go
    /// `handleVideoStop` 的 8s handler ctx 包住整个 shutdown 序列。
    pub fn stop(&self, outdoor_uri: &str) {
        let sess = {
            let mut cur = self.current.lock().unwrap_or_else(|e| e.into_inner());
            match cur.as_ref() {
                Some(c) if c.outdoor.uri == outdoor_uri => cur.take(),
                _ => None,
            }
        };
        // 真正 teardown 在锁外执行（避免持锁等待 RTP socket 关闭等慢操作）。
        if let Some(sess) = sess {
            let deadline = Instant::now() + self.stop_budget_value();
            self.teardown(&sess, "explicit-stop", deadline);
        }
    }

    /// 按 session_id 查 active session（HTTP `/video/<id>/stream.flv` 用）。
    /// 锚 Go `CurrentByID`。
    pub fn current_by_id(&self, id: &str) -> Option<Arc<Session>> {
        let cur = self.current.lock().unwrap_or_else(|e| e.into_inner());
        cur.as_ref().filter(|c| c.id == id).cloned()
    }

    /// 返当前 active session（无 active 时 `None`）。锚 Go `Current`。
    pub fn current(&self) -> Option<Arc<Session>> {
        self.current
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// 重置当前 session 的 TTL 倒计时（stream handler 拿到 ID 后调用，作为
    /// 消费者活跃信号）。锚 Go `RefreshTTL`。
    pub fn refresh_ttl(&self, id: &str) -> bool {
        let cur = self.current.lock().unwrap_or_else(|e| e.into_inner());
        match cur.as_ref() {
            Some(c) if c.id == id => {
                *c.ttl_deadline.lock().unwrap_or_else(|e| e.into_inner()) =
                    Instant::now() + self.ttl_value();
                true
            }
            _ => false,
        }
    }

    /// 返指定 session 的 TTL 剩余时间（read-only 观测口；session 不存在返 `None`）。
    ///
    /// 组 F 追加：HTTP stream handler 的「失败分支禁刷 TTL / 就绪后单次刷」语义
    /// 对 **concrete** `Manager` 无法以 fake 计数验证（Go 用 fakeVideoMgr 数
    /// RefreshTTL 调用），改以 deadline 剩余量前后对比断言。不改任何写路径。
    pub fn ttl_remaining(&self, id: &str) -> Option<Duration> {
        let cur = self.current.lock().unwrap_or_else(|e| e.into_inner());
        cur.as_ref().filter(|c| c.id == id).map(|c| {
            c.ttl_deadline
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .saturating_duration_since(Instant::now())
        })
    }

    /// daemon graceful exit：停止 active session + 对账全部 detached cleanup。
    /// 锚 Go `Manager.Shutdown(ctx)`（deadline 约束 cleanup 对账等待）。
    ///
    /// 必须等 TTL monitor 异步发起的 detached cleanup 完成——否则 SIGTERM 落在
    /// TTL-expired 之后、stop_internal 仍在 StopPreview / BYE / drain 之间时，
    /// daemon 看 current==None 立即退出，cleanup 被杀在半路。超时则放弃并 log
    /// （不无限阻塞 daemon 退出）。
    pub fn shutdown(&self, deadline: Duration) {
        let overall = Instant::now() + deadline;

        let sess = {
            let mut cur = self.current.lock().unwrap_or_else(|e| e.into_inner());
            cur.take()
        };
        if let Some(sess) = sess {
            // teardown 共享 caller deadline（同一总预算，不叠加自己的）——锚 Go
            // Manager.Shutdown 把 ctx 原样传给 shutdown(ctx, sess, ...)。
            self.teardown(&sess, "daemon-shutdown", overall);
        }

        // 对账 detached cleanup（Go cleanupWG.Wait 受 ctx deadline 约束）。
        let drained: Vec<JoinHandle<()>> = {
            let mut v = self.cleanups.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *v)
        };
        if drained.is_empty() {
            return;
        }
        let (tx, rx) = mpsc::channel::<()>();
        let waiter = thread::spawn(move || {
            for h in drained {
                let _ = h.join();
            }
            let _ = tx.send(());
        });
        let remaining = overall.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remaining) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = waiter.join();
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // waiter 线程保持 detached（等价 Go 超时后遗留的 Wait goroutine）。
                self.logf(&format!(
                    "video: shutdown timeout waiting for TTL cleanups (deadline {deadline:?})"
                ));
            }
        }
    }

    /// TTL monitor 触发的自动停止：摘 current 比对（CAS 式守卫），再 teardown。
    /// 锚 Go `stopInternal`。
    ///
    /// 与 [`Manager::stop`] 区分：stop 是外部 HTTP 驱动；本函数是内部 timer 驱动。
    /// 双驱动同刻竞争同一 session 时，只有摘到 current 的一方执行 teardown
    /// （Go `m.current != sess` 早返等价）。
    ///
    /// teardown 受 [`Manager::ttl_cleanup_budget`]（默认 5s）总预算约束——锚 Go
    /// `stopInternal` 自建 `context.WithTimeout(5s)`。
    fn stop_internal(&self, sess: &Arc<Session>, reason: &str) {
        {
            let mut cur = self.current.lock().unwrap_or_else(|e| e.into_inner());
            match cur.as_ref() {
                Some(c) if Arc::ptr_eq(c, sess) => {
                    *cur = None;
                }
                _ => return, // 没摘到 current：另一驱动已执行 teardown，静默返回。
            }
        }
        let deadline = Instant::now() + self.ttl_cleanup_budget_value();
        self.teardown(sess, reason, deadline);
    }

    /// 终结 session 资源（teardown 序列）。锚 Go `Manager.shutdown(ctx, sess, reason)`。
    ///
    /// 失败不返业务错（best-effort cleanup），wire send 失败仅 log。
    ///
    /// `deadline` 是整个 teardown 的总预算（锚 Go：显式 stop 由 handler 8s ctx
    /// 包住、TTL cleanup 5s ctx、daemon shutdown 收 main stopCtx）：各步骤按剩余
    /// 预算执行/裁短/跳过；唯独第 5 步 FrameBuffer close **不受预算约束永远执行**
    /// （资源释放不可跳，stop flag 同理永远置位）。
    fn teardown(&self, sess: &Arc<Session>, reason: &str, deadline: Instant) {
        self.logf(&format!(
            "video: stopping session id={} outdoor={} reason={}",
            sess.id, sess.outdoor.uri, reason
        ));

        // 1. 发 req=708 stop preview（best-effort；按剩余预算裁短，预算耗尽则跳过）。
        //    PreviewClient 的 timeout 是 per-segment 语义（dial 一段 + 连接绝对
        //    deadline 一段，最坏 2×timeout）；Go 等价物靠 dialAndExchange 的 ctx
        //    watchdog 主动 close conn 把**总**时长硬 cap 在 ctx 剩余内。Rust 拿不到
        //    socket 句柄做 watchdog，按 剩余/2 折算 per-segment 值保证两段总和
        //    ≤ 剩余预算（剩余 ≥10s 时与既有默认 5s 一致，行为不变）。
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            self.logf("video: teardown budget exhausted; skipping stop-preview wire");
        } else {
            let per_segment = PREVIEW_DIAL_TIMEOUT
                .min(remaining / 2)
                .max(Duration::from_millis(1));
            if let Err(e) =
                self.preview
                    .stop_preview(&sess.outdoor, &self.caller, Some(per_segment))
            {
                self.logf(&format!(
                    "video: stop-preview wire failed (continuing): {e}"
                ));
            }
        }

        // 2. 发 RTCP BYE（best-effort；单 UDP sendto 快速无阻塞面，不受预算约束照发）。
        if let Err(e) = self
            .rtcp
            .send_bye(&sess.outdoor.addr_rtcp(), sess.reporter_ssrc)
        {
            self.logf(&format!("video: rtcp bye failed (continuing): {e}"));
        }

        // 3. 等 ~50ms 残余 RTP 尾（observations §3.4 启动延迟与停止尾，平均 52ms；
        //    按剩余预算裁短——锚 Go `select{time.After(50ms), ctx.Done()}`）。
        let tail = self
            .teardown_tail_wait
            .min(deadline.saturating_duration_since(Instant::now()));
        if !tail.is_zero() {
            thread::sleep(tail);
        }

        // 4. 置位 stop（永远执行）+ 等三线程退出（2s 兜底按剩余预算裁短，
        //    超时放弃并 log）。
        sess.stop.store(true, Ordering::SeqCst);
        let rx = sess
            .done_rx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(rx) = rx {
            let wait =
                SESSION_THREADS_EXIT_WAIT.min(deadline.saturating_duration_since(Instant::now()));
            match rx.recv_timeout(wait) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {}
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    self.logf(&format!(
                        "video: session threads did not exit within {wait:?}; abandoning"
                    ));
                }
            }
        }

        // 5. 标记 FrameBuffer 关闭（让 stream consumers 退出）——不受预算约束。
        sess.frame_buf.close();
    }

    /// 登记 detached cleanup handle（retain 回收套路，对齐 daemon.rs PushTracker）。
    fn register_cleanup(&self, h: JoinHandle<()>) {
        let mut v = self.cleanups.lock().unwrap_or_else(|e| e.into_inner());
        v.retain(|h| !h.is_finished());
        v.push(h);
    }
}

// ---------------------------------------------------------------------------
// helpers（锚 Go newUUIDv4 / newRandomSSRC / IsValidUUID）
// ---------------------------------------------------------------------------

/// 读随机源 `len` 字节（design D5：`std::fs` 读 `/dev/urandom`，零新 FFI 面；
/// 失败返错不 panic）。
fn read_random_from(path: &str, buf: &mut [u8]) -> io::Result<()> {
    let mut f = std::fs::File::open(path)?;
    f.read_exact(buf)
}

fn uuid_v4_from(path: &str) -> io::Result<String> {
    let mut b = [0u8; 16];
    read_random_from(path, &mut b)?;
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // variant RFC 4122
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(36);
    for (i, by) in b.iter().enumerate() {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            out.push(b'-');
        }
        out.push(HEX[(by >> 4) as usize]);
        out.push(HEX[(by & 0x0f) as usize]);
    }
    Ok(String::from_utf8(out).expect("uuid hex output is ASCII"))
}

fn random_ssrc_from(path: &str) -> io::Result<u32> {
    let mut f = std::fs::File::open(path)?;
    loop {
        let mut b = [0u8; 4];
        f.read_exact(&mut b)?;
        let s = u32::from_be_bytes(b);
        // 循环重抽至非零：SSRC=0 会撞「streamSSRC==0=未知」哨兵语义。
        if s != 0 {
            return Ok(s);
        }
    }
}

/// 生成 RFC 4122 v4 UUID 字符串（标准 8-4-4-4-12）。锚 Go `newUUIDv4`。
pub fn new_uuid_v4() -> io::Result<String> {
    uuid_v4_from(URANDOM_PATH)
}

/// 生成随机非零 32-bit SSRC（RFC 3550 推荐随机化避免冲突）。锚 Go `newRandomSSRC`。
pub fn new_random_ssrc() -> io::Result<u32> {
    random_ssrc_from(URANDOM_PATH)
}

/// 简单校验 36-char UUID 格式（HTTP 路径解析用——组 F 动态路由复用，
/// 坏 UUID → 400 且先于 method 检查）。锚 Go `IsValidUUID`。
pub fn is_valid_uuid(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 36 {
        return false;
    }
    for (i, &c) in b.iter().enumerate() {
        match i {
            8 | 13 | 18 | 23 => {
                if c != b'-' {
                    return false;
                }
            }
            _ => {
                if !c.is_ascii_hexdigit() {
                    return false;
                }
            }
        }
    }
    true
}

// ── 单测（移植 Go session_test.go 全部 + 五件套之一~四 + 任务点名场景）──────────

#[cfg(test)]
mod tests {
    use super::super::rtp::{NalUnit, NAL_TYPE_IDR, NAL_TYPE_PPS, NAL_TYPE_SPS};
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;
    use std::sync::atomic::AtomicU32;

    // --- fakes（锚 Go fakePreview / fakeRTP / fakeRTCP）---

    /// PreviewPort 测试 fake：可注入 start 错误 / start・stop 延迟、记录调用。
    #[derive(Default)]
    struct FakePreview {
        starts: AtomicU32,
        stops: AtomicU32,
        /// StopPreview 真实返回（含 delay 后）才递增；区分「stop 已发起但还在
        /// delay」vs「stop 完整返回」（锚 Go stopsCompleted）。
        stops_completed: AtomicU32,
        start_err: Mutex<Option<String>>,
        start_delay_ms: AtomicU32,
        stop_delay_ms: AtomicU32,
        /// stop_preview 最近一次收到的 timeout 参数（外层 None = 尚未被调）。
        last_stop_timeout: Mutex<Option<Option<Duration>>>,
        /// true 时 stop 延迟尊重传入 timeout（睡 min(delay, 2×timeout) 后返
        /// Timeout 错），模拟真 PreviewClient 在外机挂死时按 deadline 自行超时。
        honor_stop_timeout: AtomicBool,
    }

    impl PreviewPort for FakePreview {
        fn start_preview(&self, _o: &Outdoor, _c: &Caller) -> Result<(), PreviewError> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            let d = self.start_delay_ms.load(Ordering::SeqCst);
            if d > 0 {
                thread::sleep(Duration::from_millis(u64::from(d)));
            }
            if let Some(msg) = self
                .start_err
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
            {
                return Err(PreviewError::BadAck(msg));
            }
            Ok(())
        }

        fn stop_preview(
            &self,
            _o: &Outdoor,
            _c: &Caller,
            timeout: Option<Duration>,
        ) -> Result<(), PreviewError> {
            self.stops.fetch_add(1, Ordering::SeqCst);
            *self
                .last_stop_timeout
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(timeout);
            let d = Duration::from_millis(u64::from(self.stop_delay_ms.load(Ordering::SeqCst)));
            // honor 模式：真 PreviewClient 最坏阻塞 2×timeout（dial 段 + 连接段），
            // 挂死被 deadline 裁短后返 Timeout 错。
            let clipped = match (self.honor_stop_timeout.load(Ordering::SeqCst), timeout) {
                (true, Some(t)) => d.min(t * 2),
                _ => d,
            };
            if !clipped.is_zero() {
                thread::sleep(clipped);
            }
            self.stops_completed.fetch_add(1, Ordering::SeqCst);
            if clipped < d {
                return Err(PreviewError::Timeout { stage: "read" });
            }
            Ok(())
        }
    }

    /// RtpPort fake：立即 close 传入 conn 避免 leak，阻塞到 stop 置位（锚 Go fakeRTP）。
    #[derive(Default)]
    struct FakeRtp {
        runs: AtomicU32,
    }

    impl RtpPort for FakeRtp {
        fn run_with_conn(
            &self,
            conn: UdpSocket,
            _expect_src_ip: &str,
            stop: &AtomicBool,
        ) -> io::Result<()> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            drop(conn);
            while !stop.load(Ordering::SeqCst) {
                thread::park_timeout(Duration::from_millis(5));
            }
            Ok(())
        }
    }

    /// RtcpPort fake（锚 Go fakeRTCP）。
    #[derive(Default)]
    struct FakeRtcp {
        runs: AtomicU32,
        byes: AtomicU32,
    }

    impl RtcpPort for FakeRtcp {
        fn run(
            &self,
            _dst: &str,
            _ssrc_source: &dyn Fn() -> Option<u32>,
            _reporter_ssrc: u32,
            stop: &AtomicBool,
        ) -> io::Result<()> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            while !stop.load(Ordering::SeqCst) {
                thread::park_timeout(Duration::from_millis(5));
            }
            Ok(())
        }

        fn send_bye(&self, _dst: &str, _reporter_ssrc: u32) -> io::Result<()> {
            self.byes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// 锚 Go newTestManager：注入三 fake + 测试加速 TTL（200ms）/ ephemeral 端口 /
    /// monitor 压缩到 25ms（tunable，Go 固定 1s ticker——D8 压缩纪律）。
    /// 返回**未包 Arc** 的 Manager 供 per-test 再调参。
    fn test_manager() -> (Manager, Arc<FakePreview>, Arc<FakeRtp>, Arc<FakeRtcp>) {
        let caller = parse_caller("06021103@10.0.0.91:18022").expect("caller");
        let fp = Arc::new(FakePreview::default());
        let frtp = Arc::new(FakeRtp::default());
        let frtcp = Arc::new(FakeRtcp::default());
        let mut m = Manager::new(caller, None);
        m.ttl = Duration::from_millis(200);
        m.rtp_listen_addr = "127.0.0.1:0".to_string();
        m.ttl_monitor_interval = Duration::from_millis(25);
        m.preview = Arc::clone(&fp) as Arc<dyn PreviewPort>;
        m.rtp = Some(Arc::clone(&frtp) as Arc<dyn RtpPort>);
        m.rtcp = Arc::clone(&frtcp) as Arc<dyn RtcpPort>;
        (m, fp, frtp, frtcp)
    }

    fn must_outdoor(uri: &str) -> Outdoor {
        parse_outdoor(uri).expect("outdoor")
    }

    fn nal(nal_type: u8, data: &[u8]) -> NalUnit {
        NalUnit {
            nal_type,
            data: data.to_vec(),
            timestamp: 0,
        }
    }

    // --- 基本行为（移植 Go TestManager_*）---

    /// 移植 Go `TestManager_StartReturnsSessionInfo`。
    #[test]
    fn start_returns_session_info() {
        let (m, fp, _, _) = test_manager();
        let m = Arc::new(m);
        let out = must_outdoor("06020000@172.16.106.152:18022");
        let info = m.start(out.clone()).expect("start");
        assert!(!info.id.is_empty(), "ID empty");
        assert!(is_valid_uuid(&info.id), "ID {:?} not UUID", info.id);
        assert_eq!(info.outdoor_uri, out.uri);
        assert_eq!(info.stream_url, format!("/video/{}/stream.flv", info.id));
        assert_eq!(info.ttl_secs, 0, "ttl 200ms 取整秒 = 0（测试压缩值）");
        assert_eq!(fp.starts.load(Ordering::SeqCst), 1, "StartPreview 次数");
        m.shutdown(Duration::from_secs(3));
    }

    /// SessionInfo ttl_secs 用生产 TTL 时为 60（组 F 200 响应 ttl=60 字段源）。
    #[test]
    fn session_info_ttl_secs_production_value() {
        let (mut m, _, _, _) = test_manager();
        m.ttl = DEFAULT_SESSION_TTL;
        let m = Arc::new(m);
        let info = m
            .start(must_outdoor("06020000@172.16.106.152:18022"))
            .expect("start");
        assert_eq!(info.ttl_secs, 60);
        m.shutdown(Duration::from_secs(3));
    }

    /// 移植 Go `TestManager_StartIdempotentSameOutdoor`。
    #[test]
    fn start_idempotent_same_outdoor() {
        let (m, fp, _, _) = test_manager();
        let m = Arc::new(m);
        let out = must_outdoor("06020000@172.16.106.152:18022");
        let info1 = m.start(out.clone()).expect("start 1");
        let info2 = m.start(out).expect("start 2");
        assert_eq!(info1.id, info2.id, "idempotent reuse");
        assert_eq!(
            fp.starts.load(Ordering::SeqCst),
            1,
            "idempotent 复用不得重发 preview"
        );
        m.shutdown(Duration::from_secs(3));
    }

    /// 移植 Go `TestManager_StartConflictDifferentOutdoor`（spec 场景「不同 outdoor 冲突」）。
    #[test]
    fn start_conflict_different_outdoor() {
        let (m, _, _, _) = test_manager();
        let m = Arc::new(m);
        let a = must_outdoor("06020000@172.16.106.152:18022");
        let b = must_outdoor("06020001@172.16.106.151:18022");
        m.start(a.clone()).expect("start a");
        let err = m.start(b.clone()).expect_err("conflict expected");
        assert!(err.is_conflict(), "err = {err:?}, want Conflict");
        assert!(!err.is_preview_timeout());
        // Display 字面对齐 Go fmt.Errorf("%w: active=%s, requested=%s")。
        assert_eq!(
            err.to_string(),
            format!(
                "video: another outdoor session is active: active={}, requested={}",
                a.uri, b.uri
            )
        );
        m.shutdown(Duration::from_secs(3));
    }

    /// 移植 Go `TestManager_StopReleasesSession`。
    #[test]
    fn stop_releases_session() {
        let (m, _, _, frtcp) = test_manager();
        let m = Arc::new(m);
        let out = must_outdoor("06020000@172.16.106.152:18022");
        m.start(out.clone()).expect("start");
        m.stop(&out.uri);
        assert!(m.current().is_none(), "after stop, current should be None");
        assert_eq!(frtcp.byes.load(Ordering::SeqCst), 1, "BYE not sent");
    }

    /// 移植 Go `TestManager_StopMissingOutdoorIdempotent` + URI 不匹配分支。
    #[test]
    fn stop_missing_outdoor_idempotent() {
        let (m, fp, _, frtcp) = test_manager();
        let m = Arc::new(m);
        // 无 active session：静默幂等。
        m.stop("06020000@172.16.106.152:18022");
        // 有 session 但 URI 不匹配：不 teardown。
        let out = must_outdoor("06020000@172.16.106.152:18022");
        m.start(out).expect("start");
        m.stop("06029999@172.16.106.199:18022");
        assert!(m.current().is_some(), "URI 不匹配不得摘 session");
        assert_eq!(fp.stops.load(Ordering::SeqCst), 0);
        assert_eq!(frtcp.byes.load(Ordering::SeqCst), 0);
        m.shutdown(Duration::from_secs(3));
    }

    /// 移植 Go `TestManager_TTLExpiryAutoStop`。
    #[test]
    fn ttl_expiry_auto_stop() {
        let (mut m, _, _, _) = test_manager();
        m.ttl = Duration::from_millis(100);
        let m = Arc::new(m);
        m.start(must_outdoor("06020000@172.16.106.152:18022"))
            .expect("start");
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if m.current().is_none() {
                m.shutdown(Duration::from_secs(3));
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("session did not auto-stop after TTL");
    }

    /// 移植 Go `TestManager_RefreshTTLPreventsExpiry`（时长 tunable 压缩）。
    #[test]
    fn refresh_ttl_prevents_expiry() {
        let (mut m, _, _, _) = test_manager();
        m.ttl = Duration::from_millis(300);
        let m = Arc::new(m);
        let info = m
            .start(must_outdoor("06020000@172.16.106.152:18022"))
            .expect("start");
        for i in 0..5 {
            thread::sleep(Duration::from_millis(150)); // 接近 TTL 但未到。
            assert!(m.refresh_ttl(&info.id), "refresh_ttl false at i={i}");
        }
        assert!(m.current().is_some(), "session expired despite TTL refresh");
        m.shutdown(Duration::from_secs(3));
    }

    /// 移植 Go `TestManager_RefreshTTLWrongID`。
    #[test]
    fn refresh_ttl_wrong_id() {
        let (m, _, _, _) = test_manager();
        let m = Arc::new(m);
        assert!(!m.refresh_ttl("not-a-real-id-aaaaaaaaaaaaaaaaaaaa"));
    }

    /// current_by_id：命中返 Arc、不命中返 None（组 F 动态路由依赖面）。
    #[test]
    fn current_by_id_matches_only_active_id() {
        let (m, _, _, _) = test_manager();
        let m = Arc::new(m);
        let info = m
            .start(must_outdoor("06020000@172.16.106.152:18022"))
            .expect("start");
        let sess = m.current_by_id(&info.id).expect("current_by_id hit");
        assert_eq!(sess.id(), info.id);
        assert_eq!(sess.outdoor_uri(), "06020000@172.16.106.152:18022");
        assert!(m
            .current_by_id("ffffffff-ffff-4fff-8fff-ffffffffffff")
            .is_none());
        m.shutdown(Duration::from_secs(3));
    }

    // --- Start 失败回滚链（移植 Go + 任务 5.2 两段回滚断言）---

    /// 移植 Go `TestManager_StartFailsOnRTPBindBusy`：bind 失败必须先于 preview
    /// （starts==0），且不留 active session。
    #[test]
    fn start_fails_on_rtp_bind_busy() {
        let occupied = UdpSocket::bind("127.0.0.1:0").expect("occupy port");
        let busy_addr = occupied.local_addr().unwrap().to_string();

        let (mut m, fp, _, _) = test_manager();
        m.rtp_listen_addr = busy_addr;
        let m = Arc::new(m);
        let err = m
            .start(must_outdoor("06020000@172.16.106.152:18022"))
            .expect_err("Start should fail on busy bind");
        assert!(matches!(err, StartError::Bind { .. }), "err = {err:?}");
        assert!(m.current().is_none(), "after bind failure, current None");
        assert_eq!(
            fp.starts.load(Ordering::SeqCst),
            0,
            "bind failure must precede preview"
        );
    }

    /// 移植 Go `TestManager_StartFailsOnPreviewError` + 回滚链第一段断言：
    /// preview 失败**仅关 socket**（不发 StopPreview）。
    #[test]
    fn start_fails_on_preview_error_rolls_back_socket_only() {
        let (m, fp, _, _) = test_manager();
        fp.start_err.lock().unwrap().replace("boom".to_string());
        let m = Arc::new(m);
        let err = m
            .start(must_outdoor("06020000@172.16.106.152:18022"))
            .expect_err("preview error expected");
        assert!(matches!(err, StartError::Preview(_)), "err = {err:?}");
        assert!(
            err.to_string()
                .starts_with("video: outdoor preview signal failed: "),
            "Display = {err}"
        );
        assert!(m.current().is_none());
        assert_eq!(
            fp.stops.load(Ordering::SeqCst),
            0,
            "preview 失败回滚仅关 socket，不得发 StopPreview"
        );
    }

    /// 回滚链第二段（任务 5.2）：UUID/SSRC 生成失败（urandom 读失败）= 关 socket +
    /// best-effort StopPreview（3s）。Go 无此测试（crypto/rand 不可注入）——Rust
    /// 经 `urandom_path` 注入口补上。
    #[test]
    fn start_uuid_failure_rolls_back_with_stop_preview() {
        let (mut m, fp, _, _) = test_manager();
        m.urandom_path = "/nonexistent-urandom-for-test".to_string();
        let m = Arc::new(m);
        let err = m
            .start(must_outdoor("06020000@172.16.106.152:18022"))
            .expect_err("uuid gen must fail");
        assert!(matches!(err, StartError::Uuid(_)), "err = {err:?}");
        assert!(
            err.to_string().starts_with("video: gen UUID: "),
            "Display = {err}"
        );
        assert!(m.current().is_none());
        assert_eq!(
            fp.starts.load(Ordering::SeqCst),
            1,
            "preview 已 ack 才走到 UUID 段"
        );
        assert_eq!(
            fp.stops.load(Ordering::SeqCst),
            1,
            "UUID 失败回滚必须含 best-effort StopPreview"
        );
    }

    // --- in-flight 互斥（spec 场景「并发 Start 互斥含 in-flight」）---

    /// Start 持锁跨 bind+preview 信令全程：B 在 A 信令期间调 Start 必须阻塞到 A
    /// 完成，绝不与其并行通过 bind/preview（starts 恒 1），之后按幂等规则复用。
    #[test]
    fn concurrent_start_in_flight_mutual_exclusion() {
        let (m, fp, _, _) = test_manager();
        fp.start_delay_ms.store(300, Ordering::SeqCst); // A 的 preview 信令耗 300ms。
        let m = Arc::new(m);
        let out = must_outdoor("06020000@172.16.106.152:18022");

        let h = {
            let m = Arc::clone(&m);
            let out = out.clone();
            thread::spawn(move || m.start(out).expect("start A"))
        };
        thread::sleep(Duration::from_millis(80)); // 确保 A 已持锁进入信令。
        let t0 = Instant::now();
        let info_b = m.start(out).expect("start B");
        let blocked = t0.elapsed();
        let info_a = h.join().unwrap();

        assert_eq!(info_a.id, info_b.id, "B 必须幂等复用 A 的 session");
        assert_eq!(
            fp.starts.load(Ordering::SeqCst),
            1,
            "并发 Start 不得双双通过 preview（starts 必须为 1）"
        );
        assert!(
            blocked >= Duration::from_millis(100),
            "B 必须阻塞到 A 信令完成（blocked={blocked:?}）"
        );
        m.shutdown(Duration::from_secs(3));
    }

    // --- 五件套之一：死 session 不续命 ---

    /// 等价移植 Go `TestManager_StartIdempotentDoesNotExtendDeadSession`：
    /// active session 未收到 IDR（外机 ack 但未推 RTP）时，同 outdoor 幂等 Start
    /// 不刷 TTL；原 TTL 过期后 session 仍被 auto-stop。
    #[test]
    fn start_idempotent_does_not_extend_dead_session() {
        let (mut m, fp, _, _) = test_manager();
        m.ttl = Duration::from_millis(200);
        let m = Arc::new(m);
        let out = must_outdoor("06020000@172.16.106.152:18022");
        let info1 = m.start(out.clone()).expect("start");
        let sess = m.current().expect("no current session after start");
        let initial_deadline = *sess.ttl_deadline.lock().unwrap();

        thread::sleep(Duration::from_millis(100)); // < TTL 200ms，session 还活着。

        let info2 = m.start(out).expect("idempotent start");
        assert_eq!(info1.id, info2.id, "idempotent reuse");
        assert_eq!(
            fp.starts.load(Ordering::SeqCst),
            1,
            "no preview re-issue on idempotent reuse"
        );
        let after_reuse_deadline = *sess.ttl_deadline.lock().unwrap();
        assert_eq!(
            after_reuse_deadline, initial_deadline,
            "无 IDR 的幂等复用不得推进 ttl_deadline"
        );

        // 原 TTL 过期后必须 auto-stop（即便有第二次 Start retry）。
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if m.current().is_none() {
                m.shutdown(Duration::from_secs(3));
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("session did not auto-stop after original TTL despite no IDR ever pushed");
    }

    /// 对偶（移植 Go `TestManager_StartIdempotentRefreshesTTLWhenIDRSeen`）：
    /// healthy session（三件套已齐）的幂等 Start 应当刷 TTL。
    #[test]
    fn start_idempotent_refreshes_ttl_when_idr_seen() {
        let (mut m, _, _, _) = test_manager();
        m.ttl = Duration::from_secs(1);
        let m = Arc::new(m);
        let out = must_outdoor("06020000@172.16.106.152:18022");
        m.start(out.clone()).expect("start");
        let sess = m.current().expect("current");

        // 模拟外机已推流：灌 SPS/PPS/IDR 让 latest_idr 齐。
        sess.frame_buf.push(nal(NAL_TYPE_SPS, &[0x67, 0x00]));
        sess.frame_buf.push(nal(NAL_TYPE_PPS, &[0x68, 0x00]));
        sess.frame_buf.push(nal(NAL_TYPE_IDR, &[0x65, 0x00]));
        assert!(sess.frame_buf.latest_idr().is_some(), "seed not ready");

        thread::sleep(Duration::from_millis(100));
        let before = *sess.ttl_deadline.lock().unwrap();
        m.start(out).expect("idempotent start");
        let after = *sess.ttl_deadline.lock().unwrap();
        assert!(
            after > before,
            "healthy 幂等复用必须推进 ttl_deadline（before={before:?} after={after:?}）"
        );
        m.shutdown(Duration::from_secs(3));
    }

    // --- 五件套之二：TTL cleanup 及时 ---

    /// 等价移植 Go `TestManager_TTLExpiryCompletesPromptly`：TTL 过期 cleanup
    /// （BYE 发出 + FrameBuffer 关闭）必须在 2s 内完成，不被 teardown 的 2s
    /// abandoning 兜底拖死（旧 buggy 路径 monitor 自 join 必卡 ≥2s+）。
    #[test]
    fn ttl_expiry_completes_promptly() {
        let (mut m, _, _, frtcp) = test_manager();
        m.ttl = Duration::from_millis(50);
        let m = Arc::new(m);
        m.start(must_outdoor("06020000@172.16.106.152:18022"))
            .expect("start");
        let sess = m.current().expect("current");

        let start_wait = Instant::now();
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            // teardown 顺序：StopPreview → BYE → tail wait → stop+join → close。
            // FrameBuffer closed 是最后一步，反映完整 cleanup 完成。
            if m.current().is_none()
                && frtcp.byes.load(Ordering::SeqCst) >= 1
                && sess.frame_buf.closed()
            {
                let _ = start_wait.elapsed();
                m.shutdown(Duration::from_secs(3));
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!(
            "TTL auto-stop did not complete within 2s (current={:?}, byes={}, closed={}); likely the 2s abandoning timeout was hit",
            m.current().map(|s| s.id().to_string()),
            frtcp.byes.load(Ordering::SeqCst),
            sess.frame_buf.closed()
        );
    }

    // --- 五件套之三：Shutdown 等 detached cleanup ---

    /// 等价移植 Go `TestManager_ShutdownWaitsForTTLCleanup`：TTL detached cleanup
    /// 进行中（StopPreview 慢 wire 阻塞 200ms）收到 shutdown，必须等 cleanup
    /// 完整跑完（stops_completed/BYE/FrameBuffer close）才返回。
    #[test]
    fn shutdown_waits_for_ttl_cleanup() {
        let (mut m, fp, _, frtcp) = test_manager();
        m.ttl = Duration::from_millis(50);
        fp.stop_delay_ms.store(200, Ordering::SeqCst); // 慢 wire send。
        let m = Arc::new(m);
        m.start(must_outdoor("06020000@172.16.106.152:18022"))
            .expect("start");
        let sess = m.current().expect("current");

        // 等 TTL 过期触发 + StopPreview 已被调用但仍在 delay 中。
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if fp.stops.load(Ordering::SeqCst) >= 1
                && fp.stops_completed.load(Ordering::SeqCst) == 0
            {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            fp.stops.load(Ordering::SeqCst) >= 1,
            "TTL cleanup never fired within 2s"
        );

        // cleanup 在 200ms delay 中；shutdown（1s deadline）必须等它完成。
        m.shutdown(Duration::from_secs(1));
        assert!(
            fp.stops_completed.load(Ordering::SeqCst) >= 1,
            "shutdown returned but StopPreview not yet completed: did not wait for in-flight TTL cleanup"
        );
        assert!(
            frtcp.byes.load(Ordering::SeqCst) >= 1,
            "shutdown returned but RTCP BYE not sent"
        );
        assert!(
            sess.frame_buf.closed(),
            "shutdown returned but FrameBuffer not closed"
        );
    }

    // --- 五件套之四：Shutdown 受 deadline 约束 ---

    /// 等价移植 Go `TestManager_ShutdownTimeoutOnStuckCleanup`：cleanup 卡死
    /// （StopPreview 1.5s）超出 shutdown 100ms deadline 时，shutdown 必须按时返
    /// （不无限阻塞 daemon 退出）。
    #[test]
    fn shutdown_timeout_on_stuck_cleanup() {
        let (mut m, fp, _, _) = test_manager();
        m.ttl = Duration::from_millis(50);
        fp.stop_delay_ms.store(1500, Ordering::SeqCst); // 远超 100ms deadline。
        let m = Arc::new(m);
        m.start(must_outdoor("06020000@172.16.106.152:18022"))
            .expect("start");

        // 等 TTL 触发（StopPreview 已被调用）。
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if fp.stops.load(Ordering::SeqCst) >= 1 {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            fp.stops.load(Ordering::SeqCst) >= 1,
            "TTL cleanup never fired"
        );

        let t0 = Instant::now();
        m.shutdown(Duration::from_millis(100));
        let elapsed = t0.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "shutdown blocked {elapsed:?} despite 100ms deadline; should bail out on timeout"
        );

        // 让 stuck cleanup 自然完成（避免线程 leak 跨测试）。
        let leak_deadline = Instant::now() + Duration::from_millis(2500);
        while Instant::now() < leak_deadline {
            if fp.stops_completed.load(Ordering::SeqCst) >= 1 {
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    // --- teardown deadline 总预算（修复「stop 最坏 ~12s 超 Go 8s handler ctx」）---

    /// 显式 stop 在 preview-stop 挂死（fake 延迟远超预算）时必须在 ~stop_budget 内
    /// 返回，且 FrameBuffer 已 close、BYE 照发（budget tunable 压缩到 300ms，
    /// 禁真实 8s 等待）。锚 Go handleVideoStop 8s ctx：StopPreview 受 ctx 裁短。
    #[test]
    fn stop_returns_within_budget_when_preview_stop_hangs() {
        let (mut m, fp, _, frtcp) = test_manager();
        m.stop_budget = Duration::from_millis(300);
        fp.honor_stop_timeout.store(true, Ordering::SeqCst);
        fp.stop_delay_ms.store(10_000, Ordering::SeqCst); // 挂死：远超预算。
        let m = Arc::new(m);
        let out = must_outdoor("06020000@172.16.106.152:18022");
        m.start(out.clone()).expect("start");
        let sess = m.current().expect("current");

        let t0 = Instant::now();
        m.stop(&out.uri);
        let elapsed = t0.elapsed();
        // 预算 300ms：preview ≤ 2×150ms + tail/join 被剩余预算裁到 ~0。
        assert!(
            elapsed < Duration::from_millis(900),
            "stop took {elapsed:?} despite 300ms budget; teardown not deadline-bounded"
        );
        assert!(
            sess.frame_buf.closed(),
            "FrameBuffer must be closed even when budget is exhausted mid-teardown"
        );
        assert_eq!(frtcp.byes.load(Ordering::SeqCst), 1, "BYE 不受预算约束照发");
        // preview-stop 的 per-segment timeout 必须被预算裁短（≤ min(5s, 预算/2)）。
        let got = fp
            .last_stop_timeout
            .lock()
            .unwrap()
            .expect("stop_preview not called")
            .expect("teardown must pass an explicit clipped timeout");
        assert!(
            got <= Duration::from_millis(150),
            "per-segment timeout {got:?} exceeds half of 300ms budget"
        );
        assert!(m.current().is_none());
    }

    /// 预算已耗尽（shutdown 收到立即过期的 deadline）时：preview-stop 被跳过、
    /// 尾等/join 被裁到 0，但 BYE 照发、stop flag 置位、FrameBuffer close 仍执行
    /// （资源释放不受预算约束）。锚 Go shutdown 各步 select ctx.Done() 即过 +
    /// FrameBuf.Close 无条件执行。
    #[test]
    fn exhausted_budget_skips_preview_stop_but_still_closes() {
        let (m, fp, _, frtcp) = test_manager();
        let m = Arc::new(m);
        m.start(must_outdoor("06020000@172.16.106.152:18022"))
            .expect("start");
        let sess = m.current().expect("current");

        let t0 = Instant::now();
        m.shutdown(Duration::ZERO); // teardown 共享同一 deadline：入场即耗尽。
        assert!(
            t0.elapsed() < Duration::from_millis(500),
            "zero-budget teardown must return promptly, took {:?}",
            t0.elapsed()
        );
        assert_eq!(
            fp.stops.load(Ordering::SeqCst),
            0,
            "预算耗尽必须跳过 stop-preview wire"
        );
        assert_eq!(frtcp.byes.load(Ordering::SeqCst), 1, "BYE 不受预算约束照发");
        assert!(
            sess.frame_buf.closed(),
            "FrameBuffer close 永远执行（不受预算约束）"
        );
        assert!(m.current().is_none());
    }

    // --- 双驱动竞态 CAS 守卫（spec 场景「双 teardown 竞态单方执行」）---

    /// 显式 Stop 已摘 current 后，TTL 驱动的 stop_internal 必须静默早返
    /// （teardown 单方执行：StopPreview/BYE 各恰一次）。
    #[test]
    fn double_teardown_cas_guard_single_executor() {
        let (m, fp, _, frtcp) = test_manager();
        let m = Arc::new(m);
        let out = must_outdoor("06020000@172.16.106.152:18022");
        m.start(out.clone()).expect("start");
        let sess = m.current().expect("current");

        m.stop(&out.uri); // 显式 Stop 摘到 current 并完整 teardown。
        assert_eq!(fp.stops.load(Ordering::SeqCst), 1);
        assert_eq!(frtcp.byes.load(Ordering::SeqCst), 1);

        // TTL 驱动方迟到：没摘到 current → 静默返回，不重复 teardown。
        m.stop_internal(&sess, "ttl-expired");
        assert_eq!(
            fp.stops.load(Ordering::SeqCst),
            1,
            "CAS 守卫必须保证 teardown 单方执行"
        );
        assert_eq!(frtcp.byes.load(Ordering::SeqCst), 1);
    }

    // --- mock 18022 外机：真 PreviewClient 全链路 Start 成功 / teardown stop ---

    /// req=705 / req=709 ack 样本（同 preview.rs 测试 / golden video_wire.txt）。
    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// mock 外机 18022 ack server：循环 accept，按收到帧里的 req 文本回对应 ack。
    fn mock_outdoor_18022() -> (String, u16) {
        let ln = TcpListener::bind("127.0.0.1:0").expect("mock listen");
        let addr = ln.local_addr().unwrap();
        thread::spawn(move || {
            // start + teardown stop 共两连接；多余 accept 随测试结束被丢弃。
            for _ in 0..4 {
                let Ok((mut conn, _)) = ln.accept() else {
                    return;
                };
                let mut buf = [0u8; 64];
                let n = conn.read(&mut buf).unwrap_or(0);
                let frame = &buf[..n];
                let ack = if frame.windows(7).any(|w| w == b"req=704") {
                    hex("07b8180000007265713d3730352671756572792a00008000000002000001")
                } else {
                    hex("07b8180000007265713d3730392671756572792a00008000000002000001")
                };
                let _ = conn.write_all(&ack);
            }
        });
        (addr.ip().to_string(), addr.port())
    }

    /// mock 外机支撑的 Start 成功路径（验收要求）：默认 WirePreview（真
    /// PreviewClient）+ 真 RtpReceiver（ephemeral 端口）+ fake RTCP，全链路
    /// start → stop teardown（req=704/708 各走一次 mock ack）。
    #[test]
    fn start_and_stop_against_mock_outdoor() {
        let (ip, port) = mock_outdoor_18022();
        let caller = parse_caller("06021103@10.0.0.91:18022").expect("caller");
        let frtcp = Arc::new(FakeRtcp::default());
        let mut m = Manager::new(caller, None); // preview 用默认 WirePreview。
        m.rtp_listen_addr = "127.0.0.1:0".to_string();
        m.ttl_monitor_interval = Duration::from_millis(25);
        m.rtcp = Arc::clone(&frtcp) as Arc<dyn RtcpPort>;
        let m = Arc::new(m);

        let out = must_outdoor(&format!("06020000@{ip}:{port}"));
        let info = m.start(out.clone()).expect("start against mock outdoor");
        assert!(is_valid_uuid(&info.id));
        assert!(m.current_by_id(&info.id).is_some());

        m.stop(&out.uri);
        assert!(m.current().is_none(), "stop must release session");
        assert_eq!(frtcp.byes.load(Ordering::SeqCst), 1, "BYE sent in teardown");
    }

    /// mock 外机支撑的 Start 失败路径（验收要求）：mock 回错 ack（req 文本不符）
    /// → StartError::Preview，无 session 残留。
    #[test]
    fn start_against_mock_outdoor_bad_ack_fails() {
        let ln = TcpListener::bind("127.0.0.1:0").expect("mock listen");
        let addr = ln.local_addr().unwrap();
        thread::spawn(move || {
            if let Ok((mut conn, _)) = ln.accept() {
                let mut buf = [0u8; 64];
                let _ = conn.read(&mut buf);
                // 回 stop ack 冒充 start ack → 校验必须拒绝。
                let _ = conn.write_all(&hex("07b8180000007265713d3730392671756572792a0000"));
            }
        });
        let caller = parse_caller("06021103@10.0.0.91:18022").expect("caller");
        let mut m = Manager::new(caller, None);
        m.rtp_listen_addr = "127.0.0.1:0".to_string();
        let m = Arc::new(m);
        let err = m
            .start(must_outdoor(&format!(
                "06020000@{}:{}",
                addr.ip(),
                addr.port()
            )))
            .expect_err("bad ack must fail start");
        assert!(matches!(err, StartError::Preview(_)), "err = {err:?}");
        assert!(m.current().is_none());
    }

    // --- helpers（移植 Go TestUUIDv4_FormatAndUnique / TestRandomSSRC_NonZero / IsValidUUID）---

    /// 移植 Go `TestUUIDv4_FormatAndUnique` + version/variant 位断言。
    #[test]
    fn uuid_v4_format_and_unique() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            let u = new_uuid_v4().expect("new_uuid_v4");
            assert!(is_valid_uuid(&u), "invalid UUID: {u}");
            let b = u.as_bytes();
            assert_eq!(b[14], b'4', "version nibble must be 4: {u}");
            assert!(
                matches!(b[19], b'8' | b'9' | b'a' | b'b'),
                "variant nibble must be 8/9/a/b: {u}"
            );
            assert!(seen.insert(u.clone()), "duplicate UUID: {u}");
        }
    }

    /// 移植 Go `TestRandomSSRC_NonZero`。
    #[test]
    fn random_ssrc_non_zero() {
        for i in 0..50 {
            let s = new_random_ssrc().expect("new_random_ssrc");
            assert_ne!(s, 0, "zero SSRC at i={i}");
        }
    }

    /// urandom 读失败返错不 panic（D5）。
    #[test]
    fn random_source_failure_returns_error() {
        assert!(uuid_v4_from("/nonexistent-urandom-for-test").is_err());
        assert!(random_ssrc_from("/nonexistent-urandom-for-test").is_err());
    }

    /// IsValidUUID 边界（锚 Go IsValidUUID 行为）。
    #[test]
    fn is_valid_uuid_boundaries() {
        assert!(is_valid_uuid("12345678-1234-4234-8234-123456789abc"));
        assert!(is_valid_uuid("ABCDEF01-2345-6789-abcd-ef0123456789")); // 大写 hex 接受。
        assert!(!is_valid_uuid("")); // 空。
        assert!(!is_valid_uuid("12345678-1234-4234-8234-123456789ab")); // 35 字符。
        assert!(!is_valid_uuid("12345678-1234-4234-8234-123456789abcd")); // 37 字符。
        assert!(!is_valid_uuid("12345678x1234-4234-8234-123456789abc")); // hyphen 位错。
        assert!(!is_valid_uuid("1234567g-1234-4234-8234-123456789abc")); // 非 hex。
        assert!(!is_valid_uuid("12345678-1234-4234-8234-12345678九abc")); // 非 ASCII。
    }

    // --- Outdoor/Caller 解析（移植 Go TestParseOutdoor / TestParseOutdoor_BadURI）---

    /// 移植 Go `TestParseOutdoor`。
    #[test]
    fn parse_outdoor_fields() {
        let o = parse_outdoor("06020000@172.16.106.152:18022").expect("parse_outdoor");
        assert_eq!(o.name, "06020000");
        assert_eq!(o.ip, "172.16.106.152");
        assert_eq!(o.port, 18022);
        assert_eq!(o.bcd_name, [0x06, 0x02, 0x00, 0x00]);
        assert_eq!(o.addr_tcp(), "172.16.106.152:18022");
        assert_eq!(o.addr_rtcp(), "172.16.106.152:6671");
        assert_eq!(o.uri, "06020000@172.16.106.152:18022");
    }

    /// 移植 Go `TestParseOutdoor_BadURI` + Caller。
    #[test]
    fn parse_outdoor_and_caller_bad_uri() {
        assert!(matches!(
            parse_outdoor("not-a-uri"),
            Err(ParseError::Uri(_))
        ));
        assert!(matches!(parse_caller("not-a-uri"), Err(ParseError::Uri(_))));
        let c = parse_caller("06021103@10.0.0.91:18022").expect("caller");
        assert_eq!(c.name, "06021103");
        assert_eq!(c.bcd_name, [0x06, 0x02, 0x11, 0x03]);
    }
}
