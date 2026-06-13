//! `listen18022`：门禁网 TCP 18022 wire 帧监听 + Subscribe 订阅。
//!
//! 组成：
//!   - `parse_frame`（req + body 抽取，复用 `crate::wire18022::parse_response_frame`
//!     兼容 `&query=` / `&query*` 双 sep）
//!   - `extract_tcp_payload`（L2→ethertype 0x0800→IP(ihl/proto=6)→TCP(dataOff)→
//!     payload + src/dst IP + src/dst port，全 `from_be_bytes` 读 wire BE）
//!   - dedup ringbuffer（key=src_ip^src_port<<32^FNV-1a(payload[..64])，64 容量 /
//!     200ms 窗口，**时钟注入 seam**，与 listen6672 同风格）
//!   - **`trait Subscribable`** + `Subscription{ch, cancel}`：`subscribe(filter)`
//!     注册、buffered 8、filter panic 用 `catch_unwind` 隔离、chan 满 `try_send`
//!     失败即 silent drop、cancel 移除订阅 + 多次 cancel no-op（unlock bye 早停的
//!     依赖缝 — unlock 核心注入 mock 实现 `trait Subscribable`；持
//!     `Option<&dyn Subscribable>` 为 `None` 时不订阅）。
//!   - `dispatch_for_test` 等价 hook（绕 PF_PACKET 直驱 dispatch + Subscribe 投递，
//!     跨模块测试用）。
//!   - PF_PACKET recv 循环（Linux-only，喂 `bpf::BPF_18022`）；非 Linux stub 返错。
//!
//! BPF const 用 `crate::bpf::BPF_18022`；socket 缝用 `crate::ffi`。

use std::sync::atomic::AtomicBool;
#[cfg(target_os = "linux")]
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{Arc, Mutex};

// ===========================================================================
// parser（parse_frame + extract_tcp_payload）
// ===========================================================================

/// 解析后的 18022 wire 帧结果。
///
/// 字段：`req` / `body` / `src_ip` / `dst_ip` / `src_port` / `dst_port`。
/// IP 用 `[u8; 4]`（IPv4-only，与 BPF 过滤一致；mock/测试构造方便）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedFrame {
    /// anjubao 协议 req= 编号（如 704 / 705 / 708 / 518 / 710 / 711）。
    ///
    /// 注：复用 `wire18022::parse_response_frame` 返 `i64`。
    pub req: i64,
    /// wire 帧 `&query=` / `&query*` 之后的二进制 body（不含 magic/length/req= 前缀）。
    pub body: Vec<u8>,
    /// 帧的 IPv4 源地址。
    pub src_ip: [u8; 4],
    /// 帧的 IPv4 目的地址。
    pub dst_ip: [u8; 4],
    /// TCP 源端口。
    pub src_port: u16,
    /// TCP 目的端口。
    pub dst_port: u16,
}

/// 把 wire payload 解析为 `(req, body)`。
///
/// 直接委托 `crate::wire18022::parse_response_frame`，兼容 `&query=` / `&query*`
/// 双分隔符。错误时返 `None`（上游 dispatch 把非合法 anjubao wire 当 TCP 控制帧残留
/// silent drop）。
pub fn parse_frame(payload: &[u8]) -> Option<(i64, Vec<u8>)> {
    crate::wire18022::parse_response_frame(payload).ok()
}

/// 从 L2 frame 中抽 TCP payload + src/dst IP + src/dst port。
///
/// BPF 已过滤为 IPv4/TCP/port=18022。untagged-only（slave 接口 frame 已被 kernel
/// 剥 802.1Q tag）。全用 `from_be_bytes` 读 wire 多字节字段（wire 永远 big-endian）。
///
/// L2 layout: 14B Ethernet | 20-60B IPv4 | 20+ B TCP(含 options) | payload
///
/// 返回 `Some((payload, src_ip, dst_ip, src_port, dst_port))`；任何长度/类型不符返
/// `None`。无 payload（纯 SYN/ACK/FIN）返 `Some` + 空 payload，调用方 `parse_frame`
/// 失败再 drop（双层防御：返 `Some` + 空 payload 语义）。
#[allow(clippy::type_complexity)]
pub fn extract_tcp_payload(frame: &[u8]) -> Option<(Vec<u8>, [u8; 4], [u8; 4], u16, u16)> {
    const ETH_HDR_LEN: usize = 14;
    const MIN_IP_HDR_LEN: usize = 20;
    const MIN_TCP_HDR_LEN: usize = 20;

    if frame.len() < ETH_HDR_LEN + MIN_IP_HDR_LEN + MIN_TCP_HDR_LEN {
        return None;
    }
    // ethertype（byte 12-13）必须是 IPv4 0x0800。
    if u16::from_be_bytes([frame[12], frame[13]]) != 0x0800 {
        return None;
    }

    let ip_hdr = &frame[ETH_HDR_LEN..];
    let ihl = (ip_hdr[0] & 0x0f) as usize * 4;
    if ihl < MIN_IP_HDR_LEN || ip_hdr.len() < ihl + MIN_TCP_HDR_LEN {
        return None;
    }
    // IP proto（byte 9）必须是 TCP 6。
    if ip_hdr[9] != 6 {
        return None;
    }

    let src_ip = [ip_hdr[12], ip_hdr[13], ip_hdr[14], ip_hdr[15]];
    let dst_ip = [ip_hdr[16], ip_hdr[17], ip_hdr[18], ip_hdr[19]];

    let tcp_hdr = &ip_hdr[ihl..];
    let src_port = u16::from_be_bytes([tcp_hdr[0], tcp_hdr[1]]);
    let dst_port = u16::from_be_bytes([tcp_hdr[2], tcp_hdr[3]]);

    let data_off = (tcp_hdr[12] >> 4) as usize * 4;
    if data_off < MIN_TCP_HDR_LEN || tcp_hdr.len() < data_off {
        return None;
    }

    let payload = tcp_hdr[data_off..].to_vec();
    Some((payload, src_ip, dst_ip, src_port, dst_port))
}

// ===========================================================================
// 诊断纯函数：infer_direction + format_log
// ===========================================================================

/// wire 帧的协议层方向。
///
/// 判别值从 `Unknown=0` 起按顺序排列。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// 未能匹配任何已知设备对。
    Unknown,
    /// 外机 → 真实室内机。
    OutdoorToIndoor,
    /// 真实室内机 → 外机。
    IndoorToOutdoor,
    /// daemon 自身 → 外机（如 unlock-A）。
    DaemonToOutdoor,
    /// daemon 自身 → 真实室内机（如 bye 自指）。
    DaemonToIndoor,
    /// 外机 → daemon（如 711 ack）。
    OutdoorToDaemon,
    /// 真实室内机 → daemon。
    IndoorToDaemon,
}

impl Direction {
    /// 方向字符串（log 用）。
    pub fn as_str(&self) -> &'static str {
        match self {
            Direction::OutdoorToIndoor => "outdoor→indoor",
            Direction::IndoorToOutdoor => "indoor→outdoor",
            Direction::DaemonToOutdoor => "daemon→outdoor",
            Direction::DaemonToIndoor => "daemon→indoor",
            Direction::OutdoorToDaemon => "outdoor→daemon",
            Direction::IndoorToDaemon => "indoor→daemon",
            Direction::Unknown => "unknown",
        }
    }
}

impl core::fmt::Display for Direction {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 根据 src/dst IP + 已知设备 IP 列表推断方向。
///
/// 已知 IP（任一为 `None` 则该方向不参与匹配，返 `Unknown`）：
///   - `daemon_ip`: hAP daemon 自身 IP（如 .202）
///   - `indoor_ip`: 真实室内机 IP（如 .91）
///   - `outdoor_ips`: 外机 IP 列表（如 .151~.157）
///
/// IPv4-only：用 `[u8; 4]` 等值比较（listener 已把帧 IP
/// 收敛为 `[u8; 4]`，无 v4-in-v6 歧义）。
pub fn infer_direction(
    src_ip: [u8; 4],
    dst_ip: [u8; 4],
    daemon_ip: Option<[u8; 4]>,
    indoor_ip: Option<[u8; 4]>,
    outdoor_ips: &[[u8; 4]],
) -> Direction {
    let src_is_daemon = daemon_ip == Some(src_ip);
    let dst_is_daemon = daemon_ip == Some(dst_ip);
    let src_is_indoor = indoor_ip == Some(src_ip);
    let dst_is_indoor = indoor_ip == Some(dst_ip);
    let src_is_outdoor = outdoor_ips.contains(&src_ip);
    let dst_is_outdoor = outdoor_ips.contains(&dst_ip);

    if src_is_outdoor && dst_is_indoor {
        Direction::OutdoorToIndoor
    } else if src_is_indoor && dst_is_outdoor {
        Direction::IndoorToOutdoor
    } else if src_is_daemon && dst_is_outdoor {
        Direction::DaemonToOutdoor
    } else if src_is_daemon && dst_is_indoor {
        Direction::DaemonToIndoor
    } else if src_is_outdoor && dst_is_daemon {
        Direction::OutdoorToDaemon
    } else if src_is_indoor && dst_is_daemon {
        Direction::IndoorToDaemon
    } else {
        Direction::Unknown
    }
}

/// 把 IPv4 字节渲染成点分十进制。
fn fmt_ip(ip: [u8; 4]) -> String {
    format!("{}.{}.{}.{}", ip[0], ip[1], ip[2], ip[3])
}

/// 已知 req= 编号的协议语义注释。无注释返 `""`。
fn req_note(req: i64) -> &'static str {
    match req {
        704 => "invite-query",
        705 => "invite-ack",
        708 => "bye",
        709 => "bye-ack",
        710 => "unlock-A",
        711 => "ack-OK",
        519 => "ack-OK",
        564 | 565 | 840 | 841 | 888 | 889 => "ring-late-state",
        _ => "",
    }
}

/// 生成探针 syslog log 行。
///
/// 格式：
///
/// ```text
/// listen18022: detected req=<NNN> <direction> <src> → <dst> [byte0=0x<hex>] [<note>]
/// ```
///
/// 关键 req 编号附协议语义注释（如 req=708 → "bye"，req=710 → "unlock-A"）。
/// req=518 额外含 byte0（区分门禁卡通知 0x1b / unlock-B 标准 0x22 / appoint 变体 0xed）。
pub fn format_log(
    req: i64,
    src_ip: [u8; 4],
    dst_ip: [u8; 4],
    body: &[u8],
    dir: Direction,
) -> String {
    let note = req_note(req);
    let direction_str = dir.as_str();
    let src = fmt_ip(src_ip);
    let dst = fmt_ip(dst_ip);

    if req == 518 && !body.is_empty() {
        // req=518 byte0 区分变体。
        let variant = match body[0] {
            0x1b => "state-notify".to_string(),
            0x22 => "unlock-B-standard".to_string(),
            0xed => "appoint-variant".to_string(),
            b => format!("unknown-variant-0x{b:02x}"),
        };
        return format!(
            "listen18022: detected req={req} {direction_str} {src} → {dst} byte0=0x{:02x} ({variant})",
            body[0]
        );
    }

    if !note.is_empty() {
        return format!("listen18022: detected req={req} {direction_str} {src} → {dst} ({note})");
    }
    format!("listen18022: detected req={req} {direction_str} {src} → {dst}")
}

// ===========================================================================
// dedup ringbuffer（时钟注入 seam — 与 listen6672 同风格）
// ===========================================================================

/// ringbuffer 固定容量（与 listen6672 一致：broadcast/forward 重复 frame 跨 slave
/// socket 到达，64 容量 4× 安全余量）。
pub const DEDUP_CAPACITY: usize = 64;

/// 同 key 视为重复的时间窗（200ms 覆盖 bridge forward 最坏延迟）。
pub const DEDUP_WINDOW_MS: u64 = 200;

/// dedup_key 哈希时 payload 截断上限（防 malformed 帧污染 ringbuffer；18022 wire
/// frame 通常 ≤ 36 byte，截 64 即够）。
const MAX_HASH_BYTES: usize = 64;

/// 可注入时钟 seam：返回单调毫秒时刻。
///
/// 生产用 `MonotonicClock`（包 `std::time::Instant`）；测试用固定时刻 fake clock
/// 让时间相关 ringbuffer 行为确定（单调时刻可注入封装）。
pub trait Clock: Send + Sync {
    /// 返回单调毫秒时刻（任意起点，仅差值有意义）。
    fn now_ms(&self) -> u64;
}

/// 生产时钟：基于 `std::time::Instant`（单调）。
pub struct MonotonicClock {
    start: std::time::Instant,
}

impl MonotonicClock {
    /// 新建一个以当前时刻为零点的单调时钟。
    pub fn new() -> Self {
        Self {
            start: std::time::Instant::now(),
        }
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MonotonicClock {
    fn now_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

#[derive(Clone, Copy)]
struct DedupEntry {
    key: u64,
    ts_ms: u64,
    set: bool,
}

/// 固定容量 ringbuffer，用于多 slave 模式去重相同 frame。
///
/// 线程安全：内部 `Mutex`。`seen` 是唯一写入路径。
pub struct Dedup {
    inner: Mutex<DedupInner>,
}

struct DedupInner {
    entries: [DedupEntry; DEDUP_CAPACITY],
    head: usize,
}

impl Dedup {
    /// 新建空 dedup。
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(DedupInner {
                entries: [DedupEntry {
                    key: 0,
                    ts_ms: 0,
                    set: false,
                }; DEDUP_CAPACITY],
                head: 0,
            }),
        }
    }

    /// 检查 `key` 是否在 `DEDUP_WINDOW_MS` 内被见过。返回 true 表示重复，调用方应 drop。
    ///
    /// 命中后**不**更新 ts；未命中则把 (key, now) 写入 head 位置（环形覆盖最旧 entry）。
    /// 边界含 cutoff 时刻自身（now-ts == window 算窗口内重复，即 `ts >= cutoff`）。
    pub fn seen(&self, key: u64, now_ms: u64) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let cutoff = now_ms.saturating_sub(DEDUP_WINDOW_MS);
        for e in inner.entries.iter() {
            if !e.set {
                continue;
            }
            if e.key == key && e.ts_ms >= cutoff {
                return true;
            }
        }
        let head = inner.head;
        inner.entries[head] = DedupEntry {
            key,
            ts_ms: now_ms,
            set: true,
        };
        inner.head = (head + 1) % DEDUP_CAPACITY;
        false
    }
}

impl Default for Dedup {
    fn default() -> Self {
        Self::new()
    }
}

/// 算 frame 的 dedup 哈希。
///
/// key = src_ip(4B BE) ^ (src_port(2B) << 32) ^ FNV-1a(payload[0:min(len, 64)])。
pub fn dedup_key(src_ip: [u8; 4], src_port: u16, payload: &[u8]) -> u64 {
    let n = payload.len().min(MAX_HASH_BYTES);
    let hash = fnv1a_64(&payload[..n]);
    let ip_bits = u32::from_be_bytes(src_ip) as u64;
    hash ^ ip_bits ^ ((src_port as u64) << 32)
}

/// FNV-1a 64-bit。
fn fnv1a_64(data: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h
}

// ===========================================================================
// Subscribe 订阅模型
// ===========================================================================

/// 单订阅者的 filter 类型：在 dispatch 路径上同步调用判断是否投递。
///
/// 应是纯函数（cheap、no I/O）；其 panic 由 dispatch 路径的 `catch_unwind` 隔离，
/// 不影响其它订阅者或 listener 主循环。
pub type SubFilter = Box<dyn Fn(&DetectedFrame) -> bool + Send + Sync>;

/// `subscribe()` 返回给调用方的句柄（持 `ch` + `cancel`）。
///
/// `ch` 在 filter 通过的 frame 到达时收到投递（buffered 8 防止 dispatch 短暂卡住
/// listener 主循环）。`cancel()` 释放订阅；cancel 后再投递被 silent drop，再次
/// cancel 是 no-op。
///
/// 调用方仍可在 bye-watch / 消费者退出时显式 `cancel()`（清晰表意）；此外 `Subscription`
/// 实现 **`Drop`-on-drop 自动 cancel** 作安全网：当 `Subscription` 被 drop 而**未**显式
/// cancel 时（如 `spawn_consumer` 把 sub move 进失败的线程闭包、闭包随即 drop），Drop
/// 兜底 cancel，避免 listener 残留 orphan SubEntry 持续跑 filter/try_send。cancel 幂等
/// （id 找不到即 no-op），故显式 + Drop 双触发安全。
pub struct Subscription {
    /// filter 通过的 frame 投递目的 channel（buffered 8）。
    pub ch: Receiver<DetectedFrame>,
    /// 显式取消订阅。多次调用 no-op。
    cancel: Box<dyn Fn() + Send + Sync>,
}

impl Subscription {
    /// 取消订阅：从 listener 移除 + 后续投递 silent drop。多次调用 no-op。
    pub fn cancel(&self) {
        (self.cancel)();
    }
}

impl Drop for Subscription {
    /// 安全网：drop 时兜底 cancel（幂等）——保证任何 `Subscription` 离开作用域都不留
    /// orphan SubEntry，即便调用方未显式 `cancel()`（如 spawn 失败路径）。
    fn drop(&mut self) {
        (self.cancel)();
    }
}

/// 运行时订阅注册缝（`subscribe(filter) -> Subscription`）。
///
/// 这是 **unlock 核心的依赖缝**：`execute_unlock` 的 bye 早停依赖
/// `subscribe(filter_req708_for_target(...))`。把它抽象成 trait 让 unlock 能注入 mock
/// 实现（无真 PF_PACKET listener 也能 e2e 测早停）；持有 `Option<&dyn Subscribable>`
/// 为 `None` 时不订阅、bye channel 永不命中。形状宁窄勿宽。
pub trait Subscribable: Send + Sync {
    /// 注册一个运行时订阅者。`filter` 返回 true 的 frame 投到 `Subscription.ch`
    /// （buffered 8）。`filter == None` 视为「全通过」。
    fn subscribe(&self, filter: Option<SubFilter>) -> Subscription;
}

/// 单个 subscribe() 调用对应的内部条目。
struct SubEntry {
    /// 订阅唯一 id（cancel 时按 id 移除，避免比较 fn 指针）。
    id: usize,
    filter: SubFilter,
    tx: SyncSender<DetectedFrame>,
    /// 由 cancel 设置，避免 cancel 后被 dispatch 投递（与 dispatch 之间的 race 二次检查）。
    closed: bool,
}

/// 订阅者列表的共享句柄。`Listener` 持有它、`subscribe()` 返回的 cancel 闭包也捕获
/// 它的克隆——这样 cancel 不依赖 `Listener` 本体存活（用 `Arc` 显式管理共享所有权）。
type SubsHandle = Arc<Mutex<Vec<SubEntry>>>;

// ===========================================================================
// Listener
// ===========================================================================

/// frame 检测回调。收到合法 anjubao 18022 wire 帧时调用。
pub type OnDetect = Box<dyn Fn(&DetectedFrame) + Send + Sync>;

/// 可选 log hook 签名。
pub type LogFn = Box<dyn Fn(&str) + Send + Sync>;

/// 监听一组物理 slave 接口上的 TCP 18022 wire 帧，按 anjubao 协议解析后调 `on_detect`
/// 回调 + 投递给 `subscribe()` 订阅者。
///
/// 跨平台部分（`dispatch_with_dedup` / Subscribe / 回调）所有平台可用；`run` 仅
/// Linux 实现（PF_PACKET + BPF），非 Linux stub 返错。
pub struct Listener {
    /// 物理 slave 接口名列表（与 listen6672 共享同 slaves）。空列表视为错误。
    pub slaves: Vec<String>,

    /// 收到合法 anjubao 18022 wire 帧时调用（探针定位：仅 log，不改业务状态）。`None` 时跳过。
    pub on_detect: Option<OnDetect>,

    /// 可选 log hook。`None` 时不 log。
    pub logf: Option<LogFn>,

    /// 多 slave 模式下用于跨 socket 去重；单 slave 模式 `None`（跳过 dedup）。
    dedup: Option<Arc<Dedup>>,

    /// dedup 时钟 seam（生产 `MonotonicClock`，测试可注入 fake）。
    clock: Arc<dyn Clock>,

    /// 运行时订阅者列表（`subscribe()` 注册；`dispatch_with_dedup` 在 on_detect 之后遍历）。
    /// 用共享句柄让 cancel 闭包不依赖 `Listener` 本体存活。
    subs: SubsHandle,

    /// 订阅 id 单调计数器。
    next_sub_id: std::sync::atomic::AtomicUsize,
}

impl Listener {
    /// 新建 Listener。`slaves` 长度 > 1 时自动启 dedup。
    pub fn new(slaves: Vec<String>, on_detect: Option<OnDetect>) -> Self {
        let dedup = if slaves.len() > 1 {
            Some(Arc::new(Dedup::new()))
        } else {
            None
        };
        Self {
            slaves,
            on_detect,
            logf: None,
            dedup,
            clock: Arc::new(MonotonicClock::new()),
            subs: Arc::new(Mutex::new(Vec::new())),
            next_sub_id: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// 注入自定义时钟（测试用固定时刻 fake clock）。
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// 强制启用 dedup（测试用：单 slave 也想验 dedup 路径）。
    pub fn with_dedup(mut self, dedup: Arc<Dedup>) -> Self {
        self.dedup = Some(dedup);
        self
    }

    fn logf(&self, msg: &str) {
        if let Some(f) = &self.logf {
            f(msg);
        }
    }

    /// 测试用 hook：绕过 PROMISC PF_PACKET 真抓帧路径直接驱动 dispatch（on_detect 回调
    /// + subscribe 订阅者投递）。
    ///
    /// 仅供跨包测试使用（如 unlock 验证重试期间收到 req=708 bye 早停）；生产代码不应调用。
    pub fn dispatch_for_test(
        &self,
        src_ip: [u8; 4],
        dst_ip: [u8; 4],
        src_port: u16,
        dst_port: u16,
        payload: &[u8],
    ) {
        self.dispatch_with_dedup(src_ip, dst_ip, src_port, dst_port, payload);
    }

    /// 多 slave 模式用 dedup 去重后再 dispatch；单 slave 直触发。
    pub fn dispatch_with_dedup(
        &self,
        src_ip: [u8; 4],
        dst_ip: [u8; 4],
        src_port: u16,
        dst_port: u16,
        payload: &[u8],
    ) {
        if let Some(dedup) = &self.dedup {
            let key = dedup_key(src_ip, src_port, payload);
            if dedup.seen(key, self.clock.now_ms()) {
                return;
            }
        }

        let (req, body) = match parse_frame(payload) {
            Some(rb) => rb,
            // 不是合法 anjubao wire（SYN/ACK/FIN 等 TCP 控制 frame 残留），silent drop。
            None => return,
        };

        let frame = DetectedFrame {
            req,
            body,
            src_ip,
            dst_ip,
            src_port,
            dst_port,
        };

        if let Some(cb) = &self.on_detect {
            cb(&frame);
        }
        self.dispatch_to_subs(&frame);
    }

    /// 把 frame 投递给所有 filter 通过的活跃订阅者。
    ///
    /// 每个订阅者 filter 调用用 `catch_unwind` 隔离 panic（不影响其它订阅者或主循环）。
    /// chan 满（buffered 8 已堵）时 `try_send` 失败即 silent drop。cancel 后的订阅在
    /// 锁内被二次检查 `closed` 跳过（cancel 与 dispatch 之间的 race）。
    fn dispatch_to_subs(&self, frame: &DetectedFrame) {
        // 锁内逐个评估并投递。先取需要的 (filter eval) 在锁内做 closed 二次检查
        // （锁内 closed 检查 + try-send）。
        let subs = self.subs.lock().unwrap();
        if subs.is_empty() {
            return;
        }
        for sub in subs.iter() {
            if sub.closed {
                continue;
            }
            // filter panic 隔离：catch_unwind。filter 为纯函数无 unwind-safety 顾虑，
            // 用 AssertUnwindSafe 包裹引用。
            let pass =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (sub.filter)(frame)));
            let pass = match pass {
                Ok(p) => p,
                Err(_) => {
                    self.logf("listen18022: subscription filter panic recovered");
                    continue;
                }
            };
            if !pass {
                continue;
            }
            // chan 满 → try_send 失败 → silent drop（防慢消费者卡主循环）。
            let _ = sub.tx.try_send(frame.clone());
        }
    }

    /// 启动监听主循环（Linux PF_PACKET 实现，非 Linux 立即返错）。
    ///
    /// 阻塞直到 `shutdown` 置位；返回时清理所有 slave socket。
    ///
    /// 空 slaves 在跨平台入口先判 [`ListenerError::NoSlaves`]（两平台一致；否则非 Linux
    /// stub 会无条件返 Unsupported 掩盖空 slaves 契约）。
    pub fn run(&self, shutdown: Arc<AtomicBool>) -> Result<(), ListenerError> {
        if self.slaves.is_empty() {
            return Err(ListenerError::NoSlaves);
        }
        self.run_platform(shutdown)
    }
}

impl Subscribable for Listener {
    /// 注册运行时订阅者。`filter == None` → 全通过。buffered 8。
    ///
    /// cancel 闭包捕获 `subs` 共享句柄克隆 + 本订阅 id：cancel 时按 id 标记 closed 并
    /// 移除（先置 `sub.closed=true` 再从切片删除 + drop tx）。
    /// 多次 cancel no-op（按 id 找不到即返回）。
    fn subscribe(&self, filter: Option<SubFilter>) -> Subscription {
        let filter: SubFilter = filter.unwrap_or_else(|| Box::new(|_| true));
        let (tx, rx) = std::sync::mpsc::sync_channel::<DetectedFrame>(8);
        let id = self
            .next_sub_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        {
            let mut subs = self.subs.lock().unwrap();
            subs.push(SubEntry {
                id,
                filter,
                tx,
                closed: false,
            });
        }

        let subs_handle = self.subs.clone();
        let cancel = Box::new(move || {
            let mut subs = subs_handle.lock().unwrap();
            if let Some(pos) = subs.iter().position(|s| s.id == id && !s.closed) {
                // 标记 closed（dispatch 锁内二次检查会跳过）再移除（drop tx → rx 端
                // 后续 recv 得到 Disconnected）。多次 cancel：
                // 第二次 position 找不到（已移除）即 no-op。
                subs[pos].closed = true;
                subs.remove(pos);
            }
        });

        Subscription { ch: rx, cancel }
    }
}

/// listener 运行错误。
#[derive(Debug)]
pub enum ListenerError {
    /// `slaves` 为空。
    NoSlaves,
    /// PF_PACKET 仅 Linux 支持（非 Linux stub 路径）。
    Unsupported,
    /// 某个 slave socket 阶段失败（携带 slave 名 + 底层 FFI 错误）。
    Slave(String, crate::ffi::FfiError),
}

impl core::fmt::Display for ListenerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ListenerError::NoSlaves => write!(f, "listen18022: Slaves required"),
            ListenerError::Unsupported => write!(
                f,
                "listen18022: PF_PACKET only supported on Linux; build for linux/* and run as root"
            ),
            ListenerError::Slave(iface, e) => write!(f, "slave {iface}: {e}"),
        }
    }
}

impl std::error::Error for ListenerError {}

// --- Linux PF_PACKET recv 循环 ---

#[cfg(target_os = "linux")]
impl Listener {
    fn run_platform(&self, shutdown: Arc<AtomicBool>) -> Result<(), ListenerError> {
        if self.slaves.is_empty() {
            return Err(ListenerError::NoSlaves);
        }

        let result: Mutex<Result<(), ListenerError>> = Mutex::new(Ok(()));

        std::thread::scope(|scope| {
            for iface in &self.slaves {
                let shutdown = shutdown.clone();
                let result = &result;
                scope.spawn(move || {
                    if let Err(e) = self.run_slave(iface, shutdown.clone()) {
                        // 任一 slave 致命错 → 记录首个 + 置 shutdown 停所有。
                        let mut slot = result.lock().unwrap();
                        if slot.is_ok() {
                            *slot = Err(ListenerError::Slave(iface.clone(), e));
                        }
                        shutdown.store(true, Ordering::SeqCst);
                    }
                });
            }
        });

        result.into_inner().unwrap()
    }

    /// 在单个 slave 上跑 PF_PACKET recv 循环（每 slave 一个 thread 调用）。
    fn run_slave(
        &self,
        iface: &str,
        shutdown: Arc<AtomicBool>,
    ) -> Result<(), crate::ffi::FfiError> {
        use crate::ffi;

        let ifindex = ffi::iface_index(iface)?;
        let fd = ffi::open_packet_socket()?;

        // RAII 守门：函数返回 / panic 时关 fd。
        struct FdGuard(std::os::unix::io::RawFd);
        impl Drop for FdGuard {
            fn drop(&mut self) {
                crate::ffi::close(self.0);
            }
        }
        let _guard = FdGuard(fd);

        ffi::bind_to_ifindex(fd, ifindex)?;
        ffi::set_promisc(fd, ifindex)?;

        // BPF filter 用 BPF_18022（区别于 6672 的唯一处）。
        let fprog = crate::bpf::as_fprog(&crate::bpf::BPF_18022);
        ffi::attach_filter(fd, &fprog)?;

        // SO_RCVTIMEO=500ms 周期 wakeup 查 shutdown flag（失败不致命，仅 log）。
        if let Err(e) = ffi::set_rcvtimeo(fd, 0, 500_000) {
            self.logf(&format!(
                "listen18022: SO_RCVTIMEO set failed on {iface}: {e} (continuing)"
            ));
        }

        self.logf(&format!(
            "listen18022: started on {iface} (ifindex={ifindex}, PROMISC, BPF active, dedup={})",
            self.dedup.is_some()
        ));

        let mut buf = [0u8; 1514]; // ETH header(14) + MTU(1500)
        loop {
            if shutdown.load(Ordering::SeqCst) {
                return Ok(());
            }
            match ffi::recv(fd, &mut buf) {
                Ok(n) => {
                    if let Some((payload, src_ip, dst_ip, src_port, dst_port)) =
                        extract_tcp_payload(&buf[..n])
                    {
                        self.dispatch_with_dedup(src_ip, dst_ip, src_port, dst_port, &payload);
                    }
                }
                Err(crate::ffi::FfiError::Syscall { errno, .. }) if ffi::is_retry_errno(errno) => {
                    // SO_RCVTIMEO 周期 wakeup（EAGAIN/EWOULDBLOCK）或信号（EINTR）：
                    // loop 顶部已查 shutdown，这里 continue 再查一次。
                    continue;
                }
                Err(e) => {
                    if shutdown.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    return Err(e);
                }
            }
        }
    }
}

// --- 非 Linux stub ---

#[cfg(not(target_os = "linux"))]
impl Listener {
    fn run_platform(&self, _shutdown: Arc<AtomicBool>) -> Result<(), ListenerError> {
        Err(ListenerError::Unsupported)
    }
}

// ===========================================================================
// tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // --- 测试辅助：构造 18022 wire 帧 + L2 封装 ---

    /// 构造一个合法的 18022 wire 帧 `req=NNN&query=<body>`（0x07b8 magic + LE len）。
    fn wire_frame(req: u32, body: &[u8]) -> Vec<u8> {
        let mut frame_body = Vec::new();
        frame_body.extend_from_slice(format!("req={req}").as_bytes());
        frame_body.extend_from_slice(b"&query=");
        frame_body.extend_from_slice(body);

        let mut frame = Vec::new();
        frame.push(0x07); // magic hi
        frame.push(0xb8); // magic lo
        frame.extend_from_slice(&(frame_body.len() as u16).to_le_bytes()); // length LE
        frame.push(0x00); // reserved
        frame.push(0x00); // reserved
        frame.extend_from_slice(&frame_body);
        frame
    }

    /// 构造一个 untagged Ethernet/IPv4/TCP frame 承载给定 TCP payload。
    fn build_l2_tcp(
        src_ip: [u8; 4],
        dst_ip: [u8; 4],
        src_port: u16,
        dst_port: u16,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut frame = Vec::new();
        // Ethernet: dst(6) + src(6) + ethertype(2)
        frame.extend_from_slice(&[0xff; 6]);
        frame.extend_from_slice(&[0x11; 6]);
        frame.extend_from_slice(&0x0800u16.to_be_bytes()); // IPv4

        // IP header (20 bytes, ihl=5)
        let tcp_len = 20 + payload.len();
        let total_len = 20 + tcp_len;
        let mut ip = vec![0x45, 0x00];
        ip.extend_from_slice(&(total_len as u16).to_be_bytes());
        ip.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // id + flags/frag
        ip.push(0x40); // ttl
        ip.push(6); // proto TCP
        ip.extend_from_slice(&[0x00, 0x00]); // checksum (ignored)
        ip.extend_from_slice(&src_ip);
        ip.extend_from_slice(&dst_ip);
        assert_eq!(ip.len(), 20);
        frame.extend_from_slice(&ip);

        // TCP header (20 bytes, dataOff=5 → 0x50 in byte 12)
        frame.extend_from_slice(&src_port.to_be_bytes());
        frame.extend_from_slice(&dst_port.to_be_bytes());
        frame.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // seq
        frame.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // ack
        frame.push(0x50); // data offset 5 (<<4)
        frame.push(0x18); // flags PSH+ACK
        frame.extend_from_slice(&[0x00, 0x00]); // window
        frame.extend_from_slice(&[0x00, 0x00]); // checksum
        frame.extend_from_slice(&[0x00, 0x00]); // urgent ptr
        frame.extend_from_slice(payload);
        frame
    }

    // --- parse_frame ---

    #[test]
    fn parse_frame_extracts_req_and_body() {
        let frame = wire_frame(708, b"target=06020000");
        let (req, body) = parse_frame(&frame).unwrap();
        assert_eq!(req, 708);
        assert_eq!(body, b"target=06020000");
    }

    #[test]
    fn parse_frame_accepts_star_separator() {
        // 外机 ack 用 &query* 分隔符（双 sep 兼容）。手工构造 star sep 帧。
        let frame = {
            let mut fb = Vec::new();
            fb.extend_from_slice(b"req=711&query*");
            fb.extend_from_slice(b"ok");
            let mut f = vec![0x07, 0xb8];
            f.extend_from_slice(&(fb.len() as u16).to_le_bytes());
            f.push(0x00);
            f.push(0x00);
            f.extend_from_slice(&fb);
            f
        };
        let (req, body) = parse_frame(&frame).unwrap();
        assert_eq!(req, 711);
        assert_eq!(body, b"ok");
    }

    #[test]
    fn parse_frame_rejects_bad_magic() {
        let mut frame = wire_frame(708, b"x");
        frame[0] = 0x00;
        assert!(parse_frame(&frame).is_none());
    }

    #[test]
    fn parse_frame_rejects_non_wire() {
        // 纯 TCP 控制帧残留（空 / 随机），parse_frame 失败 → None。
        assert!(parse_frame(&[]).is_none());
        assert!(parse_frame(&[0x00; 4]).is_none());
    }

    // --- infer_direction / format_log（golden 向量）---

    #[test]
    fn infer_direction_outdoor_to_indoor() {
        let daemon = [172, 16, 106, 202];
        let indoor = [172, 16, 106, 91];
        let outdoors = [[172, 16, 106, 152]];
        let got = infer_direction(
            [172, 16, 106, 152],
            [172, 16, 106, 91],
            Some(daemon),
            Some(indoor),
            &outdoors,
        );
        assert_eq!(got, Direction::OutdoorToIndoor);
    }

    #[test]
    fn infer_direction_indoor_to_outdoor() {
        let daemon = [172, 16, 106, 202];
        let indoor = [172, 16, 106, 91];
        let outdoors = [[172, 16, 106, 152]];
        let got = infer_direction(
            [172, 16, 106, 91],
            [172, 16, 106, 152],
            Some(daemon),
            Some(indoor),
            &outdoors,
        );
        assert_eq!(got, Direction::IndoorToOutdoor);
    }

    #[test]
    fn infer_direction_daemon_to_outdoor() {
        let daemon = [172, 16, 106, 202];
        let indoor = [172, 16, 106, 91];
        let outdoors = [[172, 16, 106, 152]];
        let got = infer_direction(
            daemon,
            [172, 16, 106, 152],
            Some(daemon),
            Some(indoor),
            &outdoors,
        );
        assert_eq!(got, Direction::DaemonToOutdoor);
    }

    #[test]
    fn infer_direction_unknown() {
        let daemon = [172, 16, 106, 202];
        let indoor = [172, 16, 106, 91];
        let outdoors = [[172, 16, 106, 152]];
        // 邻居单元 .92 → broadcast .255。
        let got = infer_direction(
            [172, 16, 106, 92],
            [172, 16, 106, 255],
            Some(daemon),
            Some(indoor),
            &outdoors,
        );
        assert_eq!(got, Direction::Unknown);
    }

    #[test]
    fn infer_direction_none_ip_does_not_match() {
        // daemon/indoor 未知（None）→ 仅靠 outdoor 列表，跨设备对返 Unknown。
        let outdoors = [[172, 16, 106, 152]];
        let got = infer_direction(
            [172, 16, 106, 152],
            [172, 16, 106, 91],
            None,
            None,
            &outdoors,
        );
        // src 是 outdoor 但 dst .91 既非 indoor（None）也非 daemon（None）→ Unknown。
        assert_eq!(got, Direction::Unknown);
    }

    #[test]
    fn format_log_req704_outdoor_to_indoor() {
        let body = [0x06, 0x02, 0x11, 0x03];
        let got = format_log(
            704,
            [172, 16, 106, 152],
            [172, 16, 106, 91],
            &body,
            Direction::OutdoorToIndoor,
        );
        assert_eq!(
            got,
            "listen18022: detected req=704 outdoor→indoor 172.16.106.152 → 172.16.106.91 (invite-query)"
        );
    }

    #[test]
    fn format_log_req518_byte0_1b() {
        let body = [0x1b, 0x06, 0x02, 0x11, 0x03, 0x96];
        let got = format_log(
            518,
            [172, 16, 106, 152],
            [172, 16, 106, 91],
            &body,
            Direction::OutdoorToIndoor,
        );
        assert!(got.contains("byte0=0x1b"), "{got}");
        assert!(got.contains("state-notify"), "{got}");
    }

    #[test]
    fn format_log_req518_byte0_22() {
        let body = [0x22, 0x06, 0x02, 0x11, 0x03];
        let got = format_log(
            518,
            [172, 16, 106, 202],
            [172, 16, 106, 152],
            &body,
            Direction::DaemonToOutdoor,
        );
        assert!(got.contains("unlock-B-standard"), "{got}");
    }

    #[test]
    fn format_log_req518_byte0_ed() {
        let body = [0xed, 0x00, 0x01, 0x00, 0x00];
        let got = format_log(
            518,
            [172, 16, 106, 202],
            [172, 16, 106, 152],
            &body,
            Direction::DaemonToOutdoor,
        );
        assert!(got.contains("appoint-variant"), "{got}");
    }

    #[test]
    fn format_log_req518_byte0_unknown_variant() {
        let body = [0x77];
        let got = format_log(518, [1, 2, 3, 4], [5, 6, 7, 8], &body, Direction::Unknown);
        assert!(got.contains("byte0=0x77"), "{got}");
        assert!(got.contains("unknown-variant-0x77"), "{got}");
    }

    #[test]
    fn format_log_req710_unlock_a() {
        let body = [0x06, 0x02, 0x00, 0x00, 0x06, 0x02, 0x11, 0x03];
        let got = format_log(
            710,
            [172, 16, 106, 202],
            [172, 16, 106, 152],
            &body,
            Direction::DaemonToOutdoor,
        );
        assert_eq!(
            got,
            "listen18022: detected req=710 daemon→outdoor 172.16.106.202 → 172.16.106.152 (unlock-A)"
        );
    }

    #[test]
    fn format_log_no_note_no_parens() {
        // 无语义注释的 req（如 999）+ 非 518 → 不带括号尾。
        let got = format_log(999, [1, 2, 3, 4], [5, 6, 7, 8], &[], Direction::Unknown);
        assert_eq!(
            got,
            "listen18022: detected req=999 unknown 1.2.3.4 → 5.6.7.8"
        );
    }

    // --- extract_tcp_payload ---

    #[test]
    fn extract_tcp_payload_roundtrip() {
        let payload = wire_frame(704, b"q");
        let frame = build_l2_tcp(
            [172, 16, 106, 201],
            [172, 16, 106, 202],
            50000,
            18022,
            &payload,
        );
        let (got, src_ip, dst_ip, src_port, dst_port) = extract_tcp_payload(&frame).unwrap();
        assert_eq!(got, payload);
        assert_eq!(src_ip, [172, 16, 106, 201]);
        assert_eq!(dst_ip, [172, 16, 106, 202]);
        assert_eq!(src_port, 50000);
        assert_eq!(dst_port, 18022);
    }

    #[test]
    fn extract_tcp_payload_empty_payload_ok() {
        // 纯 SYN/ACK（无 payload）→ Some + 空 payload（双层防御）。
        let frame = build_l2_tcp([1, 2, 3, 4], [5, 6, 7, 8], 1, 18022, &[]);
        let (got, ..) = extract_tcp_payload(&frame).unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn extract_tcp_payload_rejects_non_ipv4() {
        let mut frame = build_l2_tcp([1, 2, 3, 4], [5, 6, 7, 8], 1, 18022, &[0u8; 10]);
        frame[12] = 0x86;
        frame[13] = 0xdd; // IPv6
        assert!(extract_tcp_payload(&frame).is_none());
    }

    #[test]
    fn extract_tcp_payload_rejects_non_tcp() {
        let mut frame = build_l2_tcp([1, 2, 3, 4], [5, 6, 7, 8], 1, 18022, &[0u8; 10]);
        frame[14 + 9] = 17; // UDP
        assert!(extract_tcp_payload(&frame).is_none());
    }

    #[test]
    fn extract_tcp_payload_rejects_short_frame() {
        assert!(extract_tcp_payload(&[0u8; 20]).is_none());
    }

    // --- dedup（时钟注入，与 6672 同行为） ---

    #[test]
    fn dedup_dedups_within_window() {
        let d = Dedup::new();
        let key = 0xdead_beef;
        assert!(!d.seen(key, 1000));
        assert!(d.seen(key, 1100));
        assert!(d.seen(key, 1200)); // 200ms boundary inclusive
    }

    #[test]
    fn dedup_expires_after_window() {
        let d = Dedup::new();
        let key = 0xabc;
        assert!(!d.seen(key, 1000));
        assert!(!d.seen(key, 1201)); // cutoff 1001 > 1000 → expired
    }

    #[test]
    fn dedup_key_distinguishes_inputs() {
        let p1 = wire_frame(708, b"a");
        let p2 = wire_frame(708, b"b");
        let k_a = dedup_key([1, 2, 3, 4], 5000, &p1);
        assert_ne!(k_a, dedup_key([1, 2, 3, 5], 5000, &p1)); // src ip
        assert_ne!(k_a, dedup_key([1, 2, 3, 4], 5001, &p1)); // src port
        assert_ne!(k_a, dedup_key([1, 2, 3, 4], 5000, &p2)); // payload
        assert_eq!(k_a, dedup_key([1, 2, 3, 4], 5000, &p1)); // same → same
    }

    #[test]
    fn fnv1a_known_vector() {
        assert_eq!(fnv1a_64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a_64(b"a"), 0xaf63_dc4c_8601_ec8c);
    }

    // --- Subscribe / dispatch_for_test ---

    /// fake clock：固定时刻。
    struct FakeClock(AtomicUsize);
    impl Clock for FakeClock {
        fn now_ms(&self) -> u64 {
            self.0.load(Ordering::SeqCst) as u64
        }
    }

    #[test]
    fn dispatch_for_test_drives_on_detect() {
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        let on_detect: OnDetect = Box::new(move |f| {
            assert_eq!(f.req, 708);
            h.fetch_add(1, Ordering::SeqCst);
        });
        let l = Listener::new(vec!["eth0".into()], Some(on_detect));
        l.dispatch_for_test(
            [1, 2, 3, 4],
            [5, 6, 7, 8],
            500,
            18022,
            &wire_frame(708, b"x"),
        );
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn dispatch_for_test_silent_drops_non_wire() {
        let l = Listener::new(vec!["eth0".into()], None);
        // 不 panic、无订阅命中。
        l.dispatch_for_test([1, 2, 3, 4], [5, 6, 7, 8], 1, 18022, &[0xab; 3]);
    }

    #[test]
    fn subscribe_delivers_filtered_frames() {
        let l = Listener::new(vec!["eth0".into()], None);
        // 只订阅 req=708。
        let sub = l.subscribe(Some(Box::new(|f| f.req == 708)));

        l.dispatch_for_test([1, 2, 3, 4], [5, 6, 7, 8], 1, 18022, &wire_frame(704, b"a"));
        l.dispatch_for_test([1, 2, 3, 4], [5, 6, 7, 8], 1, 18022, &wire_frame(708, b"b"));

        // 只收到 708（704 被 filter 挡掉）。
        let got = sub.ch.try_recv().unwrap();
        assert_eq!(got.req, 708);
        assert_eq!(got.body, b"b");
        assert!(sub.ch.try_recv().is_err());
    }

    #[test]
    fn subscribe_none_filter_passes_all() {
        let l = Listener::new(vec!["eth0".into()], None);
        let sub = l.subscribe(None);
        l.dispatch_for_test([1, 2, 3, 4], [5, 6, 7, 8], 1, 18022, &wire_frame(704, b"a"));
        assert_eq!(sub.ch.try_recv().unwrap().req, 704);
    }

    #[test]
    fn subscribe_buffered_8_then_silent_drop() {
        let l = Listener::new(vec!["eth0".into()], None);
        let sub = l.subscribe(None);
        // 投 10 帧；buffered 8 → 第 9/10 silent drop（不阻塞主循环）。
        for i in 0..10u32 {
            l.dispatch_for_test(
                [1, 2, 3, 4],
                [5, 6, 7, 8],
                1,
                18022,
                &wire_frame(700 + i, b"x"),
            );
        }
        let mut n = 0;
        while sub.ch.try_recv().is_ok() {
            n += 1;
        }
        assert_eq!(n, 8, "buffered 8, rest silent drop");
    }

    #[test]
    fn subscribe_filter_panic_isolated() {
        let l = Listener::new(vec!["eth0".into()], None);
        // sub1 filter panic；sub2 正常仍应收到。
        let _sub1 = l.subscribe(Some(Box::new(|_| panic!("boom"))));
        let sub2 = l.subscribe(Some(Box::new(|_| true)));
        l.dispatch_for_test([1, 2, 3, 4], [5, 6, 7, 8], 1, 18022, &wire_frame(708, b"x"));
        assert_eq!(sub2.ch.try_recv().unwrap().req, 708);
    }

    #[test]
    fn subscribe_cancel_removes_and_is_idempotent() {
        let l = Listener::new(vec!["eth0".into()], None);
        let sub = l.subscribe(None);
        sub.cancel();
        // cancel 后投递 silent drop。
        l.dispatch_for_test([1, 2, 3, 4], [5, 6, 7, 8], 1, 18022, &wire_frame(708, b"x"));
        assert!(sub.ch.try_recv().is_err());
        // 多次 cancel no-op（不 panic）。
        sub.cancel();
        sub.cancel();
    }

    #[test]
    fn dispatch_dedup_skips_duplicate() {
        let clock = Arc::new(FakeClock(AtomicUsize::new(1000)));
        // 2 slaves → dedup on。
        let l = Listener::new(vec!["eth0".into(), "eth1".into()], None).with_clock(clock.clone());
        let sub = l.subscribe(None);
        let payload = wire_frame(708, b"x");
        l.dispatch_for_test([1, 2, 3, 4], [5, 6, 7, 8], 5000, 18022, &payload);
        l.dispatch_for_test([1, 2, 3, 4], [5, 6, 7, 8], 5000, 18022, &payload);
        // window 内重复 → 只投一次。
        assert_eq!(sub.ch.try_recv().unwrap().req, 708);
        assert!(sub.ch.try_recv().is_err());
        // 越窗 → 再投。
        clock.0.store(1500, Ordering::SeqCst);
        l.dispatch_for_test([1, 2, 3, 4], [5, 6, 7, 8], 5000, 18022, &payload);
        assert_eq!(sub.ch.try_recv().unwrap().req, 708);
    }

    #[test]
    fn subscribable_trait_object_usable() {
        // 注入缝：经 &dyn Subscribable 订阅（验证 trait 对象安全）。
        let l = Listener::new(vec!["eth0".into()], None);
        let s: &dyn Subscribable = &l;
        let sub = s.subscribe(Some(Box::new(|f| f.req == 708)));
        l.dispatch_for_test([1, 2, 3, 4], [5, 6, 7, 8], 1, 18022, &wire_frame(708, b"b"));
        assert_eq!(sub.ch.try_recv().unwrap().req, 708);
    }

    // --- run（platform） ---

    #[test]
    fn run_rejects_empty_slaves_or_unsupported() {
        let l = Listener::new(vec![], None);
        // empty slaves → NoSlaves on **all** platforms（跨平台入口先判，非 Linux 不再
        // 掩盖成 Unsupported）。
        assert!(matches!(
            l.run(Arc::new(AtomicBool::new(false))),
            Err(ListenerError::NoSlaves)
        ));
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn run_non_linux_returns_unsupported() {
        let l = Listener::new(vec!["eth0".into()], None);
        let r = l.run(Arc::new(AtomicBool::new(false)));
        assert!(matches!(r, Err(ListenerError::Unsupported)));
    }
}
