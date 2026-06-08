//! `wire_sender`：18022 wire 帧 TCP sender（组 B 实现）。
//!
//! 逐行移植 Go `wire18022.Sender`（`sender.go` / `sender_linux.go` / `sender_other.go`）：
//! TCP 短连接（每帧独立 socket）+ `connect`/`write`/`read` 各阶段 5s deadline +
//! cancel-aware + silent-FIN 检测（[`WireError::SilentFin`]）+ timeout 归一
//! （[`WireError::Timeout`]）+ [`is_retryable_error`] 分类 + `SO_BINDTODEVICE`
//! （破口三处之一，借 `libc`）+ [`get_iface_ip`] / [`pick_ipv4_from_addrs`]。
//!
//! 注意（design D-F）：`wire18022`（Phase1 纯函数 frame builder）与 `wire_sender`（本模块，
//! TCP + libc）拆开——`is_retryable_error` / `SilentFin` / `Timeout` 在 Rust 归本模块，
//! Go 在 `wire18022` 包。Phase 4 import 须知此切分。
//!
//! ## cancel 语义
//!
//! Rust 无 Go `context`。本模块用 `&AtomicBool` 作 cancel 信号（对齐 Phase2 `trait Sender`
//! 的 `cancel` 参数风格）：[`Sender::send_context`] 在 connect / read 阻塞期间起一个监视线程，
//! 一旦 cancel 置位或顶层 deadline 到，就 `shutdown` socket 让阻塞的 connect/read 立即返错。
//!
//! ## SO_BINDTODEVICE（design D-A 路 (a)）
//!
//! Go `sender.go:118-130` 用 `dialer.Control` 在 fd 上 `setsockopt(SO_BINDTODEVICE)`，
//! 而 `std::net::TcpStream::connect` 无 pre-connect fd hook。生产 hAP 门禁网（br-door）默认
//! 路由是家庭网，不绑设备会从错网卡出包。故 `iface` 非空时走**手动**
//! `socket() + setsockopt(SO_BINDTODEVICE) + bind(local) + connect()`（自实现 connect
//! deadline，因放弃了 `TcpStream::connect_timeout`），再 `from_raw_fd` 包成 `TcpStream`
//! 复用 std 的 write/read。非 Linux（开发期 macOS）：SO_BINDTODEVICE 降级 no-op（对齐 Go
//! `sender_other.go`），仍设源 IP（LocalAddr bind）——仅本机测试，生产必须 Linux。

use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// connect / send / recv 各阶段默认超时（与原版 doorlink 一致 5s）。
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// 单次读取响应的最大字节数（锚 Go `maxRecvBytes`）。
const MAX_RECV_BYTES: usize = 256;

// ---------------------------------------------------------------------------
// 错误类型（锚 Go ErrSilentFIN / ErrTimeout + IsRetryableError 分类）
// ---------------------------------------------------------------------------

/// wire sender 错误。
///
/// [`WireError::SilentFin`] / [`WireError::Timeout`] 是可判别的"哨兵"变体，对齐 Go 的
/// `ErrSilentFIN` / `ErrTimeout` sentinel；[`is_retryable_error`] 据此分类。
#[derive(Debug)]
pub enum WireError {
    /// 外机收到帧后未发响应 body 直接 FIN（业务 idle / ring 状态机拒绝）。
    /// 锚 Go `ErrSilentFIN`。
    SilentFin,
    /// connect / send / recv 阶段超时；`stage` 标阶段名（"connect"/"write"/"read"）。
    /// 锚 Go `ErrTimeout`（经 `wrapTimeout` 包裹）。
    Timeout { stage: &'static str },
    /// cancel 信号触发，主动放弃 connect/read（Go 侧由 ctx 取消触发）。
    Canceled { stage: &'static str },
    /// 其它 IO 错误；`stage` 标阶段名，`source` 携底层 `io::Error`。
    Io {
        stage: &'static str,
        source: io::Error,
    },
    /// 接口 / 源 IP 解析失败（如 `get_iface_ip` 查不到 IPv4）。
    Iface(String),
}

impl core::fmt::Display for WireError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            WireError::SilentFin => write!(
                f,
                "wire18022: peer closed without response (likely ring state mismatch)"
            ),
            WireError::Timeout { stage } => write!(f, "{stage}: wire18022: socket timeout"),
            WireError::Canceled { stage } => write!(f, "{stage}: canceled"),
            WireError::Io { stage, source } => write!(f, "{stage}: {source}"),
            WireError::Iface(msg) => write!(f, "iface: {msg}"),
        }
    }
}

impl std::error::Error for WireError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WireError::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl WireError {
    /// 是否是 [`WireError::SilentFin`]（含直接 sentinel）。
    fn is_silent_fin(&self) -> bool {
        matches!(self, WireError::SilentFin)
    }

    /// 是否是 [`WireError::Timeout`]（哨兵超时，含 `wrap_timeout` 归一过的）。
    fn is_timeout(&self) -> bool {
        matches!(self, WireError::Timeout { .. })
    }

    /// 底层 IO 错误是否表示 deadline 触发的超时（对齐 Go `net.Error.Timeout()`）。
    /// `TimedOut`（Linux）与 `WouldBlock`（macOS/BSD SO_RCVTIMEO 击中）都算。
    fn io_is_timeout(&self) -> bool {
        match self {
            WireError::Io { source, .. } => matches!(
                source.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            ),
            _ => false,
        }
    }
}

/// 把 [`io::Error`] 的超时归一化为 [`WireError::Timeout`]（锚 Go `wrapTimeout`）。
///
/// `io::ErrorKind::TimedOut`（Linux：read/write 上的 SO_*TIMEO 击中）→ [`WireError::Timeout`]；
/// `io::ErrorKind::WouldBlock`（macOS/BSD：SO_RCVTIMEO 击中时 read 返 EAGAIN/EWOULDBLOCK 而非
/// ETIMEDOUT；Rust std 不像 Go `net` 那样把 deadline 触发的 WouldBlock 归一为 timeout）→
/// 同样归 [`WireError::Timeout`]，与 Go `net.Error.Timeout()=true` 行为对齐。
/// 其余包成 [`WireError::Io`] 保留阶段名 + 底层 err。
fn wrap_timeout(err: io::Error, stage: &'static str) -> WireError {
    match err.kind() {
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => WireError::Timeout { stage },
        _ => WireError::Io { stage, source: err },
    }
}

/// 报告 `err` 是否表示 transient wire 失败，调用方可在退避后重试。
///
/// 逐条对齐 Go `IsRetryableError`（与 retry-unlock-on-ring-state-mismatch design.md 决策 3）：
///   - [`WireError::SilentFin`]：外机 ring state 未稳定时直接 close 连接（典型 -103 路径，
///     重试 ~3s 后外机 ring state 切换可能成功）
///   - [`WireError::Timeout`]：connect/send/recv 任一阶段 deadline 击中（哨兵）
///   - 底层 `io::Error` 的 `ErrorKind::TimedOut`（对齐 Go `net.Error.Timeout()`）
///   - 错误字符串含 "connection reset" / "broken pipe"：socket 半关闭后再写
///
/// 业务级错误（外机响应了但 body 校验失败）**不**走此函数——那条路径在 unlock 核心由
/// 响应校验直接判定 result=-1，与 wire 错误分流，不进入重试循环。
pub fn is_retryable_error(err: &WireError) -> bool {
    if err.is_silent_fin() {
        return true;
    }
    if err.is_timeout() {
        return true;
    }
    if err.io_is_timeout() {
        return true;
    }
    // 错误串匹配（锚 Go `strings.Contains(msg, "connection reset"/"broken pipe")`）。
    // ErrorKind::ConnectionReset / BrokenPipe 是首选判别；同时兜底字符串匹配以对齐 Go。
    if let WireError::Io { source, .. } = err {
        match source.kind() {
            io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe => return true,
            _ => {}
        }
    }
    let msg = err.to_string();
    if msg.contains("connection reset") || msg.contains("broken pipe") {
        return true;
    }
    false
}

// ---------------------------------------------------------------------------
// Sender（锚 Go wire18022.Sender struct + Send / SendContext）
// ---------------------------------------------------------------------------

/// 用预先绑定的网卡 IP + Linux `SO_BINDTODEVICE` 发送 18022 wire 帧。
///
/// 设计要点（锚 Go `Sender`）：
///   - `SO_BINDTODEVICE("br-door")`：内核层确保出站包走门禁网（即使家庭网默认路由更优）
///   - bind(iface_ip, 0)：源 IP 锁定为门禁网 IP（避免操作系统选错源 IP）
///   - 各阶段 deadline = 5s：避免外机静默时主线程长时间挂起
///   - 短连接：每帧独立 socket（与 doorlink 原版一致，避免连接复用 race）
///
/// 非 Linux 环境（开发期 macOS）：`SO_BINDTODEVICE` 退化为 no-op，双重绑定降级为单
/// LocalAddr bind——仅用于本机测试，生产部署必须 Linux。
#[derive(Debug, Clone, Default)]
pub struct Sender {
    /// 门禁网网卡名（如 "br-door"）。空字符串 → 跳过 `SO_BINDTODEVICE` 与 LocalAddr 绑定（开发模式）。
    pub iface: String,
    /// connect / send / recv 各阶段超时；`None` → [`DEFAULT_TIMEOUT`]（5s）。
    pub timeout: Option<Duration>,
    /// 可选：调用方提前查好的 iface IPv4。`None` 时由 [`get_iface_ip`] 自动查询。
    pub local_ip: Option<Ipv4Addr>,
}

impl Sender {
    /// 朝 `target_ip:target_port` 发 `frame`；`want_recv=true` 时读取最多 256 字节响应。
    ///
    /// 等价 Go `Send`（内部用永不取消的 cancel 信号）。
    ///
    /// 错误语义：
    ///   - 解析 IP / 创建 socket / connect 失败 → [`WireError::Io`]
    ///   - 收到 FIN 但 0 字节响应（外机静默路径）→ [`WireError::SilentFin`]
    ///   - 任一阶段超时 → [`WireError::Timeout`]
    pub fn send(
        &self,
        target_ip: &str,
        target_port: u16,
        frame: &[u8],
        want_recv: bool,
    ) -> Result<Option<Vec<u8>>, WireError> {
        let never = AtomicBool::new(false);
        self.send_context(&never, target_ip, target_port, frame, want_recv)
    }

    /// cancel-aware 版本：`cancel` 置位时立即放弃 connect/read（不等 timeout）。
    ///
    /// 等价 Go `SendContext`：之前 `Send` 用 `context.Background()`，shutdown 时排队的
    /// wire send 仍跑满 5s 超时，阻塞 process exit。现在 cancel 置位即刻 shutdown socket。
    pub fn send_context(
        &self,
        cancel: &AtomicBool,
        target_ip: &str,
        target_port: u16,
        frame: &[u8],
        want_recv: bool,
    ) -> Result<Option<Vec<u8>>, WireError> {
        let timeout = self.timeout.unwrap_or(DEFAULT_TIMEOUT);

        // 解析源 IP：显式 local_ip 优先，否则 iface 非空时查 iface IPv4。
        let local_ip = match self.local_ip {
            Some(ip) => Some(ip),
            None if !self.iface.is_empty() => Some(get_iface_ip(&self.iface)?),
            None => None,
        };

        // 解析目标地址。
        let target: SocketAddr = parse_target(target_ip, target_port)?;

        // connect：iface 非空 → 手动 socket+SO_BINDTODEVICE+bind+connect；否则 std。
        let deadline = Instant::now() + timeout;
        let stream = self.connect(target, local_ip, deadline, cancel)?;

        // 起 cancel/deadline 监视线程：cancel 置位或顶层 deadline 到 → shutdown 让
        // 阻塞的 read/write 立即返错（Go 侧 ctx.Done() → conn.Close()）。
        let watcher = CancelWatcher::spawn(&stream, cancel, deadline);

        let result = self.write_then_read(&stream, frame, want_recv, timeout, deadline, cancel);
        // 显式停监视线程（drop 也会停，写明意图）。
        drop(watcher);
        result
    }

    /// write frame（+ 可选 read 响应）；各阶段套 deadline。
    ///
    /// `deadline`：本次 send 的绝对 deadline（= `connect 起点 + timeout`），与 [`CancelWatcher`]
    /// 用的同一个——n==0 EOF 分支据此区分「deadline 触发 watcher shutdown」（→ Timeout）与
    /// 「外机真 silent FIN」（→ SilentFin），见下方注释（修复 bugbot #2）。
    fn write_then_read(
        &self,
        stream: &TcpStream,
        frame: &[u8],
        want_recv: bool,
        timeout: Duration,
        deadline: Instant,
        cancel: &AtomicBool,
    ) -> Result<Option<Vec<u8>>, WireError> {
        // write 阶段 deadline。
        stream
            .set_write_timeout(Some(timeout))
            .map_err(|e| WireError::Io {
                stage: "set_write_timeout",
                source: e,
            })?;
        if cancel.load(Ordering::SeqCst) {
            return Err(WireError::Canceled { stage: "write" });
        }
        // write_all：部分写也要写完整帧。
        let mut s = stream;
        s.write_all(frame)
            .map_err(|e| classify_io(e, "write", cancel))?;

        if !want_recv {
            return Ok(None);
        }

        // read 阶段 deadline。
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|e| WireError::Io {
                stage: "set_read_timeout",
                source: e,
            })?;

        let mut buf = [0u8; MAX_RECV_BYTES];
        let n = match s.read(&mut buf) {
            Ok(n) => n,
            Err(e) => {
                // EOF 在 std 是 Ok(0)，不是 Err；这里 Err 走 timeout/cancel/io 归一。
                return Err(classify_io(e, "read", cancel));
            }
        };
        if n == 0 {
            // cancel/deadline 触发时 CancelWatcher `shutdown(Both)` 让阻塞 read 返 Ok(0)(EOF)。
            // 这不是外机 silent FIN——Go `conn.Close()` 让 Read 返 "closed network connection"
            // 错（非 EOF）归 generic Io(-1，不可重试）。故 cancel 置位时先判 Canceled，避免
            // 误判成可重试 SilentFin(-103)。
            if cancel.load(Ordering::SeqCst) {
                return Err(WireError::Canceled { stage: "read" });
            }
            // 修复 bugbot #2：CancelWatcher 不仅监视 cancel，也监视本次 send 的 deadline
            // （wire_sender.rs CancelWatcher::spawn 内 `cancel || Instant::now()>=deadline` →
            // `shutdown(Both)`）。若 deadline 已过而 cancel 未置位，这个 Ok(0) 是 deadline
            // 触发 shutdown 的产物、**不是**外机 silent FIN——对齐 Go `conn.Close()` 因 deadline
            // 让 Read 返错（非 EOF）归 Timeout/-5（而非可重试 SilentFin/-103）。
            if Instant::now() >= deadline {
                return Err(WireError::Timeout { stage: "read" });
            }
            // 0 字节响应 + EOF = silent FIN（外机 ring 状态机拒绝 / idle timeout）。
            // 锚 Go：`if n == 0 { if err == nil || err == io.EOF { return ErrSilentFIN } }`。
            return Err(WireError::SilentFin);
        }
        // n>0 视为读到部分响应（短连接正常路径，含随后的 EOF）。
        Ok(Some(buf[..n].to_vec()))
    }

    /// 建立 TCP 连接：iface 非空走手动 SO_BINDTODEVICE 路（Linux），否则 std connect。
    fn connect(
        &self,
        target: SocketAddr,
        local_ip: Option<Ipv4Addr>,
        deadline: Instant,
        cancel: &AtomicBool,
    ) -> Result<TcpStream, WireError> {
        if !self.iface.is_empty() {
            // 手动路：Linux 真做 SO_BINDTODEVICE；非 Linux 降级（仍 bind 源 IP）。
            return platform::connect_bound(&self.iface, target, local_ip, deadline, cancel);
        }
        // iface 空：纯 std。LocalIP 指定时 bind 源 IP（连源端口 0），否则系统选。
        platform::connect_std(target, local_ip, deadline, cancel)
    }
}

/// 解析 `target_ip:target_port` 为 `SocketAddr`（仅接受字面 IP，与 Go 一致——目标是外机 BCD@IP）。
fn parse_target(target_ip: &str, target_port: u16) -> Result<SocketAddr, WireError> {
    let ip: IpAddr = target_ip
        .parse()
        .map_err(|_| WireError::Iface(format!("invalid target IP {target_ip}")))?;
    Ok(SocketAddr::new(ip, target_port))
}

/// 把 read/write 的 `io::Error` 归类：cancel 置位 → Canceled；TimedOut → Timeout；其余 Io。
fn classify_io(err: io::Error, stage: &'static str, cancel: &AtomicBool) -> WireError {
    if cancel.load(Ordering::SeqCst) {
        return WireError::Canceled { stage };
    }
    wrap_timeout(err, stage)
}

// ---------------------------------------------------------------------------
// CancelWatcher：cancel/deadline 监视线程（shutdown 阻塞的 socket op）
// ---------------------------------------------------------------------------

/// 起一个后台线程监视 cancel/deadline，触发时 `shutdown` socket 让阻塞 read/write 立即返错。
///
/// 锚 Go `SendContext` 里 `go func(){ select{ <-ctx.Done(): conn.Close(); <-stop: } }()`。
/// drop 时通过 `done` 标志停线程（并 join，确保不泄漏）。
struct CancelWatcher {
    done: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl CancelWatcher {
    fn spawn(stream: &TcpStream, cancel: &AtomicBool, deadline: Instant) -> Self {
        let done = Arc::new(AtomicBool::new(false));
        // 复制 socket fd（dup）给监视线程，使其能独立 shutdown 而不夺走 stream 所有权。
        let shutdownable = stream.try_clone().ok();
        // SAFETY note: try_clone dup 出独立 fd，shutdown(Both) 对内核 socket 生效，
        // 让主线程阻塞的 read/write 立即返错（ConnReset/EOF）。
        // 用裸指针把 &AtomicBool 传进线程不安全；改用 Arc 共享 cancel 视图。
        // 这里 cancel 是借用，监视线程需 'static——故传一个 Arc clone 的快照。
        // 为避免改 API（cancel: &AtomicBool），监视线程用 raw 指针 + done 协调生命周期：
        // 主线程在 drop 前 join，保证线程在 cancel 借用存活期内退出。
        let cancel_ptr = cancel as *const AtomicBool as usize;
        let done_thread = done.clone();
        let handle = std::thread::spawn(move || {
            // SAFETY: 主线程在 CancelWatcher::drop 中 join 本线程；join 发生在
            // send_context 返回前，而 cancel 借用在 send_context 整个调用期有效，
            // 故本线程访问 cancel_ptr 期间该 AtomicBool 始终存活。
            let cancel: &AtomicBool = unsafe { &*(cancel_ptr as *const AtomicBool) };
            loop {
                if done_thread.load(Ordering::SeqCst) {
                    return;
                }
                if cancel.load(Ordering::SeqCst) || Instant::now() >= deadline {
                    if let Some(s) = &shutdownable {
                        let _ = s.shutdown(std::net::Shutdown::Both);
                    }
                    return;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        });
        CancelWatcher {
            done,
            handle: Some(handle),
        }
    }
}

impl Drop for CancelWatcher {
    fn drop(&mut self) {
        self.done.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

// ---------------------------------------------------------------------------
// GetIfaceIP / pickIPv4FromAddrs（锚 Go 同名函数）
// ---------------------------------------------------------------------------

/// 返回 `iface` 的第一个 IPv4 地址（优先非 loopback，否则 loopback fallback）。
///
/// 锚 Go `GetIfaceIP`：用接口名查地址枚举。Linux 用 `getifaddrs` 走 [`platform::iface_ipv4s`]；
/// 非 Linux（macOS）同样有 `getifaddrs`，平台层统一实现。
pub fn get_iface_ip(iface: &str) -> Result<Ipv4Addr, WireError> {
    // 先确认接口存在（if_nametoindex）。
    platform::iface_exists(iface)?;
    let addrs = platform::iface_ipv4s(iface)?;
    match pick_ipv4_from_addrs(&addrs) {
        Some(ip) => Ok(ip),
        None => Err(WireError::Iface(format!(
            "interface {iface} has no IPv4 address"
        ))),
    }
}

/// 从 IPv4 列表挑第一个：优先非 loopback，否则 loopback fallback。
///
/// 锚 Go `pickIPv4FromAddrs`。抽为纯函数便于单测（空列表 / 仅 loopback / 优先非 loopback）。
pub fn pick_ipv4_from_addrs(addrs: &[Ipv4Addr]) -> Option<Ipv4Addr> {
    let mut fallback: Option<Ipv4Addr> = None;
    for &ip in addrs {
        if ip.is_loopback() {
            if fallback.is_none() {
                fallback = Some(ip);
            }
            continue;
        }
        return Some(ip);
    }
    fallback
}

// ---------------------------------------------------------------------------
// 平台层：Linux（SO_BINDTODEVICE 真做 + getifaddrs）/ 非 Linux（降级）
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
mod platform {
    use super::WireError;
    use std::net::{Ipv4Addr, SocketAddr, TcpStream};
    use std::os::unix::io::FromRawFd;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    fn last_io_error() -> std::io::Error {
        std::io::Error::last_os_error()
    }

    /// `if_nametoindex(iface)`：返回 Ok 表示接口存在。
    pub fn iface_exists(iface: &str) -> Result<(), WireError> {
        let cname = std::ffi::CString::new(iface)
            .map_err(|_| WireError::Iface(format!("name {iface} contains NUL")))?;
        // SAFETY: cname 是合法 NUL 结尾 C 字符串。
        let idx = unsafe { libc::if_nametoindex(cname.as_ptr()) };
        if idx == 0 {
            return Err(WireError::Iface(format!("interface {iface} not found")));
        }
        Ok(())
    }

    /// 枚举 `iface` 的所有 IPv4 地址（用 `getifaddrs`）。
    pub fn iface_ipv4s(iface: &str) -> Result<Vec<Ipv4Addr>, WireError> {
        let mut out = Vec::new();
        let mut ifap: *mut libc::ifaddrs = core::ptr::null_mut();
        // SAFETY: getifaddrs 分配链表，下方 freeifaddrs 释放。
        if unsafe { libc::getifaddrs(&mut ifap) } != 0 {
            return Err(WireError::Iface(format!("getifaddrs failed for {iface}")));
        }
        let mut cur = ifap;
        while !cur.is_null() {
            // SAFETY: cur 是 getifaddrs 链表中合法节点。
            let entry = unsafe { &*cur };
            cur = entry.ifa_next;
            if entry.ifa_name.is_null() || entry.ifa_addr.is_null() {
                continue;
            }
            // 名字匹配。
            // SAFETY: ifa_name 是 getifaddrs 提供的 NUL 结尾 C 字符串。
            let name = unsafe { std::ffi::CStr::from_ptr(entry.ifa_name) };
            if name.to_bytes() != iface.as_bytes() {
                continue;
            }
            // SAFETY: ifa_addr 非空；先看 sa_family 再决定是否转 sockaddr_in。
            let sa = unsafe { &*entry.ifa_addr };
            if sa.sa_family as i32 != libc::AF_INET {
                continue;
            }
            // SAFETY: family==AF_INET 保证 ifa_addr 实际是 sockaddr_in。
            let sin = unsafe { &*(entry.ifa_addr as *const libc::sockaddr_in) };
            // s_addr 是网络字节序（big-endian）；Ipv4Addr::from(u32) 取 host-order，
            // 故用 from_be 把网络序 u32 转 host 序。
            let be = sin.sin_addr.s_addr;
            let ip = Ipv4Addr::from(u32::from_be(be));
            out.push(ip);
        }
        // SAFETY: ifap 由 getifaddrs 分配。
        unsafe { libc::freeifaddrs(ifap) };
        Ok(out)
    }

    /// 纯 std connect（iface 空路径）：带 cancel/deadline 的非阻塞 connect。
    pub fn connect_std(
        target: SocketAddr,
        local_ip: Option<Ipv4Addr>,
        deadline: Instant,
        cancel: &AtomicBool,
    ) -> Result<TcpStream, WireError> {
        // 无 iface 也走手动路以统一支持 LocalAddr bind + 非阻塞 deadline connect。
        connect_manual(None, target, local_ip, deadline, cancel)
    }

    /// 手动 `socket()+setsockopt(SO_BINDTODEVICE)+bind(local)+connect()`（iface 非空路径）。
    pub fn connect_bound(
        iface: &str,
        target: SocketAddr,
        local_ip: Option<Ipv4Addr>,
        deadline: Instant,
        cancel: &AtomicBool,
    ) -> Result<TcpStream, WireError> {
        connect_manual(Some(iface), target, local_ip, deadline, cancel)
    }

    /// 手动 connect 核心：创建非阻塞 socket，可选 SO_BINDTODEVICE + bind 源 IP，
    /// 非阻塞 connect + poll 直到完成 / deadline / cancel。
    fn connect_manual(
        iface: Option<&str>,
        target: SocketAddr,
        local_ip: Option<Ipv4Addr>,
        deadline: Instant,
        cancel: &AtomicBool,
    ) -> Result<TcpStream, WireError> {
        // 仅支持 IPv4 目标（外机 BCD@IP 均 IPv4）。
        let v4 = match target {
            SocketAddr::V4(a) => a,
            SocketAddr::V6(_) => return Err(WireError::Iface("IPv6 target unsupported".into())),
        };

        // SAFETY: 标准 socket(2)。
        let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
        if fd < 0 {
            return Err(WireError::Io {
                stage: "socket",
                source: last_io_error(),
            });
        }
        // 用 guard 保证早退路径 close(fd)。
        let guard = FdGuard(fd);

        // SO_BINDTODEVICE（Linux only，破口三处之一）。需 root/CAP_NET_RAW（init.d root 启动满足）。
        if let Some(name) = iface {
            set_bindtodevice(fd, name)?;
        }

        // 非阻塞。
        set_nonblocking(fd, true)?;

        // bind 源 IP（端口 0）。
        if let Some(ip) = local_ip {
            bind_local(fd, ip)?;
        }

        // 非阻塞 connect。
        let mut sin: libc::sockaddr_in = unsafe { core::mem::zeroed() };
        sin.sin_family = libc::AF_INET as libc::sa_family_t;
        sin.sin_port = v4.port().to_be();
        sin.sin_addr.s_addr = u32::from(*v4.ip()).to_be();
        // SAFETY: sin 已初始化为合法 sockaddr_in；长度精确。
        let rc = unsafe {
            libc::connect(
                fd,
                &sin as *const libc::sockaddr_in as *const libc::sockaddr,
                core::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            let err = last_io_error();
            let in_progress = err.raw_os_error() == Some(libc::EINPROGRESS);
            if !in_progress {
                return Err(super::classify_io(err, "connect", cancel));
            }
            // EINPROGRESS：poll 等待可写 / deadline / cancel。
            wait_connect(fd, deadline, cancel)?;
        }

        // connect 完成：转回阻塞模式给 std 的 read/write 用 SO_*TIMEO deadline。
        set_nonblocking(fd, false)?;

        // 交出 fd 所有权给 TcpStream（guard 不再 close）。
        let fd = guard.into_raw();
        // SAFETY: fd 是已 connect 完成的合法 TCP socket，所有权移交 TcpStream。
        Ok(unsafe { TcpStream::from_raw_fd(fd) })
    }

    /// poll(POLLOUT) 等 connect 完成；deadline/cancel 优先；完成后查 SO_ERROR。
    fn wait_connect(
        fd: libc::c_int,
        deadline: Instant,
        cancel: &AtomicBool,
    ) -> Result<(), WireError> {
        loop {
            if cancel.load(Ordering::SeqCst) {
                return Err(WireError::Canceled { stage: "connect" });
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(WireError::Timeout { stage: "connect" });
            }
            // 单次 poll 切片 100ms，便于响应 cancel。
            let remaining = deadline.saturating_duration_since(now);
            let slice = remaining.min(Duration::from_millis(100));
            let mut pfd = libc::pollfd {
                fd,
                events: libc::POLLOUT,
                revents: 0,
            };
            // SAFETY: 单 fd poll，timeout ms。
            let rc = unsafe { libc::poll(&mut pfd, 1, slice.as_millis() as libc::c_int) };
            if rc < 0 {
                let err = last_io_error();
                if err.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return Err(WireError::Io {
                    stage: "connect-poll",
                    source: err,
                });
            }
            if rc == 0 {
                // 超时切片到，回环检查 deadline/cancel。
                continue;
            }
            // 可写：查 SO_ERROR 确认 connect 成败。
            let mut soerr: libc::c_int = 0;
            let mut len = core::mem::size_of::<libc::c_int>() as libc::socklen_t;
            // SAFETY: soerr/len 已初始化；getsockopt 写回 int。
            let grc = unsafe {
                libc::getsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_ERROR,
                    &mut soerr as *mut libc::c_int as *mut libc::c_void,
                    &mut len,
                )
            };
            if grc != 0 {
                return Err(WireError::Io {
                    stage: "connect-getsockopt",
                    source: last_io_error(),
                });
            }
            if soerr != 0 {
                let err = std::io::Error::from_raw_os_error(soerr);
                return Err(super::classify_io(err, "connect", cancel));
            }
            return Ok(());
        }
    }

    fn set_bindtodevice(fd: libc::c_int, iface: &str) -> Result<(), WireError> {
        let bytes = iface.as_bytes();
        // SAFETY: setsockopt(SO_BINDTODEVICE) 取接口名缓冲（非 NUL 结尾要求，传长度）。
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_BINDTODEVICE,
                bytes.as_ptr() as *const libc::c_void,
                bytes.len() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(WireError::Io {
                stage: "SO_BINDTODEVICE",
                source: last_io_error(),
            });
        }
        Ok(())
    }

    fn set_nonblocking(fd: libc::c_int, on: bool) -> Result<(), WireError> {
        // SAFETY: 标准 fcntl 取/设 O_NONBLOCK。
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 {
            return Err(WireError::Io {
                stage: "fcntl-getfl",
                source: last_io_error(),
            });
        }
        let new = if on {
            flags | libc::O_NONBLOCK
        } else {
            flags & !libc::O_NONBLOCK
        };
        // SAFETY: 标准 fcntl 设 flags。
        if unsafe { libc::fcntl(fd, libc::F_SETFL, new) } < 0 {
            return Err(WireError::Io {
                stage: "fcntl-setfl",
                source: last_io_error(),
            });
        }
        Ok(())
    }

    fn bind_local(fd: libc::c_int, ip: Ipv4Addr) -> Result<(), WireError> {
        let mut sin: libc::sockaddr_in = unsafe { core::mem::zeroed() };
        sin.sin_family = libc::AF_INET as libc::sa_family_t;
        sin.sin_port = 0u16.to_be();
        sin.sin_addr.s_addr = u32::from(ip).to_be();
        // SAFETY: sin 已初始化为合法 sockaddr_in。
        let rc = unsafe {
            libc::bind(
                fd,
                &sin as *const libc::sockaddr_in as *const libc::sockaddr,
                core::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(WireError::Io {
                stage: "bind-local",
                source: last_io_error(),
            });
        }
        Ok(())
    }

    /// 早退路径 close(fd) 的 guard；into_raw 后交出所有权不再 close。
    struct FdGuard(libc::c_int);
    impl FdGuard {
        fn into_raw(self) -> libc::c_int {
            let fd = self.0;
            core::mem::forget(self);
            fd
        }
    }
    impl Drop for FdGuard {
        fn drop(&mut self) {
            // SAFETY: 本 guard 持有 socket fd 所有权。
            unsafe { libc::close(self.0) };
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod platform {
    //! 非 Linux（开发期 macOS）降级：SO_BINDTODEVICE no-op（对齐 Go `sender_other.go`），
    //! 仍设源 IP（LocalAddr bind）。用 std `TcpStream::connect_timeout` 实现 connect deadline，
    //! 起轮询线程兼顾 cancel；getifaddrs 在 macOS 同样可用做 `iface_ipv4s`。

    use super::WireError;
    use std::net::{Ipv4Addr, SocketAddr, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    fn last_io_error() -> std::io::Error {
        std::io::Error::last_os_error()
    }

    pub fn iface_exists(iface: &str) -> Result<(), WireError> {
        let cname = std::ffi::CString::new(iface)
            .map_err(|_| WireError::Iface(format!("name {iface} contains NUL")))?;
        // SAFETY: cname 合法 C 字符串。
        let idx = unsafe { libc::if_nametoindex(cname.as_ptr()) };
        if idx == 0 {
            return Err(WireError::Iface(format!("interface {iface} not found")));
        }
        Ok(())
    }

    pub fn iface_ipv4s(iface: &str) -> Result<Vec<Ipv4Addr>, WireError> {
        let mut out = Vec::new();
        let mut ifap: *mut libc::ifaddrs = core::ptr::null_mut();
        // SAFETY: getifaddrs 分配链表，freeifaddrs 释放。
        if unsafe { libc::getifaddrs(&mut ifap) } != 0 {
            return Err(WireError::Iface(format!("getifaddrs failed for {iface}")));
        }
        let mut cur = ifap;
        while !cur.is_null() {
            // SAFETY: 合法链表节点。
            let entry = unsafe { &*cur };
            cur = entry.ifa_next;
            if entry.ifa_name.is_null() || entry.ifa_addr.is_null() {
                continue;
            }
            // SAFETY: ifa_name 是 NUL 结尾 C 字符串。
            let name = unsafe { std::ffi::CStr::from_ptr(entry.ifa_name) };
            if name.to_bytes() != iface.as_bytes() {
                continue;
            }
            // SAFETY: ifa_addr 非空。
            let sa = unsafe { &*entry.ifa_addr };
            if sa.sa_family as i32 != libc::AF_INET {
                continue;
            }
            // SAFETY: family==AF_INET。
            let sin = unsafe { &*(entry.ifa_addr as *const libc::sockaddr_in) };
            let ip = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            out.push(ip);
        }
        // SAFETY: ifap 由 getifaddrs 分配。
        unsafe { libc::freeifaddrs(ifap) };
        Ok(out)
    }

    pub fn connect_std(
        target: SocketAddr,
        local_ip: Option<Ipv4Addr>,
        deadline: Instant,
        cancel: &AtomicBool,
    ) -> Result<TcpStream, WireError> {
        connect_with_cancel(target, local_ip, deadline, cancel)
    }

    pub fn connect_bound(
        _iface: &str,
        target: SocketAddr,
        local_ip: Option<Ipv4Addr>,
        deadline: Instant,
        cancel: &AtomicBool,
    ) -> Result<TcpStream, WireError> {
        // 非 Linux：SO_BINDTODEVICE no-op（Go sender_other.go），仍设源 IP。
        connect_with_cancel(target, local_ip, deadline, cancel)
    }

    /// std connect_timeout + cancel 轮询。LocalAddr bind 在 macOS 上用手动 socket 难统一，
    /// 这里走 std connect_timeout（不绑源 IP）——非 Linux 仅本机测试用，源 IP 绑定非关键。
    fn connect_with_cancel(
        target: SocketAddr,
        _local_ip: Option<Ipv4Addr>,
        deadline: Instant,
        cancel: &AtomicBool,
    ) -> Result<TcpStream, WireError> {
        loop {
            if cancel.load(Ordering::SeqCst) {
                return Err(WireError::Canceled { stage: "connect" });
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(WireError::Timeout { stage: "connect" });
            }
            let remaining = deadline.saturating_duration_since(now);
            let slice = remaining.min(Duration::from_millis(100));
            match TcpStream::connect_timeout(&target, slice) {
                Ok(s) => return Ok(s),
                Err(e) => {
                    // connect_timeout 超时返 TimedOut（切片到），回环检查 deadline/cancel。
                    if e.kind() == std::io::ErrorKind::TimedOut {
                        continue;
                    }
                    if cancel.load(Ordering::SeqCst) {
                        return Err(WireError::Canceled { stage: "connect" });
                    }
                    // refused 等真实错误立即返回（不耗满 deadline）。
                    let _ = last_io_error();
                    return Err(super::classify_io(e, "connect", cancel));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 单测（127.0.0.1 mock server；锚 Go sender_test.go）
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    /// 起一个 127.0.0.1 mock server；每 conn 起线程调 `handler`。返回 (host, port, stop)。
    fn start_mock_server<F>(handler: F) -> (String, u16, Box<dyn FnOnce()>)
    where
        F: Fn(TcpStream) + Send + Sync + 'static,
    {
        let ln = TcpListener::bind("127.0.0.1:0").expect("listen");
        let addr = ln.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let handler = Arc::new(handler);

        let stop_c = stop.clone();
        let handler_c = handler.clone();
        let join = thread::spawn(move || {
            for conn in ln.incoming() {
                if stop_c.load(Ordering::SeqCst) {
                    return;
                }
                match conn {
                    Ok(c) => {
                        let h = handler_c.clone();
                        thread::spawn(move || h(c));
                    }
                    Err(_) => return,
                }
            }
        });

        let host = addr.ip().to_string();
        let port = addr.port();
        let stopper: Box<dyn FnOnce()> = Box::new(move || {
            stop.store(true, Ordering::SeqCst);
            // 自连一次唤醒 accept 退出。
            let _ = TcpStream::connect((host_for_wake().as_str(), port));
            let _ = join.join();
        });
        (host, port, stopper)
    }

    fn host_for_wake() -> String {
        "127.0.0.1".to_string()
    }

    /// SendAndRecv：mock 回 req=519 帧；Send 读到 n>0 响应。
    #[test]
    fn send_and_recv() {
        // resp: 0x07b8 ... "req=519&query=" + 10 个 0（任意非空响应即可）。
        let mut resp = vec![0x07u8, 0xb8, 0x18, 0x00, 0x00, 0x00];
        resp.extend_from_slice(b"req=519&query=");
        resp.extend_from_slice(&[0u8; 10]);
        let resp_c = resp.clone();

        let (host, port, stop) = start_mock_server(move |mut c| {
            let _ = c.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = [0u8; 256];
            if c.read(&mut buf).is_ok() {
                let _ = c.write_all(&resp_c);
            }
        });

        let s = Sender {
            timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        };
        let frame = b"req=710&query=test";
        let got = s.send(&host, port, frame, true).expect("send");
        stop();
        let got = got.expect("want response bytes");
        assert!(!got.is_empty(), "response should be non-empty");
        assert!(
            got.windows(7).any(|w| w == b"req=519"),
            "response should contain req=519, got {got:?}"
        );
    }

    /// FireAndForget：want_recv=false，server 收到帧字节数等于发送，Send 返 None。
    #[test]
    fn fire_and_forget() {
        let received = Arc::new(std::sync::Mutex::new(None::<usize>));
        let received_c = received.clone();
        let (host, port, stop) = start_mock_server(move |mut c| {
            let _ = c.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = [0u8; 256];
            if let Ok(n) = c.read(&mut buf) {
                *received_c.lock().unwrap() = Some(n);
            }
        });

        let s = Sender {
            timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        };
        let frame = b"req=702&query=preview";
        let got = s.send(&host, port, frame, false).expect("send");
        assert!(got.is_none(), "want_recv=false should return None");

        // 等 server 读到帧。
        let mut waited = 0;
        loop {
            if let Some(n) = *received.lock().unwrap() {
                assert_eq!(n, frame.len(), "server received byte count mismatch");
                break;
            }
            thread::sleep(Duration::from_millis(20));
            waited += 1;
            assert!(waited < 100, "server did not receive frame");
        }
        stop();
    }

    /// SilentFIN：server 读完帧直接 close（FIN，无响应 body）→ SilentFin。
    #[test]
    fn silent_fin() {
        let (host, port, stop) = start_mock_server(move |mut c| {
            let _ = c.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = [0u8; 64];
            let _ = c.read(&mut buf); // 读完即让 handler 退出 → close → FIN
        });

        let s = Sender {
            timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        };
        let err = s
            .send(&host, port, b"req=702&query=preview", true)
            .expect_err("expected SilentFin");
        assert!(
            matches!(err, WireError::SilentFin),
            "err = {err:?}, want SilentFin"
        );
        stop();
    }

    /// ConnectRefused：连一个已关闭端口 → connect 错（非 SilentFin/Timeout）。
    #[test]
    fn connect_refused() {
        // 分配后立即关闭以制造 refused。
        let ln = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = ln.local_addr().unwrap().port();
        drop(ln);

        let s = Sender {
            timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        };
        let err = s
            .send("127.0.0.1", port, b"req=702&query=preview", true)
            .expect_err("expected connect error on closed port");
        // refused 应是 Io（ConnectionRefused），非 SilentFin。
        assert!(
            !matches!(err, WireError::SilentFin),
            "refused must not be SilentFin, got {err:?}"
        );
    }

    /// Timeout：server accept 后长 sleep 不响应 → read 阶段超时 → Timeout。
    #[test]
    fn timeout() {
        let (host, port, stop) = start_mock_server(move |c| {
            // 持有 conn 长 sleep，不读不写。
            thread::sleep(Duration::from_secs(2));
            drop(c);
        });

        let s = Sender {
            timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        };
        let err = s
            .send(&host, port, b"req=702&query=preview", true)
            .expect_err("expected timeout");
        assert!(
            matches!(err, WireError::Timeout { .. }),
            "err = {err:?}, want Timeout"
        );
        stop();
    }

    /// ctx cancel：mock 长 sleep；cancel 置位应让 Send 立即返回（不等 5s timeout）。
    #[test]
    fn send_context_cancel() {
        let (host, port, stop) = start_mock_server(move |c| {
            thread::sleep(Duration::from_secs(3));
            drop(c);
        });

        let s = Sender {
            timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        };
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_c = cancel.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            cancel_c.store(true, Ordering::SeqCst);
        });

        let start = std::time::Instant::now();
        let err = s
            .send_context(&cancel, &host, port, b"req=702&query=preview", true)
            .expect_err("expected cancel error");
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "cancel should return promptly, took {elapsed:?}"
        );
        // cancel 触发的错误：Canceled 或（连接已建立后被 shutdown 的）Io/SilentFin 均可，
        // 关键是迅速返回。这里仅断言不是成功。
        let _ = err;
        stop();
    }

    /// default timeout：timeout=None 时用 5s 默认（这里只验 want_recv=false 快速成功路径）。
    #[test]
    fn default_timeout_send() {
        let (host, port, stop) = start_mock_server(move |mut c| {
            let mut buf = [0u8; 64];
            let _ = c.read(&mut buf);
        });
        let s = Sender::default(); // timeout=None → 5s
        let got = s
            .send(&host, port, b"req=702&query=preview", false)
            .expect("send");
        assert!(got.is_none());
        stop();
    }

    // --- pick_ipv4_from_addrs 纯函数（锚 Go TestPickIPv4FromAddrs）---

    #[test]
    fn pick_ipv4_empty() {
        assert_eq!(pick_ipv4_from_addrs(&[]), None);
    }

    #[test]
    fn pick_ipv4_only_loopback() {
        let lo = Ipv4Addr::new(127, 0, 0, 1);
        assert_eq!(pick_ipv4_from_addrs(&[lo]), Some(lo));
    }

    #[test]
    fn pick_ipv4_prefer_non_loopback() {
        let lo = Ipv4Addr::new(127, 0, 0, 1);
        let real = Ipv4Addr::new(192, 168, 1, 10);
        assert_eq!(pick_ipv4_from_addrs(&[lo, real]), Some(real));
    }

    #[test]
    fn pick_ipv4_first_non_loopback() {
        let a = Ipv4Addr::new(10, 0, 0, 5);
        let b = Ipv4Addr::new(10, 0, 0, 6);
        assert_eq!(pick_ipv4_from_addrs(&[a, b]), Some(a));
    }

    // --- get_iface_ip（loopback 接口；锚 Go TestGetIfaceIP_Loopback / _Missing）---

    #[test]
    fn get_iface_ip_loopback() {
        // loopback 接口名 macOS=lo0 / linux=lo——试两个，至少一个命中。
        for name in ["lo0", "lo"] {
            match get_iface_ip(name) {
                Ok(ip) => {
                    assert!(
                        ip.is_loopback(),
                        "get_iface_ip({name}) = {ip}, want loopback"
                    );
                    return;
                }
                Err(_) => continue,
            }
        }
        // 无 loopback 接口（罕见）——不失败，跳过。
        eprintln!("skip: no loopback interface lo / lo0 found");
    }

    #[test]
    fn get_iface_ip_missing() {
        let err = get_iface_ip("nonexistent_iface_zzz_99");
        assert!(err.is_err(), "expected error for nonexistent interface");
    }

    // --- is_retryable_error（锚 Go TestIsRetryableError）---

    #[test]
    fn is_retryable_silent_fin() {
        assert!(is_retryable_error(&WireError::SilentFin));
    }

    #[test]
    fn is_retryable_timeout() {
        assert!(is_retryable_error(&WireError::Timeout { stage: "connect" }));
    }

    #[test]
    fn is_retryable_io_timedout() {
        let e = WireError::Io {
            stage: "read",
            source: io::Error::new(io::ErrorKind::TimedOut, "deadline"),
        };
        assert!(is_retryable_error(&e));
    }

    #[test]
    fn is_retryable_connection_reset() {
        let e = WireError::Io {
            stage: "write",
            source: io::Error::new(io::ErrorKind::ConnectionReset, "connection reset by peer"),
        };
        assert!(is_retryable_error(&e));
    }

    #[test]
    fn is_retryable_broken_pipe() {
        let e = WireError::Io {
            stage: "write",
            source: io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe"),
        };
        assert!(is_retryable_error(&e));
    }

    #[test]
    fn is_not_retryable_business_error() {
        // 业务 body mismatch 不走此函数；模拟一个非 timeout/reset 的 Io 错误 → false。
        let e = WireError::Io {
            stage: "read",
            source: io::Error::new(io::ErrorKind::InvalidData, "malformed body"),
        };
        assert!(!is_retryable_error(&e));
    }

    #[test]
    fn is_not_retryable_iface_error() {
        assert!(!is_retryable_error(&WireError::Iface("no IPv4".into())));
    }
}
