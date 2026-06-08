//! `listen6672`：门禁网 UDP 6672 事件信号监听（组 C）。
//!
//! 移植 Go `internal/listen6672`（`parser.go` / `dedup.go` / `numquery.go` /
//! `listener.go` / `listener_linux.go` / `listener_other.go`）：
//!   - parser `extract_udp_payload`（L2→ethertype 0x0800→IP(ihl/proto=17)→UDP→
//!     payload+srcIP+srcPort，全 `from_be_bytes` 读 wire，untagged-only）
//!   - `parse_frame`（21 字节定长）+ `classify`（**响铃 byte19 ∈ {0x8c,0x94,0x95}
//!     多值，锚 Go parser.go:88** / 呼梯 0x90 仅 log / keepalive silent drop /
//!     号码查询 + 响应）
//!   - dedup ringbuffer（key=srcIP^srcPort<<32^FNV-1a(payload)，64 容量 / 200ms 窗口，
//!     **时钟注入 seam**）
//!   - numquery 响应构造（`build_number_query_response`）
//!   - dispatch（按 byte19 trailer 路由回调）+ `dispatch_with_dedup`
//!   - PF_PACKET recv 循环（per slave `std::thread` + SO_RCVTIMEO=500ms wakeup +
//!     shutdown flag `AtomicBool` + 关 fd）；非 Linux stub 返错（对齐 Go `listener_other.go`）。
//!
//! BPF const 用 `crate::bpf::BPF_6672`；socket 缝用 `crate::ffi`。

use std::sync::atomic::AtomicBool;
#[cfg(target_os = "linux")]
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

// ===========================================================================
// parser（锚 Go parser.go）
// ===========================================================================

/// 6672 帧固定 21 字节。
pub const FRAME_SIZE: usize = 21;

/// 解析后的 6672 帧结构。`raw` 保留原始 21 字节，便于回调时再次 inspect。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// byte 0: 0x00=request, 0x01=response。
    pub flag: u8,
    /// byte 1-4。
    pub target_bcd: [u8; 4],
    /// byte 5-8（request 帧 = 发送方 BCD；response 帧 = 目标 IPv4 NBO）。
    pub self_or_ip: [u8; 4],
    /// byte 9-12（事件起始全 0；状态保持帧含序列号 f3 b5）。
    pub reserved: [u8; 4],
    /// byte 13-16 LE。
    pub event_flag: u32,
    /// byte 17-18（应固定 0x00 0x40）。
    pub magic: [u8; 2],
    /// byte 19 ★ 事件子类型（dispatch key）。
    pub subtype: u8,
    /// byte 20（事件帧 0xb6，保持帧 0x00）。
    pub event_id: u8,
    /// 完整 21 字节。
    pub raw: [u8; FRAME_SIZE],
}

/// 解析错误（锚 Go `ErrFrameSize` / `ErrFrameMagic`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// payload 长度不是 21 字节。
    FrameSize(usize),
    /// byte 17-18 != 00 40。
    FrameMagic(u8, u8),
}

impl core::fmt::Display for ParseError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ParseError::FrameSize(n) => {
                write!(f, "listen6672: payload not 21 bytes: got {n} bytes")
            }
            ParseError::FrameMagic(a, b) => {
                write!(f, "listen6672: byte 17-18 not 00 40: got {a:02x} {b:02x}")
            }
        }
    }
}

impl std::error::Error for ParseError {}

/// 把 21 字节 UDP payload 解析为 `Frame`。
///
/// 严格模式：长度 != 21 或 byte 17-18 != 00 40 → 报错（不变量）。
/// 锚 Go `ParseFrame`。byte 13-16 是 little-endian（与 wire BE 字段不同——这是
/// 安居宝 6672 帧的固有 LE 字段，逐字复刻 Go `binary.LittleEndian.Uint32`）。
pub fn parse_frame(payload: &[u8]) -> Result<Frame, ParseError> {
    if payload.len() != FRAME_SIZE {
        return Err(ParseError::FrameSize(payload.len()));
    }
    if payload[17] != 0x00 || payload[18] != 0x40 {
        return Err(ParseError::FrameMagic(payload[17], payload[18]));
    }
    let mut raw = [0u8; FRAME_SIZE];
    raw.copy_from_slice(payload);
    let mut target_bcd = [0u8; 4];
    target_bcd.copy_from_slice(&payload[1..5]);
    let mut self_or_ip = [0u8; 4];
    self_or_ip.copy_from_slice(&payload[5..9]);
    let mut reserved = [0u8; 4];
    reserved.copy_from_slice(&payload[9..13]);
    Ok(Frame {
        flag: payload[0],
        target_bcd,
        self_or_ip,
        reserved,
        event_flag: u32::from_le_bytes([payload[13], payload[14], payload[15], payload[16]]),
        magic: [payload[17], payload[18]],
        subtype: payload[19],
        event_id: payload[20],
        raw,
    })
}

/// 6672 帧分类（锚 Go `EventKind`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// 未知。
    Unknown,
    /// byte 0=0x00, byte 13-16=0x01000000, byte 19 ∈ {0x8c, 0x94, 0x95} ★ 触发 wire 序列。
    Ring,
    /// byte 0=0x00, byte 13-16=0x01000000, byte 19=0x90  仅 log。
    ElevatorKey,
    /// byte 0=0x00, byte 13-16=0x01000000, byte 19=0x00  silent drop。
    KeepAlive,
    /// byte 0=0x00, byte 13-16=0x00000000  号码查询请求。
    NumberQuery,
    /// byte 0=0x01  号码查询响应（byte 5-8 是 IPv4）。
    NumberResponse,
}

impl EventKind {
    /// 易读名（log 用），锚 Go `EventKind.String`。
    pub fn as_str(&self) -> &'static str {
        match self {
            EventKind::Ring => "ring",
            EventKind::ElevatorKey => "elevator_key",
            EventKind::KeepAlive => "keep_alive",
            EventKind::NumberQuery => "number_query",
            EventKind::NumberResponse => "number_response",
            EventKind::Unknown => "unknown",
        }
    }
}

/// 把已解析的 `Frame` 归入 `EventKind`（锚 Go `Classify`）。
///
/// **响铃 byte19 多值识别**：`subtype ∈ {0x8c, 0x94, 0x95}` 都归 `Ring`（锚 Go
/// parser.go:88 `case 0x8c, 0x94, 0x95:`，v0.3.3 fix-doorbell-pipeline）。只认 0x94
/// 会漏判 0x8c/0x95 的真实响铃帧——即 Go v0.3.3 已修的漏分类 bug，**禁止退回单值**。
pub fn classify(f: &Frame) -> EventKind {
    if f.flag == 0x01 {
        return EventKind::NumberResponse;
    }
    if f.flag != 0x00 {
        return EventKind::Unknown;
    }
    match f.event_flag {
        0x0000_0000 => EventKind::NumberQuery,
        0x0000_0001 => match f.subtype {
            // 多值识别 ring trailer：byte 19 疑似 session counter / event token，
            // 同一事件内 8 帧一致、跨事件取不同值（实测 0x94 / 0x95 / 0x8c）。
            0x8c | 0x94 | 0x95 => EventKind::Ring,
            0x90 => EventKind::ElevatorKey,
            0x00 => EventKind::KeepAlive,
            _ => EventKind::Unknown,
        },
        _ => EventKind::Unknown,
    }
}

/// 从 L2 帧中抽 UDP payload + src IP + src port（锚 Go `extractUDPPayload`）。
///
/// BPF 已过滤为 IPv4/UDP/dst:6672。untagged-only（slave 接口 frame 已被 kernel 剥
/// 802.1Q tag，**不需要 VLAN-aware 双路径**）。全用 `from_be_bytes` 读 wire 多字节字段
/// （wire 永远 big-endian，macOS 也能 golden 认证）。
///
/// 返回 `Some((payload, src_ip, src_port))`；任何长度/类型不符返 `None`。
pub fn extract_udp_payload(frame: &[u8]) -> Option<(Vec<u8>, [u8; 4], u16)> {
    const MIN_LEN: usize = 14 + 20 + 8;
    if frame.len() < MIN_LEN {
        return None;
    }
    // ethertype（byte 12-13）必须是 IPv4 0x0800。
    if u16::from_be_bytes([frame[12], frame[13]]) != 0x0800 {
        return None;
    }

    let ip_hdr = &frame[14..];
    let ihl = (ip_hdr[0] & 0x0f) as usize * 4;
    if ihl < 20 || ip_hdr.len() < ihl + 8 {
        return None;
    }
    // IP proto（byte 9）必须是 UDP 17。
    if ip_hdr[9] != 17 {
        return None;
    }
    let src_ip = [ip_hdr[12], ip_hdr[13], ip_hdr[14], ip_hdr[15]];

    let udp_hdr = &ip_hdr[ihl..];
    let src_port = u16::from_be_bytes([udp_hdr[0], udp_hdr[1]]);
    let udp_len = u16::from_be_bytes([udp_hdr[4], udp_hdr[5]]) as usize;
    if udp_len < 8 || udp_hdr.len() < udp_len {
        return None;
    }
    let payload = udp_hdr[8..udp_len].to_vec();
    Some((payload, src_ip, src_port))
}

// ===========================================================================
// numquery（锚 Go numquery.go）
// ===========================================================================

/// anjubao 6672 协议的标准端口。
pub const DEFAULT_UDP_PORT: u16 = 6672;

/// 构造 21 字节号码查询响应帧（锚 Go `BuildNumberQueryResponse`）。
///
/// 字段 layout：
///   byte 0:     0x01 (response)
///   byte 1-4:   echo target BCD（请求方查的号码）
///   byte 5-8:   IPv4 NBO（target BCD 对应的 IP = 本机门禁网 IP）
///   byte 9-12:  0x00 0x00 0x00 0x00
///   byte 13-16: echo event flag（来自 request 帧 LE）
///   byte 17-18: 0x00 0x40 echo
///   byte 19-20: echo subtype + event id
///
/// `req_frame` 必须是已成功 `parse_frame` 的 request 帧（byte 0=0x00）。
pub fn build_number_query_response(req_frame: &Frame, our_ip: [u8; 4]) -> [u8; FRAME_SIZE] {
    let mut out = [0u8; FRAME_SIZE];
    out[0] = 0x01;
    out[1..5].copy_from_slice(&req_frame.target_bcd);
    out[5..9].copy_from_slice(&our_ip);
    // byte 9-12 已经是 0。
    out[13..17].copy_from_slice(&req_frame.event_flag.to_le_bytes());
    out[17] = 0x00;
    out[18] = 0x40;
    out[19] = req_frame.subtype;
    out[20] = req_frame.event_id;
    out
}

// ===========================================================================
// dedup ringbuffer（锚 Go dedup.go，时钟注入 seam）
// ===========================================================================

/// ringbuffer 固定容量（design.md 决策 4：一次响铃 broadcast burst 8 帧 × 2 slave =
/// 16，4× 安全余量 = 64）。
pub const DEDUP_CAPACITY: usize = 64;

/// 同 key 视为重复的时间窗（design.md 决策 4：200ms 覆盖 bridge forward 最坏延迟）。
pub const DEDUP_WINDOW_MS: u64 = 200;

/// 可注入时钟 seam：返回单调毫秒时刻。
///
/// 生产用 `MonotonicClock`（包 `std::time::Instant`）；测试用固定时刻 fake clock
/// 让时间相关 ringbuffer 行为确定（锚 Go `monotonicNow` 可注入封装）。
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

/// 固定容量 ringbuffer，用于多 slave 模式去重相同 frame（锚 Go `Dedup`）。
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
    /// 命中后**不**更新 ts（避免连续同 key 永久 dedup）；未命中则把当前 (key, now) 写入
    /// head 位置（环形覆盖最旧 entry）。
    ///
    /// 边界含 cutoff 时刻自身（now-ts == window 算窗口内重复）——锚 Go `!ts.Before(cutoff)`
    /// 让 200ms 边界 frame 也被 dedup。
    pub fn seen(&self, key: u64, now_ms: u64) -> bool {
        let mut inner = self.inner.lock().unwrap();
        // cutoff = now - window；用 saturating_sub 防早期 now < window 下溢。
        let cutoff = now_ms.saturating_sub(DEDUP_WINDOW_MS);
        for e in inner.entries.iter() {
            if !e.set {
                continue;
            }
            // !ts.Before(cutoff) ≡ ts >= cutoff（含边界）。
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

/// 算 frame 的 dedup 哈希（锚 Go `dedupKey`）。
///
/// key = src_ip(4B BE) ^ (src_port(2B) << 32) ^ FNV-1a(payload[0:FrameSize])。
///
/// payload 长度限到 `FRAME_SIZE`——防御 malformed 帧污染 ringbuffer（上游
/// `extract_udp_payload` 不严格保证长度，用 min 截断让 dedup 对长度不敏感）。
pub fn dedup_key(src_ip: [u8; 4], src_port: u16, payload: &[u8]) -> u64 {
    let n = payload.len().min(FRAME_SIZE);
    let hash = fnv1a_64(&payload[..n]);
    // src_ip BE → u32（锚 Go binary.BigEndian.Uint32(v4)）。
    let ip_bits = u32::from_be_bytes(src_ip) as u64;
    hash ^ ip_bits ^ ((src_port as u64) << 32)
}

/// FNV-1a 64-bit（锚 Go `hash/fnv.New64a`）。无加密强度需求，仅需冲突概率 ≈ 0。
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
// dispatch / Listener（锚 Go listener.go）
// ===========================================================================

/// frame 分发回调签名 `(src_ip, frame)`：`src_ip` 来自 PF_PACKET 解析，
/// `frame.self_or_ip` 是发送方 BCD。
pub type FrameCallback = Box<dyn Fn([u8; 4], &Frame) + Send + Sync>;

/// 可选 log hook 签名。
pub type LogFn = Box<dyn Fn(&str) + Send + Sync>;

/// frame 分发回调集合（锚 Go `Listener` 的 `OnRing` / `OnElevatorKey` /
/// `OnNumberQuery` / `OnNumberResponse` 字段）。
///
/// 各回调可为 `None`。
#[derive(Default)]
pub struct Callbacks {
    /// 收到响铃帧（byte19 ∈ {0x8c,0x94,0x95}）时调用。
    pub on_ring: Option<FrameCallback>,
    /// 收到呼梯帧（byte19=0x90）时调用；本机仅 log，不发 wire。
    pub on_elevator_key: Option<FrameCallback>,
    /// 收到号码查询请求（byte13-16=0）时调用。
    pub on_number_query: Option<FrameCallback>,
    /// 收到号码↔IP 路由响应（byte0=0x01）时调用。
    pub on_number_response: Option<FrameCallback>,
}

/// 监听一组物理 slave 接口上的 UDP 6672 帧，按 `EventKind` 分发到注册回调
/// （锚 Go `Listener`）。
///
/// 跨平台部分（`dispatch` / 回调）在所有平台可用；`run` 仅 Linux 实现（PF_PACKET +
/// BPF），非 Linux stub 返错。
pub struct Listener {
    /// 物理 slave 接口名列表（由 config 算出）。每个 slave 起独立 PF_PACKET socket。
    /// 空列表视为错误。
    pub slaves: Vec<String>,

    /// 分发回调集合。
    pub callbacks: Callbacks,

    /// 可选 log hook。`None` 时不 log。
    pub logf: Option<LogFn>,

    /// 多 slave 模式下用于跨 socket 去重；单 slave 模式 `None`（跳过 dedup）。
    dedup: Option<Arc<Dedup>>,

    /// dedup 时钟 seam（生产 `MonotonicClock`，测试可注入 fake）。
    clock: Arc<dyn Clock>,
}

impl Listener {
    /// 新建 Listener。`slaves` 长度 > 1 时自动启 dedup。
    pub fn new(slaves: Vec<String>, callbacks: Callbacks) -> Self {
        let dedup = if slaves.len() > 1 {
            Some(Arc::new(Dedup::new()))
        } else {
            None
        };
        Self {
            slaves,
            callbacks,
            logf: None,
            dedup,
            clock: Arc::new(MonotonicClock::new()),
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

    /// 把单个 frame + 源 IP 路由到对应回调（锚 Go `Dispatch`）。
    ///
    /// 用于：① Linux `run` 主循环每收到一帧调用一次；② 单元测试直接喂 mock payload
    /// 绕过 PF_PACKET socket。
    pub fn dispatch(&self, src_ip: [u8; 4], payload: &[u8]) {
        if payload.len() != FRAME_SIZE {
            self.logf(&format!(
                "listen6672: drop non-21B payload (len={})",
                payload.len()
            ));
            return;
        }
        let f = match parse_frame(payload) {
            Ok(f) => f,
            Err(e) => {
                self.logf(&format!("listen6672: parse error: {e}"));
                return;
            }
        };
        match classify(&f) {
            EventKind::Ring => {
                self.logf(&format!(
                    "Capture: Ring captured from {}.{}.{}.{}",
                    src_ip[0], src_ip[1], src_ip[2], src_ip[3]
                ));
                if let Some(cb) = &self.callbacks.on_ring {
                    cb(src_ip, &f);
                }
            }
            EventKind::ElevatorKey => {
                self.logf("indoor station elevator key, ignored");
                if let Some(cb) = &self.callbacks.on_elevator_key {
                    cb(src_ip, &f);
                }
            }
            EventKind::KeepAlive => {
                // silent drop
            }
            EventKind::NumberQuery => {
                if let Some(cb) = &self.callbacks.on_number_query {
                    cb(src_ip, &f);
                }
            }
            EventKind::NumberResponse => {
                if let Some(cb) = &self.callbacks.on_number_response {
                    cb(src_ip, &f);
                }
            }
            EventKind::Unknown => {
                self.logf(&format!(
                    "listen6672: unknown frame subtype=0x{:02x}",
                    f.subtype
                ));
            }
        }
    }

    /// 在多 slave 模式下用 dedup 去重后再 `dispatch`；单 slave 直 `dispatch`
    /// （锚 Go `dispatchWithDedup`）。
    pub fn dispatch_with_dedup(&self, src_ip: [u8; 4], src_port: u16, payload: &[u8]) {
        if let Some(dedup) = &self.dedup {
            let key = dedup_key(src_ip, src_port, payload);
            if dedup.seen(key, self.clock.now_ms()) {
                return;
            }
        }
        self.dispatch(src_ip, payload);
    }

    /// 启动监听主循环（Linux PF_PACKET 实现，非 Linux 立即返错；锚 Go `Run`）。
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

/// listener 运行错误。
#[derive(Debug)]
pub enum ListenerError {
    /// `slaves` 为空。
    NoSlaves,
    /// PF_PACKET 仅 Linux 支持（非 Linux stub 路径，对齐 Go `listener_other.go`）。
    Unsupported,
    /// 某个 slave socket 阶段失败（携带 slave 名 + 底层 FFI 错误）。
    Slave(String, crate::ffi::FfiError),
}

impl core::fmt::Display for ListenerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ListenerError::NoSlaves => write!(
                f,
                "listen6672: Slaves required (config.ResolveIfaceList must be called)"
            ),
            ListenerError::Unsupported => write!(
                f,
                "listen6672: PF_PACKET only supported on Linux; build for linux/* and run as root"
            ),
            ListenerError::Slave(iface, e) => write!(f, "slave {iface}: {e}"),
        }
    }
}

impl std::error::Error for ListenerError {}

// --- Linux PF_PACKET recv 循环（锚 Go listener_linux.go runPlatform/runSlave）---

#[cfg(target_os = "linux")]
impl Listener {
    fn run_platform(&self, shutdown: Arc<AtomicBool>) -> Result<(), ListenerError> {
        if self.slaves.is_empty() {
            return Err(ListenerError::NoSlaves);
        }

        // 多 slave 模式启 dedup（new() 已按 slaves.len() 决定；这里仅作防御，
        // 若 with_dedup 显式注入则保留）。
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

        let fprog = crate::bpf::as_fprog(&crate::bpf::BPF_6672);
        ffi::attach_filter(fd, &fprog)?;

        // SO_RCVTIMEO=500ms 周期 wakeup 查 shutdown flag（失败不致命，仅 log）。
        if let Err(e) = ffi::set_rcvtimeo(fd, 0, 500_000) {
            self.logf(&format!(
                "listen6672: SO_RCVTIMEO set failed on {iface}: {e} (continuing)"
            ));
        }

        self.logf(&format!(
            "listen6672: started on {iface} (ifindex={ifindex}, PROMISC, BPF active, dedup={})",
            self.dedup.is_some()
        ));

        let mut buf = [0u8; 1500];
        loop {
            if shutdown.load(Ordering::SeqCst) {
                return Ok(());
            }
            match ffi::recv(fd, &mut buf) {
                Ok(n) => {
                    if let Some((payload, src_ip, src_port)) = extract_udp_payload(&buf[..n]) {
                        self.dispatch_with_dedup(src_ip, src_port, &payload);
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

// --- 非 Linux stub（对齐 Go listener_other.go）---

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

    // --- 测试辅助：构造 21 字节帧 ---

    fn ring_frame(subtype: u8) -> [u8; FRAME_SIZE] {
        let mut p = [0u8; FRAME_SIZE];
        p[0] = 0x00; // request
        p[1..5].copy_from_slice(&[0x06, 0x02, 0x00, 0x00]); // target BCD
        p[5..9].copy_from_slice(&[0x06, 0x02, 0x11, 0x03]); // sender BCD
                                                            // byte 13-16 = 0x01000000 LE → event_flag = 0x00000001
        p[13] = 0x01;
        p[17] = 0x00;
        p[18] = 0x40;
        p[19] = subtype;
        p[20] = 0xb6;
        p
    }

    fn keepalive_frame() -> [u8; FRAME_SIZE] {
        let mut p = ring_frame(0x00);
        p[20] = 0x00;
        p
    }

    fn numquery_frame() -> [u8; FRAME_SIZE] {
        let mut p = [0u8; FRAME_SIZE];
        p[0] = 0x00;
        p[1..5].copy_from_slice(&[0x06, 0x02, 0x00, 0x00]);
        // event_flag = 0 (byte 13-16 all zero)
        p[17] = 0x00;
        p[18] = 0x40;
        p[19] = 0x10;
        p[20] = 0x00;
        p
    }

    fn numresp_frame() -> [u8; FRAME_SIZE] {
        let mut p = ring_frame(0x94);
        p[0] = 0x01; // response
        p
    }

    // --- parser ---

    #[test]
    fn parse_frame_rejects_wrong_size() {
        assert_eq!(parse_frame(&[0u8; 20]), Err(ParseError::FrameSize(20)));
        assert_eq!(parse_frame(&[0u8; 22]), Err(ParseError::FrameSize(22)));
    }

    #[test]
    fn parse_frame_rejects_bad_magic() {
        let mut p = ring_frame(0x94);
        p[17] = 0x01;
        assert_eq!(parse_frame(&p), Err(ParseError::FrameMagic(0x01, 0x40)));
    }

    #[test]
    fn parse_frame_extracts_fields() {
        let p = ring_frame(0x94);
        let f = parse_frame(&p).unwrap();
        assert_eq!(f.flag, 0x00);
        assert_eq!(f.target_bcd, [0x06, 0x02, 0x00, 0x00]);
        assert_eq!(f.self_or_ip, [0x06, 0x02, 0x11, 0x03]);
        assert_eq!(f.event_flag, 0x0000_0001); // byte 13-16 LE
        assert_eq!(f.magic, [0x00, 0x40]);
        assert_eq!(f.subtype, 0x94);
        assert_eq!(f.event_id, 0xb6);
        assert_eq!(&f.raw[..], &p[..]);
    }

    // --- classify：byte19 多值响铃 ---

    #[test]
    fn classify_ring_multivalue() {
        // ★ 核心：三个响铃 byte19 值都须识别为 Ring（锚 Go parser.go:88）。
        for st in [0x8c, 0x94, 0x95] {
            let f = parse_frame(&ring_frame(st)).unwrap();
            assert_eq!(
                classify(&f),
                EventKind::Ring,
                "subtype 0x{st:02x} must be Ring"
            );
        }
    }

    #[test]
    fn classify_elevator_key() {
        let f = parse_frame(&ring_frame(0x90)).unwrap();
        assert_eq!(classify(&f), EventKind::ElevatorKey);
    }

    #[test]
    fn classify_keepalive() {
        let f = parse_frame(&keepalive_frame()).unwrap();
        assert_eq!(classify(&f), EventKind::KeepAlive);
    }

    #[test]
    fn classify_number_query_and_response() {
        let q = parse_frame(&numquery_frame()).unwrap();
        assert_eq!(classify(&q), EventKind::NumberQuery);
        let r = parse_frame(&numresp_frame()).unwrap();
        assert_eq!(classify(&r), EventKind::NumberResponse);
    }

    #[test]
    fn classify_unknown_subtype() {
        let f = parse_frame(&ring_frame(0x77)).unwrap();
        assert_eq!(classify(&f), EventKind::Unknown);
    }

    // --- extract_udp_payload ---

    /// 构造一个 untagged Ethernet/IPv4/UDP 帧承载给定 UDP payload。
    fn build_l2_udp(src_ip: [u8; 4], src_port: u16, dst_port: u16, payload: &[u8]) -> Vec<u8> {
        let mut frame = Vec::new();
        // Ethernet: dst(6) + src(6) + ethertype(2)
        frame.extend_from_slice(&[0xff; 6]);
        frame.extend_from_slice(&[0x11; 6]);
        frame.extend_from_slice(&0x0800u16.to_be_bytes()); // IPv4
                                                           // IP header (20 bytes, ihl=5)
        let udp_len = 8 + payload.len();
        let total_len = 20 + udp_len;
        let mut ip = vec![
            0x45, // version 4 + ihl 5
            0x00, // dscp
        ];
        ip.extend_from_slice(&(total_len as u16).to_be_bytes());
        ip.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // id + flags/frag (0)
        ip.push(0x40); // ttl
        ip.push(17); // proto UDP
        ip.extend_from_slice(&[0x00, 0x00]); // checksum (ignored)
        ip.extend_from_slice(&src_ip);
        ip.extend_from_slice(&[172, 16, 106, 202]); // dst ip
        assert_eq!(ip.len(), 20);
        frame.extend_from_slice(&ip);
        // UDP header
        frame.extend_from_slice(&src_port.to_be_bytes());
        frame.extend_from_slice(&dst_port.to_be_bytes());
        frame.extend_from_slice(&(udp_len as u16).to_be_bytes());
        frame.extend_from_slice(&[0x00, 0x00]); // checksum
        frame.extend_from_slice(payload);
        frame
    }

    #[test]
    fn extract_udp_payload_roundtrip() {
        let payload = ring_frame(0x94);
        let frame = build_l2_udp([172, 16, 106, 201], 5000, 6672, &payload);
        let (got, src_ip, src_port) = extract_udp_payload(&frame).unwrap();
        assert_eq!(got, &payload[..]);
        assert_eq!(src_ip, [172, 16, 106, 201]);
        assert_eq!(src_port, 5000);
    }

    #[test]
    fn extract_udp_payload_rejects_non_ipv4() {
        let mut frame = build_l2_udp([1, 2, 3, 4], 1, 6672, &[0u8; 21]);
        frame[12] = 0x86;
        frame[13] = 0xdd; // IPv6 ethertype
        assert!(extract_udp_payload(&frame).is_none());
    }

    #[test]
    fn extract_udp_payload_rejects_non_udp() {
        let mut frame = build_l2_udp([1, 2, 3, 4], 1, 6672, &[0u8; 21]);
        frame[14 + 9] = 6; // TCP
        assert!(extract_udp_payload(&frame).is_none());
    }

    #[test]
    fn extract_udp_payload_rejects_short_frame() {
        assert!(extract_udp_payload(&[0u8; 10]).is_none());
    }

    // --- numquery ---

    #[test]
    fn build_number_query_response_layout() {
        let req = parse_frame(&numquery_frame()).unwrap();
        let resp = build_number_query_response(&req, [172, 16, 106, 202]);
        assert_eq!(resp[0], 0x01);
        assert_eq!(&resp[1..5], &req.target_bcd);
        assert_eq!(&resp[5..9], &[172, 16, 106, 202]);
        assert_eq!(&resp[9..13], &[0, 0, 0, 0]);
        assert_eq!(
            u32::from_le_bytes([resp[13], resp[14], resp[15], resp[16]]),
            req.event_flag
        );
        assert_eq!(resp[17], 0x00);
        assert_eq!(resp[18], 0x40);
        assert_eq!(resp[19], req.subtype);
        assert_eq!(resp[20], req.event_id);
    }

    // --- dedup（时钟注入）---

    #[test]
    fn dedup_dedups_within_window() {
        let d = Dedup::new();
        let key = 0xdead_beef;
        assert!(!d.seen(key, 1000)); // first: not seen
        assert!(d.seen(key, 1100)); // 100ms later: dup
        assert!(d.seen(key, 1200)); // 200ms boundary: still dup (inclusive)
    }

    #[test]
    fn dedup_expires_after_window() {
        let d = Dedup::new();
        let key = 0xabc;
        assert!(!d.seen(key, 1000));
        // 201ms later: outside window (cutoff = 1201-200 = 1001 > 1000) → not dup, re-inserted.
        assert!(!d.seen(key, 1201));
    }

    #[test]
    fn dedup_hit_does_not_refresh_ts() {
        let d = Dedup::new();
        let key = 0x55;
        assert!(!d.seen(key, 0)); // inserted at ts=0
        assert!(d.seen(key, 150)); // dup, ts NOT refreshed
                                   // at 201, cutoff=1; original ts=0 < 1 → expired despite hit at 150.
        assert!(!d.seen(key, 201));
    }

    #[test]
    fn dedup_key_distinguishes_src_and_payload() {
        let p1 = ring_frame(0x94);
        let p2 = ring_frame(0x95);
        let k_a = dedup_key([1, 2, 3, 4], 5000, &p1);
        let k_b = dedup_key([1, 2, 3, 5], 5000, &p1); // diff src ip
        let k_c = dedup_key([1, 2, 3, 4], 5001, &p1); // diff src port
        let k_d = dedup_key([1, 2, 3, 4], 5000, &p2); // diff payload
        assert_ne!(k_a, k_b);
        assert_ne!(k_a, k_c);
        assert_ne!(k_a, k_d);
        // same inputs → same key
        assert_eq!(k_a, dedup_key([1, 2, 3, 4], 5000, &p1));
    }

    #[test]
    fn fnv1a_known_vector() {
        // FNV-1a 64 of empty = offset basis; of "a" = 0xaf63dc4c8601ec8c (well-known).
        assert_eq!(fnv1a_64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a_64(b"a"), 0xaf63_dc4c_8601_ec8c);
    }

    // --- dispatch（hook 测分发；Listener 无真 socket）---

    /// fake clock：返回固定时刻（测试 dedup 时间相关行为）。
    struct FakeClock(std::sync::atomic::AtomicUsize);
    impl Clock for FakeClock {
        fn now_ms(&self) -> u64 {
            self.0.load(Ordering::SeqCst) as u64
        }
    }

    #[test]
    fn dispatch_routes_ring_to_callback() {
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        let cb = Callbacks {
            on_ring: Some(Box::new(move |_ip, f| {
                assert!(matches!(f.subtype, 0x8c | 0x94 | 0x95));
                h.fetch_add(1, Ordering::SeqCst);
            })),
            ..Default::default()
        };
        let l = Listener::new(vec!["eth0".into()], cb);
        for st in [0x8c, 0x94, 0x95] {
            l.dispatch([1, 2, 3, 4], &ring_frame(st));
        }
        // all three ring values routed to on_ring
        assert_eq!(hits.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn dispatch_elevator_and_keepalive() {
        let elev = Arc::new(AtomicUsize::new(0));
        let e = elev.clone();
        let cb = Callbacks {
            on_elevator_key: Some(Box::new(move |_ip, _f| {
                e.fetch_add(1, Ordering::SeqCst);
            })),
            ..Default::default()
        };
        let l = Listener::new(vec!["eth0".into()], cb);
        l.dispatch([1, 2, 3, 4], &ring_frame(0x90)); // elevator
        l.dispatch([1, 2, 3, 4], &keepalive_frame()); // keepalive: silent drop
        assert_eq!(elev.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn dispatch_drops_non_21b() {
        let l = Listener::new(vec!["eth0".into()], Callbacks::default());
        // should not panic
        l.dispatch([1, 2, 3, 4], &[0u8; 5]);
    }

    #[test]
    fn dispatch_with_dedup_skips_duplicate() {
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        let cb = Callbacks {
            on_ring: Some(Box::new(move |_ip, _f| {
                h.fetch_add(1, Ordering::SeqCst);
            })),
            ..Default::default()
        };
        let clock = Arc::new(FakeClock(std::sync::atomic::AtomicUsize::new(1000)));
        // 2 slaves → dedup on; inject fake clock.
        let l = Listener::new(vec!["eth0".into(), "eth1".into()], cb).with_clock(clock.clone());
        let payload = ring_frame(0x94);
        // same frame from two slaves within window → dispatched once.
        l.dispatch_with_dedup([1, 2, 3, 4], 5000, &payload);
        l.dispatch_with_dedup([1, 2, 3, 4], 5000, &payload);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        // advance past window → dispatched again.
        clock.0.store(1500, Ordering::SeqCst);
        l.dispatch_with_dedup([1, 2, 3, 4], 5000, &payload);
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn dispatch_with_dedup_single_slave_no_dedup() {
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        let cb = Callbacks {
            on_ring: Some(Box::new(move |_ip, _f| {
                h.fetch_add(1, Ordering::SeqCst);
            })),
            ..Default::default()
        };
        // single slave → no dedup → both dispatch.
        let l = Listener::new(vec!["eth0".into()], cb);
        let payload = ring_frame(0x94);
        l.dispatch_with_dedup([1, 2, 3, 4], 5000, &payload);
        l.dispatch_with_dedup([1, 2, 3, 4], 5000, &payload);
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn run_rejects_empty_slaves() {
        let l = Listener::new(vec![], Callbacks::default());
        let r = l.run(Arc::new(AtomicBool::new(false)));
        // empty slaves → NoSlaves on **all** platforms（跨平台入口先判，非 Linux 不再
        // 掩盖成 Unsupported）。
        assert!(matches!(r, Err(ListenerError::NoSlaves)));
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn run_non_linux_returns_unsupported() {
        let l = Listener::new(vec!["eth0".into()], Callbacks::default());
        let r = l.run(Arc::new(AtomicBool::new(false)));
        assert!(matches!(r, Err(ListenerError::Unsupported)));
    }
}
