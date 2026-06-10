//! rtp：RTP 头解析 + Annex-B NAL 提取 + receiver 主循环
//! （移植 Go `internal/video/rtp.go` 的解析部分 + `BindUDP`/`RunWithConn`）。
//!
//! 等价纪律（spec「RTP receiver 与专有分片重组器等价」）：
//!   - 校验集与 Go **完全一致——仅校验 V=2 / X=0 / CC=0**。P 位 Go 不校验，
//!     P=1 包被接受且 padding 字节留在 payload（已知宽松行为，禁「顺手」加 P 校验
//!     造成接受集分叉）；PT 仅解析存字段，**不过滤**。
//!   - 多字节字段 NBO 显式 `from_be_bytes`（新 BE 面，golden + 单测锚定）。
//!
//! receiver 主循环（[`RtpReceiver::run_with_conn`]）：
//!   - bind 经 [`bind_udp`] **独立同步**暴露——session 层必须先 bind 再发 preview
//!     信令（否则外机推流到未就绪端口）
//!   - 1s socket read-timeout 轮询 stop flag（对齐 Go 1s deadline 循环）
//!   - `expect_src_ip` 过滤非外机来源；SSRC **仅首包**锁存经 sink 回调
//!   - Go 的 stats ticker / conn-watch 两个辅助 goroutine 在 Rust **内联**进
//!     recv 循环（design D2 登记的有意简化）；周期行 10s 仅值变化时打印；
//!     final 行 Rust 退出**必打**（Go 仅取消路径 racy best-effort——spec 登记
//!     为确定性超集的有意差异）

/// RTP 固定头长度（RFC 3550，无 CSRC/extension）。
pub const RTP_HEADER_LEN: usize = 12;

/// H.264 NAL 类型常量（transmux keyframe 判定 / FrameBuffer 种子分类共用）。
pub const NAL_TYPE_NON_IDR: u8 = 1;
/// IDR（keyframe）。
pub const NAL_TYPE_IDR: u8 = 5;
/// SPS。
pub const NAL_TYPE_SPS: u8 = 7;
/// PPS。
pub const NAL_TYPE_PPS: u8 = 8;

/// RTP 头解析结果（最小字段集，外机不发 CSRC/extension；锚 Go `rtpPacket`）。
///
/// `payload` 持有所有权拷贝：Go receiver 在 `reasm.Push` 前显式深拷 payload
/// （read loop 复用缓冲会被下一次 ReadFromUDP 覆盖）；Rust 把这次必然的
/// per-packet 拷贝收进 [`parse_rtp`]，重组器与测试可安全持有。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtpPacket {
    /// marker bit（frame 边界唯一可信信号）。
    pub marker: bool,
    /// payload type（仅解析存字段，不过滤——Go 等价）。
    pub pt: u8,
    /// 16-bit 序列号（回绕比较见 reassembler）。
    pub seq: u16,
    /// 90kHz RTP 时间戳。
    pub timestamp: u32,
    /// 同步源标识。
    pub ssrc: u32,
    /// RTP payload（头后全部字节；P=1 时含 padding，等价 Go）。
    pub payload: Vec<u8>,
}

/// RTP 解析错误（语义等价 Go `parseRTP` 的 error 上下文，message 非 parity 面）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtpError {
    /// 人类可读失败原因。
    pub reason: String,
}

impl core::fmt::Display for RtpError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "rtp: {}", self.reason)
    }
}

impl std::error::Error for RtpError {}

/// 解析最小 RTP 包（V=2 X=0 CC=0；外机实测）。锚 Go `parseRTP`。
///
/// 校验顺序与 Go 一致：长度 → version → CC → X。P 位不检（P=1 接受），
/// PT 仅解析。多字节字段 `from_be_bytes`。
pub fn parse_rtp(b: &[u8]) -> Result<RtpPacket, RtpError> {
    if b.len() < RTP_HEADER_LEN {
        return Err(RtpError {
            reason: format!("too short ({} < {})", b.len(), RTP_HEADER_LEN),
        });
    }
    let v = (b[0] >> 6) & 0x03;
    if v != 2 {
        return Err(RtpError {
            reason: format!("version={v}, want 2"),
        });
    }
    let cc = b[0] & 0x0f;
    if cc != 0 {
        return Err(RtpError {
            reason: format!("CSRC count={cc} not supported"),
        });
    }
    let x = (b[0] >> 4) & 0x01;
    if x != 0 {
        return Err(RtpError {
            reason: "extension header not supported".to_string(),
        });
    }
    Ok(RtpPacket {
        marker: (b[1] >> 7) == 1,
        pt: b[1] & 0x7f,
        seq: u16::from_be_bytes([b[2], b[3]]),
        timestamp: u32::from_be_bytes([b[4], b[5], b[6], b[7]]),
        ssrc: u32::from_be_bytes([b[8], b[9], b[10], b[11]]),
        payload: b[RTP_HEADER_LEN..].to_vec(),
    })
}

/// H.264 NAL 单元（含 1-byte header + RBSP payload；不含 Annex-B start code）。
/// 锚 Go `nalUnit`。
///
/// `data` 持有所有权：切 frame 时 NAL 字节脱离重组 buffer（Go 深拷的等价物），
/// 下一 frame 复写 buffer 不会污染已交付的 NAL。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NalUnit {
    /// NAL 类型（5 IDR / 7 SPS / 8 PPS / 1 non-IDR / ...）。
    pub nal_type: u8,
    /// 包含 NAL header byte（offset 0 = 0x67/0x68/0x65/0x61 等）。
    pub data: Vec<u8>,
    /// RTP 时间戳 90kHz。
    pub timestamp: u32,
}

/// 4 字节 Annex-B start code `00 00 00 01`（实测外机用 4 字节版本，
/// observations §3 + spec format.md）。
const START_CODE: [u8; 4] = [0x00, 0x00, 0x00, 0x01];

fn find_start_code(hay: &[u8]) -> Option<usize> {
    hay.windows(START_CODE.len()).position(|w| w == START_CODE)
}

/// 从字节流中拆出 Annex-B start-code 分隔的 NAL 单元。锚 Go `extractNALsAnnexB`。
///
/// 返回的 `NalUnit.data` 不含 start code，从 NAL header byte 开始（所有权拷贝）。
/// 无 start code = 续传 fragment，整段作为单个 NAL 返回（后续 transmux 不切，原样写出）；
/// 第一个 start code 之前若有数据，视作前续分片。
pub fn extract_nals_annexb(payload: &[u8], ts: u32) -> Vec<NalUnit> {
    let mut out = Vec::new();
    let idx = match find_start_code(payload) {
        None => {
            if !payload.is_empty() {
                out.push(NalUnit {
                    nal_type: payload[0] & 0x1f,
                    data: payload.to_vec(),
                    timestamp: ts,
                });
            }
            return out;
        }
        Some(i) => i,
    };
    if idx > 0 {
        out.push(NalUnit {
            nal_type: payload[0] & 0x1f,
            data: payload[..idx].to_vec(),
            timestamp: ts,
        });
    }
    let mut rest = &payload[idx + START_CODE.len()..];
    loop {
        match find_start_code(rest) {
            None => {
                if !rest.is_empty() {
                    out.push(NalUnit {
                        nal_type: rest[0] & 0x1f,
                        data: rest.to_vec(),
                        timestamp: ts,
                    });
                }
                return out;
            }
            Some(next) => {
                let nal = &rest[..next];
                if !nal.is_empty() {
                    out.push(NalUnit {
                        nal_type: nal[0] & 0x1f,
                        data: nal.to_vec(),
                        timestamp: ts,
                    });
                }
                rest = &rest[next + START_CODE.len()..];
            }
        }
    }
}

// ---------------------------------------------------------------------------
// receiver 主循环（锚 Go BindUDP / RTPReceiver.RunWithConn）
// ---------------------------------------------------------------------------

use super::frame_buffer::FrameBuffer;
use super::reassembler::{FrameReassembler, ReassemblerStats};
use super::{LogFn, SharedLogFn};
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// read loop 的 socket read-timeout（轮询 stop flag 用；锚 Go 1s SetReadDeadline）。
const RECV_POLL_TIMEOUT: Duration = Duration::from_secs(1);

/// 单包接收缓冲（锚 Go `buf := make([]byte, 2048)`；外机包 ≤ MTU）。
const RECV_BUF_LEN: usize = 2048;

/// stats 周期行间隔（ms，生产 10s）。test-tunable（D8：`AtomicU32` 毫秒，
/// MIPS32 禁 64-bit 原子）——CI 禁真实 10s 等待。
static STATS_INTERVAL_MS: AtomicU32 = AtomicU32::new(10_000);

fn stats_interval() -> Duration {
    Duration::from_millis(u64::from(STATS_INTERVAL_MS.load(Ordering::SeqCst)))
}

/// 测试专用：临时缩短 stats 周期（ms）。返回旧值（调用方负责复原）。
#[doc(hidden)]
pub fn set_stats_interval_ms_for_test(ms: u64) -> u64 {
    u64::from(STATS_INTERVAL_MS.swap(ms as u32, Ordering::SeqCst))
}

/// 同步绑定 UDP 监听 socket（addr 如 ":9880" 形式须带 host，例 "0.0.0.0:9880"），
/// 失败立刻返错。锚 Go `BindUDP`。
///
/// 与 run 拆分的原因（抄 Go 注释语义）：Manager.Start 必须先确认 UDP socket 已绑定
/// 才告知外机推流——否则外机以为流可达而 daemon 永远收不到包。
/// 调用方拿到 socket 后传给 [`RtpReceiver::run_with_conn`] 接管 read loop。
pub fn bind_udp(addr: &str) -> std::io::Result<UdpSocket> {
    UdpSocket::bind(addr)
}

/// SSRC sink 回调形态（session 层注入：写 `AtomicU32` streamSSRC）。
pub type SsrcSink = Box<dyn Fn(u32) + Send + Sync>;

/// 测试 hook 形态（每个合法 RTP 包调一次；锚 Go `PacketHook`）。
pub type PacketHook = Box<dyn Fn(&RtpPacket) + Send + Sync>;

/// UDP 9880 RTP receiver：解析 RTP + 重组 frame + 推 NAL 进 [`FrameBuffer`]。
/// 锚 Go `RTPReceiver`。
///
/// `logf` 用 [`SharedLogFn`]（Arc 形态）：重组器需独立持有 log hook
/// （drop 即时行从重组器内部打），receiver 自身行与之共享同一 sink。
pub struct RtpReceiver {
    /// 可选 log hook（receiver 行 + 派生给重组器）。
    pub logf: Option<SharedLogFn>,
    /// NAL 写入目标。
    pub buf: Arc<FrameBuffer>,
    /// 解析到外机 SSRC 时回调（**仅首包**锁存；RTCP RR report block 字段用——
    /// 外机中途重启换 SSRC 不更新，Go 已知行为保持）。
    pub ssrc_sink: Option<SsrcSink>,
    /// 测试 hook：每收到一个合法 RTP 包调一次（锚 Go `PacketHook`）。
    pub packet_hook: Option<PacketHook>,
}

impl RtpReceiver {
    fn logf(&self, msg: &str) {
        if let Some(f) = &self.logf {
            f(msg);
        }
    }

    /// 接管已绑定的 socket 跑 read loop，直到 `stop` 置位（返 `Ok`）或致命
    /// recv 错误（返 `Err`）。锚 Go `RunWithConn`（ctx → `AtomicBool` stop flag）。
    ///
    /// socket 所有权移入，退出时随 drop 关闭（Go `defer conn.Close()` 等价）。
    /// `expect_src_ip` 非空时仅处理来自该源 IP 的包（防御邻居外机/广播误入）。
    ///
    /// stats ticker 内联：周期行（10s，仅值变化时打）+ 退出必打 final 行
    /// （含错误退出——spec 登记的确定性超集）。
    pub fn run_with_conn(
        &self,
        conn: UdpSocket,
        expect_src_ip: &str,
        stop: &AtomicBool,
    ) -> std::io::Result<()> {
        let local = conn
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "?".into());
        self.logf(&format!(
            "video: rtp listening on {local} (expect src={expect_src_ip})"
        ));

        conn.set_read_timeout(Some(RECV_POLL_TIMEOUT))?;

        // 重组器持派生 log hook（drop 即时行从其内部打）。
        let reasm_logf: Option<LogFn> = self.logf.as_ref().map(|a| -> LogFn {
            let a = Arc::clone(a);
            Box::new(move |m: &str| a(m))
        });
        let mut reasm = FrameReassembler::new(reasm_logf);

        // stats ticker 内联（design D2 有意简化）：仅值变化时打周期行。
        let interval = stats_interval();
        let mut last_stats = ReassemblerStats::default();
        let mut next_stats = Instant::now() + interval;

        let mut ssrc_seen = false;
        let mut buf = [0u8; RECV_BUF_LEN];
        loop {
            if stop.load(Ordering::SeqCst) {
                self.logf(&reasm.stats().final_line());
                return Ok(());
            }
            // 内联 stats tick：socket 1s timeout 保证本检查至少每秒跑一次。
            let now = Instant::now();
            if now >= next_stats {
                let s = reasm.stats();
                if s != last_stats {
                    self.logf(&s.periodic_line());
                    last_stats = s;
                }
                next_stats = now + interval;
            }

            let (n, src) = match conn.recv_from(&mut buf) {
                Ok(v) => v,
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    // read-timeout 击中：回环轮询 stop flag / stats tick。
                    continue;
                }
                Err(e) => {
                    // 致命 recv 错误：final 行仍打（退出必打），错误上抛。
                    self.logf(&reasm.stats().final_line());
                    return Err(e);
                }
            };
            if !expect_src_ip.is_empty() && src.ip().to_string() != expect_src_ip {
                continue;
            }
            let pkt = match parse_rtp(&buf[..n]) {
                Ok(p) => p,
                Err(e) => {
                    self.logf(&format!("video: rtp parse: {e}"));
                    continue;
                }
            };
            // SSRC 仅首包锁存（锚 Go `if !ssrcSeen && r.SSRCSink != nil`）。
            if !ssrc_seen {
                if let Some(sink) = &self.ssrc_sink {
                    sink(pkt.ssrc);
                    ssrc_seen = true;
                }
            }
            if let Some(hook) = &self.packet_hook {
                hook(&pkt);
            }
            // payload 所有权已在 parse_rtp 深拷（read loop 复用 buf 不会踩烂
            // 重组器持有的 frame 字节流——Go 显式 pktCopy 深拷的等价物）。
            let (nals, _) = reasm.push(pkt);
            for nal in nals {
                self.buf.push(nal);
            }
        }
    }
}

// ── 单测（移植 Go rtp_test.go 等价用例 + P=1 接受边界）─────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        assert!(s.len().is_multiple_of(2), "odd hex: {s:?}");
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// 移植 Go `TestParseRTP_FirstPacketSample`：实测首包头
    /// （observations §3 / spec format.md）。
    #[test]
    fn parse_rtp_first_packet_sample() {
        let hdr = hex("8062c55682f6d2103e4ab183"); // 12B fixed header
        let payload = hex("000000016764c016ac1b1aa0");
        let mut pkt = hdr;
        pkt.extend_from_slice(&payload);

        let got = parse_rtp(&pkt).expect("parse_rtp");
        assert_eq!(got.pt, 98, "PT");
        assert_eq!(got.seq, 0xc556, "seq");
        assert_eq!(got.timestamp, 0x82f6d210, "timestamp");
        assert_eq!(got.ssrc, 0x3e4ab183, "ssrc");
        assert!(!got.marker, "marker");
        assert_eq!(got.payload, payload, "payload");
    }

    /// 移植 Go `TestParseRTP_RejectsWrongVersion`。
    #[test]
    fn parse_rtp_rejects_wrong_version() {
        let mut bad = vec![0u8; 12];
        bad[0] = 0x40; // V=1
        assert!(parse_rtp(&bad).is_err(), "V=1 must be rejected");
    }

    /// 移植 Go `TestParseRTP_RejectsCSRCList`。
    #[test]
    fn parse_rtp_rejects_csrc_list() {
        let mut bad = vec![0u8; 12];
        bad[0] = 0x82; // V=2 CC=2
        assert!(parse_rtp(&bad).is_err(), "CC>0 must be rejected");
    }

    /// X=1 拒收（Go parseRTP extension 分支）。
    #[test]
    fn parse_rtp_rejects_extension() {
        let mut bad = vec![0u8; 12];
        bad[0] = 0x90; // V=2 X=1
        assert!(parse_rtp(&bad).is_err(), "X=1 must be rejected");
    }

    /// 移植 Go `TestParseRTP_RejectsTooShort`。
    #[test]
    fn parse_rtp_rejects_too_short() {
        assert!(parse_rtp(&[0x80, 0x62]).is_err(), "short packet accepted");
    }

    /// P=1 接受集等价（spec 场景「P=1 包接受集等价」）：Go 不校验 P 位，
    /// P=1 包被接受且 padding 字节留在 payload。
    #[test]
    fn parse_rtp_accepts_padding_bit() {
        let mut pkt = vec![0u8; 16];
        pkt[0] = 0xa0; // V=2 P=1 X=0 CC=0
        pkt[1] = 0x62; // M=0 PT=98
        pkt[2] = 0x00;
        pkt[3] = 0x01;
        pkt[12] = 0x61; // payload 首字节
        pkt[15] = 0x03; // RFC 3550 padding count 字节（留在 payload，不剥）
        let got = parse_rtp(&pkt).expect("P=1 must be accepted (Go parity)");
        assert_eq!(got.payload.len(), 4, "padding 字节须留在 payload");
        assert_eq!(got.payload[3], 0x03);
    }

    /// 移植 Go `TestExtractNALsAnnexB_FirstPacket`：SPS+PPS+IDR 拼包。
    #[test]
    fn extract_nals_annexb_first_packet() {
        let payload = hex(&format!(
            "{}{}{}{}{}{}",
            "00000001", "6711aa", // SPS (type=7)
            "00000001", "6822bb", // PPS (type=8)
            "00000001", "6533cc", // IDR (type=5)
        ));
        let nals = extract_nals_annexb(&payload, 1000);
        assert_eq!(nals.len(), 3, "want 3 NALs");
        assert_eq!(nals[0].nal_type, NAL_TYPE_SPS);
        assert_eq!(nals[1].nal_type, NAL_TYPE_PPS);
        assert_eq!(nals[2].nal_type, NAL_TYPE_IDR);
        for n in &nals {
            assert_eq!(n.timestamp, 1000);
        }
        assert_eq!(nals[0].data, hex("6711aa"));
        assert_eq!(nals[2].data, hex("6533cc"));
    }

    /// 移植 Go `TestExtractNALsAnnexB_NoStartCodeIsFragment`：无 start code = 续传分片。
    #[test]
    fn extract_nals_annexb_no_start_code_is_fragment() {
        let payload = hex("6133aabbcc"); // type=1 P-frame fragment 续传
        let nals = extract_nals_annexb(&payload, 0);
        assert_eq!(nals.len(), 1);
        assert_eq!(nals[0].nal_type, NAL_TYPE_NON_IDR);
        assert_eq!(nals[0].data, payload);
    }

    /// 首个 start code 之前有数据 → 视作前续分片（Go idx>0 分支）。
    #[test]
    fn extract_nals_annexb_leading_fragment_before_start_code() {
        let payload = hex(&format!("{}{}{}", "61aabb", "00000001", "6533cc"));
        let nals = extract_nals_annexb(&payload, 7);
        assert_eq!(nals.len(), 2);
        assert_eq!(nals[0].nal_type, NAL_TYPE_NON_IDR, "前续分片 type 取首字节低 5 位");
        assert_eq!(nals[0].data, hex("61aabb"));
        assert_eq!(nals[1].nal_type, NAL_TYPE_IDR);
    }

    /// 空 payload / 空 NAL 段不产出（Go len 守卫）。
    #[test]
    fn extract_nals_annexb_empty_and_adjacent_start_codes() {
        assert!(extract_nals_annexb(&[], 0).is_empty(), "empty payload");
        // 紧邻两个 start code（中间空 NAL）→ 跳过空段
        let payload = hex(&format!("{}{}{}", "00000001", "00000001", "6533cc"));
        let nals = extract_nals_annexb(&payload, 0);
        assert_eq!(nals.len(), 1, "空 NAL 段须跳过");
        assert_eq!(nals[0].nal_type, NAL_TYPE_IDR);
    }
}

// ── receiver 主循环单测（真 UDP socket pair；移植 Go RTPReceiver 用例）──────────

#[cfg(test)]
mod receiver_tests {
    use super::*;
    use std::sync::mpsc::RecvTimeoutError;
    use std::sync::Mutex;
    use std::thread;

    /// 构 RTP wire 包：12B 头 + payload（锚 Go mkRTP）。
    fn mk_rtp(seq: u16, ts: u32, ssrc: u32, marker: bool, payload: &[u8]) -> Vec<u8> {
        let mut pkt = vec![0u8; 12 + payload.len()];
        pkt[0] = 0x80; // V=2 P=0 X=0 CC=0
        pkt[1] = if marker { 0xe2 } else { 0x62 }; // M | PT=98
        pkt[2..4].copy_from_slice(&seq.to_be_bytes());
        pkt[4..8].copy_from_slice(&ts.to_be_bytes());
        pkt[8..12].copy_from_slice(&ssrc.to_be_bytes());
        pkt[12..].copy_from_slice(payload);
        pkt
    }

    /// 起 receiver 线程；返 (target_addr, frame_buf, stop, join, log_lines)。
    #[allow(clippy::type_complexity)]
    fn start_receiver(
        expect_src_ip: &'static str,
        ssrc_sink: Option<Box<dyn Fn(u32) + Send + Sync>>,
    ) -> (
        std::net::SocketAddr,
        Arc<FrameBuffer>,
        Arc<AtomicBool>,
        thread::JoinHandle<std::io::Result<()>>,
        Arc<Mutex<Vec<String>>>,
    ) {
        let conn = bind_udp("127.0.0.1:0").expect("bind_udp");
        let target = conn.local_addr().unwrap();
        let fbuf = Arc::new(FrameBuffer::new());
        let stop = Arc::new(AtomicBool::new(false));
        let lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        let sink_lines = Arc::clone(&lines);
        let logf: SharedLogFn = Arc::new(move |m: &str| {
            sink_lines.lock().unwrap().push(m.to_string());
        });
        let r = RtpReceiver {
            logf: Some(logf),
            buf: Arc::clone(&fbuf),
            ssrc_sink,
            packet_hook: None,
        };
        let stop_c = Arc::clone(&stop);
        let join = thread::spawn(move || r.run_with_conn(conn, expect_src_ip, &stop_c));
        (target, fbuf, stop, join, lines)
    }

    /// 移植 Go `TestRTPReceiver_NALsNotCorruptedByBufferReuse`（receiver 层
    /// buffer-reuse 安全；reassembler 层版本在 reassembler.rs 五件套之五）。
    #[test]
    fn nals_not_corrupted_by_buffer_reuse() {
        const NAL_LEN: usize = 200;
        let mut payload1 = vec![0xAAu8; NAL_LEN];
        payload1[0] = 0x61;
        let mut payload2 = vec![0xBBu8; NAL_LEN];
        payload2[0] = 0x61;
        // Annex-B start code 前缀（重组器整 frame 切 NAL）。
        let mut p1 = vec![0x00, 0x00, 0x00, 0x01];
        p1.extend_from_slice(&payload1);
        let mut p2 = vec![0x00, 0x00, 0x00, 0x01];
        p2.extend_from_slice(&payload2);

        let (target, fbuf, stop, join, _lines) = start_receiver("", None);
        let (rx, _sub) = fbuf.subscribe();

        let cli = UdpSocket::bind("127.0.0.1:0").expect("client bind");
        cli.send_to(&mk_rtp(1, 1000, 0xdead_beef, true, &p1), target)
            .expect("send p1");
        thread::sleep(Duration::from_millis(50));
        cli.send_to(&mk_rtp(2, 2000, 0xdead_beef, true, &p2), target)
            .expect("send p2");

        let first = rx.recv_timeout(Duration::from_secs(2)).expect("NAL 1");
        let second = rx.recv_timeout(Duration::from_secs(2)).expect("NAL 2");
        for (i, &b) in first.data.iter().enumerate().skip(1) {
            assert_eq!(b, 0xAA, "first NAL byte[{i}] corrupted by buf reuse");
        }
        for (i, &b) in second.data.iter().enumerate().skip(1) {
            assert_eq!(b, 0xBB, "second NAL byte[{i}]");
        }
        assert_eq!(first.timestamp, 1000);

        stop.store(true, Ordering::SeqCst);
        join.join().unwrap().expect("receiver exit");
    }

    /// expectSrcIP 过滤：来源不匹配的包整包丢弃（不解析不投递）。
    #[test]
    fn expect_src_ip_filters_foreign_packets() {
        // 127.0.0.1 来包，但期望源是 10.9.9.9 → 全部过滤。
        let (target, fbuf, stop, join, _lines) = start_receiver("10.9.9.9", None);
        let (rx, _sub) = fbuf.subscribe();

        let cli = UdpSocket::bind("127.0.0.1:0").unwrap();
        let mut payload = vec![0x00, 0x00, 0x00, 0x01, 0x61];
        payload.extend_from_slice(&[0x42; 8]);
        cli.send_to(&mk_rtp(1, 100, 1, true, &payload), target)
            .unwrap();

        assert_eq!(
            rx.recv_timeout(Duration::from_millis(200)).unwrap_err(),
            RecvTimeoutError::Timeout,
            "filtered packet must not reach FrameBuffer"
        );
        stop.store(true, Ordering::SeqCst);
        join.join().unwrap().expect("receiver exit");
    }

    /// SSRC 仅首包锁存（spec：外机中途换 SSRC 不更新，已知行为保持）。
    #[test]
    fn ssrc_sink_latches_first_packet_only() {
        let seen: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_c = Arc::clone(&seen);
        let sink: Box<dyn Fn(u32) + Send + Sync> = Box::new(move |s| {
            seen_c.lock().unwrap().push(s);
        });
        let (target, fbuf, stop, join, _lines) = start_receiver("", Some(sink));
        let (rx, _sub) = fbuf.subscribe();

        let cli = UdpSocket::bind("127.0.0.1:0").unwrap();
        let payload = [0x00, 0x00, 0x00, 0x01, 0x61, 0x01];
        cli.send_to(&mk_rtp(1, 100, 0x1111_1111, true, &payload), target)
            .unwrap();
        let _ = rx.recv_timeout(Duration::from_secs(2)).expect("NAL 1");
        // 第二包换 SSRC——sink 不得再触发。
        cli.send_to(&mk_rtp(2, 200, 0x2222_2222, true, &payload), target)
            .unwrap();
        let _ = rx.recv_timeout(Duration::from_secs(2)).expect("NAL 2");

        let got = seen.lock().unwrap().clone();
        assert_eq!(got, vec![0x1111_1111], "SSRC must latch on first packet only");

        stop.store(true, Ordering::SeqCst);
        join.join().unwrap().expect("receiver exit");
    }

    /// stop flag 1s 轮询退出 + 退出必打 final 行（含 listening 起始行）。
    #[test]
    fn stop_flag_exits_within_poll_window_and_prints_final_line() {
        let (target, _fbuf, stop, join, lines) = start_receiver("", None);

        // 非法包（parse 错）也要 log 但不致命。
        let cli = UdpSocket::bind("127.0.0.1:0").unwrap();
        cli.send_to(&[0x40, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], target)
            .unwrap();
        thread::sleep(Duration::from_millis(100));

        let t0 = Instant::now();
        stop.store(true, Ordering::SeqCst);
        join.join().unwrap().expect("receiver exit Ok on stop");
        assert!(
            t0.elapsed() < Duration::from_millis(1500),
            "stop must be honored within ~1s poll window, took {:?}",
            t0.elapsed()
        );

        let got = lines.lock().unwrap();
        assert!(
            got.iter().any(|l| l.starts_with("video: rtp listening on ")),
            "missing listening line: {got:?}"
        );
        assert!(
            got.iter().any(|l| l.starts_with("video: rtp parse: ")),
            "missing parse error line: {got:?}"
        );
        assert!(
            got.iter().any(|l| l.starts_with("video: rtp stats final ")),
            "exit must print final stats line: {got:?}"
        );
    }

    /// stats 周期行（tunable 压缩到 50ms）：仅值变化时打印。
    ///
    /// 内联 ticker 在 recv 唤醒时检查（1s read-timeout 粒度兜底）；测试用
    /// malformed 包唤醒循环（parse 错不入 stats）驱动 tick 判定，避免真实等待。
    #[test]
    fn periodic_stats_line_only_on_change() {
        let prev = set_stats_interval_ms_for_test(50);

        let (target, fbuf, stop, join, lines) = start_receiver("", None);
        let (_rx, _sub) = fbuf.subscribe();
        let cli = UdpSocket::bind("127.0.0.1:0").unwrap();
        let payload = [0x00, 0x00, 0x00, 0x01, 0x61, 0x01];
        cli.send_to(&mk_rtp(1, 100, 1, true, &payload), target)
            .unwrap();

        // 过一个周期后用 malformed 包（V=1，parse 错、stats 不变）唤醒循环 →
        // tick 检查见 stats 已变 → 打第一条周期行。
        thread::sleep(Duration::from_millis(120));
        cli.send_to(&[0x40u8; 12], target).unwrap();
        // 再过一个周期再唤醒一次 → stats 与上次相同 → 不再打。
        thread::sleep(Duration::from_millis(120));
        cli.send_to(&[0x40u8; 12], target).unwrap();
        thread::sleep(Duration::from_millis(60));

        stop.store(true, Ordering::SeqCst);
        join.join().unwrap().expect("receiver exit");
        set_stats_interval_ms_for_test(prev);

        let got = lines.lock().unwrap();
        let periodic: Vec<&String> = got
            .iter()
            .filter(|l| l.starts_with("video: rtp stats packets="))
            .collect();
        assert_eq!(
            periodic.len(),
            1,
            "stats unchanged after first tick must not re-print: {got:?}"
        );
        assert!(
            periodic[0].contains("packets=1 nals=1 frames=1"),
            "periodic line content: {}",
            periodic[0]
        );
    }
}
