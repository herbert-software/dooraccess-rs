//! rtcp：RTCP keepalive sender + RR/SDES/BYE 构造（移植 Go `internal/video/rtcp.go` 全部）。
//!
//! 协议锚 `specs/anjubao-video-stream/messages/udp9881-rtcp/format.md` +
//! observations §4；逐字节 golden 锚 Go 导出（`testdata/golden/video_rtcp.txt`）。
//!
//! 等价纪律（spec「RTCP keepalive 与 BYE 字节等价」）：
//!   - 5s 周期向 outdoor:6671 发 RR+SDES 复合包；ticker **无 0 时刻 tick——首包
//!     在 t≈5s**（等价 Go `time.Ticker`）
//!   - RR 32B（V=2/RC=1/PT=201/length 字段=7，loss/jitter 全 0）；SDES 含 CNAME
//!     item，**CNAME 字面必须保持 `"dooraccess-go"`**（design D7：字节 golden
//!     纪律，禁改名——待替换生产后要改走独立 change 连 golden 一起换）
//!   - 外机 SSRC 未知（RTP 未到达）时跳过本轮；SSRC 取值由调用方（session 层）
//!     仅首包锁存
//!   - 失败模式：UDP dial 失败 → 本函数带错返回（keepalive 线程退出、**session
//!     照常存活**，无保活靠 TTL 兜底——线程包装归 session 层）；周期 send 失败 →
//!     仅 log、继续下轮
//!   - 全部字段 `to_be_bytes` NBO
//!
//! 计时套路：分片 `park_timeout` + 单调 `Instant` 判据（复刻 ② auto-hangup 计时
//! 线程模式，`self_unlock.rs:469`——spurious wakeup 不会误判早发）；周期 ms 存
//! `AtomicU32`（D8，MIPS32 禁 64-bit 原子），test-tunable 压缩。

use std::io;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use super::LogFn;

/// RTCP packet type：Receiver Report。
pub const RTCP_PT_RR: u8 = 201; // 0xc9
/// RTCP packet type：Source Description。
pub const RTCP_PT_SDES: u8 = 202; // 0xca
/// RTCP packet type：Goodbye。
pub const RTCP_PT_BYE: u8 = 203; // 0xcb

/// SDES CNAME item 字面。锚 Go `rtcpCName`——**字节等价 golden 纪律，禁改名**
/// （design D7：改成 dooraccess-rs 会让 SDES 包与 Go fixture 字节漂移，golden 直接红）。
pub const RTCP_CNAME: &str = "dooraccess-go";

/// 报告周期（ms，生产 5s）。test-tunable（D8 `AtomicU32` 毫秒）。
static REPORT_INTERVAL_MS: AtomicU32 = AtomicU32::new(5_000);

/// 分片轮询片长（援引 self_unlock HANGUP_POLL_SLICE 20ms 惯用法）。
const TICK_POLL_SLICE: Duration = Duration::from_millis(20);

fn report_interval() -> Duration {
    Duration::from_millis(u64::from(REPORT_INTERVAL_MS.load(Ordering::SeqCst)))
}

/// 测试专用：临时缩短报告周期（ms）。返回旧值（调用方负责复原）。
#[doc(hidden)]
pub fn set_report_interval_ms_for_test(ms: u64) -> u64 {
    u64::from(REPORT_INTERVAL_MS.swap(ms as u32, Ordering::SeqCst))
}

// ---------------------------------------------------------------------------
// packet builders（锚 Go buildRR / buildSDES / buildRRSDES / buildBYE）
// ---------------------------------------------------------------------------

/// 构造 32B Receiver Report：V=2 P=0 RC=1 PT=201 length=7 word（= (32-4)/4）。
/// 锚 Go `buildRR`。
///
/// 1 个报告 block，被报告 SSRC = `stream_ssrc`，loss/jitter 字段全 0
/// （外机不响应 RTCP 反馈，填精确值无意义）。
pub fn build_rr(reporter_ssrc: u32, stream_ssrc: u32) -> Vec<u8> {
    let mut pkt = vec![0u8; 32];
    pkt[0] = 0x80 | 0x01; // V=2 (10) | P=0 | RC=1
    pkt[1] = RTCP_PT_RR;
    pkt[2..4].copy_from_slice(&7u16.to_be_bytes()); // length in 32-bit words minus 1
    pkt[4..8].copy_from_slice(&reporter_ssrc.to_be_bytes());
    // report block：
    pkt[8..12].copy_from_slice(&stream_ssrc.to_be_bytes());
    // pkt[12..16] fraction lost (0) + cumulative lost (0)
    // pkt[16..20] extended highest seq (0；外机不验证)
    // pkt[20..24] interarrival jitter (0)
    // pkt[24..28] LSR (0)
    // pkt[28..32] DLSR (0)
    pkt
}

/// 构造 SDES 包（含 CNAME item），长度按 4-byte align 补齐。锚 Go `buildSDES`。
///
/// 结构：
///   - 0     V|P|SC = 0x81 (V=2 P=0 SC=1)
///   - 1     PT = 202
///   - 2-3   length in words minus 1
///   - 4-7   chunk SSRC
///   - 8     item type = 1 (CNAME)
///   - 9     item length
///   - 10..  item value（[`RTCP_CNAME`]）
///   - +1    null terminator (0x00)
///   - +pad  补齐 4-byte align
pub fn build_sdes(chunk_ssrc: u32) -> Vec<u8> {
    let cn = RTCP_CNAME.as_bytes();
    // item bytes = 1 (type) + 1 (length) + len(cn) + 1 (null term)
    let item_bytes = 1 + 1 + cn.len() + 1;
    // chunk = 4 (SSRC) + item；整体 SDES 包 = 4 (header) + chunk，pad 到 4 倍数。
    let chunk_size = 4 + item_bytes;
    let pad = (4 - chunk_size % 4) % 4;
    let total_len = 4 + chunk_size + pad;
    let mut pkt = vec![0u8; total_len];
    pkt[0] = 0x81; // V=2 P=0 SC=1
    pkt[1] = RTCP_PT_SDES;
    pkt[2..4].copy_from_slice(&((total_len / 4 - 1) as u16).to_be_bytes());
    pkt[4..8].copy_from_slice(&chunk_ssrc.to_be_bytes());
    pkt[8] = 1; // item type CNAME
    pkt[9] = cn.len() as u8;
    pkt[10..10 + cn.len()].copy_from_slice(cn);
    // null term + pad 已为 0。
    pkt
}

/// 构造周期保活 RR + SDES 复合包。锚 Go `buildRRSDES`。
pub fn build_rr_sdes(reporter_ssrc: u32, stream_ssrc: u32) -> Vec<u8> {
    let rr = build_rr(reporter_ssrc, stream_ssrc);
    let sdes = build_sdes(reporter_ssrc);
    let mut out = Vec::with_capacity(rr.len() + sdes.len());
    out.extend_from_slice(&rr);
    out.extend_from_slice(&sdes);
    out
}

/// 构造 session 终结复合包：空 RR (8B, SC=0) + SDES (CNAME) + BYE (8B)。
/// 锚 Go `buildBYE`（format.md「监视段结束的 BYE 复合包」）。
pub fn build_bye(reporter_ssrc: u32) -> Vec<u8> {
    let mut empty_rr = vec![0u8; 8];
    empty_rr[0] = 0x80; // V=2 RC=0
    empty_rr[1] = RTCP_PT_RR;
    empty_rr[2..4].copy_from_slice(&1u16.to_be_bytes()); // length = 1 word
    empty_rr[4..8].copy_from_slice(&reporter_ssrc.to_be_bytes());

    let sdes = build_sdes(reporter_ssrc);

    let mut bye = vec![0u8; 8];
    bye[0] = 0x81; // V=2 SC=1
    bye[1] = RTCP_PT_BYE;
    bye[2..4].copy_from_slice(&1u16.to_be_bytes());
    bye[4..8].copy_from_slice(&reporter_ssrc.to_be_bytes());

    let mut out = Vec::with_capacity(empty_rr.len() + sdes.len() + bye.len());
    out.extend_from_slice(&empty_rr);
    out.extend_from_slice(&sdes);
    out.extend_from_slice(&bye);
    out
}

// ---------------------------------------------------------------------------
// RtcpSender（锚 Go RTCPSender.Run / SendBYE）
// ---------------------------------------------------------------------------

/// UDP "dial"：bind 临时端口 + connect（等价 Go `net.Dial("udp", dst)`）。
fn udp_dial(dst: &str) -> io::Result<UdpSocket> {
    let sock = UdpSocket::bind("0.0.0.0:0")?;
    sock.connect(dst)?;
    Ok(sock)
}

/// RTCP keepalive sender：周期发 RR+SDES，session 终结发 BYE。锚 Go `RTCPSender`。
#[derive(Default)]
pub struct RtcpSender {
    /// 可选 log hook。
    pub logf: Option<LogFn>,
}

impl RtcpSender {
    fn logf(&self, msg: &str) {
        if let Some(f) = &self.logf {
            f(msg);
        }
    }

    /// 周期（5s，tunable）向 `dst`（outdoor:6671）发 RR+SDES 直到 `stop` 置位。
    /// 锚 Go `Run`（ctx → `AtomicBool` stop flag）。
    ///
    /// - `ssrc_source` 返回当前 RTP 流 SSRC（`None` = 尚未到达，跳过本轮但继续
    ///   ticker，避免空 SSRC 下发）
    /// - `reporter_ssrc` 是本端报告者 SSRC（session 生命周期内不变）
    /// - dial 失败 → 带错返回（keepalive 线程退出、session 照常活，靠 TTL 兜底）
    /// - 周期 send 失败 → 仅 log、继续下轮
    /// - 首包在 t≈周期处（无 0 时刻 tick，等价 Go `time.Ticker`）
    pub fn run(
        &self,
        dst: &str,
        ssrc_source: &dyn Fn() -> Option<u32>,
        reporter_ssrc: u32,
        stop: &AtomicBool,
    ) -> io::Result<()> {
        let conn = udp_dial(dst)?;
        self.logf(&format!(
            "video: rtcp keepalive to {dst} reporter=0x{reporter_ssrc:08x}"
        ));

        let interval = report_interval();
        let mut next_tick = Instant::now() + interval; // 无 0 时刻 tick。
        loop {
            // 分片 park_timeout 等到 next_tick 或 stop（单调 Instant 判据，
            // spurious 提前唤醒不会误判到点——复刻 ② 计时线程模式）。
            loop {
                if stop.load(Ordering::SeqCst) {
                    return Ok(());
                }
                let now = Instant::now();
                if now >= next_tick {
                    break;
                }
                std::thread::park_timeout(TICK_POLL_SLICE.min(next_tick - now));
            }
            next_tick += interval;

            let stream_ssrc = match ssrc_source() {
                Some(s) => s,
                None => continue, // SSRC 未知跳过本轮，下轮重查。
            };
            let pkt = build_rr_sdes(reporter_ssrc, stream_ssrc);
            if let Err(e) = conn.send(&pkt) {
                self.logf(&format!("video: rtcp send: {e}")); // 仅 log，继续下轮。
            }
        }
    }

    /// 一次性发空 RR + SDES + BYE 复合包（session 终结，best-effort）。
    /// 锚 Go `SendBYE`。
    pub fn send_bye(&self, dst: &str, reporter_ssrc: u32) -> io::Result<()> {
        let conn = udp_dial(dst)?;
        conn.send(&build_bye(reporter_ssrc))?;
        Ok(())
    }
}

// ── 单测（移植 Go rtcp_test.go + mock UDP receiver 端到端）─────────────────────
// 逐字节 golden 在 tests/golden_video.rs（video_rtcp.txt：rr/sdes/rrsdes/bye ×3 组 SSRC）。

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::thread;

    /// 移植 Go `TestBuildRR_Format`。
    #[test]
    fn build_rr_format() {
        let pkt = build_rr(0x2238_9cb9, 0x3e4a_b183);
        assert_eq!(pkt.len(), 32);
        assert_eq!(pkt[0], 0x81, "byte 0 = V=2 RC=1");
        assert_eq!(pkt[1], 201, "PT");
        assert_eq!(u16::from_be_bytes([pkt[2], pkt[3]]), 7, "length 字段=7");
        assert_eq!(
            u32::from_be_bytes([pkt[4], pkt[5], pkt[6], pkt[7]]),
            0x2238_9cb9,
            "reporter SSRC"
        );
        assert_eq!(
            u32::from_be_bytes([pkt[8], pkt[9], pkt[10], pkt[11]]),
            0x3e4a_b183,
            "stream SSRC"
        );
        assert!(pkt[12..].iter().all(|&b| b == 0), "loss/jitter 全 0");
    }

    /// 移植 Go `TestBuildSDES_FormatAndAlign`。
    #[test]
    fn build_sdes_format_and_align() {
        let pkt = build_sdes(0x2238_9cb9);
        assert!(pkt.len().is_multiple_of(4), "SDES 必须 4-byte align");
        assert_eq!(pkt[0], 0x81, "V=2 SC=1");
        assert_eq!(pkt[1], 202, "PT");
        assert_eq!(
            u16::from_be_bytes([pkt[2], pkt[3]]) as usize,
            pkt.len() / 4 - 1,
            "length 字段"
        );
        assert_eq!(
            u32::from_be_bytes([pkt[4], pkt[5], pkt[6], pkt[7]]),
            0x2238_9cb9,
            "chunk SSRC"
        );
        assert_eq!(pkt[8], 1, "item type CNAME");
        assert_eq!(pkt[9] as usize, RTCP_CNAME.len(), "item length");
        assert!(
            pkt[10..]
                .windows(RTCP_CNAME.len())
                .any(|w| w == RTCP_CNAME.as_bytes()),
            "CNAME not found in SDES body"
        );
    }

    /// 移植 Go `TestBuildBYE_TripleCompound`：空 RR + SDES + BYE 三段复合。
    #[test]
    fn build_bye_triple_compound() {
        let pkt = build_bye(0x2238_9cb9);
        assert!(pkt.len() >= 8 + 8 + 8, "compound too short");
        assert_eq!(pkt[1], 201, "first PT = 空 RR");
        // 按 length 字段走包，三种 PT 全须出现。
        let (mut has_rr, mut has_sdes, mut has_bye) = (false, false, false);
        let mut idx = 0;
        while idx + 4 <= pkt.len() {
            let pt = pkt[idx + 1];
            let words = u16::from_be_bytes([pkt[idx + 2], pkt[idx + 3]]) as usize;
            let size = (words + 1) * 4;
            if idx + size > pkt.len() {
                break;
            }
            match pt {
                201 => has_rr = true,
                202 => has_sdes = true,
                203 => has_bye = true,
                _ => {}
            }
            idx += size;
        }
        assert!(
            has_rr && has_sdes && has_bye,
            "missing PT: RR={has_rr} SDES={has_sdes} BYE={has_bye}"
        );
        assert_eq!(idx, pkt.len(), "compound 必须恰好走完");
    }

    /// mock UDP receiver 端到端：周期发 RR+SDES（tunable 80ms 压缩）；
    /// 首包 t≈周期（无 0 时刻 tick）；字节与 build_rr_sdes 一致。
    #[test]
    fn run_sends_rr_sdes_periodically_first_at_interval() {
        let prev = set_report_interval_ms_for_test(80);

        let recv = UdpSocket::bind("127.0.0.1:0").expect("mock receiver");
        let dst = recv.local_addr().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_c = Arc::clone(&stop);
        let join = thread::spawn(move || {
            let r = RtcpSender::default();
            r.run(&dst, &|| Some(0x3e4a_b183), 0x2238_9cb9, &stop_c)
        });

        // 无 0 时刻 tick：周期 80ms，前 ~30ms 内不应有包。
        recv.set_read_timeout(Some(Duration::from_millis(30))).unwrap();
        let mut buf = [0u8; 256];
        assert!(
            recv.recv_from(&mut buf).is_err(),
            "first packet must not arrive at t=0 (no zero-time tick)"
        );

        // 首包在 t≈80ms 到达，字节 == build_rr_sdes。
        recv.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let (n, _) = recv.recv_from(&mut buf).expect("first keepalive packet");
        assert_eq!(
            &buf[..n],
            build_rr_sdes(0x2238_9cb9, 0x3e4a_b183).as_slice(),
            "periodic packet bytes"
        );
        // 第二包继续到达（周期持续）。
        let (n2, _) = recv.recv_from(&mut buf).expect("second keepalive packet");
        assert_eq!(n2, n);

        stop.store(true, Ordering::SeqCst);
        join.join().unwrap().expect("run exits Ok on stop");
        set_report_interval_ms_for_test(prev);
    }

    /// 移植 Go `TestRTCPSender_RunSkipsWhenSSRCUnknown`：source 返 None 不发包、
    /// 不报错不退出。
    #[test]
    fn run_skips_when_ssrc_unknown() {
        let prev = set_report_interval_ms_for_test(50);

        let recv = UdpSocket::bind("127.0.0.1:0").expect("mock receiver");
        let dst = recv.local_addr().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_c = Arc::clone(&stop);
        let join = thread::spawn(move || {
            let r = RtcpSender::default();
            r.run(&dst, &|| None, 0x2238_9cb9, &stop_c)
        });

        recv.set_read_timeout(Some(Duration::from_millis(180))).unwrap();
        let mut buf = [0u8; 256];
        assert!(
            recv.recv_from(&mut buf).is_err(),
            "SSRC unknown must skip all rounds"
        );

        let t0 = Instant::now();
        stop.store(true, Ordering::SeqCst);
        join.join().unwrap().expect("run exits Ok on stop");
        assert!(
            t0.elapsed() < Duration::from_millis(500),
            "stop must be honored promptly"
        );
        set_report_interval_ms_for_test(prev);
    }

    /// dial 失败（spec 场景「RTCP dial 失败不拖垮 session」的本模块面）：
    /// run 带错返回——线程退出、session 照常活由 session 层兜底。
    #[test]
    fn run_dial_failure_returns_error() {
        let r = RtcpSender::default();
        let stop = AtomicBool::new(false);
        let err = r
            .run("definitely-not-an-addr", &|| Some(1), 0x2238_9cb9, &stop)
            .expect_err("invalid dst must fail dial");
        let _ = err;
    }

    /// 移植 Go `TestRTCPSender_SendBYE_ReachesReceiver`：BYE 复合包到达且含三 PT。
    #[test]
    fn send_bye_reaches_receiver() {
        let recv = UdpSocket::bind("127.0.0.1:0").expect("mock receiver");
        let dst = recv.local_addr().unwrap().to_string();
        let r = RtcpSender::default();
        r.send_bye(&dst, 0x2238_9cb9).expect("send_bye");

        recv.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let mut buf = [0u8; 256];
        let (n, _) = recv.recv_from(&mut buf).expect("BYE compound");
        assert_eq!(
            &buf[..n],
            build_bye(0x2238_9cb9).as_slice(),
            "BYE compound bytes"
        );
    }
}
