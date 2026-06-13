//! reassembler：专有分片 frame 重组器。
//!
//! 为什么需要这个：外机推流**不是 RFC 6184**，是 raw H.264 Annex-B 字节流按 MTU
//! 任意切片（memory `anjubao_rtp_fragmentation_proprietary`，expA.pcap 1539 包实证：
//! 仅 ~19% 包含起始码）。必须按 RTP seq 重组 frame 字节流后整 frame 切 NAL；
//! 对单包独立扫 start code 会静默丢 81%。
//!
//! 并发模型：stats 周期日志（10s）由 receiver 主循环**内联**触发，重组器单线程
//! 独占 —— 无锁、无原子（MIPS32 红线禁 64-bit 原子，u64 计数器走普通字段天然合规）。
//! 本模块提供 [`ReassemblerStats::periodic_line`] / [`ReassemblerStats::final_line`]
//! 纯格式化函数 + `PartialEq`（「仅值变化时打印」的 `s != last` 判据），
//! 周期触发与 final 行的打印时机归 receiver 主循环。

use super::rtp::{extract_nals_annexb, NalUnit, RtpPacket};
use super::LogFn;

/// 排序窗口：按 seq 缓 8 个包（~80ms 容忍乱序，外机 ~99 包/s）。
pub const REASSEMBLER_WINDOW_SIZE: usize = 8;

/// 单 frame 累积上限 256KB，超限丢弃。
pub const FRAME_MAX_BYTES: usize = 256 * 1024;

/// 累积统计快照（receiver 主循环周期 log 用）。
///
/// `PartialEq` 支撑「stats 与上次相同则不打印」判据；`last_drop_reason`
/// 仅在 dropFrame 即时单行日志输出，不进周期行（5 项闭集）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReassemblerStats {
    /// 收到的 RTP 包总数。
    pub packets: u64,
    /// 切出的 NAL 总数。
    pub nals: u64,
    /// 完整切出的 frame 总数。
    pub frames: u64,
    /// 丢弃的 frame 总数（seq-gap / oversize）。
    pub dropped_frames: u64,
    /// 单 frame 最大字节数。
    pub max_frame_bytes: usize,
    /// 最近一次 drop 的原因（"seq-gap" / "oversize"；周期行不含此项）。
    pub last_drop_reason: &'static str,
}

impl ReassemblerStats {
    /// 周期 stats 行（10s，5 项）。
    /// 打印时机与「仅值变化时打印」判据由 receiver 主循环持有。
    pub fn periodic_line(&self) -> String {
        format!(
            "video: rtp stats packets={} nals={} frames={} dropped_frames={} max_frame_bytes={}",
            self.packets, self.nals, self.frames, self.dropped_frames, self.max_frame_bytes
        )
    }

    /// 退出时 final stats 行（内联实现保证退出必打 final 行）。
    pub fn final_line(&self) -> String {
        format!(
            "video: rtp stats final packets={} nals={} frames={} dropped_frames={} max_frame_bytes={}",
            self.packets, self.nals, self.frames, self.dropped_frames, self.max_frame_bytes
        )
    }
}

/// frame 重组器：把外机推流的 RTP 包按 seq 累积到 frame 字节流缓冲，
/// marker=true 时整段按 Annex-B 切 NAL 输出。
pub struct FrameReassembler {
    logf: Option<LogFn>,

    /// frame 字节流缓冲（cap 由 [`FRAME_MAX_BYTES`] 限制；预 alloc 32KB 覆盖 99% frame）。
    frame_buf: Vec<u8>,

    /// frame 起始时的 RTP timestamp；M=1 时切 NAL 用作 NAL 的 ts。
    frame_timestamp: u32,

    /// 排序窗口：按 seq 升序缓 [`REASSEMBLER_WINDOW_SIZE`] 个包。
    pending: Vec<RtpPacket>,

    /// 当前 frame 内期望的下一个 seq（从首包初始化）；用于检测 gap。
    expect_seq: u16,
    expect_seq_set: bool,

    /// frame 首尾 seq（dropFrame log 的 seq_range 字段，便于线上排障）。
    frame_first_seq: u16,
    frame_last_seq: u16,

    stats: ReassemblerStats,
}

impl FrameReassembler {
    /// 构造空重组器。`logf=None` 时不 log。
    pub fn new(logf: Option<LogFn>) -> Self {
        FrameReassembler {
            logf,
            frame_buf: Vec::with_capacity(32 * 1024),
            frame_timestamp: 0,
            pending: Vec::with_capacity(REASSEMBLER_WINDOW_SIZE),
            expect_seq: 0,
            expect_seq_set: false,
            frame_first_seq: 0,
            frame_last_seq: 0,
            stats: ReassemblerStats::default(),
        }
    }

    fn logf(&self, msg: &str) {
        if let Some(f) = &self.logf {
            f(msg);
        }
    }

    /// 把一个解析过的 RTP 包喂进重组器。
    ///
    /// 行为：
    ///   - 包入 pending 排序窗口（按 seq 升序，int16 差值回绕比较）
    ///   - 窗口满或当前包 marker=true：把 pending 前缀按 seq 升序逐包追加进
    ///     frame_buf，遇 marker 即刻切 frame（保证跨 frame 边界字节流不混合）
    ///
    /// 返回 `(nals, frame_complete)`：切好的 NAL 列表（多 frame 一次完成时累计）+
    /// 本次调用是否至少切完一 frame。NAL 字节持所有权（脱离重组 buffer 深拷）。
    ///
    /// 注（expA.pcap 实证）：外机给每个 RTP packet 独立递增 timestamp（~3600/包），
    /// **不是**标准「同 frame 多包共 timestamp」约定——timestamp 切换不能作 frame
    /// 边界兜底。真正的边界靠 marker bit；marker 丢失由 append_packet 的
    /// seq gap > window 检测兜底（丢 frame + 重置）。observations §3.6。
    pub fn push(&mut self, pkt: RtpPacket) -> (Vec<NalUnit>, bool) {
        self.stats.packets += 1;

        let marker = pkt.marker;
        self.insert_sorted(pkt);

        // 窗口未满 + 当前不是 marker → 等更多包。
        if self.pending.len() < REASSEMBLER_WINDOW_SIZE && !marker {
            return (Vec::new(), false);
        }

        let flushed = self.pop_pending_prefix(marker);

        let mut nals = Vec::new();
        let mut frame_complete = false;
        for p in flushed {
            let p_marker = p.marker;
            self.append_packet(p);
            if p_marker {
                let fnals = self.cut_frame();
                nals.extend(fnals);
                frame_complete = true;
            }
        }
        (nals, frame_complete)
    }

    /// 按 seq 升序把 pkt 插进 pending（int16 减法判先后处理 wrap）。
    fn insert_sorted(&mut self, pkt: RtpPacket) {
        for i in (0..self.pending.len()).rev() {
            if (pkt.seq.wrapping_sub(self.pending[i].seq) as i16) >= 0 {
                self.pending.insert(i + 1, pkt);
                return;
            }
        }
        // 比所有现有都小 → 头插。
        self.pending.insert(0, pkt);
    }

    /// 从 pending 取出能确定顺序的前缀返回（不修改 frame_buf）。
    ///
    /// 策略：
    ///   - `marker_in_batch=true`（当前包 marker=1）：pop 整个 pending（frame 末尾不留尾巴）
    ///   - `marker_in_batch=false`（窗口满）：保留最后 1 个包待下次（防被它前面的乱序包追上）
    fn pop_pending_prefix(&mut self, marker_in_batch: bool) -> Vec<RtpPacket> {
        let pop_count = if marker_in_batch {
            self.pending.len()
        } else {
            self.pending.len().saturating_sub(1)
        };
        if pop_count == 0 {
            return Vec::new();
        }
        self.pending.drain(..pop_count).collect()
    }

    /// 把单个包的 payload 追加到 frame_buf；负责 gap 检测 + cap 检查。
    fn append_packet(&mut self, p: RtpPacket) {
        if !self.expect_seq_set {
            self.expect_seq = p.seq;
            self.expect_seq_set = true;
            self.frame_timestamp = p.timestamp;
            self.frame_first_seq = p.seq;
            self.frame_last_seq = p.seq;
        } else {
            let gap = p.seq.wrapping_sub(self.expect_seq) as i16;
            if gap < 0 {
                // 已经追加过更新的（不该走到这；insert_sorted 保证升序）。
                self.logf(&format!(
                    "video: rtp reassembler: out-of-order after sort seq={} expect={}",
                    p.seq, self.expect_seq
                ));
                return;
            }
            if gap > REASSEMBLER_WINDOW_SIZE as i16 {
                // 跳变超窗 → 丢当前 frame。
                self.drop_frame("seq-gap");
                self.expect_seq = p.seq;
                self.expect_seq_set = true;
                self.frame_timestamp = p.timestamp;
                self.frame_first_seq = p.seq;
                self.frame_last_seq = p.seq;
            }
        }

        // cap 保护。
        if self.frame_buf.len() + p.payload.len() > FRAME_MAX_BYTES {
            self.drop_frame("oversize");
            // drop 后丢弃当前包（其属于已损坏 frame）；expect 重置等下一 marker
            // 之后 cut_frame 自然恢复。
            self.expect_seq = p.seq.wrapping_add(1);
            self.expect_seq_set = true;
            self.frame_timestamp = p.timestamp;
            self.frame_first_seq = p.seq;
            self.frame_last_seq = p.seq;
            return;
        }
        self.frame_buf.extend_from_slice(&p.payload);
        self.expect_seq = p.seq.wrapping_add(1);
        self.frame_last_seq = p.seq;
    }

    /// 丢弃当前累积的 frame 字节流并重置状态；reason 记入 `stats.last_drop_reason`
    /// 并打即时单行日志（不入周期行）。
    fn drop_frame(&mut self, reason: &'static str) {
        if self.frame_buf.is_empty() {
            return;
        }
        self.logf(&format!(
            "video: rtp frame dropped reason={} ts={} size={} seq_range=[{}..{}]",
            reason,
            self.frame_timestamp,
            self.frame_buf.len(),
            self.frame_first_seq,
            self.frame_last_seq
        ));
        self.stats.dropped_frames += 1;
        self.stats.last_drop_reason = reason;
        self.frame_buf.clear();
        self.expect_seq_set = false;
    }

    /// 把 frame_buf 按 Annex-B 切 NAL 返回；重置 frame 状态进入下一 frame 累积态。
    ///
    /// NAL 所有权：[`extract_nals_annexb`] 返回的 `NalUnit.data` 是 `Vec<u8>` 拷贝
    /// ——随后 `frame_buf.clear()` + 下一 frame 复写不会污染已交付的 NAL（结构性保证）。
    fn cut_frame(&mut self) -> Vec<NalUnit> {
        if self.frame_buf.is_empty() {
            return Vec::new();
        }
        let nals = extract_nals_annexb(&self.frame_buf, self.frame_timestamp);
        let buf_len = self.frame_buf.len();
        if buf_len > self.stats.max_frame_bytes {
            self.stats.max_frame_bytes = buf_len;
        }
        self.stats.nals += nals.len() as u64;
        self.stats.frames += 1;
        self.frame_buf.clear();
        self.expect_seq_set = false;
        nals
    }

    /// 返当前累积统计的快照（receiver 主循环 stats 周期日志用）。
    pub fn stats(&self) -> ReassemblerStats {
        self.stats.clone()
    }
}

// ── 单测 ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::super::rtp::{NAL_TYPE_IDR, NAL_TYPE_PPS, NAL_TYPE_SPS};
    use super::*;

    /// 构造 RtpPacket 测试输入（payload 是裸字节，不含 RTP 头）。
    fn mk_pkt(seq: u16, ts: u32, marker: bool, payload: &[u8]) -> RtpPacket {
        RtpPacket {
            seq,
            timestamp: ts,
            marker,
            pt: 98,
            ssrc: 0xdead_beef,
            payload: payload.to_vec(),
        }
    }

    /// 一个完整 frame 多包到达。
    /// 包 1 = SPS+PPS+IDR Annex-B 拼包（首包）/ 包 2 = IDR 中段（无 start code）/
    /// 包 3 = IDR 末段 + marker=1。
    #[test]
    fn normal_frame() {
        let mut r = FrameReassembler::new(None);

        let pkt1 = mk_pkt(
            1000,
            90000,
            false,
            &[
                0x00, 0x00, 0x00, 0x01, 0x67, 0xAA, // SPS
                0x00, 0x00, 0x00, 0x01, 0x68, 0xBB, // PPS
                0x00, 0x00, 0x00, 0x01, 0x65, 0xCC, // IDR
            ],
        );
        let pkt2 = mk_pkt(1001, 90000, false, &[0xDD, 0xEE, 0xFF]);
        let pkt3 = mk_pkt(1002, 90000, true, &[0x11, 0x22, 0x33]);

        for p in [pkt1, pkt2, pkt3] {
            let marker = p.marker;
            let (nals, complete) = r.push(p);
            if marker {
                assert!(complete, "expected frame_complete on marker=1 packet");
                assert_eq!(nals.len(), 3, "want 3 NALs");
                assert_eq!(nals[0].nal_type, NAL_TYPE_SPS);
                assert_eq!(nals[1].nal_type, NAL_TYPE_PPS);
                assert_eq!(nals[2].nal_type, NAL_TYPE_IDR);
                // IDR.data 必须含尾包字节。
                let idr = &nals[2].data;
                assert!(
                    idr.len() >= 7 && idr[idr.len() - 3..] == [0x11, 0x22, 0x33],
                    "IDR data missing pkt3 bytes; got {idr:02x?}"
                );
                // NAL 的 ts 取 frame 起始 timestamp。
                assert!(nals.iter().all(|n| n.timestamp == 90000));
            }
        }

        let stats = r.stats();
        assert_eq!(stats.frames, 1);
        assert_eq!(stats.nals, 3);
        assert_eq!(stats.dropped_frames, 0);
        assert_eq!(stats.packets, 3);
    }

    /// 窗口内乱序 1,3,2,4(M=1)。
    #[test]
    fn out_of_order_within_window() {
        let mut r = FrameReassembler::new(None);

        let mk = |seq: u16| -> RtpPacket {
            match seq {
                1 => mk_pkt(1, 100, false, &[0x00, 0x00, 0x00, 0x01, 0x67, 0xAA]),
                2 => mk_pkt(2, 100, false, &[0xBB]),
                3 => mk_pkt(3, 100, false, &[0xCC]),
                4 => mk_pkt(4, 100, true, &[0xDD]),
                _ => unreachable!(),
            }
        };

        let mut last_nals = Vec::new();
        for s in [1u16, 3, 2, 4] {
            let (nals, complete) = r.push(mk(s));
            if complete {
                last_nals = nals;
            }
        }

        assert_eq!(last_nals.len(), 1, "want 1 NAL");
        assert_eq!(
            last_nals[0].data,
            vec![0x67, 0xAA, 0xBB, 0xCC, 0xDD],
            "seq sort failed"
        );
    }

    /// 16-bit seq 回绕 65534,65535,0,1(M=1) + 窗口内乱序变体。
    #[test]
    fn seq_wrap() {
        let mut r = FrameReassembler::new(None);
        let pkts = [
            mk_pkt(65534, 200, false, &[0x00, 0x00, 0x00, 0x01, 0x61, 0x01]),
            mk_pkt(65535, 200, false, &[0x02]),
            mk_pkt(0, 200, false, &[0x03]),
            mk_pkt(1, 200, true, &[0x04]),
        ];
        let mut nals = Vec::new();
        for p in pkts {
            let (out, complete) = r.push(p);
            if complete {
                nals = out;
            }
        }
        assert_eq!(nals.len(), 1, "want 1 NAL");
        assert_eq!(
            nals[0].data,
            vec![0x61, 0x01, 0x02, 0x03, 0x04],
            "wrap-around handling failed"
        );
    }

    /// seq 跨 65535→0 回绕且窗口内**乱序**到达（int16 差值比较，禁无符号直接比较——
    /// 无符号比较会把 seq=0 排到 65534 之前）。
    #[test]
    fn seq_wrap_out_of_order_within_window() {
        let mut r = FrameReassembler::new(None);
        // 按 seq 应为 65534,65535,0,1；乱序投入 65534, 0, 65535, 1(M=1)。
        let pkts = [
            mk_pkt(65534, 200, false, &[0x00, 0x00, 0x00, 0x01, 0x61, 0x01]),
            mk_pkt(0, 200, false, &[0x03]),
            mk_pkt(65535, 200, false, &[0x02]),
            mk_pkt(1, 200, true, &[0x04]),
        ];
        let mut nals = Vec::new();
        for p in pkts {
            let (out, complete) = r.push(p);
            if complete {
                nals = out;
            }
        }
        assert_eq!(nals.len(), 1, "want 1 NAL");
        assert_eq!(
            nals[0].data,
            vec![0x61, 0x01, 0x02, 0x03, 0x04],
            "wrap-around + reorder failed"
        );
        assert_eq!(r.stats().dropped_frames, 0, "回绕不得误判 seq-gap");
    }

    /// 超窗 gap（1 → 100）丢 frame。
    #[test]
    fn seq_gap_drops_frame() {
        let mut r = FrameReassembler::new(None);
        r.push(mk_pkt(1, 100, false, &[0x00, 0x00, 0x00, 0x01, 0x61, 0xAA]));
        // gap = 99 >> window 8 → 触发 drop_frame。
        r.push(mk_pkt(100, 100, true, &[0xBB]));

        let stats = r.stats();
        assert!(
            stats.dropped_frames >= 1,
            "dropped_frames = {}, want >= 1",
            stats.dropped_frames
        );
        assert_eq!(stats.last_drop_reason, "seq-gap");
    }

    /// 单 frame 超 256KB 触发丢弃 + 状态重置。
    #[test]
    fn frame_size_cap() {
        let mut r = FrameReassembler::new(None);

        // 5 个 200KB 连续包（共 1MB > 256KB cap），最后 marker=1。
        let mut huge = vec![0x42u8; 200 * 1024];
        huge[0] = 0x61; // 首包 NAL header 合法
        for (i, marker) in [
            (10u16, false),
            (11, false),
            (12, false),
            (13, false),
            (14, true),
        ] {
            r.push(mk_pkt(i, 300, marker, &huge));
        }
        let stats = r.stats();
        assert!(
            stats.dropped_frames >= 1,
            "expected >= 1 dropped frame, got {}",
            stats.dropped_frames
        );
        assert_eq!(stats.last_drop_reason, "oversize");

        // 验下一 frame 不被前面遗留状态污染。
        let (nals, complete) = r.push(mk_pkt(20, 400, true, &[0x00, 0x00, 0x00, 0x01, 0x65, 0xCC]));
        assert!(complete, "expected frame_complete on next marker");
        assert_eq!(nals.len(), 1);
        assert_eq!(nals[0].nal_type, NAL_TYPE_IDR, "post-cap recovery NAL");
    }

    /// cut_frame 后底层 buffer 复用不污染前 frame 已交付的 NAL。
    #[test]
    fn buffer_reuse_safety() {
        let mut r = FrameReassembler::new(None);

        // frame 1：全 0xAA。
        let (nals1, _) = r.push(mk_pkt(
            1,
            100,
            true,
            &[0x00, 0x00, 0x00, 0x01, 0x61, 0xAA, 0xAA, 0xAA, 0xAA],
        ));
        assert_eq!(nals1.len(), 1, "frame1 NALs");
        let frame1_data = nals1[0].data.clone();

        // frame 2 用完全不同的字节填充，触发 frame_buf 复用。
        let (nals2, _) = r.push(mk_pkt(
            2,
            200,
            true,
            &[0x00, 0x00, 0x00, 0x01, 0x61, 0xBB, 0xBB, 0xBB, 0xBB],
        ));
        assert_eq!(nals2.len(), 1, "frame2 NALs");

        // frame1 的 NAL 必须依然是 0xAA 全填充。
        for (i, &b) in frame1_data.iter().enumerate().skip(1) {
            assert_eq!(
                b, 0xAA,
                "frame1_data[{i}] = {b:#04x}, want 0xAA (corrupted by frame2 buf reuse)"
            );
        }
        // 原 NalUnit 内的字节同样未被污染（move 出 buffer 的所有权保证）。
        assert_eq!(nals1[0].data, frame1_data);
    }

    /// stats 行格式化纯函数（周期 5 项 / final 行；周期触发归 receiver 主循环）。
    #[test]
    fn stats_line_format() {
        let s = ReassemblerStats {
            packets: 1539,
            nals: 326,
            frames: 296,
            dropped_frames: 2,
            max_frame_bytes: 28614,
            last_drop_reason: "seq-gap",
        };
        assert_eq!(
            s.periodic_line(),
            "video: rtp stats packets=1539 nals=326 frames=296 dropped_frames=2 max_frame_bytes=28614"
        );
        assert_eq!(
            s.final_line(),
            "video: rtp stats final packets=1539 nals=326 frames=296 dropped_frames=2 max_frame_bytes=28614"
        );
        // last_drop_reason 不入周期行（5 项闭集）。
        assert!(!s.periodic_line().contains("seq-gap"));
        // 「仅值变化时打印」判据用 PartialEq。
        let s2 = s.clone();
        assert_eq!(s, s2);
    }

    /// drop 即时单行日志格式（reason / ts / size / seq_range 字段）。
    #[test]
    fn drop_frame_immediate_log_line() {
        use std::sync::{Arc, Mutex};
        let lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&lines);
        let mut r = FrameReassembler::new(Some(Box::new(move |m: &str| {
            sink.lock().unwrap().push(m.to_string());
        })));
        r.push(mk_pkt(
            1,
            7777,
            false,
            &[0x00, 0x00, 0x00, 0x01, 0x61, 0xAA],
        ));
        r.push(mk_pkt(100, 8888, true, &[0xBB]));
        let got = lines.lock().unwrap();
        assert!(
            got.iter()
                .any(|l| l
                    == "video: rtp frame dropped reason=seq-gap ts=7777 size=6 seq_range=[1..1]"),
            "drop 即时行缺失或格式漂移: {got:?}"
        );
    }
}
