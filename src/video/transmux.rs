//! transmux：H.264 NAL → FLV 静态构造 + [`StreamWriter`]（FLV header /
//! AVCDecoderConfigurationRecord / seq-header tag / NALU tag / 流写出 /
//! 相对时间戳换算）。
//!
//! 多字节字段全部显式 BE（FLV 是 big-endian 容器格式），逐字节 golden 锚
//! `testdata/golden/video_flv.txt`。
//!
//! StreamWriter 纪律：
//!   - 启动顺序：写 FLV header → 等首 IDR（**超时由 HTTP handler 侧持有**——
//!     handler 先以自己的 deadline 调 `FrameBuffer::wait_idr`，StreamWriter 自身
//!     **无超时**、不再造一层等待）→ SPS/PPS/IDR 三件套校验 → seq-header + 首
//!     IDR（均 ts=0）→ 订阅增量 → 逐 tag 写 + flush
//!   - 订阅循环**仅跳过 SPS/PPS**（重复参数集）；**IDR 不跳过——后续每个 IDR 以
//!     keyframe tag（0x17）写出**（只有 IDR 持续写出，HA stream/HLS 才能切段与恢复）
//!   - ts 换算 RTP 90kHz → FLV 1ms：`wrapping_sub` 32-bit 回绕安全后 /90，
//!     **base 取种子 IDR 的 RTP ts**（非订阅到的首 NAL）
//!   - `SnapshotFLV` **不实现**（`serveVideoSnapshot` 直接 503 stub、无调用方
//!     ——有意省略）

use std::io::{self, Write};
use std::sync::Arc;

use super::frame_buffer::FrameBuffer;
use super::rtp::{NalUnit, NAL_TYPE_IDR, NAL_TYPE_PPS, NAL_TYPE_SPS};

/// FLV header 长度（9B，不含紧随的 4B PreviousTagSize0）。
pub const FLV_HEADER_SIZE: usize = 9;
/// FLV tag header 长度（11B）。
pub const FLV_TAG_HEADER_SIZE: usize = 11;
/// video tag 类型。
pub const FLV_TAG_TYPE_VIDEO: u8 = 0x09;
/// VideoTagHeader codec id：AVC/H.264。
pub const FLV_VIDEO_CODEC_H264: u8 = 0x07;
/// AVC packet type：sequence header。
pub const FLV_AVC_SEQ_HEADER: u8 = 0x00;
/// AVC packet type：NALU。
pub const FLV_AVC_NALU: u8 = 0x01;
/// FrameType 高 nibble：keyframe (I/IDR)。
pub const FLV_FRAME_TYPE_KEY: u8 = 0x10;
/// FrameType 高 nibble：inter (P/B)。
pub const FLV_FRAME_TYPE_INTER: u8 = 0x20;

const FLV_SIGNATURE: &[u8; 3] = b"FLV";
const FLV_VERSION: u8 = 0x01;
const FLV_FLAGS_VIDEO: u8 = 0x01;

/// 返 13B FLV 头部块 = 9B header（仅 video flag）+ 4B PreviousTagSize0。
///
/// 字节布局：
///   - 0-2   "FLV" signature
///   - 3     version 0x01
///   - 4     flags：bit 0 = audio (0), bit 2 = video (1) → 0x01
///   - 5-8   data offset = 9（big-endian）
///   - 9-12  PreviousTagSize0 = 0（不含在 9B 内但 FLV 拉流必须存在）
pub fn flv_header() -> [u8; FLV_HEADER_SIZE + 4] {
    let mut out = [0u8; FLV_HEADER_SIZE + 4];
    out[0..3].copy_from_slice(FLV_SIGNATURE);
    out[3] = FLV_VERSION;
    out[4] = FLV_FLAGS_VIDEO;
    out[5..9].copy_from_slice(&(FLV_HEADER_SIZE as u32).to_be_bytes());
    // out[9..13] PreviousTagSize0 = 0（已为 0）。
    out
}

/// 把 SPS+PPS 打包成 AVCDecoderConfigurationRecord（ISO 14496-15）。
///
/// SPS/PPS 不含 Annex-B 前缀（`extract_nals_annexb` 已剥），从 NAL header byte 开始。
///
/// **`sps.len() < 4` 或 `pps` 空（<1B）返 `None`**；下游静默写无 seq-header 的流
/// （已知行为保持）。
///
/// 字段：configurationVersion=1 / profile=sps[1] / profile_compat=sps[2] /
/// level=sps[3] / lengthSizeMinusOne 整字节 0xff（字段值 3 + 6 reserved 位）/
/// numOfSPS=1（0xe1，reserved 111）/ SPS len(2B BE)+bytes / numOfPPS=1 /
/// PPS len(2B BE)+bytes。
pub fn pack_avc_decoder_config(sps: &[u8], pps: &[u8]) -> Option<Vec<u8>> {
    if sps.len() < 4 || pps.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(11 + sps.len() + pps.len());
    out.extend_from_slice(&[
        1,      // configurationVersion
        sps[1], // profile
        sps[2], // profile_compat
        sps[3], // level
        0xff,   // lengthSizeMinusOne (low 2 bits=3) | 6 reserved 1
        0xe1,   // numOfSPS (low 5 bits=1) | reserved 111
    ]);
    out.extend_from_slice(&(sps.len() as u16).to_be_bytes());
    out.extend_from_slice(sps);
    out.push(1); // numOfPPS
    out.extend_from_slice(&(pps.len() as u16).to_be_bytes());
    out.extend_from_slice(pps);
    Some(out)
}

/// 构造 FLV video tag 携带 AVCDecoderConfigurationRecord（AVC sequence header）。
///
/// 必须是流首个 video tag（HA stream component / ffplay 解码必读）。
/// config 打包失败（SPS/PPS 边界）时返 `None`（下游静默跳过）。
pub fn build_avc_sequence_header_tag(sps: &[u8], pps: &[u8], ts: u32) -> Option<Vec<u8>> {
    let cfg = pack_avc_decoder_config(sps, pps)?;
    // AVC payload 头：1 (frametype+codec) + 1 (avcPacketType) + 3 (composition time)。
    let mut body = Vec::with_capacity(5 + cfg.len());
    body.push(FLV_FRAME_TYPE_KEY | FLV_VIDEO_CODEC_H264); // 0x17
    body.push(FLV_AVC_SEQ_HEADER); // 0x00
    body.extend_from_slice(&[0, 0, 0]); // composition time = 0
    body.extend_from_slice(&cfg);
    Some(wrap_tag(FLV_TAG_TYPE_VIDEO, ts, &body))
}

/// 构造 FLV video tag 携带一个或多个 H.264 NAL 单元。
///
/// frameType：任一 NAL type==5（IDR）→ keyframe 0x17，否则 inter 0x27。
/// 每个 NAL 用 4 字节 big-endian length 前缀打包（lengthSizeMinusOne=3）；
/// 多 NAL 可拼到同一 tag。`nals` 空时返 `None`。
pub fn build_avc_nalu_tag(nals: &[NalUnit], ts: u32) -> Option<Vec<u8>> {
    if nals.is_empty() {
        return None;
    }
    let keyframe = nals.iter().any(|n| n.nal_type == NAL_TYPE_IDR);
    let frame_type_byte = if keyframe {
        FLV_FRAME_TYPE_KEY | FLV_VIDEO_CODEC_H264 // 0x17
    } else {
        FLV_FRAME_TYPE_INTER | FLV_VIDEO_CODEC_H264 // 0x27
    };

    // body = 5 (1 frametype + 1 avcPacketType + 3 composition time) + Σ(4 + nal)。
    let size = 5 + nals.iter().map(|n| 4 + n.data.len()).sum::<usize>();
    let mut body = Vec::with_capacity(size);
    body.extend_from_slice(&[frame_type_byte, FLV_AVC_NALU, 0, 0, 0]);
    for n in nals {
        body.extend_from_slice(&(n.data.len() as u32).to_be_bytes());
        body.extend_from_slice(&n.data);
    }
    Some(wrap_tag(FLV_TAG_TYPE_VIDEO, ts, &body))
}

/// 拼装 FLV tag header (11B) + body + PreviousTagSize (4B)。
///
/// timestamp 低 24 位放 bytes 4-6（BE），高 8 位放 byte 7（extended timestamp）。
fn wrap_tag(tag_type: u8, timestamp: u32, body: &[u8]) -> Vec<u8> {
    let data_size = body.len() as u32;
    let ts24 = timestamp & 0x00ff_ffff;
    let ts_ext = ((timestamp >> 24) & 0xff) as u8;
    let mut out = vec![0u8; FLV_TAG_HEADER_SIZE + body.len() + 4];
    out[0] = tag_type;
    out[1] = (data_size >> 16) as u8;
    out[2] = (data_size >> 8) as u8;
    out[3] = data_size as u8;
    out[4] = (ts24 >> 16) as u8;
    out[5] = (ts24 >> 8) as u8;
    out[6] = ts24 as u8;
    out[7] = ts_ext;
    // out[8..11] StreamID = 0（已为 0）。
    out[FLV_TAG_HEADER_SIZE..FLV_TAG_HEADER_SIZE + body.len()].copy_from_slice(body);
    let prev = FLV_TAG_HEADER_SIZE as u32 + data_size;
    let n = out.len();
    out[n - 4..].copy_from_slice(&prev.to_be_bytes());
    out
}

// ---------------------------------------------------------------------------
// StreamWriter（FLV 流写出 run + 相对时间戳换算）
// ---------------------------------------------------------------------------

/// StreamWriter 错误。
#[derive(Debug)]
pub enum StreamError {
    /// 上游 FrameBuffer 已关 / 三件套缺失。
    Closed,
    /// `w` 写失败（典型为 client disconnect）。
    Io(io::Error),
}

impl core::fmt::Display for StreamError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            StreamError::Closed => write!(f, "video: stream closed"),
            StreamError::Io(e) => write!(f, "video: stream write: {e}"),
        }
    }
}

impl std::error::Error for StreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StreamError::Io(e) => Some(e),
            _ => None,
        }
    }
}

/// 把 [`FrameBuffer`] 的 NAL 流转封装成 FLV bytes 写到 `W`。
///
/// 协议时间基：RTP 90kHz → FLV 1ms；`flv_ts = wrapping_sub(base)/90`，
/// base = 种子 IDR 的 RTP ts。
///
/// flush 缝（chunked HTTP 接上）：每 tag 写出后调 `w.flush()`——chunked writer
/// 在 `Write::flush` 里出 chunk 边界即得 per-tag flush；flush
/// 错误被吞（下一次 Write 自然失败退出）。
///
/// 等首 IDR 的 deadline/取消语义归调用方（生产路径 handler 已先行 `wait_idr`，
/// run 内部等待立即返）：[`StreamWriter::run`] **不等待**，调用方必须先以自己的
/// deadline 完成 `FrameBuffer::wait_idr` 再调 run；run 只做 closed / 三件套校验。
pub struct StreamWriter<W: Write> {
    /// 输出目标（chunked HTTP body writer）。
    pub w: W,
    /// 0 点基准：priming 取**种子 IDR** 的 RTP 时间戳（非订阅到的首 NAL）。
    base_rtp_timestamp: u32,
    base_rtp_set: bool,
}

impl<W: Write> StreamWriter<W> {
    /// 构造（基准未置位；run priming 时取种子 IDR ts）。
    pub fn new(w: W) -> Self {
        StreamWriter {
            w,
            base_rtp_timestamp: 0,
            base_rtp_set: false,
        }
    }

    /// 阻塞写流直到 buffer 关闭（含订阅 channel 断开）或写失败。
    ///
    /// 前置：调用方已用自己的 deadline 完成 `buf.wait_idr(...)`（超时/closed 由
    /// 调用方映射 504/503，**不进入本方法**）。
    ///
    /// 返回：
    ///   - `Ok(())` 正常退出（buffer close → consumer channel 断开；或进入时已 close）
    ///   - `Err(StreamError::Closed)` 三件套缺失
    ///   - `Err(StreamError::Io)` w 写失败（client disconnect）
    pub fn run(&mut self, buf: &Arc<FrameBuffer>) -> Result<(), StreamError> {
        self.w.write_all(&flv_header()).map_err(StreamError::Io)?;
        self.flush();

        // 等首 IDR 归调用方（见 struct 文档）；此处的 latest_idr 判序：
        // close 保留三件套缓存 → closed 竞态下 latest_idr 仍返 Some → 照常写种子帧
        // （退化为单帧可解码 FLV 而非零帧空流）；三件套缺失 → ErrStreamClosed。
        let Some((sps, pps, idr)) = buf.latest_idr() else {
            return Err(StreamError::Closed);
        };

        // 0 点基准 = 种子 IDR 的 RTP ts。
        self.base_rtp_timestamp = idr.timestamp;
        self.base_rtp_set = true;
        // AVC sequence header（流首 tag，ts=0）；打包失败（SPS/PPS 边界）静默跳过
        // （已知行为保持）。
        if let Some(seq) = build_avc_sequence_header_tag(&sps.data, &pps.data, 0) {
            self.w.write_all(&seq).map_err(StreamError::Io)?;
        }
        // 首 IDR tag（ts=0）。
        if let Some(t) = build_avc_nalu_tag(std::slice::from_ref(&idr), 0) {
            self.w.write_all(&t).map_err(StreamError::Io)?;
        }
        self.flush();

        // 订阅后续 NAL stream（种子与订阅起点之间的间隙是已知行为）。
        let (rx, _sub) = buf.subscribe();
        loop {
            let n = match rx.recv() {
                Ok(n) => n,
                // channel 断开 = buffer close / 退订 → 正常退出。
                Err(_) => return Ok(()),
            };
            // 仅跳过 SPS/PPS（重复参数集，已在 sequence header 中）；IDR 不跳过
            // ——后续每个 IDR 以 keyframe tag（0x17）写出。
            if n.nal_type == NAL_TYPE_SPS || n.nal_type == NAL_TYPE_PPS {
                continue;
            }
            let ts = self.relative_flv_timestamp(n.timestamp);
            if let Some(t) = build_avc_nalu_tag(std::slice::from_ref(&n), ts) {
                self.w.write_all(&t).map_err(StreamError::Io)?;
            }
            self.flush();
        }
    }

    /// RTP 90kHz timestamp → FLV 1ms 相对值。
    ///
    /// 32-bit `wrapping_sub` 自然处理回绕；未 priming 直接调用时退化为首个 NAL
    /// ts 作基准。
    fn relative_flv_timestamp(&mut self, rtp_ts: u32) -> u32 {
        if !self.base_rtp_set {
            self.base_rtp_timestamp = rtp_ts;
            self.base_rtp_set = true;
            return 0;
        }
        rtp_ts.wrapping_sub(self.base_rtp_timestamp) / 90
    }

    /// per-tag flush（错误吞——下一次 Write 自然失败退出）。
    fn flush(&mut self) {
        let _ = self.w.flush();
    }
}

// ── 单测（FLV 静态构造用例）──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::super::rtp::NAL_TYPE_NON_IDR;
    use super::*;

    fn nal(nal_type: u8, data: &[u8]) -> NalUnit {
        NalUnit {
            nal_type,
            data: data.to_vec(),
            timestamp: 0,
        }
    }

    /// FLV header 块格式校验。
    #[test]
    fn flv_header_format() {
        let h = flv_header();
        assert_eq!(h.len(), FLV_HEADER_SIZE + 4);
        assert_eq!(&h[0..3], b"FLV");
        assert_eq!(h[3], 0x01, "version");
        assert_eq!(h[4], 0x01, "flags = video only");
        assert_eq!(
            u32::from_be_bytes([h[5], h[6], h[7], h[8]]),
            FLV_HEADER_SIZE as u32,
            "dataOffset"
        );
        assert_eq!(
            u32::from_be_bytes([h[9], h[10], h[11], h[12]]),
            0,
            "PreviousTagSize0"
        );
    }

    /// AVCDecoderConfigurationRecord 含 SPS 与 PPS。
    #[test]
    fn pack_avc_decoder_config_has_sps_and_pps() {
        let sps = [0x67, 0x64, 0xc0, 0x16, 0xac, 0x1b];
        let pps = [0x68, 0xee, 0x31];
        let cfg = pack_avc_decoder_config(&sps, &pps).expect("config");

        assert_eq!(cfg[0], 1, "config version");
        assert_eq!(cfg[1], sps[1], "profile");
        assert_eq!(cfg[2], sps[2], "profile_compat");
        assert_eq!(cfg[3], sps[3], "level");
        assert_eq!(cfg[4], 0xff, "lengthSizeMinusOne 整字节");
        assert_eq!(cfg[5], 0xe1, "numOfSPS");
        // 必须包含 SPS + PPS bytes。
        assert!(
            cfg.windows(sps.len()).any(|w| w == sps),
            "config missing SPS"
        );
        assert!(
            cfg.windows(pps.len()).any(|w| w == pps),
            "config missing PPS"
        );
    }

    /// 坏输入 + PPS 空边界（`sps.len() < 4 || pps 空` → None）。
    #[test]
    fn pack_avc_decoder_config_bad_input() {
        assert!(
            pack_avc_decoder_config(&[0x67], &[0x68]).is_none(),
            "short SPS should return None"
        );
        assert!(
            pack_avc_decoder_config(&[0x67, 0x64, 0xc0, 0x16], &[]).is_none(),
            "empty PPS should return None"
        );
        // 边界恰好通过：SPS=4B、PPS=1B。
        assert!(pack_avc_decoder_config(&[0x67, 0x64, 0xc0, 0x16], &[0x68]).is_some());
        // 下游 tag 构造同样静默 None。
        assert!(build_avc_sequence_header_tag(&[0x67], &[0x68], 0).is_none());
    }

    /// AVC sequence header tag 结构校验。
    #[test]
    fn build_avc_sequence_header_tag_structure() {
        let sps = [0x67, 0x64, 0xc0, 0x16];
        let pps = [0x68, 0xee];
        let tag = build_avc_sequence_header_tag(&sps, &pps, 0).expect("tag");
        assert!(tag.len() >= FLV_TAG_HEADER_SIZE + 5 + 4, "tag too short");
        assert_eq!(tag[0], FLV_TAG_TYPE_VIDEO, "tag type");
        let data_size = ((tag[1] as u32) << 16 | (tag[2] as u32) << 8 | tag[3] as u32) as usize;
        assert_eq!(
            data_size,
            tag.len() - FLV_TAG_HEADER_SIZE - 4,
            "dataSize mismatch"
        );
        let body = &tag[FLV_TAG_HEADER_SIZE..FLV_TAG_HEADER_SIZE + data_size];
        assert_eq!(body[0], 0x17, "frametype byte = keyframe + h264");
        assert_eq!(body[1], FLV_AVC_SEQ_HEADER, "avc packet type");
        // PreviousTagSize 末尾。
        let n = tag.len();
        let prev = u32::from_be_bytes([tag[n - 4], tag[n - 3], tag[n - 2], tag[n - 1]]);
        assert_eq!(
            prev as usize,
            FLV_TAG_HEADER_SIZE + data_size,
            "PreviousTagSize"
        );
    }

    /// IDR → 0x17 / P-frame → 0x27 的 frameType marker 判定。
    #[test]
    fn build_avc_nalu_tag_keyframe_marker() {
        let idr = nal(NAL_TYPE_IDR, &[0x65, 0xaa, 0xbb]);
        let tag = build_avc_nalu_tag(&[idr], 0).expect("tag");
        assert_eq!(tag[FLV_TAG_HEADER_SIZE], 0x17, "IDR tag should start 0x17");

        let pframe = nal(NAL_TYPE_NON_IDR, &[0x61, 0xcc]);
        let tag2 = build_avc_nalu_tag(&[pframe], 0).expect("tag2");
        assert_eq!(
            tag2[FLV_TAG_HEADER_SIZE], 0x27,
            "P-frame tag should start 0x27"
        );

        // 多 NAL 混合：任一 IDR → keyframe。
        let mixed = [
            nal(NAL_TYPE_NON_IDR, &[0x61, 0x01]),
            nal(NAL_TYPE_IDR, &[0x65, 0x02]),
        ];
        let tag3 = build_avc_nalu_tag(&mixed, 0).expect("tag3");
        assert_eq!(tag3[FLV_TAG_HEADER_SIZE], 0x17, "any IDR → keyframe tag");

        // 空 NAL 列表 → None。
        assert!(build_avc_nalu_tag(&[], 0).is_none());
    }

    /// 每个 NAL 带 4B BE length prefix。
    #[test]
    fn build_avc_nalu_tag_length_prefixed_nals() {
        let idr = nal(NAL_TYPE_IDR, &[0x65, 0xaa, 0xbb]);
        let tag = build_avc_nalu_tag(std::slice::from_ref(&idr), 0).expect("tag");
        let data_size = ((tag[1] as u32) << 16 | (tag[2] as u32) << 8 | tag[3] as u32) as usize;
        let body = &tag[FLV_TAG_HEADER_SIZE..FLV_TAG_HEADER_SIZE + data_size];
        // body[5..9] 是 NAL length（big-endian）。
        let na_len = u32::from_be_bytes([body[5], body[6], body[7], body[8]]) as usize;
        assert_eq!(na_len, idr.data.len(), "NAL length prefix");
        assert_eq!(&body[9..9 + na_len], idr.data.as_slice(), "NAL data");
    }

    /// 扩展 timestamp（>24bit）：低 24 位 BE 在 bytes 4-6、高 8 位在 byte 7。
    #[test]
    fn wrap_tag_extended_timestamp() {
        let p = nal(NAL_TYPE_NON_IDR, &[0x61, 0x01]);
        let ts: u32 = 0x0123_4567;
        let tag = build_avc_nalu_tag(&[p], ts).expect("tag");
        assert_eq!(&tag[4..7], &[0x23, 0x45, 0x67], "ts24 BE");
        assert_eq!(tag[7], 0x01, "extended ts byte");
    }
}

// ── StreamWriter 单测（基本流 + 补充用例）─────────────────────────────────────

#[cfg(test)]
mod stream_writer_tests {
    use super::super::rtp::NAL_TYPE_NON_IDR;
    use super::*;
    use std::sync::Mutex;
    use std::thread;
    use std::time::Duration;

    /// 并发安全共享 sink。
    #[derive(Clone, Default)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);

    impl SharedBuf {
        fn bytes(&self) -> Vec<u8> {
            self.0.lock().unwrap().clone()
        }
    }

    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn nal(nal_type: u8, data: &[u8], ts: u32) -> NalUnit {
        NalUnit {
            nal_type,
            data: data.to_vec(),
            timestamp: ts,
        }
    }

    fn seed(buf: &FrameBuffer, idr_ts: u32) {
        buf.push(nal(NAL_TYPE_SPS, &[0x67, 0x64, 0xc0, 0x16], idr_ts));
        buf.push(nal(NAL_TYPE_PPS, &[0x68, 0xee, 0x31], idr_ts));
        buf.push(nal(NAL_TYPE_IDR, &[0x65, 0xaa], idr_ts));
    }

    /// 解析 FLV 字节流，返回 (timestamp, body) 的 tag 列表（跳过 13B 头部块）。
    fn walk_tags(flv: &[u8]) -> Vec<(u32, Vec<u8>)> {
        assert!(flv.len() >= 13, "missing FLV header block");
        assert_eq!(&flv[0..3], b"FLV", "signature");
        let mut tags = Vec::new();
        let mut idx = 13;
        while idx + FLV_TAG_HEADER_SIZE + 4 <= flv.len() {
            let data_size = ((flv[idx + 1] as usize) << 16)
                | ((flv[idx + 2] as usize) << 8)
                | flv[idx + 3] as usize;
            let ts = ((flv[idx + 7] as u32) << 24)
                | ((flv[idx + 4] as u32) << 16)
                | ((flv[idx + 5] as u32) << 8)
                | flv[idx + 6] as u32;
            let body_start = idx + FLV_TAG_HEADER_SIZE;
            let body_end = body_start + data_size;
            assert!(body_end + 4 <= flv.len(), "truncated tag at {idx}");
            tags.push((ts, flv[body_start..body_end].to_vec()));
            idx = body_end + 4; // 跳 PreviousTagSize。
        }
        assert_eq!(idx, flv.len(), "trailing bytes after last tag");
        tags
    }

    /// keyframe NALU tag 判定：body[0]=0x17（keyframe+H264）且 body[1]=0x01（NALU）。
    fn is_keyframe_nalu(body: &[u8]) -> bool {
        body.len() >= 2 && body[0] == 0x17 && body[1] == FLV_AVC_NALU
    }

    /// 基本流：seq-header + 首 IDR ts=0 + 订阅增量 P-frame ts=+1ms。
    #[test]
    fn basic_flow() {
        let buf = Arc::new(FrameBuffer::new());
        seed(&buf, 90000);
        // 调用方已确认 IDR 就绪（deadline 归 handler——run 自身不等待）。
        buf.wait_idr(Duration::from_millis(100)).expect("seeded");

        let out = SharedBuf::default();
        let out_c = out.clone();
        let buf_c = Arc::clone(&buf);
        let join = thread::spawn(move || StreamWriter::new(out_c).run(&buf_c));

        thread::sleep(Duration::from_millis(50)); // 让 priming + 订阅就位。
        buf.push(nal(NAL_TYPE_NON_IDR, &[0x61, 0xbb], 90090)); // +90 ticks = +1ms
                                                               // 订阅循环跳过重复参数集。
        buf.push(nal(NAL_TYPE_SPS, &[0x67, 0x64, 0xc0, 0x16], 90090));
        thread::sleep(Duration::from_millis(50));
        buf.close();
        join.join().unwrap().expect("run exits Ok on close");

        let got = out.bytes();
        assert!(got.starts_with(b"FLV"), "output missing FLV signature");
        let tags = walk_tags(&got);
        assert_eq!(tags.len(), 3, "seq-header + first IDR + P-frame: {tags:?}");
        // tag 1：AVC sequence header（ts=0，keyframe + packet type 0）。
        assert_eq!(tags[0].0, 0);
        assert_eq!(tags[0].1[0], 0x17);
        assert_eq!(tags[0].1[1], FLV_AVC_SEQ_HEADER);
        // tag 2：首 IDR（ts=0，NALU keyframe）。
        assert_eq!(tags[1].0, 0);
        assert!(is_keyframe_nalu(&tags[1].1));
        // tag 3：P-frame（ts=1ms，inter 0x27）。
        assert_eq!(tags[2].0, 1, "P-frame ts = (90090-90000)/90 = 1ms");
        assert_eq!(tags[2].1[0], 0x27);
        // 重复 SPS 被跳过（不出现第 4 个 tag）。
    }

    /// 多 IDR 持续写出：订阅循环仅跳 SPS/PPS，
    /// 第 2、3 个 IDR 均以 keyframe tag 0x17 写出 → 输出含 ≥2 个订阅期 keyframe tag。
    #[test]
    fn subsequent_idrs_written_as_keyframe_tags() {
        let buf = Arc::new(FrameBuffer::new());
        seed(&buf, 9000);
        buf.wait_idr(Duration::from_millis(100)).expect("seeded");

        let out = SharedBuf::default();
        let out_c = out.clone();
        let buf_c = Arc::clone(&buf);
        let join = thread::spawn(move || StreamWriter::new(out_c).run(&buf_c));

        thread::sleep(Duration::from_millis(50));
        buf.push(nal(NAL_TYPE_NON_IDR, &[0x61, 0x01], 9090));
        buf.push(nal(NAL_TYPE_IDR, &[0x65, 0x02], 9180)); // 第 2 个 IDR
        buf.push(nal(NAL_TYPE_NON_IDR, &[0x61, 0x03], 9270));
        buf.push(nal(NAL_TYPE_IDR, &[0x65, 0x04], 9360)); // 第 3 个 IDR
        thread::sleep(Duration::from_millis(50));
        buf.close();
        join.join().unwrap().expect("run exits Ok");

        let tags = walk_tags(&out.bytes());
        // keyframe NALU tag：首 IDR + 订阅期 2 个 = 3。
        let kf: Vec<&(u32, Vec<u8>)> = tags.iter().filter(|(_, b)| is_keyframe_nalu(b)).collect();
        assert_eq!(
            kf.len(),
            3,
            "first IDR + 2 subsequent IDRs must all be keyframe tags: {tags:?}"
        );
        // 订阅期 keyframe tag ≥2（断言下限）且 ts 单调。
        assert!(kf.len() > 2, "订阅期 keyframe tag 须 ≥2");
        assert_eq!(kf[1].0, 2, "IDR2 ts = (9180-9000)/90 = 2ms");
        assert_eq!(kf[2].0, 4, "IDR3 ts = (9360-9000)/90 = 4ms");
    }

    /// RTP ts 回绕：base 取近 u32::MAX 的种子 IDR ts，
    /// 后续 NAL ts 跨回绕——wrapping_sub 后 /90 单调连续，不出现跳变。
    #[test]
    fn timestamp_wraparound_is_monotonic() {
        let base: u32 = u32::MAX - 44; // 距回绕 45 ticks（半 ms）。
        let buf = Arc::new(FrameBuffer::new());
        seed(&buf, base);
        buf.wait_idr(Duration::from_millis(100)).expect("seeded");

        let out = SharedBuf::default();
        let out_c = out.clone();
        let buf_c = Arc::clone(&buf);
        let join = thread::spawn(move || StreamWriter::new(out_c).run(&buf_c));

        thread::sleep(Duration::from_millis(50));
        // base + 90（跨 u32 回绕）→ 1ms；base + 9000 → 100ms。
        buf.push(nal(NAL_TYPE_NON_IDR, &[0x61, 0x01], base.wrapping_add(90)));
        buf.push(nal(
            NAL_TYPE_NON_IDR,
            &[0x61, 0x02],
            base.wrapping_add(9000),
        ));
        thread::sleep(Duration::from_millis(50));
        buf.close();
        join.join().unwrap().expect("run exits Ok");

        let tags = walk_tags(&out.bytes());
        assert_eq!(tags.len(), 4, "seq + IDR + 2 P: {tags:?}");
        assert_eq!(tags[2].0, 1, "wrapped ts must be +1ms (no jump)");
        assert_eq!(tags[3].0, 100, "wrapped ts must be +100ms");
    }

    /// 启动 close 竞态：seed 三件套后 close → close 保留缓存 → run 仍写出
    /// FLV header + seq-header tag + 首 IDR tag（单帧可解码 FLV），正常退出 Ok。
    #[test]
    fn closed_after_seed_writes_seed_frame() {
        let buf = Arc::new(FrameBuffer::new());
        seed(&buf, 0);
        buf.close(); // 进入 run 前已 closed，但缓存保留。
        let out = SharedBuf::default();
        StreamWriter::new(out.clone())
            .run(&buf)
            .expect("closed-after-seed → write seed frame then Ok");

        let got = out.bytes();
        assert!(got.starts_with(b"FLV"), "output missing FLV signature");
        // 非空：超过 13B header 块。
        assert!(
            got.len() > flv_header().len(),
            "closed race must still emit seed tags, not header-only: {} bytes",
            got.len()
        );
        let tags = walk_tags(&got);
        assert_eq!(
            tags.len(),
            2,
            "seq-header + first IDR (no subscription): {tags:?}"
        );
        // tag 1：AVC sequence header（ts=0，keyframe + packet type 0）。
        assert_eq!(tags[0].0, 0);
        assert_eq!(tags[0].1[0], 0x17, "seq-header frametype keyframe+h264");
        assert_eq!(
            tags[0].1[1], FLV_AVC_SEQ_HEADER,
            "avc packet type = seq header"
        );
        // tag 2：首 IDR keyframe NALU tag（ts=0）。
        assert_eq!(tags[1].0, 0);
        assert!(
            is_keyframe_nalu(&tags[1].1),
            "second tag must be IDR keyframe NALU"
        );
    }

    /// 真 close + 无种子（buf.close() 不 seed → latest_idr 返 None）→ Err(Closed)
    /// （closed 且缓存缺失专属测试，前置 closed()==true）。
    #[test]
    fn closed_no_seed_returns_closed_error() {
        let buf = Arc::new(FrameBuffer::new());
        buf.close();
        assert!(buf.closed(), "precondition: buffer is closed");
        let out = SharedBuf::default();
        let err = StreamWriter::new(out)
            .run(&buf)
            .expect_err("closed + no seed must fail");
        assert!(matches!(err, StreamError::Closed), "err = {err:?}");
    }

    /// 三件套缺失（未 close、调用方误用未先 wait）→ Err(Closed)。
    #[test]
    fn missing_seed_returns_closed_error() {
        let buf = Arc::new(FrameBuffer::new());
        buf.push(nal(NAL_TYPE_IDR, &[0x65, 0xaa], 0)); // 缺 SPS/PPS。
        let out = SharedBuf::default();
        let err = StreamWriter::new(out)
            .run(&buf)
            .expect_err("missing SPS/PPS must fail");
        assert!(matches!(err, StreamError::Closed), "err = {err:?}");
    }

    /// 写失败（client disconnect 等价）→ Err(Io) 即刻退出。
    #[test]
    fn write_error_returns_io() {
        struct FailWriter;
        impl Write for FailWriter {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "client gone"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let buf = Arc::new(FrameBuffer::new());
        seed(&buf, 0);
        let err = StreamWriter::new(FailWriter)
            .run(&buf)
            .expect_err("write failure must surface");
        assert!(matches!(err, StreamError::Io(_)), "err = {err:?}");
    }
}
