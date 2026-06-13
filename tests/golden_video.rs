//! video golden parity 回归（协议字节层 + preview ack / RTCP）。
//!
//! 读 committed 的三份 golden 向量：
//!   - `testdata/golden/video_wire.txt`：req=704 start / req=708 stop 帧（多组 BCD）
//!     与 start_stop_diff 行 + **ack_ok/ack_err 行**（`validate_start_ack` /
//!     `validate_stop_ack` 接受/拒绝集）。
//!
//!   - `testdata/golden/video_flv.txt`：flv_header / avc_config（含 SPS<4B、PPS 空
//!     两个 NIL 边界）/ seq_header_tag / nalu_tag×4。
//!
//!   - `testdata/golden/video_rtcp.txt`：rr / sdes / rrsdes / bye ×3 组 SSRC
//!     逐字节（CNAME 字面 `"dooraccess-go"`）。
//!
//! 向量已 committed，本测试只读静态文件。

use std::fs;
use std::path::PathBuf;

use dooraccess_rs::video::preview::{validate_start_ack, validate_stop_ack};
use dooraccess_rs::video::rtcp::{build_bye, build_rr, build_rr_sdes, build_sdes};
use dooraccess_rs::video::rtp::NalUnit;
use dooraccess_rs::video::transmux::{
    build_avc_nalu_tag, build_avc_sequence_header_tag, flv_header, pack_avc_decoder_config,
};
use dooraccess_rs::wire18022::{build_start_frame, build_stop_frame};

fn load_golden(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata/golden")
        .join(name);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"))
}

fn hex_to_bytes(s: &str) -> Vec<u8> {
    let s = s.trim();
    assert!(s.len().is_multiple_of(2), "odd hex length: {s:?}");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap_or_else(|_| panic!("bad hex: {s:?}")))
        .collect()
}

fn bytes_to_hex(b: &[u8]) -> String {
    let mut out = String::with_capacity(b.len() * 2);
    for byte in b {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn bcd4(s: &str) -> [u8; 4] {
    let v = hex_to_bytes(s);
    assert_eq!(v.len(), 4, "BCD must be 4 bytes: {s:?}");
    [v[0], v[1], v[2], v[3]]
}

/// 断言两 byte 串相等，失败时打印 hex 便于定位字节漂移。
fn assert_bytes_eq(actual: &[u8], expected_hex: &str, ctx: &str) {
    assert_eq!(
        bytes_to_hex(actual),
        expected_hex.trim(),
        "byte mismatch [{ctx}]"
    );
}

/// video_wire.txt：start/stop 帧构造 + 恰差 2 处断言 + ack 接受/拒绝集
/// （ack 行由 `video::preview` 的 validate 消费）。
#[test]
fn golden_video_wire_frames() {
    let text = load_golden("video_wire.txt");

    let mut n_start = 0;
    let mut n_stop = 0;
    let mut n_diff = 0;
    let mut n_ack_ok = 0;
    let mut n_ack_err = 0;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = line.split('|').collect();
        match f[0] {
            "start" => {
                // start|<calleehex4>|<callerhex4>|<framehex36B>
                assert_eq!(f.len(), 4, "start line: {line:?}");
                let frame = build_start_frame(bcd4(f[1]), bcd4(f[2]));
                assert_bytes_eq(&frame, f[3], &format!("start {} {}", f[1], f[2]));
                assert_eq!(frame.len(), 36, "start 帧总长 36B");
                n_start += 1;
            }
            "stop" => {
                // stop|<calleehex4>|<callerhex4>|<framehex36B>
                assert_eq!(f.len(), 4, "stop line: {line:?}");
                let frame = build_stop_frame(bcd4(f[1]), bcd4(f[2]));
                assert_bytes_eq(&frame, f[3], &format!("stop {} {}", f[1], f[2]));
                n_stop += 1;
            }
            "start_stop_diff" => {
                // start_stop_diff|<calleehex4>|<callerhex4>|<comma-sep byte offsets>
                assert_eq!(f.len(), 4, "start_stop_diff line: {line:?}");
                let callee = bcd4(f[1]);
                let caller = bcd4(f[2]);
                let start = build_start_frame(callee, caller);
                let stop = build_stop_frame(callee, caller);
                assert_eq!(start.len(), stop.len());
                let diffs: Vec<usize> = (0..start.len()).filter(|&i| start[i] != stop[i]).collect();
                let want: Vec<usize> = f[3]
                    .split(',')
                    .map(|s| s.trim().parse().expect("diff offset"))
                    .collect();
                assert_eq!(
                    diffs, want,
                    "start/stop diff offsets [{} {}]：须恰差 2 处（req 数字 + sep）",
                    f[1], f[2]
                );
                assert_eq!(diffs.len(), 2, "恰差 2 处");
                n_diff += 1;
            }
            "ack_ok" => {
                // ack_ok|<start|stop>|<name>|<ackhex>（Validate 通过）
                assert_eq!(f.len(), 4, "ack_ok line: {line:?}");
                let ack = hex_to_bytes(f[3]);
                let res = match f[1] {
                    "start" => validate_start_ack(&ack),
                    "stop" => validate_stop_ack(&ack),
                    other => panic!("unknown ack kind {other:?} in {line:?}"),
                };
                res.unwrap_or_else(|e| panic!("ack_ok {} {} rejected: {e}", f[1], f[2]));
                n_ack_ok += 1;
            }
            "ack_err" => {
                // ack_err|<start|stop>|<name>|<ackhex>（Validate 拒绝）
                assert_eq!(f.len(), 4, "ack_err line: {line:?}");
                let ack = hex_to_bytes(f[3]);
                let res = match f[1] {
                    "start" => validate_start_ack(&ack),
                    "stop" => validate_stop_ack(&ack),
                    other => panic!("unknown ack kind {other:?} in {line:?}"),
                };
                assert!(
                    res.is_err(),
                    "ack_err {} {} unexpectedly accepted",
                    f[1],
                    f[2]
                );
                n_ack_err += 1;
            }
            other => panic!("unknown golden tag {other:?} in line {line:?}"),
        }
    }

    // 覆盖度下限：防 golden 文件被悄悄清空导致空跑假绿。
    assert!(n_start >= 3, "too few start vectors: {n_start}");
    assert!(n_stop >= 3, "too few stop vectors: {n_stop}");
    assert!(n_diff >= 3, "too few start_stop_diff vectors: {n_diff}");
    assert!(n_ack_ok >= 4, "too few ack_ok vectors: {n_ack_ok}");
    assert!(n_ack_err >= 4, "too few ack_err vectors: {n_ack_err}");
}

/// video_rtcp.txt：RR / SDES / RR+SDES 复合 / BYE 复合逐字节 golden
/// （×3 组 SSRC；CNAME 字面 `"dooraccess-go"`——禁改名）。
#[test]
fn golden_video_rtcp() {
    let text = load_golden("video_rtcp.txt");

    let mut n_rr = 0;
    let mut n_sdes = 0;
    let mut n_rrsdes = 0;
    let mut n_bye = 0;

    let ssrc = |s: &str| -> u32 {
        u32::from_str_radix(s, 16).unwrap_or_else(|e| panic!("bad ssrc hex {s:?}: {e}"))
    };

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = line.split('|').collect();
        match f[0] {
            "rr" => {
                // rr|<reporter_hex8>|<stream_hex8>|<pkthex>
                assert_eq!(f.len(), 4, "rr line: {line:?}");
                let pkt = build_rr(ssrc(f[1]), ssrc(f[2]));
                assert_bytes_eq(&pkt, f[3], &format!("rr {} {}", f[1], f[2]));
                assert_eq!(pkt.len(), 32, "RR 必须 32B");
                n_rr += 1;
            }
            "sdes" => {
                // sdes|<chunk_ssrc_hex8>|<pkthex>
                assert_eq!(f.len(), 3, "sdes line: {line:?}");
                let pkt = build_sdes(ssrc(f[1]));
                assert_bytes_eq(&pkt, f[2], &format!("sdes {}", f[1]));
                n_sdes += 1;
            }
            "rrsdes" => {
                // rrsdes|<reporter_hex8>|<stream_hex8>|<pkthex>
                assert_eq!(f.len(), 4, "rrsdes line: {line:?}");
                let pkt = build_rr_sdes(ssrc(f[1]), ssrc(f[2]));
                assert_bytes_eq(&pkt, f[3], &format!("rrsdes {} {}", f[1], f[2]));
                n_rrsdes += 1;
            }
            "bye" => {
                // bye|<reporter_hex8>|<pkthex>
                assert_eq!(f.len(), 3, "bye line: {line:?}");
                let pkt = build_bye(ssrc(f[1]));
                assert_bytes_eq(&pkt, f[2], &format!("bye {}", f[1]));
                n_bye += 1;
            }
            other => panic!("unknown golden tag {other:?} in line {line:?}"),
        }
    }

    assert!(n_rr >= 3, "too few rr vectors: {n_rr}");
    assert!(n_sdes >= 3, "too few sdes vectors: {n_sdes}");
    assert!(n_rrsdes >= 3, "too few rrsdes vectors: {n_rrsdes}");
    assert!(n_bye >= 3, "too few bye vectors: {n_bye}");
}

/// video_flv.txt：FLV 头部块 / AVCDecoderConfigurationRecord（含 NIL 边界）/
/// seq-header tag / NALU tag golden。
#[test]
fn golden_video_flv() {
    let text = load_golden("video_flv.txt");

    let mut n_header = 0;
    let mut n_cfg = 0;
    let mut n_cfg_nil = 0;
    let mut n_seq = 0;
    let mut n_nalu = 0;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = line.split('|').collect();
        match f[0] {
            "flv_header" => {
                // flv_header||<hex>（9B header + 4B PreviousTagSize0 = 13B）
                assert_eq!(f.len(), 3, "flv_header line: {line:?}");
                assert_bytes_eq(&flv_header(), f[2], "flv_header");
                n_header += 1;
            }
            "avc_config" => {
                // avc_config|<spshex>|<ppshex>|<cfghex 或 NIL>
                assert_eq!(f.len(), 4, "avc_config line: {line:?}");
                let sps = hex_to_bytes(f[1]);
                let pps = hex_to_bytes(f[2]);
                let got = pack_avc_decoder_config(&sps, &pps);
                if f[3] == "NIL" {
                    assert!(
                        got.is_none(),
                        "avc_config sps={} pps={} 须返 None（Go nil 等价）",
                        f[1],
                        f[2]
                    );
                    n_cfg_nil += 1;
                } else {
                    let got = got.unwrap_or_else(|| {
                        panic!("avc_config sps={} pps={} unexpectedly None", f[1], f[2])
                    });
                    assert_bytes_eq(&got, f[3], &format!("avc_config {} {}", f[1], f[2]));
                }
                n_cfg += 1;
            }
            "seq_header_tag" => {
                // seq_header_tag|<spshex>|<ppshex>|<ts>|<taghex>
                assert_eq!(f.len(), 5, "seq_header_tag line: {line:?}");
                let sps = hex_to_bytes(f[1]);
                let pps = hex_to_bytes(f[2]);
                let ts: u32 = f[3].parse().expect("ts");
                let tag = build_avc_sequence_header_tag(&sps, &pps, ts)
                    .unwrap_or_else(|| panic!("seq_header_tag unexpectedly None: {line:?}"));
                assert_bytes_eq(&tag, f[4], &format!("seq_header_tag ts={ts}"));
                n_seq += 1;
            }
            "nalu_tag" => {
                // nalu_tag|<name>|<ts>|<nal data hex, 多 NAL 逗号分隔>|<taghex>
                assert_eq!(f.len(), 5, "nalu_tag line: {line:?}");
                let ts: u32 = f[2].parse().expect("ts");
                let nals: Vec<NalUnit> = f[3]
                    .split(',')
                    .map(|h| {
                        let data = hex_to_bytes(h);
                        assert!(!data.is_empty(), "empty NAL in {line:?}");
                        NalUnit {
                            nal_type: data[0] & 0x1f,
                            data,
                            timestamp: ts,
                        }
                    })
                    .collect();
                let tag = build_avc_nalu_tag(&nals, ts)
                    .unwrap_or_else(|| panic!("nalu_tag unexpectedly None: {line:?}"));
                assert_bytes_eq(&tag, f[4], &format!("nalu_tag {} ts={ts}", f[1]));
                n_nalu += 1;
            }
            other => panic!("unknown golden tag {other:?} in line {line:?}"),
        }
    }

    assert!(n_header >= 1, "no flv_header vector");
    assert!(n_cfg >= 3, "too few avc_config vectors: {n_cfg}");
    assert!(
        n_cfg_nil >= 2,
        "too few avc_config NIL boundary vectors: {n_cfg_nil}"
    );
    assert!(n_seq >= 1, "no seq_header_tag vector");
    assert!(n_nalu >= 4, "too few nalu_tag vectors: {n_nalu}");
}
