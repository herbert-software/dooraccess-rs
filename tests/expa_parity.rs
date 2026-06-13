//! expA fixture parity。
//!
//! 读 `testdata/rtp-fragmentation/expA.packets.tsv`（1539 包）灌入
//! 重组器，对 `expected.json` 中 **11 项统计字段** 逐项断言
//! （expected.json 共 15 字段，
//! `fixture/captured_at/annexb_overhead_per_nal` 不参与断言，`input_packets`
//! 是装载前置校验）。
//!
//! 字段级断言：确保重组器吞吐
//! frames_complete=296 / nals_total=326 等。

use std::fs;
use std::path::PathBuf;

use dooraccess_rs::video::reassembler::FrameReassembler;
use dooraccess_rs::video::rtp::{
    RtpPacket, NAL_TYPE_IDR, NAL_TYPE_NON_IDR, NAL_TYPE_PPS, NAL_TYPE_SPS,
};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata/rtp-fragmentation")
        .join(name)
}

/// 从 expected.json 提取整型字段（固定 fixture 的极简解析，无 serde——crate gate）。
fn json_int(content: &str, key: &str) -> i64 {
    let needle = format!("\"{key}\"");
    let at = content
        .find(&needle)
        .unwrap_or_else(|| panic!("expected.json missing key {key:?}"));
    let rest = &content[at + needle.len()..];
    let colon = rest
        .find(':')
        .unwrap_or_else(|| panic!("no ':' after {key:?}"));
    let num: String = rest[colon + 1..]
        .trim_start()
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    num.parse()
        .unwrap_or_else(|e| panic!("bad int for {key:?}: {e}"))
}

fn hex_to_bytes(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd hex length");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("bad hex"))
        .collect()
}

/// 读取 expA.packets.tsv。
/// TSV 列：rtp.seq \t rtp.timestamp \t rtp.marker \t rtp.payload（首行 header）。
fn load_packets_tsv() -> Vec<RtpPacket> {
    let path = fixture_path("expA.packets.tsv");
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    let mut pkts = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if i == 0 || line.is_empty() {
            continue; // header / 尾空行。
        }
        let parts: Vec<&str> = line.split('\t').collect();
        assert_eq!(parts.len(), 4, "malformed TSV line {}: {line:?}", i + 1);
        let seq: u16 = parts[0].parse().expect("parse seq");
        let ts: u32 = parts[1].parse().expect("parse ts");
        let marker = parts[2] == "True";
        let payload = hex_to_bytes(parts[3]);
        pkts.push(RtpPacket {
            seq,
            timestamp: ts,
            marker,
            pt: 98,
            ssrc: 0,
            payload,
        });
    }
    pkts
}

/// 1539 包灌重组器，11 项字段逐项断言。
#[test]
fn reassembler_expa_fixture_parity() {
    let exp_path = fixture_path("expected.json");
    let exp = fs::read_to_string(&exp_path).unwrap_or_else(|e| panic!("read {exp_path:?}: {e}"));

    let pkts = load_packets_tsv();
    // 装载前置校验：加载包数须等于 expected.json 的 input_packets。
    assert_eq!(
        pkts.len() as i64,
        json_int(&exp, "input_packets"),
        "loaded packet count"
    );

    let mut r = FrameReassembler::new(None);

    let mut nals_total: i64 = 0;
    let (mut sps, mut pps, mut idr, mut nonidr, mut other) = (0i64, 0i64, 0i64, 0i64, 0i64);
    let mut nal_bytes: i64 = 0;
    let mut frames_done: i64 = 0;
    let mut payload_bytes: i64 = 0;

    for p in pkts {
        payload_bytes += p.payload.len() as i64;
        let (nals, complete) = r.push(p);
        if complete {
            frames_done += 1;
        }
        for n in nals {
            nals_total += 1;
            nal_bytes += n.data.len() as i64;
            match n.nal_type {
                NAL_TYPE_SPS => sps += 1,
                NAL_TYPE_PPS => pps += 1,
                NAL_TYPE_IDR => idr += 1,
                NAL_TYPE_NON_IDR => nonidr += 1,
                _ => other += 1,
            }
        }
    }

    let stats = r.stats();

    // 11 项字段逐项断言。
    let checks: [(&str, i64, i64); 11] = [
        (
            "input_payload_bytes",
            payload_bytes,
            json_int(&exp, "input_payload_bytes"),
        ),
        (
            "frames_complete",
            frames_done,
            json_int(&exp, "frames_complete"),
        ),
        (
            "frames_dropped",
            stats.dropped_frames as i64,
            json_int(&exp, "frames_dropped"),
        ),
        (
            "max_frame_bytes",
            stats.max_frame_bytes as i64,
            json_int(&exp, "max_frame_bytes"),
        ),
        ("nals_total", nals_total, json_int(&exp, "nals_total")),
        ("nals_sps", sps, json_int(&exp, "nals_sps")),
        ("nals_pps", pps, json_int(&exp, "nals_pps")),
        ("nals_idr", idr, json_int(&exp, "nals_idr")),
        ("nals_nonidr", nonidr, json_int(&exp, "nals_nonidr")),
        ("nals_other", other, json_int(&exp, "nals_other")),
        (
            "nal_bytes_total",
            nal_bytes,
            json_int(&exp, "nal_bytes_total"),
        ),
    ];
    for (name, got, want) in checks {
        assert_eq!(got, want, "{name}: got {got}, want {want}");
    }

    // 锚定关键值不被 expected.json 漂移悄悄放水（两项字面）。
    assert_eq!(frames_done, 296, "frames_complete 字面锚");
    assert_eq!(nals_total, 326, "nals_total 字面锚");
}
