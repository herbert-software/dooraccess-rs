//! listen6672 / listen18022 / wire_sender golden parity 回归。
//!
//! 读 committed golden 向量，逐字节 / 逐字段断言 Rust 实现正确：
//!   - `listen6672.txt`      parse_frame 字段 + classify（**响铃 byte19 0x8c/0x94/0x95
//!     三值都判 ring + 呼梯 0x90 判 elevator_key**）+ numquery 响应
//!   - `listen6672_extract.txt`  extract_udp_payload（L2 帧→payload/srcip/srcport；
//!     Rust extract 跨平台，host 读静态文件即可断言）
//!   - `listen18022.txt`     parse_frame（req + body）
//!   - `listen18022_extract.txt`  extract_tcp_payload（L2 帧→payload/ip/port）
//!   - `wire_sender.txt`     sender 帧字节（Build*Frame）+ 错误分类（retryable bool）
//!
//! errclass 的 result_code 列（classify_wire_err → -103/-5/-1）在 src/sender.rs 内的
//! `#[cfg(test)]`（map_wire_kind 私有）覆盖；本文件验 retryable bool（公开 API）。
//!
//! 向量已 committed，本测试只读静态文件。

use std::fs;
use std::path::PathBuf;

use dooraccess_rs::listen18022;
use dooraccess_rs::listen6672::{self, build_number_query_response, classify, parse_frame};
use dooraccess_rs::wire18022::{
    build_appoint_frame, build_bye_frame, build_preview_frame, build_unlock_a_frame,
    build_unlock_b_frame,
};
use dooraccess_rs::wire_sender::{is_retryable_error, WireError};

fn load(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata/golden")
        .join(name);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {:?}: {}", path, e))
}

fn hex_to_bytes(s: &str) -> Vec<u8> {
    let s = s.trim();
    assert!(s.len().is_multiple_of(2), "odd hex length: {:?}", s);
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16).unwrap_or_else(|_| panic!("bad hex: {:?}", s))
        })
        .collect()
}

fn bytes_to_hex(b: &[u8]) -> String {
    let mut out = String::with_capacity(b.len() * 2);
    for byte in b {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

fn bcd4(s: &str) -> [u8; 4] {
    let v = hex_to_bytes(s);
    assert_eq!(v.len(), 4, "BCD must be 4 bytes: {:?}", s);
    [v[0], v[1], v[2], v[3]]
}

fn ip4(s: &str) -> [u8; 4] {
    let parts: Vec<&str> = s.split('.').collect();
    assert_eq!(parts.len(), 4, "bad ipv4: {:?}", s);
    [
        parts[0].parse().unwrap(),
        parts[1].parse().unwrap(),
        parts[2].parse().unwrap(),
        parts[3].parse().unwrap(),
    ]
}

/// listen6672.txt：parse_frame 字段 + classify + numquery 响应（含 byte19 多值 ring）。
#[test]
fn golden_listen6672_parser() {
    let text = load("listen6672.txt");
    let mut n_parse_ok = 0;
    let mut n_parse_err = 0;
    let mut n_numresp = 0;
    let mut ring_subtypes: Vec<u8> = Vec::new();
    let mut saw_elevator = false;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = line.split('|').collect();
        match f[0] {
            "parse_ok" => {
                // parse_ok|<name>|<payload>|<flag>|<targetbcd>|<selfip>|<reserved>|<eventflag>|<subtype>|<eventid>|<kind>
                assert_eq!(f.len(), 11, "parse_ok line: {:?}", line);
                let payload = hex_to_bytes(f[2]);
                let frame =
                    parse_frame(&payload).unwrap_or_else(|e| panic!("parse_ok {:?}: {}", f[1], e));
                assert_eq!(frame.flag, hex_to_bytes(f[3])[0], "flag {:?}", f[1]);
                assert_eq!(frame.target_bcd, bcd4(f[4]), "target_bcd {:?}", f[1]);
                assert_eq!(frame.self_or_ip, bcd4(f[5]), "self_or_ip {:?}", f[1]);
                assert_eq!(frame.reserved, bcd4(f[6]), "reserved {:?}", f[1]);
                let want_eventflag = u32::from_str_radix(f[7], 16).unwrap();
                assert_eq!(frame.event_flag, want_eventflag, "event_flag {:?}", f[1]);
                assert_eq!(frame.subtype, hex_to_bytes(f[8])[0], "subtype {:?}", f[1]);
                assert_eq!(frame.event_id, hex_to_bytes(f[9])[0], "event_id {:?}", f[1]);
                // ★ classify 等价：Rust EventKind.as_str() == golden 向量 kind 列。
                let kind = classify(&frame);
                assert_eq!(kind.as_str(), f[10], "classify {:?}", f[1]);
                // 收集 byte19 多值 ring 证据。
                if f[10] == "ring" {
                    ring_subtypes.push(frame.subtype);
                }
                if f[10] == "elevator_key" {
                    saw_elevator = true;
                }
                n_parse_ok += 1;
            }
            "parse_err" => {
                // parse_err|<name>|<payload>
                assert_eq!(f.len(), 3, "parse_err line: {:?}", line);
                let payload = hex_to_bytes(f[2]);
                assert!(
                    parse_frame(&payload).is_err(),
                    "parse_err [{}] expected Err",
                    f[1]
                );
                n_parse_err += 1;
            }
            "numresp" => {
                // numresp|<reqpayload>|<ourip>|<resp>
                assert_eq!(f.len(), 4, "numresp line: {:?}", line);
                let req_payload = hex_to_bytes(f[1]);
                let req = parse_frame(&req_payload)
                    .unwrap_or_else(|e| panic!("numresp parse req: {}", e));
                let resp = build_number_query_response(&req, ip4(f[2]));
                assert_eq!(
                    bytes_to_hex(&resp),
                    bytes_to_hex(&hex_to_bytes(f[3])),
                    "numresp bytes: got {} want {}",
                    bytes_to_hex(&resp),
                    f[3]
                );
                n_numresp += 1;
            }
            other => panic!("unknown listen6672 tag {:?} in {:?}", other, line),
        }
    }

    // byte19 三值 0x8c/0x94/0x95 都被判 ring（多值识别）。
    ring_subtypes.sort_unstable();
    ring_subtypes.dedup();
    assert!(
        ring_subtypes.contains(&0x8c)
            && ring_subtypes.contains(&0x94)
            && ring_subtypes.contains(&0x95),
        "ring multivalue (0x8c/0x94/0x95) not all classified ring: {:02x?}",
        ring_subtypes
    );
    assert!(saw_elevator, "no elevator_key (0x90) classified");
    assert!(n_parse_ok >= 8, "too few parse_ok: {}", n_parse_ok);
    assert!(n_parse_err >= 2, "too few parse_err: {}", n_parse_err);
    assert!(n_numresp >= 1, "no numresp vectors");
}

/// listen6672_extract.txt：extract_udp_payload（L2 帧→payload/srcip/srcport）。
#[test]
fn golden_listen6672_extract() {
    let text = load("listen6672_extract.txt");
    let mut n_ok = 0;
    let mut n_reject = 0;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = line.split('|').collect();
        match f[0] {
            "extract" => {
                // extract|<frame>|<payload>|<srcip>|<srcport>
                assert_eq!(f.len(), 5, "extract line: {:?}", line);
                let frame = hex_to_bytes(f[1]);
                let (payload, src_ip, src_port) = listen6672::extract_udp_payload(&frame)
                    .unwrap_or_else(|| panic!("extract returned None on valid frame"));
                assert_eq!(
                    bytes_to_hex(&payload),
                    bytes_to_hex(&hex_to_bytes(f[2])),
                    "payload"
                );
                assert_eq!(src_ip, ip4(f[3]), "src_ip");
                assert_eq!(src_port, f[4].parse::<u16>().unwrap(), "src_port");
                n_ok += 1;
            }
            "extract_reject" => {
                // extract_reject|<reason>|<frame>
                assert_eq!(f.len(), 3, "extract_reject line: {:?}", line);
                let frame = hex_to_bytes(f[2]);
                assert!(
                    listen6672::extract_udp_payload(&frame).is_none(),
                    "extract_reject [{}] expected None",
                    f[1]
                );
                n_reject += 1;
            }
            other => panic!("unknown listen6672_extract tag {:?} in {:?}", other, line),
        }
    }
    assert!(n_ok >= 1, "no extract vectors");
    assert!(n_reject >= 2, "too few extract_reject: {}", n_reject);
}

/// listen18022.txt：parse_frame（req + body）。
#[test]
fn golden_listen18022_parser() {
    let text = load("listen18022.txt");
    let mut n_ok = 0;
    let mut n_err = 0;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = line.split('|').collect();
        match f[0] {
            "parse_ok" => {
                // parse_ok|<name>|<payload>|<req>|<body>
                assert_eq!(f.len(), 5, "parse_ok line: {:?}", line);
                let payload = hex_to_bytes(f[2]);
                let (req, body) = listen18022::parse_frame(&payload)
                    .unwrap_or_else(|| panic!("parse_ok {:?} returned None", f[1]));
                let want_req: i64 = f[3].parse().expect("req number");
                assert_eq!(req, want_req, "req {:?}", f[1]);
                assert_eq!(
                    bytes_to_hex(&body),
                    bytes_to_hex(&hex_to_bytes(f[4])),
                    "body {:?}",
                    f[1]
                );
                n_ok += 1;
            }
            "parse_err" => {
                // parse_err|<name>|<payload>  (payload 可能为空)
                assert_eq!(f.len(), 3, "parse_err line: {:?}", line);
                let payload = if f[2].is_empty() {
                    Vec::new()
                } else {
                    hex_to_bytes(f[2])
                };
                assert!(
                    listen18022::parse_frame(&payload).is_none(),
                    "parse_err [{}] expected None",
                    f[1]
                );
                n_err += 1;
            }
            other => panic!("unknown listen18022 tag {:?} in {:?}", other, line),
        }
    }
    assert!(n_ok >= 4, "too few parse_ok: {}", n_ok);
    assert!(n_err >= 3, "too few parse_err: {}", n_err);
}

/// listen18022_extract.txt：extract_tcp_payload（L2 帧→payload/ip/port）。
#[test]
fn golden_listen18022_extract() {
    let text = load("listen18022_extract.txt");
    let mut n_ok = 0;
    let mut n_reject = 0;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = line.split('|').collect();
        match f[0] {
            "extract" => {
                // extract|<frame>|<payload>|<srcip>|<dstip>|<srcport>|<dstport>
                assert_eq!(f.len(), 7, "extract line: {:?}", line);
                let frame = hex_to_bytes(f[1]);
                let (payload, src_ip, dst_ip, src_port, dst_port) =
                    listen18022::extract_tcp_payload(&frame)
                        .unwrap_or_else(|| panic!("extract returned None on valid frame"));
                assert_eq!(
                    bytes_to_hex(&payload),
                    bytes_to_hex(&hex_to_bytes(f[2])),
                    "payload"
                );
                assert_eq!(src_ip, ip4(f[3]), "src_ip");
                assert_eq!(dst_ip, ip4(f[4]), "dst_ip");
                assert_eq!(src_port, f[5].parse::<u16>().unwrap(), "src_port");
                assert_eq!(dst_port, f[6].parse::<u16>().unwrap(), "dst_port");
                n_ok += 1;
            }
            "extract_reject" => {
                assert_eq!(f.len(), 3, "extract_reject line: {:?}", line);
                let frame = hex_to_bytes(f[2]);
                assert!(
                    listen18022::extract_tcp_payload(&frame).is_none(),
                    "extract_reject [{}] expected None",
                    f[1]
                );
                n_reject += 1;
            }
            other => panic!("unknown listen18022_extract tag {:?} in {:?}", other, line),
        }
    }
    assert!(n_ok >= 1, "no extract vectors");
    assert!(n_reject >= 2, "too few extract_reject: {}", n_reject);
}

/// 构造一个 golden errclass 名对应的 `WireError`（与 golden 向量的 5 个 case 一一对应）。
fn make_wire_err(kind: &str) -> WireError {
    use std::io;
    match kind {
        "silent_fin" => WireError::SilentFin,
        "timeout" => WireError::Timeout { stage: "connect" },
        "connection_reset" => WireError::Io {
            stage: "write",
            source: io::Error::new(io::ErrorKind::ConnectionReset, "connection reset by peer"),
        },
        "broken_pipe" => WireError::Io {
            stage: "write",
            source: io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe"),
        },
        "other_connrefused" => WireError::Io {
            stage: "connect",
            source: io::Error::new(io::ErrorKind::ConnectionRefused, "connection refused"),
        },
        other => panic!("unknown errclass kind {:?}", other),
    }
}

/// wire_sender.txt：sender 帧字节（Build*Frame）+ 错误分类 retryable bool。
#[test]
fn golden_wire_sender() {
    let text = load("wire_sender.txt");
    let mut n_frame = 0;
    let mut n_errclass = 0;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = line.split('|').collect();
        match f[0] {
            "frame" => {
                // frame|<kind>|<from>|<to>|<framehex>  (from/to 可空)
                assert_eq!(f.len(), 5, "frame line: {:?}", line);
                let frame: Vec<u8> = match f[1] {
                    "unlock_a" => build_unlock_a_frame(bcd4(f[2]), bcd4(f[3])).to_vec(),
                    "unlock_b" => build_unlock_b_frame(bcd4(f[2]), bcd4(f[3])).to_vec(),
                    "bye" => build_bye_frame(bcd4(f[2]), bcd4(f[3])).to_vec(),
                    "appoint" => build_appoint_frame(bcd4(f[2])).to_vec(),
                    "preview" => build_preview_frame().to_vec(),
                    other => panic!("unknown sender frame kind {:?}", other),
                };
                assert_eq!(
                    bytes_to_hex(&frame),
                    bytes_to_hex(&hex_to_bytes(f[4])),
                    "sender frame [{}]: got {} want {}",
                    f[1],
                    bytes_to_hex(&frame),
                    f[4]
                );
                n_frame += 1;
            }
            "errclass" => {
                // errclass|<kind>|<retryable bool>|<result_code>
                // result_code 列由 src/sender.rs 内 cfg(test) 覆盖（map_wire_kind 私有）；
                // 此处验公开 API is_retryable_error 的 bool。
                assert_eq!(f.len(), 4, "errclass line: {:?}", line);
                let err = make_wire_err(f[1]);
                let want: bool = match f[2] {
                    "true" => true,
                    "false" => false,
                    other => panic!("errclass retryable expected true/false, got {:?}", other),
                };
                assert_eq!(
                    is_retryable_error(&err),
                    want,
                    "errclass [{}] retryable: got {} want {}",
                    f[1],
                    is_retryable_error(&err),
                    want
                );
                n_errclass += 1;
            }
            other => panic!("unknown wire_sender tag {:?} in {:?}", other, line),
        }
    }
    assert!(n_frame >= 5, "too few sender frames: {}", n_frame);
    assert!(n_errclass >= 5, "too few errclass: {}", n_errclass);
}
