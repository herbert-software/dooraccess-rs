//! wire18022 golden parity 回归。
//!
//! 读 `testdata/golden/wire.txt`（Go `frames.go` 逐字节导出，SoT 见同目录
//! `EXCEPTIONS.md`），对 5 模板 builder / req=518 校验和 / `parse_response_frame`
//! （含双分隔符 `&query=` / `&query*`）/ `validate_response`（711 byte0=0x00、519
//! 全零模板）逐项断言。
//!
//! Rust CI 不依赖 Go：向量已 committed，本测试只读静态文件。

use std::fs;
use std::path::PathBuf;

use dooraccess_rs::wire18022::{
    build_appoint_frame, build_bye_frame, build_preview_frame, build_unlock_a_frame,
    build_unlock_b_frame, parse_response_frame, unlock_b_checksum, validate_response,
};

fn load_golden() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/golden/wire.txt");
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

/// 断言两 byte 串相等，失败时打印 hex 便于定位字节漂移。
fn assert_bytes_eq(actual: &[u8], expected_hex: &str, ctx: &str) {
    let expected = hex_to_bytes(expected_hex);
    assert_eq!(
        bytes_to_hex(actual),
        bytes_to_hex(&expected),
        "byte mismatch [{}]: got {} want {}",
        ctx,
        bytes_to_hex(actual),
        expected_hex.trim(),
    );
}

#[test]
fn golden_wire_parity() {
    let text = load_golden();

    // 每类至少跑过 N 条，防 golden 文件被悄悄清空导致"空跑假绿"。
    let mut n_preview = 0;
    let mut n_unlock_a = 0;
    let mut n_unlock_b = 0;
    let mut n_bye = 0;
    let mut n_appoint = 0;
    let mut n_checksum = 0;
    let mut n_parse_ok = 0;
    let mut n_parse_err = 0;
    let mut n_validate = 0;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = line.split('|').collect();
        match f[0] {
            "preview" => {
                // preview||<hex>
                assert_eq!(f.len(), 3, "preview line: {:?}", line);
                assert_bytes_eq(&build_preview_frame(), f[2], "preview");
                n_preview += 1;
            }
            "unlock_a" => {
                // unlock_a|<from>|<to>|<hex>
                assert_eq!(f.len(), 4, "unlock_a line: {:?}", line);
                let frame = build_unlock_a_frame(bcd4(f[1]), bcd4(f[2]));
                assert_bytes_eq(&frame, f[3], &format!("unlock_a {} {}", f[1], f[2]));
                n_unlock_a += 1;
            }
            "unlock_b" => {
                // unlock_b|<from>|<to>|<hex>
                assert_eq!(f.len(), 4, "unlock_b line: {:?}", line);
                let frame = build_unlock_b_frame(bcd4(f[1]), bcd4(f[2]));
                assert_bytes_eq(&frame, f[3], &format!("unlock_b {} {}", f[1], f[2]));
                n_unlock_b += 1;
            }
            "bye" => {
                // bye|<from>|<to>|<hex>
                assert_eq!(f.len(), 4, "bye line: {:?}", line);
                let frame = build_bye_frame(bcd4(f[1]), bcd4(f[2]));
                assert_bytes_eq(&frame, f[3], &format!("bye {} {}", f[1], f[2]));
                n_bye += 1;
            }
            "appoint" => {
                // appoint|<from>|<hex>
                assert_eq!(f.len(), 3, "appoint line: {:?}", line);
                let frame = build_appoint_frame(bcd4(f[1]));
                assert_bytes_eq(&frame, f[2], &format!("appoint {}", f[1]));
                n_appoint += 1;
            }
            "checksum" => {
                // checksum|<from>|<to>|<cs_hex>
                assert_eq!(f.len(), 4, "checksum line: {:?}", line);
                let from = bcd4(f[1]);
                let to = bcd4(f[2]);
                let want = hex_to_bytes(f[3]);
                assert_eq!(want.len(), 1, "checksum is single byte: {:?}", line);
                // 既验独立 helper，又验整帧 offset 29 一致。
                let cs = unlock_b_checksum(from, to);
                assert_eq!(
                    cs, want[0],
                    "checksum {} {}: got {:02x} want {:02x}",
                    f[1], f[2], cs, want[0]
                );
                let frame = build_unlock_b_frame(from, to);
                assert_eq!(
                    frame[29], want[0],
                    "frame[29] checksum {} {}: got {:02x} want {:02x}",
                    f[1], f[2], frame[29], want[0]
                );
                n_checksum += 1;
            }
            "parse_ok" => {
                // parse_ok|<input_hex>|<req>|<body_hex>
                assert_eq!(f.len(), 4, "parse_ok line: {:?}", line);
                let input = hex_to_bytes(f[1]);
                let want_req: i64 = f[2].parse().expect("req number");
                let (req, body) = parse_response_frame(&input)
                    .unwrap_or_else(|e| panic!("parse_ok {:?}: {}", f[1], e));
                assert_eq!(req, want_req, "parse_ok req {:?}", f[1]);
                assert_bytes_eq(&body, f[3], &format!("parse_ok body {}", f[1]));
                n_parse_ok += 1;
            }
            "parse_err" => {
                // parse_err|<name>|<input_hex>  (input 可能为空)
                assert_eq!(f.len(), 3, "parse_err line: {:?}", line);
                let input = hex_to_bytes(f[2]);
                let res = parse_response_frame(&input);
                assert!(
                    res.is_err(),
                    "parse_err [{}] expected Err, got {:?}",
                    f[1],
                    res
                );
                n_parse_err += 1;
            }
            "validate" => {
                // validate|<input_hex>|<want_req>|<true|false>
                assert_eq!(f.len(), 4, "validate line: {:?}", line);
                let input = hex_to_bytes(f[1]);
                let want_req: i64 = f[2].parse().expect("want_req");
                let want: bool = match f[3] {
                    "true" => true,
                    "false" => false,
                    other => panic!("validate expected true/false, got {:?}", other),
                };
                let got = validate_response(&input, want_req);
                assert_eq!(
                    got, want,
                    "validate input={} want_req={} : got {} want {}",
                    f[1], want_req, got, want
                );
                n_validate += 1;
            }
            other => panic!("unknown golden tag {:?} in line {:?}", other, line),
        }
    }

    // 覆盖度下限：每类向量至少跑过若干条。
    assert!(n_preview >= 1, "no preview vectors");
    assert!(n_unlock_a >= 2, "too few unlock_a vectors: {}", n_unlock_a);
    assert!(n_unlock_b >= 3, "too few unlock_b vectors: {}", n_unlock_b);
    assert!(n_bye >= 2, "too few bye vectors: {}", n_bye);
    assert!(n_appoint >= 3, "too few appoint vectors: {}", n_appoint);
    assert!(n_checksum >= 3, "too few checksum vectors: {}", n_checksum);
    assert!(n_parse_ok >= 3, "too few parse_ok vectors: {}", n_parse_ok);
    assert!(
        n_parse_err >= 5,
        "too few parse_err vectors: {}",
        n_parse_err
    );
    assert!(n_validate >= 6, "too few validate vectors: {}", n_validate);
}

/// 显式钉死 EXCEPTIONS.md §wire 的两条实测例外，独立于 golden 文件解析路径，
/// 防文件被改后这两条隐式回归不被察觉。
#[test]
fn exceptions_dual_separator_and_711_byte0() {
    // ① 双分隔符：`&query=`(3d) 与 `&query*`(2a) 的 711 ack 都应解析成功且 validate=true。
    let ack_eq = hex_to_bytes("07b8180000007265713d3731312671756572793d00000100000003000000");
    let ack_star = hex_to_bytes("07b8180000007265713d3731312671756572792a00000100000003000000");
    assert_eq!(parse_response_frame(&ack_eq).unwrap().0, 711);
    assert_eq!(parse_response_frame(&ack_star).unwrap().0, 711);
    assert!(
        validate_response(&ack_eq, 711),
        "&query= 711 ack should validate"
    );
    assert!(
        validate_response(&ack_star, 711),
        "&query* 711 ack should validate"
    );

    // ② 711 模板 byte0 必须 = 0x00（v0.3.3 实测）；旧反编译残留 0x2a 必须被拒。
    let stale_2a = hex_to_bytes("07b8180000007265713d3731312671756572793d2a000100000003000000");
    assert!(
        !validate_response(&stale_2a, 711),
        "stale decompiled byte0=0x2a must be rejected (real value is 0x00)"
    );
}

// ── int 宽度回归（代码 review round 1 #7：req 编号 Go 平台 int=int32 on MIPS）──
#[test]
fn parse_response_req_over_i32_rejected() {
    use dooraccess_rs::wire18022::parse_response_frame;
    // req=3000000000（10 位 > i32::MAX）：Go-MIPS strconv.Atoi(int32) 拒 → ErrFrameMalformed。
    let body = b"req=3000000000&query=";
    let mut frame = vec![0x07u8, 0xb8, body.len() as u8, 0x00, 0x00, 0x00];
    frame.extend_from_slice(body);
    assert!(parse_response_frame(&frame).is_err());
    // 正常 req=711 仍解析。
    let ok_body = b"req=711&query=";
    let mut ok = vec![0x07u8, 0xb8, ok_body.len() as u8, 0x00, 0x00, 0x00];
    ok.extend_from_slice(ok_body);
    assert!(parse_response_frame(&ok).is_ok());
}
