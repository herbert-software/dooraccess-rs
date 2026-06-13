//! codec 模块 golden parity 回归。
//!
//! 读 `testdata/golden/{bcd.txt,uri.txt,results.txt}` 逐项断言：
//!   - BCD 双向逐字节 + 长度错/格式错两 sentinel 可区分
//!   - URI 字段精确 + 各拒绝 case 归到四 sentinel（Format/NameLen/IPv4Only/Port）
//!   - result 码逐值相等
//!
//! 只断言错误**分类**（sentinel 级），不断言 message 字面。

use std::fs;
use std::path::PathBuf;

use dooraccess_rs::codec::{self, BcdError, UriError};

/// 定位 committed golden 向量目录（相对 crate manifest，CI/本地一致，不依赖 CWD）。
fn golden_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("golden")
        .join(name)
}

fn read_lines(name: &str) -> Vec<String> {
    let body =
        fs::read_to_string(golden_path(name)).unwrap_or_else(|e| panic!("read golden {name}: {e}"));
    body.lines()
        .map(str::trim_end)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect()
}

/// 8 hex 字符 → [u8; 4]（伪 BCD：每 2 字符当一字节）。
fn parse_hex4(s: &str) -> [u8; 4] {
    assert_eq!(s.len(), 8, "hex4 must be 8 chars: {s:?}");
    let mut out = [0u8; 4];
    for i in 0..4 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .unwrap_or_else(|_| panic!("bad hex pair in {s:?}"));
    }
    out
}

#[test]
fn bcd_golden() {
    for line in read_lines("bcd.txt") {
        let f: Vec<&str> = line.splitn(3, '|').collect();
        match f[0] {
            "encode" => {
                let input = f[1];
                let want = parse_hex4(f[2]);
                let got = codec::encode_bcd(input)
                    .unwrap_or_else(|e| panic!("encode {input:?} failed: {e:?}"));
                assert_eq!(got, want, "encode {input:?}");
            }
            "decode" => {
                let input = parse_hex4(f[1]);
                let want = f[2];
                assert_eq!(codec::decode_bcd(input), want, "decode {input:?}");
            }
            "encode_err" => {
                let input = f[1];
                let want_sentinel = f[2];
                let err =
                    codec::encode_bcd(input).expect_err(&format!("encode {input:?} should fail"));
                let got_sentinel = match err {
                    BcdError::Length { .. } => "ErrBCDLength",
                    BcdError::Format { .. } => "ErrBCDFormat",
                };
                assert_eq!(got_sentinel, want_sentinel, "encode_err {input:?} sentinel");
            }
            other => panic!("unknown bcd.txt verb {other:?}"),
        }
    }
}

#[test]
fn uri_golden() {
    for line in read_lines("uri.txt") {
        let f: Vec<&str> = line.split('|').collect();
        match f[0] {
            "ok" => {
                // ok|<uri>|<name>|<ip>|<port>
                assert_eq!(f.len(), 5, "ok line fields: {line:?}");
                let (uri, want_name, want_ip, want_port) = (f[1], f[2], f[3], f[4]);
                let want_port: u16 = want_port.parse().expect("golden port u16");
                let (name, ip, port) = codec::parse_uri(uri)
                    .unwrap_or_else(|e| panic!("parse_uri {uri:?} failed: {e:?}"));
                assert_eq!(name, want_name, "uri {uri:?} name");
                assert_eq!(ip, want_ip, "uri {uri:?} ip");
                assert_eq!(port, want_port, "uri {uri:?} port");
            }
            "err" => {
                // err|<uri>|<class>
                assert_eq!(f.len(), 3, "err line fields: {line:?}");
                let (uri, want_class) = (f[1], f[2]);
                let err =
                    codec::parse_uri(uri).expect_err(&format!("parse_uri {uri:?} should fail"));
                let got_class = match err {
                    UriError::Format => "Format",
                    UriError::NameLen { .. } => "NameLen",
                    UriError::IPv4Only => "IPv4Only",
                    UriError::Port => "Port",
                };
                assert_eq!(got_class, want_class, "uri {uri:?} class");
            }
            other => panic!("unknown uri.txt verb {other:?}"),
        }
    }
}

#[test]
fn results_golden() {
    for line in read_lines("results.txt") {
        let f: Vec<&str> = line.split('|').collect();
        assert_eq!(f.len(), 2, "results line fields: {line:?}");
        let (name, want) = (f[0], f[1]);
        let want: i32 = want.parse().expect("golden result i32");
        let got = result_by_name(name).unwrap_or_else(|| panic!("unknown result name {name:?}"));
        assert_eq!(got, want, "result {name}");
    }
}

/// golden 名 → Rust 常量值。逐值相等是断言目标本身。
fn result_by_name(name: &str) -> Option<i32> {
    use codec::result::*;
    Some(match name {
        "ResultOK" => OK,
        "ResultNoChange" => NO_CHANGE,
        "ResultPartial" => PARTIAL,
        "ResultNotRun" => NOT_RUN,
        "ResultErr" => ERR,
        "ResultInvalid" => INVALID,
        "ResultTimeout" => TIMEOUT,
        "ResultNotFound" => NOT_FOUND,
        "ResultUnsupport" => UNSUPPORT,
        "ResultLenLimit" => LEN_LIMIT,
        "ResultNoConfig" => NO_CONFIG,
        "ResultLoginFail" => LOGIN_FAIL,
        "ResultPcapFail" => PCAP_FAIL,
        "ResultNoRing" => NO_RING,
        "ResultFileRead" => FILE_READ,
        "ResultFileWrite" => FILE_WRITE,
        "ResultIPUnreach" => IP_UNREACH,
        "ResultIPOK" => IP_OK,
        "ResultIPBusy" => IP_BUSY,
        "ResultIPConflict" => IP_CONFLICT,
        "ResultNetErr" => NET_ERR,
        "ResultConnFail" => CONN_FAIL,
        "ResultConnTime" => CONN_TIME,
        "ResultDNSFail" => DNS_FAIL,
        "ResultTLSErr" => TLS_ERR,
        "ResultNetOK" => NET_OK,
        "ResultNoReturn" => NO_RETURN,
        "ResultAuthErr" => AUTH_ERR,
        "ResultAuthFail" => AUTH_FAIL,
        "ResultAuthDeny" => AUTH_DENY,
        _ => return None,
    })
}

/// 额外断言错误**分类**可区分：
/// BCD 两 sentinel、URI 四 sentinel 互不相等。不断言 message 字面。
#[test]
fn error_sentinels_distinguishable() {
    // BCD：长度错 vs 格式错可区分。
    let len_err = codec::encode_bcd("06").expect_err("len");
    let fmt_err = codec::encode_bcd("0602hi00").expect_err("fmt");
    assert!(matches!(len_err, BcdError::Length { .. }));
    assert!(matches!(fmt_err, BcdError::Format { .. }));
    assert_ne!(len_err, fmt_err);

    // URI：四 sentinel 互不相等。
    let format = codec::parse_uri("garbage").expect_err("format");
    let namelen = codec::parse_uri("0602@1.2.3.4:18022").expect_err("namelen");
    let ipv4 = codec::parse_uri("12340000@::ffff:1.2.3.4:18022").expect_err("ipv4");
    let port = codec::parse_uri("12340000@1.2.3.4:0").expect_err("port");
    assert!(matches!(format, UriError::Format));
    assert!(matches!(namelen, UriError::NameLen { .. }));
    assert!(matches!(ipv4, UriError::IPv4Only));
    assert!(matches!(port, UriError::Port));
    // 两两不等（NameLen 携带 got 字段，用变体判定已足够区分）。
    assert_ne!(format, ipv4);
    assert_ne!(format, port);
    assert_ne!(ipv4, port);
}
