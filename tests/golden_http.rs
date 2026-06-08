//! HTTP 层 golden parity（port-rust-http-control G7 / tasks 10.2）。
//!
//! 读 `testdata/golden/http/*.txt`（Go `export_golden_test.go` 导出）对 Rust 实现断言。
//! fixture 缺失时跳过（编排者 `go test -tags export` 生成后再跑）。

use std::fs;
use std::io::{Cursor, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use dooraccess_rs::config::{Config, Hass, Listen, Station, Video};
use dooraccess_rs::control::{AutomationState, Server as ControlServer};
use dooraccess_rs::ha_push::{build_url, discover_ha_facing_ip};
use dooraccess_rs::httpx::json::{encode_map, encode_struct, JsonOptions, JsonValue};
use dooraccess_rs::httpx::mux::ServeMux;
use dooraccess_rs::httpx::parse::{read_request, read_response, MAX_BODY_BYTES};
use dooraccess_rs::httpx::server::test_serve_one_connection;
use dooraccess_rs::httpx::{
    new_request, Client, Handler, HandlerFunc, Request, ResponseWriter, METHOD_POST,
    STATUS_NOT_FOUND, STATUS_PAYLOAD_TOO_LARGE,
};
use dooraccess_rs::info;
use dooraccess_rs::sender::MockSender;

fn golden_file(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("golden")
        .join("http")
        .join(name)
}

fn read_golden_lines(name: &str) -> Option<Vec<String>> {
    if !golden_file(name).exists() {
        return None;
    }
    let body = fs::read_to_string(golden_file(name)).ok()?;
    Some(
        body.lines()
            .map(str::trim_end)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(str::to_string)
            .collect(),
    )
}

fn hex_to_bytes(s: &str) -> Vec<u8> {
    let s = s.trim();
    assert!(s.len().is_multiple_of(2), "odd hex: {s:?}");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("bad hex"))
        .collect()
}

fn bytes_to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn golden_export_config() -> Config {
    let mut cfg = Config::default();
    cfg.sip = "12345678@10.0.0.20:18022".into();
    cfg.listen = Listen {
        addr: "127.0.0.1".into(),
        port: 8080,
    };
    cfg.stations = vec![Station {
        sip: "12340000@10.0.0.10:18022".into(),
        rtsp_url: String::new(),
    }];
    cfg.hass = Hass {
        ipaddr: "127.0.0.1".into(),
        port: 8123,
        api: "/api/doorlink/diag".into(),
        token: "golden-token".into(),
    };
    cfg.video = Video {
        forward: false,
        format: "mjpeg".into(),
        cache_path: String::new(),
    };
    cfg
}

struct ParsedHttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn parse_http_response_full(raw: &[u8]) -> ParsedHttpResponse {
    let mut cursor = Cursor::new(raw);
    let resp = read_response(&mut cursor).expect("parse HTTP response");
    ParsedHttpResponse {
        status: resp.status_code,
        headers: resp
            .header
            .iter()
            .map(|(k, v)| (k.clone(), v.first().cloned().unwrap_or_default()))
            .collect(),
        body: resp.body,
    }
}

fn header_eq(headers: &[(String, String)], key: &str, want: &str) {
    let lk = key.to_ascii_lowercase();
    let got = headers
        .iter()
        .find(|(k, _)| k.to_ascii_lowercase() == lk)
        .map(|(_, v)| v.as_str())
        .unwrap_or("");
    assert_eq!(got, want, "header {key}");
}

fn header_present(headers: &[(String, String)], key: &str) -> bool {
    let lk = key.to_ascii_lowercase();
    headers.iter().any(|(k, _)| k.to_ascii_lowercase() == lk)
}

fn roundtrip_raw_server(handler: Arc<dyn Handler>, raw_req: &[u8]) -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().unwrap();
    let accept_done = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept");
        test_serve_one_connection(
            stream,
            handler,
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(1)),
            None,
        );
    });

    let mut conn = TcpStream::connect(addr).expect("connect");
    conn.write_all(raw_req).expect("write req");
    conn.shutdown(std::net::Shutdown::Write).ok();
    conn.set_read_timeout(Some(Duration::from_secs(2))).ok();
    let mut resp = Vec::new();
    let _ = conn.read_to_end(&mut resp);
    accept_done.join().ok();
    resp
}

/// 防假绿守卫：所有 golden fixture 必须存在。其它 golden 测试在 fixture 缺失时
/// `eprintln!+return` 静默跳过（便于编排者先写测试后生成向量），但 commit 后若有人删
/// fixture，那些测试会静默变 vacuous-pass。本测试让缺失变成响亮失败。
#[test]
fn all_golden_fixtures_present() {
    for name in [
        "parser.txt",
        "endpoint_body.txt",
        "endpoint_socket.txt",
        "middleware.txt",
        "hapush.txt",
        "mux404.txt",
    ] {
        assert!(
            golden_file(name).exists(),
            "missing golden fixture {name}; run `go test -tags export -run ExportGolden \
             ./internal/http8080/` in dooraccess-go to regenerate"
        );
    }
}

#[test]
fn parser_golden() {
    let Some(lines) = read_golden_lines("parser.txt") else {
        eprintln!("skip parser_golden: missing testdata/golden/http/parser.txt");
        return;
    };

    for line in lines {
        let f: Vec<&str> = line.split('|').collect();
        match f[0] {
            "parse_ok" => {
                assert_eq!(f.len(), 6, "fields: {line}");
                let raw = hex_to_bytes(f[2]);
                let mut br = Cursor::new(&raw);
                let req =
                    read_request(&mut br, || {}).unwrap_or_else(|_| panic!("parse_ok {}", f[1]));
                assert_eq!(req.method, f[3]);
                assert_eq!(req.url.path, f[4]);
                assert_eq!(req.host, f[5]);
            }
            "parse_reject" => {
                assert_eq!(f.len(), 4, "fields: {line}");
                let raw = hex_to_bytes(f[2]);
                let want_status: u16 = f[3].parse().expect("status");
                let mut br = Cursor::new(&raw);
                let err =
                    read_request(&mut br, || {}).expect_err(&format!("parse_reject {}", f[1]));
                let got = match err {
                    dooraccess_rs::httpx::ReadRequestError::Protocol(e) => e.status,
                    other => panic!("unexpected err {other:?}"),
                };
                assert_eq!(got, want_status, "case {}", f[1]);
            }
            other => panic!("unknown parser verb {other:?}"),
        }
    }
}

#[test]
fn parser_rejects_oversize_content_length_413() {
    let cl = (MAX_BODY_BYTES + 1).to_string();
    let raw = format!("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: {cl}\r\n\r\n");
    let mut br = Cursor::new(raw.as_bytes());
    let err = read_request(&mut br, || {}).expect_err("413");
    match err {
        dooraccess_rs::httpx::ReadRequestError::Protocol(e) => {
            assert_eq!(e.status, STATUS_PAYLOAD_TOO_LARGE);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn endpoint_body_golden() {
    let Some(lines) = read_golden_lines("endpoint_body.txt") else {
        eprintln!("skip endpoint_body_golden: missing fixture");
        return;
    };

    for line in lines {
        let f: Vec<&str> = line.splitn(3, '|').collect();
        assert_eq!(f[0], "body");
        let name = f[1];
        let want = hex_to_bytes(f[2]);

        let got = match name {
            "auto_unlock_on" => body_via_write_json(&[
                ("result", JsonValue::Number(0)),
                ("auto_unlock", JsonValue::Bool(true)),
            ]),
            "auto_hangup_on" => body_via_write_json(&[
                ("result", JsonValue::Number(0)),
                ("auto_hangup", JsonValue::Bool(true)),
            ]),
            "automation_tf" => body_via_write_json(&[
                ("result", JsonValue::Number(0)),
                ("auto_unlock", JsonValue::Bool(true)),
                ("auto_hangup", JsonValue::Bool(false)),
            ]),
            "stations_guard" => body_via_write_json(&[("result", JsonValue::Number(-100))]),
            "error_json_405" => {
                // 裸 body：error_json body = writeJSON 的 {"error":...} struct marshal。
                encode_struct(
                    &[(
                        "error",
                        JsonValue::String("method GET not allowed; use POST".to_string()),
                    )],
                    JsonOptions::MARSHAL,
                )
            }
            "info_render" => info::render_json(&info::build(
                Some(&golden_export_config()),
                "golden-export-v1",
                false,
            )),
            other => panic!("unknown body case {other}"),
        };

        assert_eq!(bytes_to_hex(&got), bytes_to_hex(&want), "body case {name}");
    }
}

fn body_via_write_json(fields: &[(&str, JsonValue)]) -> Vec<u8> {
    // 裸 body：writeJSON 的 body 部分 = struct marshal（声明序、MARSHAL 无尾随 \n）。
    // socket 级带框架验证在 endpoint_socket fixture，本处只比裸 body。
    encode_struct(fields, JsonOptions::MARSHAL)
}

#[test]
fn endpoint_socket_golden() {
    let Some(lines) = read_golden_lines("endpoint_socket.txt") else {
        eprintln!("skip endpoint_socket_golden: missing fixture");
        return;
    };

    let ctrl = ControlServer::new(
        golden_export_config(),
        "golden-export-v1",
        Some(AutomationState::new(true, false)),
        None,
        None,
        MockSender::success(),
    );
    let handler: Arc<dyn Handler> = Arc::new(ctrl.handler());

    for line in lines {
        let f: Vec<&str> = line.splitn(4, '|').collect();
        let name = f[1];
        let req = hex_to_bytes(f[2]);
        let want_raw = hex_to_bytes(f[3]);

        let got_raw = roundtrip_raw_server(Arc::clone(&handler), &req);
        let want = parse_http_response_full(&want_raw);
        let got = parse_http_response_full(&got_raw);

        assert_eq!(got.status, want.status, "{name} status");
        assert!(
            header_present(&got.headers, "Connection"),
            "{name} Connection"
        );
        assert_eq!(
            header_present(&got.headers, "Transfer-Encoding"),
            header_present(&want.headers, "Transfer-Encoding"),
            "{name} chunked mismatch"
        );
        assert_eq!(
            header_present(&got.headers, "Content-Length"),
            header_present(&want.headers, "Content-Length"),
            "{name} CL mismatch"
        );

        match name {
            "info" => {
                assert!(header_present(&got.headers, "Transfer-Encoding"));
                assert!(!header_present(&got.headers, "Content-Length"));
                assert!(got.body.ends_with(b"\n"));
            }
            "auto_unlock" => {
                assert!(!header_present(&got.headers, "Transfer-Encoding"));
                assert!(header_present(&got.headers, "Content-Length"));
                assert!(!got.body.ends_with(b"\n"));
            }
            _ => {}
        }

        assert_eq!(
            bytes_to_hex(&got.body),
            bytes_to_hex(&want.body),
            "{name} body"
        );
    }
}

#[test]
fn middleware_golden() {
    let Some(lines) = read_golden_lines("middleware.txt") else {
        eprintln!("skip middleware_golden: missing fixture");
        return;
    };

    for line in lines {
        let f: Vec<&str> = line.splitn(5, '|').collect();
        let name = f[1];
        let req = hex_to_bytes(f[2]);
        let want_raw = hex_to_bytes(f[3]);
        let want_status: u16 = f[4].parse().expect("status");

        let handler: Arc<dyn Handler> = match name {
            "unlock_no_stations" => {
                let mut cfg = golden_export_config();
                cfg.stations.clear();
                Arc::new(
                    ControlServer::new(
                        cfg,
                        "golden-export-v1",
                        None,
                        None,
                        None,
                        MockSender::success(),
                    )
                    .handler(),
                )
            }
            _ => Arc::new(
                ControlServer::new(
                    golden_export_config(),
                    "golden-export-v1",
                    Some(AutomationState::new(false, false)),
                    None,
                    None,
                    MockSender::success(),
                )
                .handler(),
            ),
        };

        let got_raw = roundtrip_raw_server(handler, &req);
        let want = parse_http_response_full(&want_raw);
        let got = parse_http_response_full(&got_raw);

        assert_eq!(got.status, want_status, "{name}");
        assert_eq!(got.status, want.status, "{name}");
        assert_eq!(
            bytes_to_hex(&got.body),
            bytes_to_hex(&want.body),
            "{name} body"
        );

        match name {
            "unlock_get_405" => {
                header_eq(&got.headers, "Allow", "POST");
            }
            "unlock_text_plain_415" => {
                assert_eq!(
                    String::from_utf8_lossy(&got.body),
                    r#"{"error":"Content-Type must be application/json"}"#
                );
            }
            "unlock_no_stations" => {
                assert_eq!(String::from_utf8_lossy(&got.body), r#"{"result":-100}"#);
            }
            _ => {}
        }
    }
}

#[test]
fn hapush_golden() {
    let Some(lines) = read_golden_lines("hapush.txt") else {
        eprintln!("skip hapush_golden: missing fixture");
        return;
    };

    for line in lines {
        let f: Vec<&str> = line.split('|').collect();
        let want_body = hex_to_bytes(f[2]);
        let want_method = f[3];
        let want_path = f[4];
        let want_auth = f[5];
        let want_ct = f[6];

        let mut body_map = std::collections::BTreeMap::new();
        body_map.insert("event".into(), JsonValue::String("diagnosis".into()));
        body_map.insert("video_format".into(), JsonValue::String("mjpeg".into()));
        body_map.insert("video_forward".into(), JsonValue::String("0".into()));
        let wwan = discover_ha_facing_ip("127.0.0.1", 8123);
        body_map.insert("wwan".into(), JsonValue::String(wwan));
        let got_body = encode_map(&body_map, JsonOptions::MARSHAL);

        assert_eq!(
            bytes_to_hex(&got_body),
            bytes_to_hex(&want_body),
            "hapush body"
        );

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        let capture = thread::spawn(move || {
            let (mut conn, _) = listener.accept().expect("accept");
            let mut raw = Vec::new();
            conn.read_to_end(&mut raw).ok();
            raw
        });

        let url = build_url("127.0.0.1", port as i64, "/api/doorlink/diag");
        let mut req = new_request(METHOD_POST, &url, Some(&got_body)).expect("req");
        req.set_header("Content-Type", "application/json");
        req.set_header("Authorization", "Bearer golden-token");
        let c = Client {
            timeout: Some(Duration::from_secs(2)),
        };
        let _ = c.do_request(&req);

        let raw = capture.join().expect("join");
        let raw_s = String::from_utf8_lossy(&raw);
        assert!(raw_s.starts_with(&format!("{want_method} {want_path} HTTP/1.1")));
        assert!(raw_s.contains(&format!("Authorization: {want_auth}")));
        assert!(raw_s.contains(&format!("Content-Type: {want_ct}")));
    }
}

#[test]
fn mux404_golden() {
    let Some(lines) = read_golden_lines("mux404.txt") else {
        eprintln!("skip mux404_golden: missing fixture");
        return;
    };

    let mux = ServeMux::new();
    let handler: Arc<dyn Handler> = Arc::new(HandlerFunc(
        move |w: &mut dyn ResponseWriter, r: &Request| mux.serve_http(w, r),
    ));

    for line in lines {
        let f: Vec<&str> = line.splitn(4, '|').collect();
        let req = hex_to_bytes(f[2]);
        let want_raw = hex_to_bytes(f[3]);

        let got_raw = roundtrip_raw_server(Arc::clone(&handler), &req);
        let want = parse_http_response_full(&want_raw);
        let got = parse_http_response_full(&got_raw);

        assert_eq!(got.status, STATUS_NOT_FOUND);
        assert_eq!(got.status, want.status);
        assert_eq!(got.body, b"Not Found");
        header_eq(&got.headers, "Content-Type", "text/plain; charset=utf-8");
        header_eq(&got.headers, "Content-Length", "9");
    }
}
