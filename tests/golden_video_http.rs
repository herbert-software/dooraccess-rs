//! /video/* endpoint golden parity（port-rust-video-forward 组 F / tasks 6.1-6.3）。
//!
//! 读 `testdata/golden/http/video_endpoints.txt`（Go `export_golden_video_test.go`
//! 导出，组 A / 1.3）逐行回放 REQUEST_HEX 给 Rust control server，对 RESPONSE_HEX
//! / STATUS 断言（套路对齐 `golden_http.rs` 的 endpoint_socket_golden）。
//!
//! 错误消息字面即契约——除以下三行按 **sentinel 前缀 + wrap 形态** 断言（golden
//! 文件头注明内文为 runtime-variable 的代表性定值）外，其余 body 逐字节：
//!   - `start_409_conflict`（active=/requested= 两 URI 内嵌）
//!   - `start_503_preview_timeout`（dial 错误内文随网络错误变化）
//!   - `start_503_other`（Manager bind 失败 wrap 内文）
//!
//! 固定 UUID `11111111-2222-4333-8444-555555555555` 经 `Manager.urandom_path`
//! 注入 16B 定值 fixture 复现（version/variant 位掩码后逐字节一致），使
//! `start_200` 的 200 响应可整 body 逐字节对照。

use std::fs;
use std::io::Cursor;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use dooraccess_rs::config::{Config, Listen, Station};
use dooraccess_rs::control::{set_stream_startup_timeout_ms, Server as ControlServer};
use dooraccess_rs::httpx::parse::read_response;
use dooraccess_rs::httpx::server::test_serve_one_connection;
use dooraccess_rs::httpx::Handler;
use dooraccess_rs::sender::MockSender;
use dooraccess_rs::video::preview::PreviewError;
use dooraccess_rs::video::rtp::{NalUnit, NAL_TYPE_IDR};
use dooraccess_rs::video::session::{
    parse_caller, parse_outdoor, Caller, Manager, Outdoor, PreviewPort, RtcpPort, RtpPort,
};

/// 与 Go export 一致的两个 allowlist 站点 + 固定 session UUID。
const TEST_OUTDOOR_URI: &str = "06020000@172.16.106.152:18022";
const OUTDOOR_URI2: &str = "06020001@172.16.106.153:18022";
const SESSION_ID: &str = "11111111-2222-4333-8444-555555555555";

fn golden_file(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("golden")
        .join("http")
        .join(name)
}

fn read_golden_lines(name: &str) -> Vec<String> {
    let path = golden_file(name);
    assert!(
        path.exists(),
        "missing golden fixture {name}; run `go test -tags export -run ExportGolden \
         ./internal/http8080/` in dooraccess-go to regenerate"
    );
    fs::read_to_string(path)
        .expect("read fixture")
        .lines()
        .map(str::trim_end)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect()
}

fn hex_to_bytes(s: &str) -> Vec<u8> {
    let s = s.trim();
    assert!(s.len().is_multiple_of(2), "odd hex: {s:?}");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("bad hex"))
        .collect()
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

fn header_of<'a>(headers: &'a [(String, String)], key: &str) -> Option<&'a str> {
    let lk = key.to_ascii_lowercase();
    headers
        .iter()
        .find(|(k, _)| k.to_ascii_lowercase() == lk)
        .map(|(_, v)| v.as_str())
}

fn roundtrip_raw_server(handler: Arc<dyn Handler>, raw_req: &[u8]) -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().unwrap();
    let accept_done = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept");
        test_serve_one_connection(
            stream,
            handler,
            Some(Duration::from_secs(10)),
            Some(Duration::from_secs(5)),
            None,
        );
    });

    let mut conn = TcpStream::connect(addr).expect("connect");
    conn.write_all(raw_req).expect("write req");
    conn.shutdown(std::net::Shutdown::Write).ok();
    conn.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let mut resp = Vec::new();
    let _ = conn.read_to_end(&mut resp);
    accept_done.join().ok();
    resp
}

// --- Manager 依赖 fakes（PreviewPort/RtpPort/RtcpPort 公共注入口）---

/// 信令恒成功（不触网络）。
struct OkPreview;

impl PreviewPort for OkPreview {
    fn start_preview(&self, _o: &Outdoor, _c: &Caller) -> Result<(), PreviewError> {
        Ok(())
    }

    fn stop_preview(
        &self,
        _o: &Outdoor,
        _c: &Caller,
        _timeout: Option<Duration>,
    ) -> Result<(), PreviewError> {
        Ok(())
    }
}

/// start 信令恒超时（→ `StartError::Preview` + `is_preview_timeout` → 503）。
struct TimeoutPreview;

impl PreviewPort for TimeoutPreview {
    fn start_preview(&self, _o: &Outdoor, _c: &Caller) -> Result<(), PreviewError> {
        Err(PreviewError::Timeout { stage: "dial" })
    }

    fn stop_preview(
        &self,
        _o: &Outdoor,
        _c: &Caller,
        _timeout: Option<Duration>,
    ) -> Result<(), PreviewError> {
        Ok(())
    }
}

struct FakeRtp;

impl RtpPort for FakeRtp {
    fn run_with_conn(
        &self,
        conn: UdpSocket,
        _expect_src_ip: &str,
        stop: &AtomicBool,
    ) -> std::io::Result<()> {
        drop(conn);
        while !stop.load(Ordering::SeqCst) {
            thread::park_timeout(Duration::from_millis(5));
        }
        Ok(())
    }
}

struct FakeRtcp;

impl RtcpPort for FakeRtcp {
    fn run(
        &self,
        _dst: &str,
        _ssrc_source: &dyn Fn() -> Option<u32>,
        _reporter_ssrc: u32,
        stop: &AtomicBool,
    ) -> std::io::Result<()> {
        while !stop.load(Ordering::SeqCst) {
            thread::park_timeout(Duration::from_millis(5));
        }
        Ok(())
    }

    fn send_bye(&self, _dst: &str, _reporter_ssrc: u32) -> std::io::Result<()> {
        Ok(())
    }
}

/// 写 16B 定值随机源 fixture：经 `uuid_v4_from` 的 version/variant 位掩码后
/// 得到 [`SESSION_ID`]（b[6] 低 nibble 0x3 → 0x43；b[8] 低 6 bit 0x04 → 0x84）；
/// SSRC 读首 4 字节 0x11111111（非零）。
fn write_uuid_fixture() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "dooraccess-rs-golden-video-urandom-{}",
        std::process::id()
    ));
    let bytes: [u8; 16] = [
        0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x03, 0x33, 0x04, 0x44, 0x55, 0x55, 0x55, 0x55, 0x55,
        0x55,
    ];
    fs::write(&path, bytes).expect("write urandom fixture");
    path
}

/// 构造注入 fakes 的 Manager（ttl 默认 60s = golden 200 响应的 ttl 字段源）。
fn build_mgr(preview: Arc<dyn PreviewPort>, listen_addr: &str, fixture: &Path) -> Arc<Manager> {
    let caller = parse_caller("06021103@10.0.0.91:18022").expect("caller");
    let mut m = Manager::new(caller, None);
    m.rtp_listen_addr = listen_addr.to_string();
    m.urandom_path = fixture.to_string_lossy().into_owned();
    m.preview = preview;
    m.rtp = Some(Arc::new(FakeRtp));
    m.rtcp = Arc::new(FakeRtcp);
    Arc::new(m)
}

fn ok_mgr(fixture: &Path) -> Arc<Manager> {
    build_mgr(Arc::new(OkPreview), "127.0.0.1:0", fixture)
}

/// 预建一条 active session（UUID 必为 [`SESSION_ID`]，验证 fixture 确定性）。
fn mgr_with_session(fixture: &Path) -> Arc<Manager> {
    let m = ok_mgr(fixture);
    let info = m
        .start(parse_outdoor(TEST_OUTDOOR_URI).expect("outdoor"))
        .expect("pre-start session");
    assert_eq!(info.id, SESSION_ID, "urandom fixture 须复现固定 UUID");
    m
}

/// 与 Go `exportVideoConfig` 一致的 cfg（两个 allowlist 站点）。
fn export_video_config() -> Config {
    let mut cfg = Config::default();
    cfg.sip = "06021103@10.0.0.91:18022".into();
    cfg.listen = Listen {
        addr: "127.0.0.1".into(),
        port: 8080,
    };
    cfg.stations = vec![
        Station {
            sip: TEST_OUTDOOR_URI.into(),
            rtsp_url: String::new(),
        },
        Station {
            sip: OUTDOOR_URI2.into(),
            rtsp_url: String::new(),
        },
    ];
    cfg
}

fn server_with(mgr: Option<Arc<Manager>>) -> Arc<dyn Handler> {
    let mut srv = ControlServer::new(
        export_video_config(),
        "golden-export-v1",
        None,
        None,
        None,
        MockSender::success(),
    );
    srv.set_video(mgr, None);
    Arc::new(srv.handler())
}

/// sentinel 前缀断言行（内文 runtime-variable，见文件头）→ (name, body 前缀)。
fn prefix_sentinel(name: &str) -> Option<&'static str> {
    match name {
        "start_409_conflict" => Some(r#"{"error":"video: another outdoor session is active: active="#),
        "start_503_preview_timeout" => {
            Some(r#"{"error":"video: outdoor preview signal failed: "#)
        }
        "start_503_other" => Some(r#"{"error":"video: rtp bind "#),
        _ => None,
    }
}

#[test]
fn video_endpoints_golden() {
    let lines = read_golden_lines("video_endpoints.txt");
    assert!(!lines.is_empty(), "empty video_endpoints fixture");
    let fixture = write_uuid_fixture();

    for line in lines {
        let f: Vec<&str> = line.split('|').collect();
        assert_eq!(f.len(), 5, "fields: {line}");
        assert_eq!(f[0], "video");
        let name = f[1];
        let req = hex_to_bytes(f[2]);
        let want_raw = hex_to_bytes(f[3]);
        let want_status: u16 = f[4].parse().expect("status");

        // 每行独立 server/Manager 状态（对齐 Go export 逐 case 新建 fake）。
        let mut cleanup_mgr: Option<Arc<Manager>> = None;
        let mut restore_timeout: Option<u32> = None;
        let handler: Arc<dyn Handler> = match name {
            // nil-guard 行（VideoMgr 缺位）。
            "start_503_nilguard" | "start_503_nilguard_bad_json" | "stop_503_nilguard"
            | "stop_503_nilguard_bad_json" | "dyn_503_nilguard" => server_with(None),

            // 200 成功：fixture UUID + ttl 60。
            "start_200" => {
                let m = ok_mgr(&fixture);
                cleanup_mgr = Some(Arc::clone(&m));
                server_with(Some(m))
            }

            // 409：active=TEST_OUTDOOR_URI 在位，请求 OUTDOOR_URI2。
            "start_409_conflict" => {
                let m = mgr_with_session(&fixture);
                cleanup_mgr = Some(Arc::clone(&m));
                server_with(Some(m))
            }

            // 503 preview 超时（wrap 前缀契约）。
            "start_503_preview_timeout" => {
                let m = build_mgr(Arc::new(TimeoutPreview), "127.0.0.1:0", &fixture);
                cleanup_mgr = Some(Arc::clone(&m));
                server_with(Some(m))
            }

            // 503 其它失败：bind 必败地址 → StartError::Bind wrap 形态。
            "start_503_other" => {
                let m = build_mgr(Arc::new(OkPreview), "not-a-bindable-addr", &fixture);
                cleanup_mgr = Some(Arc::clone(&m));
                server_with(Some(m))
            }

            // 需要 active session 的动态路由 / stream 行。
            "dyn_404_unknown_resource" | "snapshot_503_stub" => {
                let m = mgr_with_session(&fixture);
                cleanup_mgr = Some(Arc::clone(&m));
                server_with(Some(m))
            }
            "stream_504_no_keyframe" => {
                // 空 FrameBuffer + tunable startup timeout 压缩（锚 Go setTestStartupTimeout）。
                restore_timeout = Some(set_stream_startup_timeout_ms(100));
                let m = mgr_with_session(&fixture);
                cleanup_mgr = Some(Arc::clone(&m));
                server_with(Some(m))
            }
            "stream_504_missing_sps_pps" => {
                // 仅推 IDR：wait_idr 过、latest_idr 三件套不齐。
                let m = mgr_with_session(&fixture);
                let sess = m.current_by_id(SESSION_ID).expect("session");
                sess.frame_buf.push(NalUnit {
                    nal_type: NAL_TYPE_IDR,
                    data: vec![0x65, 0xb8, 0x00, 0x04],
                    timestamp: 100_000,
                });
                cleanup_mgr = Some(Arc::clone(&m));
                server_with(Some(m))
            }
            "stream_503_session_closed" => {
                // FrameBuffer 已 close（wait_idr 立即返 Closed 分型）。
                let m = mgr_with_session(&fixture);
                m.current_by_id(SESSION_ID)
                    .expect("session")
                    .frame_buf
                    .close();
                cleanup_mgr = Some(Arc::clone(&m));
                server_with(Some(m))
            }

            // 其余行（400×4 / 405 / 415 / stop 幂等 / dyn 判定）：Manager 在位但
            // 不会被触发 wire（stop 幂等走无匹配 session 静默路径）。
            _ => {
                let m = ok_mgr(&fixture);
                cleanup_mgr = Some(Arc::clone(&m));
                server_with(Some(m))
            }
        };

        let got_raw = roundtrip_raw_server(handler, &req);
        if let Some(old) = restore_timeout {
            set_stream_startup_timeout_ms(old);
        }
        if let Some(m) = cleanup_mgr {
            m.shutdown(Duration::from_secs(3));
        }

        let want = parse_http_response_full(&want_raw);
        let got = parse_http_response_full(&got_raw);

        assert_eq!(want.status, want_status, "{name} golden 自洽");
        assert_eq!(got.status, want_status, "{name} status");
        assert_eq!(
            header_of(&got.headers, "Content-Type"),
            header_of(&want.headers, "Content-Type"),
            "{name} Content-Type"
        );
        assert_eq!(
            header_of(&got.headers, "Allow"),
            header_of(&want.headers, "Allow"),
            "{name} Allow"
        );
        assert_eq!(
            header_of(&got.headers, "Content-Length").is_some(),
            header_of(&want.headers, "Content-Length").is_some(),
            "{name} CL presence"
        );
        assert_eq!(
            header_of(&got.headers, "Transfer-Encoding").is_some(),
            header_of(&want.headers, "Transfer-Encoding").is_some(),
            "{name} chunked presence"
        );

        if let Some(prefix) = prefix_sentinel(name) {
            // runtime-variable 内文行：sentinel 前缀 + wrap 形态断言（双侧）。
            let got_body = String::from_utf8_lossy(&got.body);
            let want_body = String::from_utf8_lossy(&want.body);
            assert!(
                want_body.starts_with(prefix),
                "{name} golden 自洽: want={want_body:?}"
            );
            assert!(
                got_body.starts_with(prefix),
                "{name} body prefix: got={got_body:?} want prefix={prefix:?}"
            );
            assert!(
                got_body.ends_with("\"}"),
                "{name} wrap 形态（errorJSON 单字段闭合）: got={got_body:?}"
            );
        } else {
            assert_eq!(
                String::from_utf8_lossy(&got.body),
                String::from_utf8_lossy(&want.body),
                "{name} body"
            );
        }
    }

    let _ = fs::remove_file(&fixture);
}
