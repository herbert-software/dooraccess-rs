//! Phase 5 mock 外机 video e2e（`port-rust-video-forward` 组 G / tasks 7.1；
//! spec「跨语言 golden、e2e 与体型 Gate」③）。
//!
//! 链路全真组件：HTTP（httpx server + control mux）→ `Manager`（默认依赖 =
//! 真 `PreviewClient` / 真 `RtpReceiver` / 真 `RtcpSender`）→ FrameBuffer →
//! `StreamWriter`。mock 仅限外机/网络对端：
//!   - mock 18022 TCP ack server（收 req=704/708 帧回 705/709 ack，事件记录）
//!   - mock UDP 6671 receiver（断言 teardown 发出 RTCP BYE 复合包）
//!   - UDP replay `testdata/rtp-fragmentation/expA.packets.tsv` 子集（含 ≥2 个
//!     完整 IDR frame 的连续段）灌真 RTP receiver
//!
//! 覆盖（任务 7.1 钉死的链条）：start→stream FLV 前缀断言 + ≥2 keyframe tag
//! （0x17）与 tag 计数断言→多消费者 fan-out→慢消费者丢帧不阻塞→stop/bye
//! teardown→TTL 过期 detached cleanup。全 tunable 压缩（TTL/monitor/tail-wait
//! 注入口），**禁真实 60s 等待**——成功路径的等待全部是「条件就绪即返回」的
//! poll，长 deadline 只在失败时才会耗尽。
//!
//! ## 沙箱
//!
//! 须沙箱外跑（loopback TCP/UDP bind）；bind 失败 `eprintln!+return` 静默跳过
//! （沙箱内不假绿），对齐 `daemon_e2e.rs` 约定。
//!
//! ## 串行化
//!
//! RTCP 反馈端口写死 outdoor:6671（`Outdoor::addr_rtcp`，协议事实非 tunable），
//! 三个 e2e 测试都要 bind 127.0.0.1:6671 —— 用文件级 `E2E_SERIAL` 锁串行执行。

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use dooraccess_rs::config::{Config, Listen, Station};
use dooraccess_rs::control::Server as ControlServer;
use dooraccess_rs::httpx::server::Server as HttpServer;
use dooraccess_rs::httpx::Handler;
use dooraccess_rs::sender::MockSender;
use dooraccess_rs::video::reassembler::FrameReassembler;
use dooraccess_rs::video::rtp::{NalUnit, RtpPacket, NAL_TYPE_IDR, NAL_TYPE_PPS, NAL_TYPE_SPS};
use dooraccess_rs::video::session::{parse_caller, Manager};
use dooraccess_rs::video::transmux::{
    build_avc_nalu_tag, build_avc_sequence_header_tag, flv_header,
};
use dooraccess_rs::video::SharedLogFn;

// ===========================================================================
// 串行化（共享 127.0.0.1:6671 + rtcp/stream tunable 全局原子）
// ===========================================================================

static E2E_SERIAL: Mutex<()> = Mutex::new(());

fn serial_guard() -> MutexGuard<'static, ()> {
    E2E_SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

// ===========================================================================
// 共享夹具
// ===========================================================================

/// 全 e2e 共用的 RTP replay 源 SSRC（首包锁存为外机 stream SSRC）。
const REPLAY_SSRC: u32 = 0x1234_abcd;

/// 室内机（caller）URI——BCD 进 preview 帧；IP 部分不参与 e2e 链路。
const CALLER_URI: &str = "06021103@127.0.0.1:18022";

fn hex_to_bytes(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd hex length");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("bad hex"))
        .collect()
}

/// req=705 start ack 典型样本（与 `preview.rs` 单测同源）。
fn start_ack_bytes() -> Vec<u8> {
    hex_to_bytes("07b8180000007265713d3730352671756572792a00008000000002000001")
}

/// req=709 stop ack 典型样本。
fn stop_ack_bytes() -> Vec<u8> {
    hex_to_bytes("07b8180000007265713d3730392671756572792a00008000000002000001")
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// 条件 poll（10ms 步进；只在失败时才耗尽 deadline——成功路径即时返回）。
fn poll_until<F: FnMut() -> bool>(deadline: Duration, mut cond: F) -> bool {
    let end = Instant::now() + deadline;
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= end {
            return false;
        }
        thread::park_timeout(Duration::from_millis(10));
    }
}

// ---------------------------------------------------------------------------
// expA fixture 装载 + 离线重放计划（与 expa_parity.rs 同源 TSV 解析）
// ---------------------------------------------------------------------------

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/rtp-fragmentation/expA.packets.tsv")
}

/// 读取 expA.packets.tsv（列：seq \t timestamp \t marker \t payload-hex）。
fn load_packets_tsv() -> Vec<RtpPacket> {
    let text = std::fs::read_to_string(fixture_path()).expect("read expA.packets.tsv");
    let mut pkts = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if i == 0 || line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        assert_eq!(parts.len(), 4, "malformed TSV line {}", i + 1);
        pkts.push(RtpPacket {
            seq: parts[0].parse().expect("seq"),
            timestamp: parts[1].parse().expect("ts"),
            marker: parts[2] == "True",
            pt: 98,
            ssrc: REPLAY_SSRC,
            payload: hex_to_bytes(parts[3]),
        });
    }
    pkts
}

/// 把 [`RtpPacket`] 编回 wire 字节（V=2 / P=0 / X=0 / CC=0 / PT=98 / NBO）。
fn raw_rtp(p: &RtpPacket) -> Vec<u8> {
    let mut b = Vec::with_capacity(12 + p.payload.len());
    b.push(0x80);
    b.push(if p.marker { 0x80 | p.pt } else { p.pt });
    b.extend_from_slice(&p.seq.to_be_bytes());
    b.extend_from_slice(&p.timestamp.to_be_bytes());
    b.extend_from_slice(&p.ssrc.to_be_bytes());
    b.extend_from_slice(&p.payload);
    b
}

/// 离线重放计划：用独立重组器扫一遍 fixture，定出「frame 1 完成包」与「第 2 个
/// IDR frame 完成包」边界 + 期望 NAL 序列（与真 receiver 灌同序输入必然同输出）。
struct ReplayPlan {
    /// frame 1（SPS+PPS+IDR 种子）完成于此包（含）。
    stage1_end: usize,
    /// 第 2 个 IDR frame 完成于此包（含）——stage 2 = stage1_end+1 ..= stage2_end。
    stage2_end: usize,
    /// 种子三件套（frame 1 切出的 NAL，序：SPS, PPS, IDR）。
    seed: Vec<NalUnit>,
    /// stage 2 期间切出的全部 NAL（订阅消费者将逐个收到）。
    stage2_nals: Vec<NalUnit>,
}

fn plan_replay(pkts: &[RtpPacket]) -> ReplayPlan {
    let mut r = FrameReassembler::new(None);
    let mut stage1_end = None;
    let mut seed = Vec::new();
    let mut stage2_nals = Vec::new();
    for (i, p) in pkts.iter().enumerate() {
        let (nals, _complete) = r.push(p.clone());
        if stage1_end.is_none() {
            if !nals.is_empty() {
                seed = nals;
                stage1_end = Some(i);
            }
            continue;
        }
        let has_idr = nals.iter().any(|n| n.nal_type == NAL_TYPE_IDR);
        stage2_nals.extend(nals);
        if has_idr {
            // 第 2 个 IDR frame 完成：stage 2 到此为止。
            let s1 = stage1_end.unwrap();
            assert_eq!(
                seed.iter().map(|n| n.nal_type).collect::<Vec<_>>(),
                vec![NAL_TYPE_SPS, NAL_TYPE_PPS, NAL_TYPE_IDR],
                "frame 1 必须切出 SPS+PPS+IDR 种子三件套"
            );
            return ReplayPlan {
                stage1_end: s1,
                stage2_end: i,
                seed,
                stage2_nals,
            };
        }
    }
    panic!("expA fixture 中未找到第 2 个 IDR frame");
}

/// 计算消费者应收到的完整 FLV 字节流：13B 头部块 + seq-header tag(ts=0) +
/// 种子 IDR tag(ts=0) + stage 2 NAL 逐 tag（跳 SPS/PPS；ts 基 = 种子 IDR RTP ts）。
/// 全部用与 StreamWriter 相同的公开构造函数——byte-exact 锚。
fn expected_stream_bytes(plan: &ReplayPlan) -> Vec<u8> {
    let sps = &plan.seed[0];
    let pps = &plan.seed[1];
    let idr = &plan.seed[2];
    let mut out = Vec::new();
    out.extend_from_slice(&flv_header());
    out.extend_from_slice(
        &build_avc_sequence_header_tag(&sps.data, &pps.data, 0).expect("seq header tag"),
    );
    out.extend_from_slice(
        &build_avc_nalu_tag(std::slice::from_ref(idr), 0).expect("seed idr tag"),
    );
    for n in &plan.stage2_nals {
        if n.nal_type == NAL_TYPE_SPS || n.nal_type == NAL_TYPE_PPS {
            continue;
        }
        let ts = n.timestamp.wrapping_sub(idr.timestamp) / 90;
        out.extend_from_slice(
            &build_avc_nalu_tag(std::slice::from_ref(n), ts).expect("stage2 nalu tag"),
        );
    }
    out
}

// ---------------------------------------------------------------------------
// mock 外机：18022 TCP ack server + 6671 RTCP UDP receiver
// ---------------------------------------------------------------------------

/// 起 mock 外机 18022 ack server：每连接单次 read，含 `req=704` 回 705 ack +
/// 记 "start"；含 `req=708` 回 709 ack + 记 "stop"。bind 失败（沙箱）返 None。
fn spawn_mock_outdoor(
    events: Arc<Mutex<Vec<String>>>,
    max_conns: usize,
) -> Option<(String, u16)> {
    let ln = match TcpListener::bind("127.0.0.1:0") {
        Ok(l) => l,
        Err(e) => {
            eprintln!("skip video e2e: tcp bind failed (sandbox?): {e}");
            return None;
        }
    };
    let addr = ln.local_addr().unwrap();
    thread::spawn(move || {
        for _ in 0..max_conns {
            let Ok((mut conn, _)) = ln.accept() else {
                return;
            };
            let mut buf = [0u8; 64];
            let n = conn.read(&mut buf).unwrap_or(0);
            let b = &buf[..n];
            if contains_subslice(b, b"req=704") {
                events
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push("start".into());
                let _ = conn.write_all(&start_ack_bytes());
            } else if contains_subslice(b, b"req=708") {
                events
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push("stop".into());
                let _ = conn.write_all(&stop_ack_bytes());
            }
        }
    });
    Some((addr.ip().to_string(), addr.port()))
}

/// bind mock 外机 RTCP 反馈口 127.0.0.1:6671（协议写死端口，E2E_SERIAL 保独占）。
fn bind_rtcp_6671() -> Option<UdpSocket> {
    match UdpSocket::bind("127.0.0.1:6671") {
        Ok(s) => {
            s.set_read_timeout(Some(Duration::from_millis(200))).ok();
            Some(s)
        }
        Err(e) => {
            eprintln!("skip video e2e: udp 6671 bind failed (sandbox?): {e}");
            None
        }
    }
}

/// RTCP 复合包内是否含 BYE（PT=203）子包（逐子包按 length 字段步进）。
fn compound_has_bye(b: &[u8]) -> bool {
    let mut o = 0;
    while o + 4 <= b.len() {
        if b[o + 1] == 203 {
            return true;
        }
        let words = u16::from_be_bytes([b[o + 2], b[o + 3]]) as usize;
        o += (words + 1) * 4;
    }
    false
}

/// 在 `deadline` 内等到一个含 BYE 的 RTCP 复合包（keepalive RR+SDES 包被跳过）。
fn wait_for_bye(sock: &UdpSocket, deadline: Duration) -> bool {
    let end = Instant::now() + deadline;
    let mut buf = [0u8; 256];
    while Instant::now() < end {
        match sock.recv_from(&mut buf) {
            Ok((n, _)) if compound_has_bye(&buf[..n]) => return true,
            Ok(_) => continue, // 周期 RR+SDES（无 BYE）。
            Err(_) => continue, // read timeout：回环再试。
        }
    }
    false
}

// ---------------------------------------------------------------------------
// daemon 侧装配（真 Manager 默认依赖 + control server + httpx）
// ---------------------------------------------------------------------------

/// 构造真依赖 Manager：preview/rtp/rtcp 全默认（真 PreviewClient / 真
/// RtpReceiver / 真 RtcpSender），logf 收集到共享 Vec（RTP 监听地址从中解析）。
fn build_manager(
    logs: &Arc<Mutex<Vec<String>>>,
    ttl: Option<Duration>,
    monitor_interval: Option<Duration>,
) -> Arc<Manager> {
    let sink = Arc::clone(logs);
    let logf: SharedLogFn = Arc::new(move |m: &str| {
        sink.lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(m.to_string());
    });
    let caller = parse_caller(CALLER_URI).expect("caller");
    let mut m = Manager::new(caller, Some(logf));
    m.rtp_listen_addr = "127.0.0.1:0".into();
    m.teardown_tail_wait = Duration::from_millis(10); // 压缩 ~50ms 尾等待。
    if let Some(t) = ttl {
        m.ttl = t;
    }
    if let Some(iv) = monitor_interval {
        m.ttl_monitor_interval = iv;
    }
    Arc::new(m)
}

/// control server（allowlist = 单 outdoor station）+ video Manager 注入。
fn build_video_handler(outdoor_uri: &str, mgr: Arc<Manager>) -> Arc<dyn Handler> {
    let mut cfg = Config::default();
    cfg.sip = CALLER_URI.into();
    cfg.listen = Listen {
        addr: "127.0.0.1".into(),
        port: 0,
    };
    cfg.stations = vec![Station {
        sip: outdoor_uri.into(),
        rtsp_url: String::new(),
    }];
    let mut srv = ControlServer::new(cfg, "video-e2e", None, None, None, MockSender::success());
    srv.set_video(Some(mgr), None);
    Arc::new(srv.handler())
}

/// 起 loopback HTTP server（套路对齐 daemon_e2e.rs）。bind 失败返 None（跳过）。
fn start_http(
    handler: Arc<dyn Handler>,
) -> Option<(SocketAddr, Arc<HttpServer>, thread::JoinHandle<()>)> {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(l) => l,
        Err(e) => {
            eprintln!("skip video e2e: http bind failed (sandbox?): {e}");
            return None;
        }
    };
    let addr = listener.local_addr().unwrap();
    let srv = Arc::new(HttpServer::new());
    let s2 = Arc::clone(&srv);
    let t = thread::spawn(move || {
        let _ = s2.serve(listener, handler);
    });
    Some((addr, srv, t))
}

fn stop_http(srv: Arc<HttpServer>, http_thread: thread::JoinHandle<()>) {
    let _ = srv.shutdown(Some(Duration::from_secs(5)));
    http_thread.join().ok();
}

/// 单次 HTTP 请求（Connection: close），返 (status, body)。
fn http_request(addr: SocketAddr, method: &str, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let mut conn = TcpStream::connect(addr).expect("connect");
    conn.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    req.extend_from_slice(body);
    conn.write_all(&req).expect("write req");
    conn.shutdown(std::net::Shutdown::Write).ok();
    let mut raw = Vec::new();
    let _ = conn.read_to_end(&mut raw);
    let status: u16 = std::str::from_utf8(&raw)
        .ok()
        .and_then(|s| s.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body_at = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|p| p + 4)
        .unwrap_or(raw.len());
    (status, raw[body_at..].to_vec())
}

/// 从 start 200 响应 body 提取 session_id。
fn extract_session_id(body: &[u8]) -> String {
    let s = String::from_utf8_lossy(body);
    let key = "\"session_id\":\"";
    let at = s.find(key).unwrap_or_else(|| panic!("no session_id in {s}"));
    s[at + key.len()..]
        .chars()
        .take_while(|c| *c != '"')
        .collect()
}

/// 等 Manager 日志里出现 RTP 监听地址行，解析实际 bind 端口。
fn wait_rtp_addr(logs: &Arc<Mutex<Vec<String>>>) -> SocketAddr {
    let mut found = None;
    let ok = poll_until(Duration::from_secs(5), || {
        let v = logs.lock().unwrap_or_else(|e| e.into_inner());
        for line in v.iter() {
            if let Some(rest) = line.strip_prefix("video: rtp listening on ") {
                if let Some(addr) = rest.split_whitespace().next() {
                    if let Ok(a) = addr.parse::<SocketAddr>() {
                        found = Some(a);
                        return true;
                    }
                }
            }
        }
        false
    });
    assert!(ok, "rtp listening log line not seen");
    found.unwrap()
}

// ---------------------------------------------------------------------------
// stream 消费者（GET stream.flv 长连接，原始字节增量收集）
// ---------------------------------------------------------------------------

struct StreamConsumer {
    raw: Arc<Mutex<Vec<u8>>>,
    reader: thread::JoinHandle<()>,
}

/// 开 GET stream.flv 长连接 + 后台贪读线程（增量收集 raw 字节直到 EOF）。
fn open_stream(addr: SocketAddr, session_id: &str) -> StreamConsumer {
    let mut conn = TcpStream::connect(addr).expect("connect stream");
    conn.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let req = format!(
        "GET /video/{session_id}/stream.flv HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
    );
    conn.write_all(req.as_bytes()).expect("write stream req");
    let raw = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&raw);
    let reader = thread::spawn(move || {
        let mut buf = [0u8; 8192];
        loop {
            match conn.read(&mut buf) {
                Ok(0) => return, // EOF：stream 收口。
                Ok(n) => sink
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .extend_from_slice(&buf[..n]),
                Err(_) => return, // read timeout / reset：测试侧兜底退出。
            }
        }
    });
    StreamConsumer { raw, reader }
}

impl StreamConsumer {
    fn dechunked(&self) -> Vec<u8> {
        let raw = self.raw.lock().unwrap_or_else(|e| e.into_inner());
        dechunk_partial(&raw)
    }

    /// 等读线程 EOF 收口，返最终去 chunked body。
    fn join_body(self) -> Vec<u8> {
        self.reader.join().expect("stream consumer join");
        let raw = self.raw.lock().unwrap_or_else(|e| e.into_inner());
        dechunk_partial(&raw)
    }
}

/// 增量去 chunked 框架：只解码 raw 中已完整到达的 chunk（容忍流仍在进行中）。
fn dechunk_partial(raw: &[u8]) -> Vec<u8> {
    let Some(he) = raw.windows(4).position(|w| w == b"\r\n\r\n") else {
        return Vec::new();
    };
    // 状态行须 200（错误响应是 Content-Length JSON 体，不会进本解码路径）。
    let mut out = Vec::new();
    let mut o = he + 4;
    loop {
        let Some(le) = raw[o..].windows(2).position(|w| w == b"\r\n") else {
            return out;
        };
        let Ok(line) = std::str::from_utf8(&raw[o..o + le]) else {
            return out;
        };
        let Ok(size) = usize::from_str_radix(line.trim(), 16) else {
            return out;
        };
        if size == 0 {
            return out; // 终止 chunk。
        }
        let data_start = o + le + 2;
        if data_start + size + 2 > raw.len() {
            return out; // chunk 尚未完整到达。
        }
        out.extend_from_slice(&raw[data_start..data_start + size]);
        o = data_start + size + 2;
    }
}

/// FLV 流静态校验：13B 头部块前缀 + 逐 tag 走链，统计 video tag 与
/// keyframe NALU tag（data[0]=0x17 且 AVCPacketType=0x01）。
struct FlvStats {
    video_tags: usize,
    keyframe_nalu_tags: usize,
}

fn parse_flv(body: &[u8]) -> FlvStats {
    assert!(
        body.len() >= 13 && body[..13] == flv_header(),
        "FLV 头部块前缀不符: {:02x?}",
        &body[..body.len().min(13)]
    );
    let mut stats = FlvStats {
        video_tags: 0,
        keyframe_nalu_tags: 0,
    };
    let mut o = 13;
    while o + 11 <= body.len() {
        let tag_type = body[o];
        let size = ((body[o + 1] as usize) << 16) | ((body[o + 2] as usize) << 8)
            | (body[o + 3] as usize);
        let end = o + 11 + size + 4; // tag header + data + PreviousTagSize。
        if end > body.len() {
            break; // 不完整尾 tag（增量读截断）——不计入。
        }
        if tag_type == 0x09 {
            stats.video_tags += 1;
            let data = &body[o + 11..o + 11 + size];
            if data.len() >= 2 && data[0] == 0x17 && data[1] == 0x01 {
                stats.keyframe_nalu_tags += 1;
            }
        }
        o = end;
    }
    stats
}

/// 把 `pkts[range]` 按序 replay 到 `dst`（pace 防 UDP rcvbuf 溢出丢包）。
fn replay(
    tx: &UdpSocket,
    dst: SocketAddr,
    pkts: &[RtpPacket],
    range: std::ops::RangeInclusive<usize>,
    pace: Duration,
) {
    for p in &pkts[*range.start()..=*range.end()] {
        tx.send_to(&raw_rtp(p), dst).expect("udp replay send");
        thread::sleep(pace);
    }
}

/// e2e 通用装配结果。
struct Rig {
    events: Arc<Mutex<Vec<String>>>,
    logs: Arc<Mutex<Vec<String>>>,
    mgr: Arc<Manager>,
    addr: SocketAddr,
    srv: Arc<HttpServer>,
    http_thread: thread::JoinHandle<()>,
    outdoor_uri: String,
    rtcp_sock: UdpSocket,
}

/// 起 mock 外机 + 真 Manager + control HTTP server。任一 bind 失败返 None（沙箱跳过）。
fn setup(ttl: Option<Duration>, monitor: Option<Duration>, max_conns: usize) -> Option<Rig> {
    let rtcp_sock = bind_rtcp_6671()?;
    let events = Arc::new(Mutex::new(Vec::new()));
    let (ip, port) = spawn_mock_outdoor(Arc::clone(&events), max_conns)?;
    let outdoor_uri = format!("06020000@{ip}:{port}");
    let logs = Arc::new(Mutex::new(Vec::new()));
    let mgr = build_manager(&logs, ttl, monitor);
    let handler = build_video_handler(&outdoor_uri, Arc::clone(&mgr));
    let (addr, srv, http_thread) = start_http(handler)?;
    Some(Rig {
        events,
        logs,
        mgr,
        addr,
        srv,
        http_thread,
        outdoor_uri,
        rtcp_sock,
    })
}

fn events_snapshot(events: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    events.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

// ===========================================================================
// e2e 1：start → stream（FLV 前缀 + ≥2 keyframe + tag 计数 byte-exact）→
//         多消费者 fan-out → stop/bye teardown
// ===========================================================================

#[test]
fn e2e_start_stream_fanout_stop_bye() {
    let _g = serial_guard();
    let Some(rig) = setup(None, None, 4) else {
        return;
    };

    let pkts = load_packets_tsv();
    let plan = plan_replay(&pkts);
    let expected = expected_stream_bytes(&plan);
    // 期望流自检：恰 2 个 keyframe NALU tag（种子 IDR + stage 2 的第 2 个 IDR）。
    let want = parse_flv(&expected);
    assert!(
        want.keyframe_nalu_tags >= 2,
        "replay 子集须含 ≥2 keyframe，got {}",
        want.keyframe_nalu_tags
    );

    // POST /video/start（经真 PreviewClient 与 mock 外机完成 704/705 往返）。
    let start_body = format!(r#"{{"outdoor":"{}"}}"#, rig.outdoor_uri);
    let (status, body) = http_request(rig.addr, "POST", "/video/start", start_body.as_bytes());
    assert_eq!(status, 200, "start: {}", String::from_utf8_lossy(&body));
    let sid = extract_session_id(&body);
    assert_eq!(events_snapshot(&rig.events), vec!["start"], "preview start 信令");

    // 真 receiver 实际 bind 的 RTP 端口（127.0.0.1:0 随机注入口）。
    let rtp_dst = wait_rtp_addr(&rig.logs);
    let tx = UdpSocket::bind("127.0.0.1:0").expect("replay socket");

    // 消费者 A 先行连上（走 wait_idr 等首 IDR 路径）。
    let a = open_stream(rig.addr, &sid);

    // stage 1：replay 至 frame 1 完成（SPS+PPS+IDR 种子三件套切出）。
    replay(&tx, rtp_dst, &pkts, 0..=plan.stage1_end, Duration::from_micros(100));

    // A 收到 13B 头部块 + seq-header + 种子 IDR（FLV 前缀断言的就绪信号）。
    let seed_len = {
        let sps = &plan.seed[0];
        let pps = &plan.seed[1];
        let idr = &plan.seed[2];
        13 + build_avc_sequence_header_tag(&sps.data, &pps.data, 0).unwrap().len()
            + build_avc_nalu_tag(std::slice::from_ref(idr), 0).unwrap().len()
    };
    assert!(
        poll_until(Duration::from_secs(5), || a.dechunked().len() >= seed_len),
        "consumer A 未收到种子前缀"
    );

    // 消费者 B 后入场（latest_idr 已缓存 → 立即种子起播）→ fan-out 双订阅。
    let b = open_stream(rig.addr, &sid);
    assert!(
        poll_until(Duration::from_secs(5), || b.dechunked().len() >= seed_len),
        "consumer B 未收到种子前缀"
    );
    // 订阅注册缝（StreamWriter 写种子后才 subscribe）的保险间隙。
    thread::sleep(Duration::from_millis(100));

    // stage 2：replay 到第 2 个 IDR frame 完成——A/B 同步收增量 tag。
    replay(
        &tx,
        rtp_dst,
        &pkts,
        plan.stage1_end + 1..=plan.stage2_end,
        Duration::from_micros(100),
    );
    assert!(
        poll_until(Duration::from_secs(5), || {
            a.dechunked().len() >= expected.len() && b.dechunked().len() >= expected.len()
        }),
        "fan-out 消费者未收齐 stage 2 增量（A={} B={} want={}）",
        a.dechunked().len(),
        b.dechunked().len(),
        expected.len()
    );

    // stop/bye teardown：POST /video/stop → 200 {"result":0} + req=708 + BYE。
    let (status, body) = http_request(rig.addr, "POST", "/video/stop", start_body.as_bytes());
    assert_eq!(status, 200, "stop: {}", String::from_utf8_lossy(&body));
    assert!(
        String::from_utf8_lossy(&body).contains("\"result\":0"),
        "stop body: {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(
        events_snapshot(&rig.events),
        vec!["start", "stop"],
        "teardown 必须发 req=708 stop preview"
    );
    assert!(
        wait_for_bye(&rig.rtcp_sock, Duration::from_secs(2)),
        "teardown 必须向 outdoor:6671 发 RTCP BYE 复合包"
    );
    assert!(rig.mgr.current().is_none(), "stop 后 active session 必须摘除");

    // FrameBuffer close → 订阅断 → StreamWriter 正常退出 → 两消费者 EOF 收口。
    // byte-exact：FLV 前缀 + 全部 tag 字节与离线期望逐字节一致（双消费者同流）。
    let got_a = a.join_body();
    let got_b = b.join_body();
    assert_eq!(got_a, expected, "consumer A FLV 字节流");
    assert_eq!(got_b, expected, "consumer B FLV 字节流（fan-out 等价）");

    // tag 计数 + ≥2 keyframe（0x17）显式断言（任务 7.1 字面要求）。
    let got = parse_flv(&got_a);
    assert_eq!(got.video_tags, want.video_tags, "video tag 计数");
    assert!(
        got.keyframe_nalu_tags >= 2,
        "≥2 keyframe tag(0x17)，got {}",
        got.keyframe_nalu_tags
    );

    rig.mgr.shutdown(Duration::from_secs(3));
    stop_http(rig.srv, rig.http_thread);
}

// ===========================================================================
// e2e 2：慢消费者丢帧不阻塞快消费者（全量 replay 压满慢端 socket/channel）
// ===========================================================================

#[test]
fn e2e_slow_consumer_does_not_block_fast() {
    let _g = serial_guard();
    let Some(rig) = setup(None, None, 4) else {
        return;
    };

    let pkts = load_packets_tsv();
    let plan = plan_replay(&pkts);

    let start_body = format!(r#"{{"outdoor":"{}"}}"#, rig.outdoor_uri);
    let (status, body) = http_request(rig.addr, "POST", "/video/start", start_body.as_bytes());
    assert_eq!(status, 200, "start: {}", String::from_utf8_lossy(&body));
    let sid = extract_session_id(&body);
    let rtp_dst = wait_rtp_addr(&rig.logs);
    let tx = UdpSocket::bind("127.0.0.1:0").expect("replay socket");

    // fast：贪读消费者；slow：发出请求后**一个字节都不读**（handler 写满 socket
    // 缓冲后阻塞 → 其 chan(64) 积满 → try_send 丢帧，但 RTP receiver 与 fast
    // 不得受影响——Go parity：丢帧静默，无计数可观测，断言面 = fast 不受阻）。
    let fast = open_stream(rig.addr, &sid);
    let mut slow_conn = TcpStream::connect(rig.addr).expect("slow connect");
    slow_conn
        .write_all(
            format!("GET /video/{sid}/stream.flv HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .expect("slow req");

    // stage 1 种子 → fast 就绪。
    replay(&tx, rtp_dst, &pkts, 0..=plan.stage1_end, Duration::from_micros(100));
    assert!(
        poll_until(Duration::from_secs(5), || !fast.dechunked().is_empty()),
        "fast 未收到种子"
    );
    thread::sleep(Duration::from_millis(150)); // fast/slow 订阅注册缝。

    // 全量 replay（1.4MB > loopback socket 缓冲 → slow 端必然进入写阻塞 + 丢帧）。
    replay(
        &tx,
        rtp_dst,
        &pkts,
        plan.stage1_end + 1..=pkts.len() - 1,
        Duration::from_micros(50),
    );

    // fast 必须持续进流：≥2 keyframe + 大量 tag（slow 若阻塞 fan-out，这里收敛不了）。
    assert!(
        poll_until(Duration::from_secs(10), || {
            let s = parse_flv(&fast.dechunked());
            s.keyframe_nalu_tags >= 2 && s.video_tags >= 150
        }),
        "fast 消费者被慢消费者拖死（tags={} keyframes={}）",
        parse_flv(&fast.dechunked()).video_tags,
        parse_flv(&fast.dechunked()).keyframe_nalu_tags
    );

    // 收口：先掐 slow 连接（解除其 handler 写阻塞），再 stop teardown。
    slow_conn.shutdown(std::net::Shutdown::Both).ok();
    drop(slow_conn);
    let (status, _) = http_request(rig.addr, "POST", "/video/stop", start_body.as_bytes());
    assert_eq!(status, 200, "stop");
    assert!(
        wait_for_bye(&rig.rtcp_sock, Duration::from_secs(2)),
        "teardown BYE"
    );
    // fast 流自洽收口（tag 链可走完，FLV 前缀成立——parse_flv 内含前缀断言）。
    let fast_body = fast.join_body();
    let s = parse_flv(&fast_body);
    assert!(s.keyframe_nalu_tags >= 2, "fast 最终 keyframe 计数");

    rig.mgr.shutdown(Duration::from_secs(3));
    stop_http(rig.srv, rig.http_thread);
}

// ===========================================================================
// e2e 3：TTL 过期 detached cleanup（tunable 压缩；禁真实 60s）
// ===========================================================================

#[test]
fn e2e_ttl_expiry_detached_cleanup() {
    let _g = serial_guard();
    // TTL 250ms + monitor 50ms：无消费者、无 IDR（死 session）→ 过期自动清场。
    let Some(rig) = setup(
        Some(Duration::from_millis(250)),
        Some(Duration::from_millis(50)),
        4,
    ) else {
        return;
    };

    let start_body = format!(r#"{{"outdoor":"{}"}}"#, rig.outdoor_uri);
    let (status, body) = http_request(rig.addr, "POST", "/video/start", start_body.as_bytes());
    assert_eq!(status, 200, "start: {}", String::from_utf8_lossy(&body));
    assert_eq!(events_snapshot(&rig.events), vec!["start"]);

    // 五件套「TTL cleanup 及时」：2s 量级内 current 摘除 + req=708 + BYE 全到位。
    let t0 = Instant::now();
    assert!(
        poll_until(Duration::from_secs(2), || rig.mgr.current().is_none()),
        "TTL 过期未在 2s 内摘除 active session"
    );
    assert!(
        poll_until(Duration::from_secs(2), || {
            events_snapshot(&rig.events).contains(&"stop".to_string())
        }),
        "detached cleanup 未发 req=708 stop preview"
    );
    assert!(
        wait_for_bye(&rig.rtcp_sock, Duration::from_secs(2)),
        "detached cleanup 未发 RTCP BYE"
    );
    assert!(
        t0.elapsed() < Duration::from_secs(2),
        "cleanup 总耗时超 2s 量级: {:?}",
        t0.elapsed()
    );

    // Manager shutdown 对账 detached cleanup（deadline 内排空，不悬挂）。
    let t1 = Instant::now();
    rig.mgr.shutdown(Duration::from_secs(3));
    assert!(
        t1.elapsed() < Duration::from_secs(3),
        "shutdown 对账超时: {:?}",
        t1.elapsed()
    );
    stop_http(rig.srv, rig.http_thread);
}
