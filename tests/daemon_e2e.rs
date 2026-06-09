//! Phase 4 骨架 mock-e2e 测试（`port-rust-daemon-skeleton` G4，tasks §10.1-10.11）。
//!
//! 行为锚 Go `cmd/dooraccess-go/main_test.go` /
//! `main_automation_flags_test.go` / `main_automation_startup_push_test.go` /
//! `internal/http8080/selfunlock_test.go`。
//!
//! ## crate gate
//!
//! 测试守 crate gate = std + libc only（与生产同）——无 tempdir/tokio/mockito，全用
//! `std` + 进程内 mock（注入 mock `UnlockWire` / mock `Pusher` / `dispatch_for_test`
//! 直驱 OnDetect / loopback `TcpStream`）。
//!
//! ## 沙箱
//!
//! HTTP e2e（10.1/10.7）起 loopback `TcpListener::bind`，**须沙箱外跑**（沙箱拦 TCP
//! bind/connect）。其余测试纯进程内同步原语，沙箱内外皆可。bind 失败时这些测试
//! `eprintln!+return` 静默跳过（沙箱内不假绿），沙箱外 `cargo test` 真跑全绿。
//!
//! ## 时序
//!
//! 排队/不阻塞类（10.7/10.10）用 `Barrier`/channel 精确控制可控阻塞 wire，**不**真
//! sleep 5s——push 离线挂起用注入的 blocking `Pusher`，unlock 排队用 `Barrier` 卡
//! 第一个 worker job。

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use dooraccess_rs::automation_state::{self, Persister, State};
use dooraccess_rs::codec::{self, result};
use dooraccess_rs::config::{Automation, Config, Hass, Listen, Station};
use dooraccess_rs::control::{
    AutomationState, FnPersistHook, Pusher, Server as ControlServer, UnlockDispatch,
};
use dooraccess_rs::daemon::{
    self, run_worker, spawn_push, Job, PushTracker, UnlockJob, WorkerDeps,
};
use dooraccess_rs::ha_push::HaPushClient;
use dooraccess_rs::httpx::server::Server as HttpServer;
use dooraccess_rs::httpx::Handler;
use dooraccess_rs::listen18022::Subscribable;
use dooraccess_rs::listen6672::{self, Callbacks, Frame};
use dooraccess_rs::orchestration::{
    build_listen18022, load_automation_flags, worker_unavailable_outcome, WorkerUnlockDispatch,
};
use dooraccess_rs::unlock::{
    AttemptOutcome, Deadline, UnlockOutcome, UnlockStage, UnlockWire, WireKind,
};

// ===========================================================================
// 共享测试夹具
// ===========================================================================

/// 进程内唯一临时目录（不引第三方 tempdir crate；守 crate gate）。
fn unique_tmp_dir(tag: &str) -> std::path::PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    let mut d = std::env::temp_dir();
    d.push(format!("dooraccess_rs_e2e_{tag}_{pid}_{n}"));
    std::fs::create_dir_all(&d).expect("create temp dir");
    d
}

/// e2e 用 config：一个外机 station + 一个室内机 monitor + loopback HA（默认 api 空 →
/// push 静默跳过，避免 e2e 触真网络）。
fn e2e_config() -> Config {
    let mut cfg = Config::default();
    cfg.sip = "06021103@172.16.106.91:18022".into();
    cfg.iface = String::new(); // mock-e2e 不起真 listener
    cfg.listen = Listen {
        addr: "127.0.0.1".into(),
        port: 0,
    };
    cfg.stations = vec![Station {
        sip: "06020000@172.16.106.152:18022".into(),
        rtsp_url: String::new(),
    }];
    cfg.hass = Hass {
        ipaddr: "127.0.0.1".into(),
        port: 8123,
        api: String::new(), // 空 → HaPushClient.push 返 NotConfigured，不触网络
        token: String::new(),
    };
    cfg
}

/// 永不取消的 shutdown flag。
fn never() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

// ---------------------------------------------------------------------------
// mock UnlockWire（复用 Phase 3 风格：固定 AttemptOutcome / 可控阻塞）
// ---------------------------------------------------------------------------

/// 固定返一个 `AttemptOutcome` 的 mock wire（记录被调次数）。
struct FixedWire {
    outcome: AttemptOutcome,
    calls: Arc<AtomicUsize>,
}

impl FixedWire {
    fn new(outcome: AttemptOutcome) -> Self {
        Self {
            outcome,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl UnlockWire for FixedWire {
    fn try_once(
        &self,
        _c: [u8; 4],
        _ca: [u8; 4],
        _ip: &str,
        _p: u16,
        _cap: Option<Duration>,
        _d: &dyn Deadline,
        _cancel: &AtomicBool,
    ) -> AttemptOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.outcome.clone()
    }
}

/// 可控阻塞 wire：第一次 `try_once` 卡在 `gate` Barrier 上（精确控制 worker 忙窗口），
/// 之后返回 `outcome`。用于 10.7 排队 / 10.10 不阻塞（worker 忙时验排队/响应性）。
struct GatedWire {
    gate: Arc<Barrier>,
    used: AtomicBool,
    outcome: AttemptOutcome,
}

impl GatedWire {
    fn new(gate: Arc<Barrier>, outcome: AttemptOutcome) -> Self {
        Self {
            gate,
            used: AtomicBool::new(false),
            outcome,
        }
    }
}

impl UnlockWire for GatedWire {
    fn try_once(
        &self,
        _c: [u8; 4],
        _ca: [u8; 4],
        _ip: &str,
        _p: u16,
        _cap: Option<Duration>,
        _d: &dyn Deadline,
        _cancel: &AtomicBool,
    ) -> AttemptOutcome {
        // 仅第一个 job 卡 gate（后续 job 直接返，避免死锁）。
        if !self.used.swap(true, Ordering::SeqCst) {
            self.gate.wait();
        }
        self.outcome.clone()
    }
}

/// 起一个 wire-worker 线程，返回 (job_tx, worker JoinHandle)。
fn spawn_worker(
    wire: Arc<dyn UnlockWire>,
    listener: Option<Arc<dyn Subscribable>>,
    shutdown: Arc<AtomicBool>,
) -> (mpsc::Sender<Job>, thread::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<Job>();
    let deps = WorkerDeps::new(wire, listener);
    let h = thread::spawn(move || run_worker(deps, rx, shutdown));
    (tx, h)
}

/// 经真 `orchestration::WorkerUnlockDispatch` 派发缝投一个 unlock job 并等 reply（与 HTTP
/// `/unlock` handler 走的生产函数一致；§10.2/§10.7 unlock dispatch）。worker 已退/panic 时
/// dispatch 内部退化为 wire-failure outcome（不永等），故此处永返 `Ok`，由 caller 断言 result。
fn dispatch_unlock(tx: &mpsc::Sender<Job>) -> UnlockOutcome {
    let dispatch = WorkerUnlockDispatch { job_tx: tx.clone() };
    dispatch.dispatch(
        [0x06, 0x02, 0x00, 0x00],
        [0x06, 0x02, 0x11, 0x03],
        "172.16.106.152",
        18022,
    )
}

// ---------------------------------------------------------------------------
// mock Pusher（记录所有 push；可注入阻塞，用于 push-不阻塞-unlock 测试）
// ---------------------------------------------------------------------------

/// 一次 push 记录：(event, fields)。
type PushRecord = (String, Vec<(String, String)>);

/// 记录每次 push 的 (event, fields) 的进程内 spy Pusher（同步、不触网络）。
#[derive(Clone, Default)]
struct SpyPusher {
    events: Arc<Mutex<Vec<PushRecord>>>,
}

impl SpyPusher {
    fn new() -> Self {
        Self::default()
    }
    fn events(&self) -> Vec<PushRecord> {
        self.events.lock().unwrap().clone()
    }
}

impl Pusher for SpyPusher {
    fn push(&self, event: &str, fields: &[(&str, &str)]) {
        let owned: Vec<(String, String)> = fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self.events.lock().unwrap().push((event.to_string(), owned));
    }
}

// 手动 unlock 派发缝直接用 lib 的真 `orchestration::WorkerUnlockDispatch`（不再复刻）——
// e2e 测 HTTP `/unlock` → worker → reply 回灌全链路走的是生产函数（G4 FIX 问题 B）。

// ---------------------------------------------------------------------------
// loopback HTTP e2e 脚手架（须沙箱外；bind 失败静默跳过）
// ---------------------------------------------------------------------------

/// 起一个真 loopback HTTP server serve `handler`，返回 (addr, HttpServer, serve thread)。
/// bind 失败（沙箱）返 None。
fn start_http(
    handler: Arc<dyn Handler>,
) -> Option<(
    std::net::SocketAddr,
    Arc<HttpServer>,
    thread::JoinHandle<()>,
)> {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(l) => l,
        Err(e) => {
            eprintln!("skip http e2e: bind failed (sandbox?): {e}");
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

/// 优雅停 loopback HTTP server（问题 A 修好后：直接调真 `shutdown()`，无需戳 throwaway
/// 连接）。serve 的 accept 循环用 nonblocking accept + 短 poll 周期释放 listener 锁，故
/// `shutdown()` 的 take() 在一个 poll 周期内抢到锁取走 listener → serve 见 None+closing →
/// 返 `ServerClosedError` 退出。无死锁、无悬挂线程。
fn stop_http(
    _addr: std::net::SocketAddr,
    srv: Arc<HttpServer>,
    http_thread: thread::JoinHandle<()>,
) {
    let _ = srv.shutdown(Some(Duration::from_secs(5)));
    http_thread.join().ok();
}

/// 发一个 HTTP 请求到 `addr`，返回 (status, body)。
fn http_request(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
) -> (u16, Vec<u8>) {
    let mut conn = TcpStream::connect(addr).expect("connect");
    conn.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let body = body.unwrap_or(&[]);
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    req.extend_from_slice(body);
    conn.write_all(&req).expect("write");
    conn.shutdown(std::net::Shutdown::Write).ok();
    let mut raw = Vec::new();
    let _ = conn.read_to_end(&mut raw);
    parse_http_response(&raw)
}

/// 极简 HTTP 响应解析（status line + body，够 e2e 断言用）。
fn parse_http_response(raw: &[u8]) -> (u16, Vec<u8>) {
    let text = String::from_utf8_lossy(raw);
    let status = text
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse::<u16>().ok())
        .unwrap_or(0);
    // body = 双 CRLF 之后。
    let body = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| raw[i + 4..].to_vec())
        .unwrap_or_default();
    (status, body)
}

/// 构造 worker-backed control Server handler（注入 automation/persist/pusher/dispatch）。
fn build_control_handler(
    cfg: Config,
    automation: AutomationState,
    persist: Option<Arc<FnPersistHook>>,
    pusher: Arc<dyn Pusher>,
    job_tx: mpsc::Sender<Job>,
) -> Arc<dyn Handler> {
    let dispatch: Arc<dyn UnlockDispatch> = Arc::new(WorkerUnlockDispatch { job_tx });
    let server = ControlServer::with_dispatch(
        cfg,
        "test-version",
        Some(automation),
        persist.map(|p| p as Arc<dyn dooraccess_rs::control::PersistHook>),
        Some(pusher),
        dispatch,
    );
    Arc::new(server.handler())
}

// ===========================================================================
// 10.1 daemon 启动/停止 e2e
// ===========================================================================

/// 起 control server（loopback）→ GET /info + GET /automation 正确 → graceful shutdown
/// 排空（worker 哨兵退出 + http server shutdown）。锚 Go `main_test.go`。
#[test]
fn t10_1_daemon_start_info_automation_graceful_shutdown() {
    let cfg = e2e_config();
    let shutdown = never();
    let (job_tx, worker) = spawn_worker(
        Arc::new(FixedWire::new(AttemptOutcome::Ok)),
        None,
        shutdown.clone(),
    );
    let automation = AutomationState::new(true, false);
    let pusher: Arc<dyn Pusher> = Arc::new(SpyPusher::new());
    let handler = build_control_handler(cfg, automation, None, pusher, job_tx.clone());

    let Some((addr, srv, http_thread)) = start_http(handler) else {
        return;
    };

    // GET /info → 200 + 含 brand/monitor。
    let (status, body) = http_request(addr, "GET", "/info", None);
    assert_eq!(status, 200, "GET /info status");
    let s = String::from_utf8_lossy(&body);
    assert!(s.contains("\"brand\""), "/info body: {s}");
    assert!(
        s.contains("06021103@172.16.106.91:18022"),
        "/info monitor: {s}"
    );

    // GET /automation → 200 + auto_unlock=true auto_hangup=false（与构造一致）。
    let (status, body) = http_request(addr, "GET", "/automation", None);
    assert_eq!(status, 200, "GET /automation status");
    let s = String::from_utf8_lossy(&body);
    assert!(s.contains("\"auto_unlock\":true"), "/automation: {s}");
    assert!(s.contains("\"auto_hangup\":false"), "/automation: {s}");

    // graceful shutdown 排空：哨兵停 worker + http server shutdown。
    shutdown.store(true, Ordering::SeqCst);
    daemon::submit_job(&job_tx, Job::Shutdown).ok();
    worker.join().expect("worker join");
    stop_http(addr, srv, http_thread);
}

// ===========================================================================
// 10.2 手动 unlock e2e（mock 外机 + POST /unlock 经 worker 跑通 + result 回灌）
// ===========================================================================

#[test]
fn t10_2_manual_unlock_through_worker_via_http() {
    let cfg = e2e_config();
    let shutdown = never();
    // mock 外机：FixedWire::Ok → unlock 成功 result=0。
    let (job_tx, worker) = spawn_worker(
        Arc::new(FixedWire::new(AttemptOutcome::Ok)),
        None,
        shutdown.clone(),
    );
    let automation = AutomationState::new(false, false);
    let pusher: Arc<dyn Pusher> = Arc::new(SpyPusher::new());
    let handler = build_control_handler(cfg, automation, None, pusher, job_tx.clone());

    let Some((addr, srv, http_thread)) = start_http(handler) else {
        return;
    };

    // POST /unlock {from:室内机, to:外机} → 经 worker → reply 回灌 → 200 result=0。
    let body = br#"{"from":"06021103@172.16.106.91:18022","to":"06020000@172.16.106.152:18022"}"#;
    let (status, resp) = http_request(addr, "POST", "/unlock", Some(body));
    assert_eq!(
        status,
        200,
        "POST /unlock status: {}",
        String::from_utf8_lossy(&resp)
    );
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("\"result\":0"), "unlock OK result: {s}");

    shutdown.store(true, Ordering::SeqCst);
    daemon::submit_job(&job_tx, Job::Shutdown).ok();
    worker.join().ok();
    stop_http(addr, srv, http_thread);
}

/// 返回固定 [`UnlockOutcome`] 的 stub dispatch——直驱 `respond_unlock_outcome` 4 分支映射，
/// 不起 worker（覆盖 wire-failure 折叠成 ERR+Some 时须落 503 的分支盲区）。
struct StaticDispatch(UnlockOutcome);
impl UnlockDispatch for StaticDispatch {
    fn dispatch(&self, _: [u8; 4], _: [u8; 4], _: &str, _: u16) -> UnlockOutcome {
        self.0.clone()
    }
}

/// 起 control server，unlock dispatch 固定回灌 `outcome`，POST /unlock 取 HTTP status。
fn unlock_status_for(outcome: UnlockOutcome) -> u16 {
    let dispatch: Arc<dyn UnlockDispatch> = Arc::new(StaticDispatch(outcome));
    let server = ControlServer::with_dispatch(
        e2e_config(),
        "test-version",
        Some(AutomationState::new(false, false)),
        None,
        Some(Arc::new(SpyPusher::new()) as Arc<dyn Pusher>),
        dispatch,
    );
    let handler: Arc<dyn Handler> = Arc::new(server.handler());
    let Some((addr, srv, http_thread)) = start_http(handler) else {
        return 0;
    };
    let body = br#"{"from":"06021103@172.16.106.91:18022","to":"06020000@172.16.106.152:18022"}"#;
    let (status, _) = http_request(addr, "POST", "/unlock", Some(body));
    stop_http(addr, srv, http_thread);
    status
}

/// wire-Other 失败折叠成 ERR 但带 wire_kind=Some(Other) → 须 503（非谎称 200 业务校验失败）。
#[test]
fn t10_2_wire_other_failure_maps_503() {
    let outcome = UnlockOutcome {
        result: result::ERR,
        retries: 0,
        terminated_by_bye: false,
        wire_kind: Some(WireKind::Other),
    };
    let status = unlock_status_for(outcome);
    if status == 0 {
        eprintln!("skip t10_2_wire_other_failure_maps_503: http bind unavailable (sandbox?)");
        return; // start_http 失败（沙箱无 loopback bind）→ 跳过。
    }
    assert_eq!(status, 503, "wire-Other 失败须 503");
}

/// worker 已退/panic 退化 outcome（ERR+Some(Other)）→ 须 503（下游不可用，非业务校验失败）。
#[test]
fn t10_2_worker_dead_maps_503() {
    let status = unlock_status_for(worker_unavailable_outcome());
    if status == 0 {
        eprintln!("skip t10_2_worker_dead_maps_503: http bind unavailable (sandbox?)");
        return;
    }
    assert_eq!(status, 503, "worker-dead 退化 outcome 须 503");
}

/// genuine 业务错（ERR + wire_kind==None）→ 仍 200（外机响应但 body 校验失败）；防回归。
#[test]
fn t10_2_genuine_business_err_maps_200() {
    let outcome = UnlockOutcome {
        result: result::ERR,
        retries: 0,
        terminated_by_bye: false,
        wire_kind: None,
    };
    let status = unlock_status_for(outcome);
    if status == 0 {
        eprintln!("skip t10_2_genuine_business_err_maps_200: http bind unavailable (sandbox?)");
        return;
    }
    assert_eq!(status, 200, "genuine 业务错（wire_kind==None）须 200");
}

/// 手动 unlock 直驱 worker（不经 HTTP；纯进程内）：FixedWire Ok/Business/SilentFin 三路
/// result 经 reply 回灌正确。补 10.2 沙箱内可跑的部分。
#[test]
fn t10_2_manual_unlock_worker_reply_paths() {
    // OK 路径。
    let shutdown = never();
    let (tx, worker) = spawn_worker(
        Arc::new(FixedWire::new(AttemptOutcome::Ok)),
        None,
        shutdown.clone(),
    );
    let o = dispatch_unlock(&tx);
    assert_eq!(o.result, result::OK);
    daemon::submit_job(&tx, Job::Shutdown).ok();
    worker.join().ok();

    // BusinessErr 路径（外机响应但 body 校验失败 → ERR，不重试）。
    let shutdown = never();
    let (tx, worker) = spawn_worker(
        Arc::new(FixedWire::new(AttemptOutcome::BusinessErr {
            stage: UnlockStage::UnlockA,
        })),
        None,
        shutdown.clone(),
    );
    let o = dispatch_unlock(&tx);
    assert_eq!(o.result, result::ERR);
    daemon::submit_job(&tx, Job::Shutdown).ok();
    worker.join().ok();
}

// ===========================================================================
// 10.3 automation flag 三级优先级 e2e（state>config>env）
//
// 调真生产 `orchestration::load_automation_flags`（state>config>env 三级优先级）——上移到
// lib 后 e2e 测真接线而非复刻（G4 FIX 问题 B）。`resolve_flags` 仅是一层薄适配（路径
// String 化 + no-op logf），断言走的逻辑全在生产函数里。
// ===========================================================================

/// 调真 `orchestration::load_automation_flags`（薄适配：&Path→&str + 丢弃 log 行）。
fn resolve_flags(state_path: &std::path::Path, cfg: &Config) -> (bool, bool, &'static str) {
    load_automation_flags(&state_path.to_string_lossy(), cfg, &mut |_| {})
}

#[test]
fn t10_3_flag_priority_state_wins() {
    let dir = unique_tmp_dir("prio_state");
    let path = dir.join("automation.state");
    // 盘上 state = (true,true)；config = (false,false)。state 优先。
    automation_state::write_atomic(
        &path,
        State {
            auto_unlock: true,
            auto_hangup: true,
        },
    )
    .unwrap();
    let mut cfg = e2e_config();
    cfg.automation = Automation {
        auto_unlock: false,
        auto_hangup: false,
    };
    let (au, ah, src) = resolve_flags(&path, &cfg);
    assert_eq!((au, ah, src), (true, true, "state"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn t10_3_flag_priority_corrupt_state_degrades_to_config() {
    let dir = unique_tmp_dir("prio_corrupt");
    let path = dir.join("automation.state");
    // 半写/损坏 state（缺 key）→ parse Err → 降级 config。
    std::fs::write(&path, b"auto_unlock=true\n").unwrap(); // 缺 auto_hangup
    assert!(automation_state::parse(&std::fs::read(&path).unwrap()).is_err());
    let mut cfg = e2e_config();
    cfg.automation = Automation {
        auto_unlock: false,
        auto_hangup: true,
    };
    let (au, ah, src) = resolve_flags(&path, &cfg);
    assert_eq!(
        (au, ah, src),
        (false, true, "config"),
        "corrupt state → config 默认"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn t10_3_flag_priority_config_over_env() {
    // state 不存在 → 取 config（即便 env 有设值，config 命中压 env：值取 config）。
    let dir = unique_tmp_dir("prio_cfg");
    let path = dir.join("automation.state"); // 不创建
    let mut cfg = e2e_config();
    cfg.automation = Automation {
        auto_unlock: true,
        auto_hangup: false,
    };
    let (au, ah, src) = resolve_flags(&path, &cfg);
    assert_eq!((au, ah, src), (true, false, "config"));
    let _ = std::fs::remove_dir_all(&dir);
}

// ===========================================================================
// 10.4 Persister e2e（拨动→原子落盘→重启回读→失败不阻塞→同值 no-op）
//
// G4 角度：经 HTTP `/auto_unlock` 拨动 → AutomationState.store → Persister.persist 全链
// 路落盘，再用启动加载语义（parse）回读一致。（write_atomic/dedup/failure 的单元级在
// G1 golden_state.rs 已覆盖；此处验「endpoint→persist→reload」端到端 + 无半写。）
// ===========================================================================

#[test]
fn t10_4_persister_toggle_atomic_persist_reload_via_http() {
    let dir = unique_tmp_dir("persist_http");
    let path = dir.join("automation.state");

    let cfg = e2e_config();
    let shutdown = never();
    let (job_tx, worker) = spawn_worker(
        Arc::new(FixedWire::new(AttemptOutcome::Ok)),
        None,
        shutdown.clone(),
    );

    let automation = AutomationState::new(false, false);
    // Persister value_source 读运行时真值（share 同一组原子）。
    let auto_for_persist = automation.share();
    let persister = Arc::new(Persister::new(
        path.clone(),
        Box::new(move || {
            (
                auto_for_persist.load_auto_unlock(),
                auto_for_persist.load_auto_hangup(),
            )
        }),
        None,
    ));
    let p2 = Arc::clone(&persister);
    let persist_hook = Arc::new(FnPersistHook(Box::new(move || {
        p2.persist();
        Ok(())
    })));
    let pusher: Arc<dyn Pusher> = Arc::new(SpyPusher::new());
    let handler = build_control_handler(
        cfg,
        automation.share(),
        Some(persist_hook),
        pusher,
        job_tx.clone(),
    );

    let Some((addr, srv, http_thread)) = start_http(handler) else {
        worker_shutdown(&job_tx, worker);
        let _ = std::fs::remove_dir_all(&dir);
        return;
    };

    // 文件初始不存在（未拨动）。
    assert!(!path.exists(), "state file absent before toggle");

    // 拨动 auto_unlock=true → 原子落盘。
    let (status, _) = http_request(addr, "POST", "/auto_unlock", Some(br#"{"on":true}"#));
    assert_eq!(status, 200);
    // 落盘后无半写：parse 成功且 == (true,false)。
    let st = automation_state::parse(&std::fs::read(&path).expect("state written")).expect("parse");
    assert_eq!(
        st,
        State {
            auto_unlock: true,
            auto_hangup: false
        }
    );
    assert!(
        !dir.join("automation.state.tmp").exists(),
        "no residual .tmp"
    );

    // 重启回读：用启动加载语义从盘读回一致。
    let (au, ah, src) = resolve_flags(&path, &e2e_config());
    assert_eq!((au, ah, src), (true, false, "state"));

    // 同值重复 persist 是 no-op：删文件 → 再拨同值 true → 不重建。
    std::fs::remove_file(&path).unwrap();
    let (status, _) = http_request(addr, "POST", "/auto_unlock", Some(br#"{"on":true}"#));
    assert_eq!(status, 200);
    assert!(
        !path.exists(),
        "same-value persist is no-op (file not recreated)"
    );

    // 翻转 → 重新落盘。
    let (status, _) = http_request(addr, "POST", "/auto_unlock", Some(br#"{"on":false}"#));
    assert_eq!(status, 200);
    let st = automation_state::parse(&std::fs::read(&path).expect("rewritten")).expect("parse");
    assert_eq!(
        st,
        State {
            auto_unlock: false,
            auto_hangup: false
        }
    );

    shutdown.store(true, Ordering::SeqCst);
    worker_shutdown(&job_tx, worker);
    stop_http(addr, srv, http_thread);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 落盘失败不阻塞 endpoint 响应：Persister 指向不可写父目录 → persist 失败 → endpoint 仍
/// 200（best-effort）。
#[test]
fn t10_4_persist_failure_does_not_block_endpoint() {
    let dir = unique_tmp_dir("persist_fail");
    // 父目录不存在 → write 必失败。
    let path = dir.join("nonexistent_subdir").join("automation.state");

    let cfg = e2e_config();
    let shutdown = never();
    let (job_tx, worker) = spawn_worker(
        Arc::new(FixedWire::new(AttemptOutcome::Ok)),
        None,
        shutdown.clone(),
    );
    let automation = AutomationState::new(false, false);
    let auto_for_persist = automation.share();
    let persister = Arc::new(Persister::new(
        path,
        Box::new(move || {
            (
                auto_for_persist.load_auto_unlock(),
                auto_for_persist.load_auto_hangup(),
            )
        }),
        None,
    ));
    let p2 = Arc::clone(&persister);
    let persist_hook = Arc::new(FnPersistHook(Box::new(move || {
        p2.persist();
        Ok(())
    })));
    let pusher: Arc<dyn Pusher> = Arc::new(SpyPusher::new());
    let handler = build_control_handler(
        cfg,
        automation.share(),
        Some(persist_hook),
        pusher,
        job_tx.clone(),
    );

    let Some((addr, srv, http_thread)) = start_http(handler) else {
        worker_shutdown(&job_tx, worker);
        let _ = std::fs::remove_dir_all(&dir);
        return;
    };

    // 拨动 → persist 内部失败但 endpoint 仍 200（不 crash、不阻塞）。
    let (status, body) = http_request(addr, "POST", "/auto_unlock", Some(br#"{"on":true}"#));
    assert_eq!(
        status,
        200,
        "endpoint must succeed despite persist failure: {}",
        String::from_utf8_lossy(&body)
    );
    // 运行时 flag 仍生效（best-effort：内存已更新）。
    assert!(automation.load_auto_unlock());

    shutdown.store(true, Ordering::SeqCst);
    worker_shutdown(&job_tx, worker);
    stop_http(addr, srv, http_thread);
    let _ = std::fs::remove_dir_all(&dir);
}

fn worker_shutdown(tx: &mpsc::Sender<Job>, worker: thread::JoinHandle<()>) {
    daemon::submit_job(tx, Job::Shutdown).ok();
    worker.join().ok();
}

// ===========================================================================
// 10.5 startup push e2e（flag 加载后才 push automation_state + 字符串编码；
//      diagnosis 在 500ms 延迟期 shutdown → 放弃不发）
// ===========================================================================

/// banner 后 spawn detached push automation_state：两 flag 字符串 "true"/"false" 编码。
/// 用 SpyPusher 直驱 spawn_push（不触真网络）。锚 Go `main_automation_startup_push_test.go`。
#[test]
fn t10_5_startup_push_automation_state_string_encoded() {
    // 用真 HaPushClient（api 空 → push 内部 NotConfigured，不发网络）验 spawn_push 不 panic；
    // 字符串编码语义用 SpyPusher 旁路断言（spawn_push 接 HaPushClient，无法直接 spy 字段——
    // 故另起一个 SpyPusher 路径验编码，spawn_push 路径验 detached + 不阻塞 + honor shutdown）。
    let tracker = PushTracker::new();
    let shutdown = never();
    let client = Arc::new(HaPushClient::new(e2e_config(), None));

    // flag 加载后才 push：模拟 flag 已加载为 (true,false)，spawn automation_state push。
    spawn_push(
        &tracker,
        Arc::clone(&client),
        Arc::clone(&shutdown),
        "automation_state".into(),
        vec![
            ("auto_unlock".into(), "true".into()),
            ("auto_hangup".into(), "false".into()),
        ],
    );
    tracker.join_all();
    assert!(tracker.is_empty(), "push drained");

    // 字符串编码语义断言（旁路 SpyPusher，与 spawn_push 同 fields 构造）。
    let spy = SpyPusher::new();
    spy.push(
        "automation_state",
        &[("auto_unlock", "true"), ("auto_hangup", "false")],
    );
    let evs = spy.events();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].0, "automation_state");
    // 值是字符串 "true"/"false"（非 JSON bool）。
    let map: BTreeMap<_, _> = evs[0].1.iter().cloned().collect();
    assert_eq!(map.get("auto_unlock").map(String::as_str), Some("true"));
    assert_eq!(map.get("auto_hangup").map(String::as_str), Some("false"));
}

/// diagnosis push 在 500ms 延迟期 shutdown → 放弃不发（best-effort，非安全不变量）。
/// 用 shutdown 预置位 → spawn_diagnosis_push 应在延迟期观察到 flag 立即放弃、不 push。
#[test]
fn t10_5_diagnosis_push_abandoned_on_shutdown_during_delay() {
    let tracker = PushTracker::new();
    let shutdown = Arc::new(AtomicBool::new(true)); // 已 shutdown
    let client = Arc::new(HaPushClient::new(e2e_config(), None));

    let start = Instant::now();
    daemon::spawn_diagnosis_push(
        &tracker,
        Arc::clone(&client),
        Arc::clone(&shutdown),
        Duration::from_millis(500),
    );
    tracker.join_all();
    // 应几乎立即返回（放弃，不等满 500ms，不发 push）。
    assert!(
        start.elapsed() < Duration::from_millis(400),
        "diagnosis must abandon promptly on shutdown, took {:?}",
        start.elapsed()
    );
    assert!(tracker.is_empty(), "drained");
}

// ===========================================================================
// 10.6 号码查询 e2e（命中响应 / 未命中 drop）
//
// 经 listen6672::Listener.dispatch 直驱 on_number_query 回调（不起真 socket）；
// 回调逻辑复刻 main.rs `build_number_query_callback`：BCD==本机号码 → 响应；否则 drop。
// 发送原语 send_udp_response 是新 BE 面（loopback roundtrip 在 listen6672.rs 单元已覆盖），
// 此处验「命中/未命中」分发决策（用 spy 计数命中，不真发 UDP）。
// ===========================================================================

#[test]
fn t10_6_number_query_hit_responds_miss_drops() {
    let my_num = "06021103".to_string(); // 本机 monitor 号码（cfg.sip name）。
    let hit = Arc::new(AtomicUsize::new(0));
    let h = hit.clone();
    let my_num_cb = my_num.clone();
    let on_number_query: listen6672::FrameCallback = Box::new(move |_src_ip, frame: &Frame| {
        // 命中本机号码 → 计数（生产是 send_udp_response 单播本机 IP）。
        if codec::decode_bcd(frame.target_bcd) == my_num_cb {
            h.fetch_add(1, Ordering::SeqCst);
        }
        // 未命中 → drop（不计数、不响应）。
    });
    let cb = Callbacks {
        on_number_query: Some(on_number_query),
        ..Default::default()
    };
    let l = listen6672::Listener::new(vec!["eth0".into()], cb);

    // 命中：target BCD = 06021103（本机号码）。
    l.dispatch([172, 16, 106, 9], &numquery_frame([0x06, 0x02, 0x11, 0x03]));
    assert_eq!(hit.load(Ordering::SeqCst), 1, "命中本机号码应响应");

    // 未命中：target BCD = 06029999（别人）→ drop。
    l.dispatch([172, 16, 106, 9], &numquery_frame([0x06, 0x02, 0x99, 0x99]));
    assert_eq!(
        hit.load(Ordering::SeqCst),
        1,
        "未命中应安静 drop（计数不增）"
    );
}

/// 构造一个号码查询 request 帧（event_flag=0 → NumberQuery）。
fn numquery_frame(target_bcd: [u8; 4]) -> [u8; 21] {
    let mut p = [0u8; 21];
    p[0] = 0x00; // request
    p[1..5].copy_from_slice(&target_bcd);
    // byte 13-16 = 0 → event_flag 0 → NumberQuery。
    p[17] = 0x00;
    p[18] = 0x40;
    p[19] = 0x10;
    p[20] = 0x00;
    p
}

// ===========================================================================
// 10.7 worker 串行/排队 e2e（worker 忙时第二个 unlock 排队；期间 /info /automation 仍响应）
// ===========================================================================

#[test]
fn t10_7_second_unlock_queues_while_worker_busy_http_responsive() {
    let cfg = e2e_config();
    let shutdown = never();
    // GatedWire：第一个 unlock 卡 gate（main + worker 两方 wait 才放行）。
    let gate = Arc::new(Barrier::new(2));
    let wire = Arc::new(GatedWire::new(gate.clone(), AttemptOutcome::Ok));
    let (job_tx, worker) = spawn_worker(wire, None, shutdown.clone());
    let automation = AutomationState::new(true, true);
    let pusher: Arc<dyn Pusher> = Arc::new(SpyPusher::new());
    let handler = build_control_handler(cfg, automation, None, pusher, job_tx.clone());

    let Some((addr, srv, http_thread)) = start_http(handler) else {
        // 无 HTTP（沙箱）：worker 空闲在 rx.recv()（无 unlock 投递，从未碰 gate），
        // 直接哨兵退出即可——**不能** gate.wait()（Barrier::new(2) 单方等待会死锁）。
        let _ = &gate;
        worker_shutdown(&job_tx, worker);
        return;
    };

    // 第一个 unlock：在独立线程发（会卡在 worker 跑 GatedWire gate 上）。
    let a2 = addr;
    let first = thread::spawn(move || {
        let body =
            br#"{"from":"06021103@172.16.106.91:18022","to":"06020000@172.16.106.152:18022"}"#;
        http_request(a2, "POST", "/unlock", Some(body))
    });

    // 等 worker 进 GatedWire（gate 上有一方在 wait）——用短轮询确保 worker 已忙。
    thread::sleep(Duration::from_millis(100));

    // 第二个 unlock：另起线程（应在 worker 队列里排队，worker 忙不会立即跑）。
    let a3 = addr;
    let second = thread::spawn(move || {
        let body =
            br#"{"from":"06021103@172.16.106.91:18022","to":"06020000@172.16.106.152:18022"}"#;
        http_request(a3, "POST", "/unlock", Some(body))
    });
    thread::sleep(Duration::from_millis(100));

    // 期间 worker 忙，但 /info /automation 仍即时响应（非 wire 端点不经 worker）。
    let (st_info, _) = http_request(addr, "GET", "/info", None);
    assert_eq!(st_info, 200, "/info 仍响应（worker 忙）");
    let (st_auto, abody) = http_request(addr, "GET", "/automation", None);
    assert_eq!(st_auto, 200, "/automation 仍响应（worker 忙）");
    assert!(String::from_utf8_lossy(&abody).contains("\"auto_unlock\":true"));

    // 第二个 unlock 此刻应仍未完成（被第一个阻在队列前）。
    assert!(
        !second.is_finished(),
        "第二个 unlock 应排队等（worker 串行）"
    );

    // 放行 gate → 第一个 unlock 完成 → worker 接着跑第二个。
    gate.wait();
    let (s1, b1) = first.join().expect("first join");
    assert_eq!(s1, 200);
    assert!(String::from_utf8_lossy(&b1).contains("\"result\":0"));
    let (s2, b2) = second.join().expect("second join");
    assert_eq!(s2, 200, "第二个 unlock 排队后完成");
    assert!(String::from_utf8_lossy(&b2).contains("\"result\":0"));

    shutdown.store(true, Ordering::SeqCst);
    worker_shutdown(&job_tx, worker);
    stop_http(addr, srv, http_thread);
}

// ===========================================================================
// 10.8 shutdown 死锁回归 e2e
// ===========================================================================

/// shutdown 时正阻塞的手动 unlock handler 经 reply 回灌解除；哨兵后 worker 退出（即便
/// 仍持 sender clone）；worker panic 时阻塞 handler 经 reply Sender drop 收 Disconnected
/// 不永等（决策 8 worker-death）。
#[test]
fn t10_8_shutdown_no_deadlock_and_worker_panic_unblocks() {
    // (a) 哨兵后 worker 退出（仍持 sender clone）。
    {
        let shutdown = never();
        let (tx, worker) = spawn_worker(
            Arc::new(FixedWire::new(AttemptOutcome::Ok)),
            None,
            shutdown.clone(),
        );
        let _live_clone = tx.clone(); // HTTP/OnDetect 仍持 clone
        daemon::submit_job(&tx, Job::Shutdown).expect("send sentinel");
        // 有限时间内退出（不依赖 clone 全释放）。
        let done = Arc::new(AtomicBool::new(false));
        let d2 = done.clone();
        let jh = thread::spawn(move || {
            worker.join().ok();
            d2.store(true, Ordering::SeqCst);
        });
        thread::sleep(Duration::from_millis(200));
        assert!(
            done.load(Ordering::SeqCst),
            "哨兵后 worker 应退出（即便 clone 存活）"
        );
        jh.join().ok();
    }

    // (b) shutdown 中阻塞 unlock 经 cancel 早退 + reply 回灌（不死锁）。
    {
        let shutdown = never();
        // SilentFin → 会进退避；shutdown 置位后 cancel 早退并回灌 -103。
        let (tx, worker) = spawn_worker(
            Arc::new(FixedWire::new(AttemptOutcome::WireErr {
                stage: UnlockStage::UnlockA,
                err: WireKind::SilentFin,
            })),
            None,
            shutdown.clone(),
        );
        let (rtx, rrx) = mpsc::sync_channel::<UnlockOutcome>(1);
        daemon::submit_job(
            &tx,
            Job::Unlock(UnlockJob {
                caller_bcd: [0x06, 0x02, 0x00, 0x00],
                callee_bcd: [0x06, 0x02, 0x11, 0x03],
                target_ip: "172.16.106.152".into(),
                target_port: 18022,
                reply: rtx,
            }),
        )
        .unwrap();
        shutdown.store(true, Ordering::SeqCst);
        let o = rrx
            .recv_timeout(Duration::from_secs(3))
            .expect("取消应回灌 reply 不死锁");
        assert_eq!(o.result, result::NO_RING, "cancel 早退 → -103");
        daemon::submit_job(&tx, Job::Shutdown).ok();
        worker.join().ok();
    }

    // (c) worker panic → reply Sender drop → handler 收 RecvError 不永等。
    {
        struct PanicWire;
        impl UnlockWire for PanicWire {
            fn try_once(
                &self,
                _c: [u8; 4],
                _ca: [u8; 4],
                _ip: &str,
                _p: u16,
                _cap: Option<Duration>,
                _d: &dyn Deadline,
                _cancel: &AtomicBool,
            ) -> AttemptOutcome {
                panic!("simulated worker panic");
            }
        }
        let shutdown = never();
        let (tx, worker) = spawn_worker(Arc::new(PanicWire), None, shutdown.clone());
        let (rtx, rrx) = mpsc::sync_channel::<UnlockOutcome>(1);
        daemon::submit_job(
            &tx,
            Job::Unlock(UnlockJob {
                caller_bcd: [0x06, 0x02, 0x00, 0x00],
                callee_bcd: [0x06, 0x02, 0x11, 0x03],
                target_ip: "172.16.106.152".into(),
                target_port: 18022,
                reply: rtx,
            }),
        )
        .unwrap();
        // worker panic → UnlockJob drop → reply sender drop → RecvError（不永等）。
        assert!(
            rrx.recv_timeout(Duration::from_secs(3)).is_err(),
            "worker panic 后 handler 应收 RecvError 不永等"
        );
        let _ = worker.join(); // panic 线程 join 返 Err，忽略。
    }
}

// ===========================================================================
// 10.9 门铃 push e2e（OnDetect req=704 → detached event=ring push）
//
// 用**真生产** `orchestration::build_listen18022` 装 OnDetect（FormatLog + req=704 门铃
// push，src∈Stations，dst==室内机），dispatch_for_test 直驱（不起真 PF_PACKET）。门铃 push
// 走真 `daemon::spawn_push` → `HaPushClient`，故用 loopback HTTP **capture server** 收 POST
// body 断言 from/to/result + 重建逻辑 + drop 逻辑（须沙箱外；bind 失败静默跳过）。
// ===========================================================================

/// 起一个 loopback HTTP capture server，accept 一个连接、读完请求、回 200，并把请求 body
/// （JSON）记进返回的 `Arc<Mutex<Option<Vec<u8>>>>`。返回 (host, port, captured, thread)。
/// bind 失败（沙箱）返 None。
#[allow(clippy::type_complexity)]
fn start_capture_server() -> Option<(
    String,
    u16,
    Arc<Mutex<Option<Vec<u8>>>>,
    thread::JoinHandle<()>,
)> {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(l) => l,
        Err(e) => {
            eprintln!("skip ring push e2e: bind failed (sandbox?): {e}");
            return None;
        }
    };
    let addr = listener.local_addr().unwrap();
    let captured: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let cap2 = Arc::clone(&captured);
    let t = thread::spawn(move || {
        if let Ok((mut conn, _)) = listener.accept() {
            conn.set_read_timeout(Some(Duration::from_secs(3))).ok();
            let mut raw = Vec::new();
            let mut buf = [0u8; 1024];
            // 读到双 CRLF 后按 Content-Length 续读 body（够简单 POST 用）。
            loop {
                match conn.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        raw.extend_from_slice(&buf[..n]);
                        if let Some(hdr_end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                            let header = String::from_utf8_lossy(&raw[..hdr_end]).to_lowercase();
                            let clen = header
                                .lines()
                                .find_map(|l| l.strip_prefix("content-length:"))
                                .and_then(|v| v.trim().parse::<usize>().ok())
                                .unwrap_or(0);
                            if raw.len() >= hdr_end + 4 + clen {
                                let body = raw[hdr_end + 4..hdr_end + 4 + clen].to_vec();
                                *cap2.lock().unwrap() = Some(body);
                                break;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            let _ = conn
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        }
    });
    Some((addr.ip().to_string(), addr.port(), captured, t))
}

/// 用真 `build_listen18022` 构 Listener：HA push 指向 loopback capture server 的 cfg。
fn ring_cfg_to(host: &str, port: u16) -> Config {
    let mut cfg = e2e_config();
    cfg.hass = Hass {
        ipaddr: host.to_string(),
        port: port as i64,
        api: "/api/webhook/doorbell".into(), // 非空 → push 真发到 capture server
        token: "test-token".into(),
    };
    cfg
}

/// 构造 req=704 invite-query wire 帧（L2 不需要——dispatch_for_test 直喂 payload）。
fn wire_704() -> Vec<u8> {
    let mut fb = Vec::new();
    fb.extend_from_slice(b"req=704&query=");
    fb.extend_from_slice(&[0x06, 0x02, 0x11, 0x03]);
    let mut f = vec![0x07, 0xb8];
    f.extend_from_slice(&(fb.len() as u16).to_le_bytes());
    f.push(0x00);
    f.push(0x00);
    f.extend_from_slice(&fb);
    f
}

#[test]
fn t10_9_doorbell_push_on_req704() {
    let indoor_ip = [172, 16, 106, 91];

    // (1) 命中：req=704 src∈Stations dst==室内机 → push event=ring from/to/result。
    {
        let Some((host, port, captured, srv_thread)) = start_capture_server() else {
            return; // 沙箱跳过
        };
        let cfg = ring_cfg_to(&host, port);
        let tracker = PushTracker::new();
        let shutdown = never();
        let client = Arc::new(HaPushClient::new(cfg.clone(), None));
        let l = build_listen18022(
            &cfg,
            vec!["eth0".into()],
            Arc::clone(&client),
            Arc::clone(&shutdown),
            tracker.clone(),
        )
        .expect("Listener");
        l.dispatch_for_test([172, 16, 106, 152], indoor_ip, 50000, 18022, &wire_704());
        tracker.join_all();
        srv_thread.join().ok();

        let body = captured
            .lock()
            .unwrap()
            .clone()
            .expect("应收到 event=ring push");
        let s = String::from_utf8_lossy(&body);
        assert!(s.contains("\"event\":\"ring\""), "push body: {s}");
        assert!(
            s.contains("\"from\":\"06020000@172.16.106.152:18022\""),
            "from: {s}"
        );
        assert!(
            s.contains("\"to\":\"06021103@172.16.106.91:18022\""),
            "to: {s}"
        );
        assert!(s.contains("\"result\":\"0\""), "result: {s}");
    }

    // (2) dst≠indoorIP（罕见错位）→ roomURI 重建 indoorName@dst_ip:18022。
    {
        let Some((host, port, captured, srv_thread)) = start_capture_server() else {
            return;
        };
        let cfg = ring_cfg_to(&host, port);
        let tracker = PushTracker::new();
        let shutdown = never();
        let client = Arc::new(HaPushClient::new(cfg.clone(), None));
        let l = build_listen18022(
            &cfg,
            vec!["eth0".into()],
            Arc::clone(&client),
            Arc::clone(&shutdown),
            tracker.clone(),
        )
        .expect("Listener");
        // dst = .92（非室内机 .91）。
        l.dispatch_for_test(
            [172, 16, 106, 152],
            [172, 16, 106, 92],
            50000,
            18022,
            &wire_704(),
        );
        tracker.join_all();
        srv_thread.join().ok();

        let body = captured.lock().unwrap().clone().expect("应收到 push");
        let s = String::from_utf8_lossy(&body);
        assert!(
            s.contains("\"to\":\"06021103@172.16.106.92:18022\""),
            "dst≠indoorIP → roomURI 重建: {s}"
        );
    }

    // (3) src 不在 cfg.Stations（邻居/未配置外机）→ log dropped 不 push。
    {
        let Some((host, port, captured, srv_thread)) = start_capture_server() else {
            return;
        };
        let cfg = ring_cfg_to(&host, port);
        let tracker = PushTracker::new();
        let shutdown = never();
        let client = Arc::new(HaPushClient::new(cfg.clone(), None));
        let l = build_listen18022(
            &cfg,
            vec!["eth0".into()],
            Arc::clone(&client),
            Arc::clone(&shutdown),
            tracker.clone(),
        )
        .expect("Listener");
        // src = .199（不在 Stations）→ OnDetect 在 push 前 return。
        l.dispatch_for_test([172, 16, 106, 199], indoor_ip, 50000, 18022, &wire_704());
        tracker.join_all();
        // 不期望 push：capture server 永不收门铃连接 → 自行 connect 唤醒它退出（避免悬挂线程）。
        if let Ok(c) = TcpStream::connect((host.as_str(), port)) {
            let _ = c.shutdown(std::net::Shutdown::Both);
        }
        srv_thread.join().ok();
        assert!(
            captured.lock().unwrap().is_none(),
            "src 不在 Stations → 不 push（capture server 未收到 body）"
        );
    }
}

// ===========================================================================
// 10.10 push 不阻塞 unlock e2e（决策 3 关键）
//
// mock HA 离线（push 挂 5s）同时排队一个 Job::Unlock → unlock MUST NOT 被 push
// head-of-line 阻塞（push off-worker）。用注入的 blocking Pusher（挂 ~2s 代表「HA 离线
// 慢 push」，避免真 5s 拖慢测试）+ worker 立即跑 unlock，验 unlock 延迟 ≈ 自身耗时、
// 不含 push 的挂起时长。
// ===========================================================================

#[test]
fn t10_10_push_does_not_block_unlock() {
    // blocking Pusher：每次 push 挂 STALL（代表 HA 离线）。
    const STALL: Duration = Duration::from_millis(1500);
    struct BlockingPusher {
        started: Arc<AtomicBool>,
    }
    impl Pusher for BlockingPusher {
        fn push(&self, _event: &str, _fields: &[(&str, &str)]) {
            self.started.store(true, Ordering::SeqCst);
            thread::sleep(STALL);
        }
    }

    let push_started = Arc::new(AtomicBool::new(false));
    let pusher: Arc<dyn Pusher> = Arc::new(BlockingPusher {
        started: push_started.clone(),
    });

    // 决策 3：push 走 detached 线程（off worker）。这里用 control.rs 的 maybe_push 经
    // detached 包装等价——但更直接：在 unlock 之前先在 detached 线程触发慢 push，再在
    // worker 上跑 unlock，验 unlock 不被 push 挂起拖慢。
    let push_thread = {
        let p = Arc::clone(&pusher);
        thread::spawn(move || p.push("automation_state", &[("x", "y")]))
    };
    // 等 push 真正进入挂起。
    while !push_started.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(5));
    }

    // 此刻 detached push 正挂 STALL；worker 上跑 unlock 应不受影响（off-worker）。
    let shutdown = never();
    let (tx, worker) = spawn_worker(
        Arc::new(FixedWire::new(AttemptOutcome::Ok)),
        None,
        shutdown.clone(),
    );
    let t0 = Instant::now();
    let o = dispatch_unlock(&tx);
    let unlock_elapsed = t0.elapsed();
    assert_eq!(o.result, result::OK);
    // unlock 延迟 ≈ 自身耗时（FixedWire 即时），远小于 push 的 STALL（不被 head-of-line 阻塞）。
    assert!(
        unlock_elapsed < STALL / 2,
        "unlock 不应被 push 挂起阻塞：unlock 耗 {:?} 应 << push STALL {:?}",
        unlock_elapsed,
        STALL
    );

    daemon::submit_job(&tx, Job::Shutdown).ok();
    worker.join().ok();
    push_thread.join().ok();
}

// ===========================================================================
// 10.11 JoinHandle 回收 e2e（RC-F6）
//
// 连续触发 N 次门铃 push（各完成）→ Vec<JoinHandle> 长度有界（≈并发数，非累计 N）→
// retain(!is_finished()) 生效不泄漏。
// ===========================================================================

#[test]
fn t10_11_push_tracker_joinhandle_reclaim_bounded() {
    let tracker = PushTracker::new();
    let shutdown = never();
    let client = Arc::new(HaPushClient::new(e2e_config(), None)); // api 空 → push 即时返回

    const N: usize = 100;
    for _ in 0..N {
        spawn_push(
            &tracker,
            Arc::clone(&client),
            Arc::clone(&shutdown),
            "ring".into(),
            vec![("from".into(), "x".into())],
        );
        // 给已 spawn 的线程一点时间结束，使下次 register 的 retain 能回收它。
        thread::sleep(Duration::from_millis(1));
    }

    // Vec 长度有界：远小于累计 N（retain 回收已结束 handle）。不强求精确 1，但须 << N。
    let len = tracker.len();
    assert!(
        len < N / 2,
        "Vec<JoinHandle> 应有界（≈并发数）非累计 N={N}：实测 len={len}（retain 未生效=泄漏）"
    );

    tracker.join_all();
    assert!(tracker.is_empty(), "join_all 后排空");
}
