//! `dooraccess-rs` daemon 编排入口（Phase 4 骨架，`port-rust-daemon-skeleton` G2）。
//!
//! 对标 Go `cmd/dooraccess-go/main.go` 的 `mainImpl` + `run`。本文件交付 **M1.5 骨架主体**：
//!   - `mainImpl(args, stderr) -> exitcode` 可测包装（锚 Go `main.go:62`）：解析 `--config`
//!     flag、加载 config、缺失/废弃/解析告警逐条 log、退出码区分（flag 错=2 / config 错=1）。
//!   - signal：SIGHUP 显式忽略 + SIGTERM/SIGINT 注册（锚 Go `signal.Ignore`/`signal.Notify`）。
//!   - `run`：构造 M1.5 并发骨架（[`daemon`] 模块的 wire-worker + Job 队列 + 单 `Arc<AtomicBool>`
//!     shutdown + [`PushTracker`] 排空），阻塞到 SIGTERM/SIGINT，执行钉死 shutdown 排序。
//!
//! **范围**（G2）：搭好 main 入口 + worker/job/shutdown/push 基础设施。listener/HTTP/手动
//! unlock/banner/automation flag 加载/号码查询的**完整接线**是 G1/G3 组的事——本骨架的
//! `run()` 是「最小但能编译」的编排，留出注入点（`#[allow(dead_code)]` 标占位依赖）。
//!
//! Phase 0-3 的 passive-shadow 探针迁至 `examples/probe.rs`（`cargo build --example probe`），
//! 逻辑零回归（决策 7）。

use std::io::Write;
use std::process::exit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;

use dooraccess_rs::config::{self, Config};
use dooraccess_rs::control::{self, AutomationState, FnPersistHook, Pusher};
use dooraccess_rs::daemon::{self, Job, PushTracker, WorkerDeps};
use dooraccess_rs::ha_push::HaPushClient;
// 中央带时间戳日志入口（组 A）：视频 SharedLogFn / 散落 eprintln 收编经它（组 B）。
use dooraccess_rs::listen18022;
use dooraccess_rs::listen6672;
use dooraccess_rs::log::log_line;
use dooraccess_rs::orchestration::{
    build_listen18022, build_number_query_callback, load_automation_flags,
    parse_stations_to_ip_map, WorkerUnlockDispatch,
};
use dooraccess_rs::self_unlock;
use dooraccess_rs::video;
use dooraccess_rs::{automation_state, info, wire_sender};

/// 主配置默认路径（锚 Go `defaultConfigPath`）。
const DEFAULT_CONFIG_PATH: &str = "/etc/dooraccess-go/config.ini";

/// automation flag 持久 state 文件路径（锚 Go `automationStatePath`）。必落 /etc
/// （overlay→flash 持久；/tmp、/var 是 tmpfs 非真持久）。与 config.ini 同目录但独立。
const AUTOMATION_STATE_PATH: &str = "/etc/dooraccess-go/automation.state";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let code = main_impl(&args, &mut std::io::stderr());
    exit(code);
}

/// `main` 的可测包装（锚 Go `mainImpl`）：参数化 args 与 stderr sink，返退出码。
///
/// 退出码（锚 Go `main.go:66-77`）：
///   - flag 解析失败 → **2**（参数错误）
///   - config 加载失败 → **1**（业务失败）
///   - `run` 返 Err → **1**
///   - 干净退出 → **0**
///
/// flag/config 失败均**不启动任何 listener/worker/HTTP 线程**（spec「flag vs config 退出码区分」）。
pub fn main_impl<W: Write>(args: &[String], stderr: &mut W) -> i32 {
    // ① flag 解析（手写，不引 clap：binary 体型是 OOM 红线）。
    let (config_path, state_path) = match parse_flags(args) {
        Ok(p) => p,
        Err(msg) => {
            // EARLY-STAGE: central logger unavailable — pre-logger CLI 诊断（flag 解析失败、退出码 2）。
            let _ = writeln!(stderr, "dooraccess-rs: {msg}"); // EARLY-STAGE: central logger unavailable
            let _ = writeln!(
                stderr, // EARLY-STAGE: central logger unavailable
                "usage: dooraccess-rs [--config <path>] [--state <path>]\n\t\
                 default config: {DEFAULT_CONFIG_PATH}\n\tdefault state: {AUTOMATION_STATE_PATH}"
            );
            return 2; // flag 解析失败 → 2。
        }
    };

    logf(
        stderr,
        &format!("starting dooraccess-rs (anjubao replica), config={config_path}"),
    );

    // ② config 加载（锚 Go `config.LoadConfig`）。
    let cfg = match config::load_config(&config_path) {
        Ok(c) => c,
        Err(e) => {
            // logger 可用后的生产错误行 → 走 logf（得时间戳 + tag；非 EARLY-STAGE）。
            logf(stderr, &format!("load config: {e}"));
            return 1; // config 加载失败 → 1。
        }
    };

    // ③ 缺失/废弃字段/解析告警逐条 log（锚 Go `main.go:73-86`）。
    for m in cfg.missing_fields() {
        logf(stderr, &format!("config: field {m} missing, using default"));
    }
    let dep = cfg.deprecated_fields();
    if !dep.is_empty() {
        logf(
            stderr,
            &format!(
                "config: {dep:?} fields deprecated and ignored; safe to remove from config.ini \
                 (brand/family/elev/notification removed in v0.1.6)"
            ),
        );
    }
    for w in cfg.parser_warnings() {
        logf(stderr, w);
    }

    // ④ hass.token 配但 hass.api 缺 → log 并继续（不退出，锚 Go `main.go:88-91`）。
    if !cfg.hass.token.is_empty() && cfg.hass.api.is_empty() {
        logf(
            stderr,
            "config: hass.token configured but hass.api missing; \
             HA push disabled until cfg.hass.api is set",
        );
    }

    // ⑤ signal：SIGHUP 显式忽略 + SIGTERM/SIGINT 注册（锚 Go `signal.Ignore`/`signal.Notify`）。
    install_signal_handlers();

    // ⑥ run：构造并发骨架、阻塞到信号、graceful shutdown。
    // state_path 来自 parse_flags（init.d 传 --state /etc/dooraccess/automation.state）；
    // MUST NOT 在此重传硬编 AUTOMATION_STATE_PATH（否则 --state 中性化失效，回 -go 死文件）。
    match run(&cfg, &config_path, &state_path, stderr) {
        Ok(()) => {
            logf(stderr, "dooraccess-rs exited cleanly");
            0
        }
        Err(e) => {
            // logger 可用后的生产错误行 → 走 logf（得时间戳 + tag；非 EARLY-STAGE）。
            logf(stderr, &format!("run: {e}"));
            1
        }
    }
}

/// 手写 `--config <path>` / `--state <path>` 解析（默认 [`DEFAULT_CONFIG_PATH`] /
/// [`AUTOMATION_STATE_PATH`]）。未知 flag / 缺值 → Err（退出码 2）。
///
/// `--state` 支撑 config/state 路径中性化（部署到 `/etc/dooraccess/`）：state 路径**不从
/// config 派生**，由 init.d 显式传 `--state`。binary 默认常量保守留 `-go`（R1-R7 临时 swap /
/// 手动 setsid 裸跑向后兼容）。返回 `(config_path, state_path)`——call site
/// [`main_impl`] MUST 把 `state_path` 喂给 [`run`]，**MUST NOT** 再传硬编 [`AUTOMATION_STATE_PATH`]。
fn parse_flags(args: &[String]) -> Result<(String, String), String> {
    let mut config_path = DEFAULT_CONFIG_PATH.to_string();
    let mut state_path = AUTOMATION_STATE_PATH.to_string();
    let mut i = 1; // args[0] = bin path
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                let v = args
                    .get(i + 1)
                    .ok_or_else(|| "--config requires a value (path to config.ini)".to_string())?;
                config_path = v.clone();
                i += 2;
            }
            other if other.starts_with("--config=") => {
                config_path = other["--config=".len()..].to_string();
                i += 1;
            }
            "--state" => {
                let v = args.get(i + 1).ok_or_else(|| {
                    "--state requires a value (path to automation.state)".to_string()
                })?;
                state_path = v.clone();
                i += 2;
            }
            other if other.starts_with("--state=") => {
                state_path = other["--state=".len()..].to_string();
                i += 1;
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok((config_path, state_path))
}

/// 行日志（写注入的 `W` sink，保留可测性）：经中央 [`format_log_line`] 加墙钟时间戳 + tag。
///
/// **MUST 写注入的 `W`、MUST NOT 委托 `log_line`（写死 stderr）**——否则 `main_impl` 现有
/// `Vec<u8>` sink 测试断言落空（design D2/F6）。两者共享 `format_log_line` 渲染，各写各的 sink。
fn logf<W: Write>(stderr: &mut W, msg: &str) {
    let line = dooraccess_rs::log::format_log_line(std::time::SystemTime::now(), msg);
    let _ = writeln!(stderr, "{line}"); // CENTRAL-SINK
}

// ---------------------------------------------------------------------------
// signal 处理（std + libc only；锚 Go signal.Ignore(SIGHUP) / Notify(SIGTERM,SIGINT)）
// ---------------------------------------------------------------------------

/// 进程级 shutdown 请求标志（SIGTERM/SIGINT handler 置位；主线程轮询）。
///
/// signal handler 内只允许 async-signal-safe 操作——仅 `store` 一个 atomic，不 log、不分配。
static SIGNAL_SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// 安装 signal 处置：SIGHUP → `SIG_IGN`（显式忽略，否则内核默认 terminate 杀 daemon，
/// 不符合 spec「SIGHUP 不响应」）；SIGTERM/SIGINT → 置位 [`SIGNAL_SHUTDOWN`]。
fn install_signal_handlers() {
    extern "C" fn on_term(_sig: libc::c_int) {
        SIGNAL_SHUTDOWN.store(true, Ordering::SeqCst);
    }
    unsafe {
        // SIGHUP 显式忽略（锚 Go signal.Ignore(SIGHUP)）。
        libc::signal(libc::SIGHUP, libc::SIG_IGN);
        // SIGTERM / SIGINT → on_term（锚 Go signal.Notify）。
        libc::signal(libc::SIGTERM, on_term as *const () as libc::sighandler_t);
        libc::signal(libc::SIGINT, on_term as *const () as libc::sighandler_t);
    }
}

/// 是否已收到 SIGTERM/SIGINT。
fn shutdown_requested() -> bool {
    SIGNAL_SHUTDOWN.load(Ordering::SeqCst)
}

// ---------------------------------------------------------------------------
// run：M1.5 并发骨架编排（G2 最小骨架；listener/HTTP/unlock 完整接线属 G3）
// ---------------------------------------------------------------------------

/// run 构造 M1.5 并发骨架，阻塞到 SIGTERM/SIGINT，执行钉死 shutdown 排序。
///
/// **G3a 接线范围**：共享依赖构造（Sender/HaPushClient/slaves）+ listener（6672/18022）
/// 线程 + OnDetect 安装（FormatLog + event=ring 门铃 push）+ HTTP 8080 server 线程 +
/// automation flag 启动加载（优先级 state>config>env）+ Persister hook + banner + startup
/// push（automation_state + diagnosis）。手动 unlock 经 worker 改写（§4，G3b）、号码查询
/// handler（§8，G3b）、fatal 上报完整化（§9，G3b）**不**在本组——worker 仍用 G2 占位 wire。
///
/// 钉死 shutdown 排序（决策 8 / spec「graceful shutdown」）：
///   ① 置位 `Arc<AtomicBool>` shutdown
///   ② 先 join listener 线程（各于下个 SO_RCVTIMEO ~500ms wakeup 退出）
///   ③ 投 `Job::Shutdown` 哨兵（容忍 worker 已死 SendError）→ worker 排空 Unlock 后 break → join worker
///   ④ shutdown HTTP server + join HTTP 线程
///   ⑤ join 残留 detached push 线程（[`PushTracker::join_all`]）
fn run<W: Write>(
    cfg: &Config,
    _config_path: &str,
    state_path: &str,
    stderr: &mut W,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 单 Arc<AtomicBool> shutdown：穿入 worker / listener recv / execute_unlock cancel /
    // detached push（决策 4：仅取消；超时由 InstantDeadline 另行承载）。
    let shutdown = Arc::new(AtomicBool::new(false));

    // detached push 排空集（决策 3：std-native pushWG 等价）。
    let push_tracker = PushTracker::new();

    // ── automation flag 启动加载优先级（state>config>env，§5.1/5.2/5.3）──
    let (auto_unlock0, auto_hangup0, flag_source) =
        load_automation_flags(state_path, cfg, &mut |m| logf(stderr, m));

    // 运行时可变两 flag（§5.4）。单一 state：HTTP server 拿一个 share() handle 拨动，
    // Persister value_source 拿另一个 share() handle 读运行时真值——两 handle 背后同一对
    // Arc<AtomicBool>，故 endpoint 拨动后 persister 立即读到翻转值（不分叉，对齐 Go 单一
    // automationState 注入两处）。
    let automation = AutomationState::new(auto_unlock0, auto_hangup0);

    // Persister：endpoint 拨动后 best-effort 原子落盘（§6.4 接线 G1 的 Persister）。
    // value_source 读运行时 atomic 当前真值（决策 6 / G1）。
    let persister = {
        let auto = automation.share();
        Arc::new(automation_state::Persister::new(
            std::path::PathBuf::from(state_path),
            Box::new(move || (auto.load_auto_unlock(), auto.load_auto_hangup())),
            None,
        ))
    };

    // ── 共享依赖构造（§3.1）──
    // ha_push client（HTTP-to-HA 反向 push；hass.api 空时 push 内部静默跳过）。
    let push_client = Arc::new(HaPushClient::new(cfg.clone(), None));

    // ── 视频转发子系统（Phase 5 组 F，锚 Go main.go:147-157）：仅 cfg.video.forward=true
    // 时构造 Manager；否则缺位（None）→ /video/* 三入口 503 nil-guard。路由本身由
    // control::Server::handler 无条件注册（不随 forward 增减）。
    let video_mgr: Option<Arc<video::session::Manager>> = if cfg.video.forward {
        let caller = video::session::parse_caller(&cfg.sip)
            .map_err(|e| format!("video: parse caller from cfg.sip: {e}"))?;
        let vlog: video::SharedLogFn = Arc::new(|m: &str| log_line(m));
        logf(
            stderr,
            &format!("video: forward enabled, format={}", cfg.video.format),
        );
        Some(Arc::new(video::session::Manager::new(caller, Some(vlog))))
    } else {
        logf(stderr, "video: forward disabled (cfg.video.forward=false)");
        None
    };

    // slaves 解析：显式 iface_list 优先，否则桥成员枚举（§3.1，移植 resolve_iface_list）。
    let slaves: Vec<String> = match cfg.resolve_iface_list() {
        Ok(s) => {
            logf(
                stderr,
                &format!(
                    "listeners: slaves={s:?} (resolved from iface={:?})",
                    cfg.iface
                ),
            );
            s
        }
        Err(e) => {
            // 解析失败非致命（HTTP-only 模式继续，锚 Go main.go:170-174）。
            logf(
                stderr,
                &format!("listeners: resolve iface list failed: {e} (listeners will not start)"),
            );
            Vec::new()
        }
    };

    // ── listen18022 Listener 构造（§3.4：OnDetect 每帧 FormatLog + req=704 门铃 push）──
    // 先构造（不起线程），以便把同一 Arc 既给 wire-worker 作 bye 早停订阅源（§4.2 listener
    // 注入），又给 listen18022 线程跑 I/O。slaves 空时 None（不启动，与 Go 一致）。
    let listener18022: Option<Arc<listen18022::Listener>> = build_listen18022(
        cfg,
        slaves.clone(),
        Arc::clone(&push_client),
        Arc::clone(&shutdown),
        push_tracker.clone(),
    )
    .map(Arc::new);

    // ── Job 队列 + 单 wire-worker 线程（决策 1/2/5）──
    // worker 跑 wire-bound Job::Unlock（free fn unlock::execute_unlock，决策 5）；Job::Shutdown
    // 哨兵 break（决策 8）。G3b：注入真 WireSenderAdapter（sender.rs，实现 UnlockWire）+
    // listen18022 作 bye 早停订阅源（§4.2）。
    let (job_tx, job_rx) = mpsc::channel::<Job>();
    let worker_listener: Option<Arc<dyn dooraccess_rs::listen18022::Subscribable>> = listener18022
        .as_ref()
        .map(|l| Arc::clone(l) as Arc<dyn dooraccess_rs::listen18022::Subscribable>);
    // auto-hangup 单帧 wire 出站缝（② 决策 2：worker 发 req=708 preview-stop）。注入
    // wire_sender::Sender（与 unlock 路径同款 iface，已认证 wire 出站，非新 socket/BE 面）；
    // 否则 Job::Hangup 走 None 分支只 log skip 不发帧。
    let worker_deps = WorkerDeps::new(worker_wire(cfg), worker_listener).with_hangup_wire(
        Arc::new(wire_sender::Sender {
            iface: cfg.iface.clone(),
            timeout: Some(std::time::Duration::from_secs(5)),
            local_ip: None,
        }),
    );
    let worker_shutdown = Arc::clone(&shutdown);
    let worker = std::thread::Builder::new()
        .name("wire-worker".into())
        .spawn(move || daemon::run_worker(worker_deps, job_rx, worker_shutdown))?;

    // ── listener 线程接线（§3.2，复用已 hAP 真机认证的 listen6672/listen18022，I/O 零改动）──
    let mut listener_threads: Vec<JoinHandle<()>> = Vec::new();

    // listen18022 线程：跑上面已构造的 Listener Arc（OnDetect 已装）。
    if let Some(l) = &listener18022 {
        logf(
            stderr,
            &format!(
                "listen18022: started on {:?} (PROMISC, BPF tcp:18022)",
                l.slaves
            ),
        );
        let l = Arc::clone(l);
        let sd = Arc::clone(&shutdown);
        listener_threads.push(
            std::thread::Builder::new()
                .name("listen18022".into())
                .spawn(move || {
                    if let Err(e) = l.run(sd) {
                        // listener 非致命：HTTP-only 模式继续（锚 Go main.go:268-271）。
                        log_line(&format!("listen18022 stopped: {e}"));
                    }
                })?,
        );
    } else {
        logf(stderr, "listen18022: no slaves resolved, will not start");
    }

    // ── self-unlock 消费者线程（②：ring→消费者→Job::Unlock，design 决策 1）──
    // 经 listener18022.subscribe(filter) 注册 + 起独立单消费者线程 drain；debounce/flag-gate/
    // 产 Job 全在消费者线程顺序完成（多 slave dispatch 由 bounded channel 汇成单流，无 TOCTOU）。
    // 钉死 shutdown 排序里：listener-join 之后、Job::Shutdown 哨兵之前 join 本 handle。
    let self_unlock_consumer: Option<JoinHandle<()>> = listener18022.as_ref().and_then(|l| {
        let deps = self_unlock::SelfUnlockDeps::try_new(
            cfg,
            parse_stations_to_ip_map(cfg),
            job_tx.clone(),
            Arc::clone(&push_client),
            push_tracker.clone(),
            Arc::clone(&shutdown),
            automation.share(),
            |m| logf(stderr, m),
        )?;
        // spawn_consumer 返 Option（spawn 失败→None，self-unlock 静默禁用）；and_then 直接展平。
        self_unlock::spawn_consumer(l.as_ref(), deps, |m| logf(stderr, m))
    });

    // listen6672：号码查询 callback 安装（§8.1/8.2）+ 起线程（on_ring=None 与 Go 一致——
    // ring 触发 self-unlock 属姊妹 change ②，骨架不消费 ring 帧产 Unlock job）。
    if !slaves.is_empty() {
        let callbacks = listen6672::Callbacks {
            on_number_query: build_number_query_callback(cfg),
            ..Default::default()
        };
        let l6672 = listen6672::Listener::new(slaves.clone(), callbacks);
        logf(
            stderr,
            &format!(
                "listen6672: started on {:?} (PROMISC, BPF udp:6672)",
                l6672.slaves
            ),
        );
        let l = Arc::new(l6672);
        let sd = Arc::clone(&shutdown);
        listener_threads.push(
            std::thread::Builder::new()
                .name("listen6672".into())
                .spawn(move || {
                    if let Err(e) = l.run(sd) {
                        log_line(&format!("listen6672 stopped: {e}"));
                    }
                })?,
        );
    }

    // ── HTTP 8080 server 接线（§3.3，复用 Phase 2 httpx + control handlers）──
    // 注入 Pusher（detached spawn_push）+ Persister hook + Automation + Sender。
    // §6.4：control handler 拨动 flag 后调 persister.persist()（经 FnPersistHook 接线）。
    // 手动 unlock 仍走 control.rs Phase 2 直调 Sender（G3b 再改写经 worker）。
    let pusher: Arc<dyn Pusher> = Arc::new(DetachedPusher {
        client: Arc::clone(&push_client),
        shutdown: Arc::clone(&shutdown),
        tracker: push_tracker.clone(),
    });
    let persist_hook = {
        let p = Arc::clone(&persister);
        Arc::new(FnPersistHook(Box::new(move || {
            p.persist();
            Ok(())
        })))
    };
    // 手动 unlock 经 wire-worker 队列（决策 5）：HTTP /unlock handler 投 Job::Unlock + 等
    // 一次性 reply channel 回灌 UnlockOutcome（worker 已退/panic → 退化 wire-failure，不永等）。
    let unlock_dispatch: Arc<dyn control::UnlockDispatch> = Arc::new(WorkerUnlockDispatch {
        job_tx: job_tx.clone(),
    });
    let mut ctrl_server = control::Server::with_dispatch(
        cfg.clone(),
        env!("CARGO_PKG_VERSION"),
        Some(automation.share()), // handler 拨动这一 handle；与 persister/banner 读的同一组原子
        Some(persist_hook),
        Some(pusher),
        unlock_dispatch,
    );
    // /video/* 三入口的 Manager 注入（forward=false → None 保持 nil-guard 503）。
    ctrl_server.set_video(
        video_mgr.clone(),
        Some(Arc::new(|m: &str| log_line(m)) as video::SharedLogFn),
    );
    let http_handler: Arc<dyn dooraccess_rs::httpx::Handler> = Arc::new(ctrl_server.handler());
    let http_server = Arc::new(dooraccess_rs::httpx::server::Server::new());
    let http_addr = format!("{}:{}", cfg.listen.addr, cfg.listen.port);
    let http_thread = {
        let srv = Arc::clone(&http_server);
        let addr = http_addr.clone();
        let handler = Arc::clone(&http_handler);
        std::thread::Builder::new().name("http8080".into()).spawn(
            move || -> Result<(), String> {
                let listener = std::net::TcpListener::bind(&addr)
                    .map_err(|e| format!("http8080: bind {addr}: {e}"))?;
                match srv.serve(listener, handler) {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        // ServerClosedError 是 graceful shutdown 正常返回，非 fatal。
                        if e.downcast_ref::<dooraccess_rs::httpx::server::ServerClosedError>()
                            .is_some()
                        {
                            Ok(())
                        } else {
                            Err(format!("http8080: {e}"))
                        }
                    }
                }
            },
        )?
    };
    logf(stderr, &format!("http8080: listening on {http_addr}"));

    // ── banner（§7.1：automation flag 加载之后渲染部署参数 + flag 最终值与来源）──
    render_banner(cfg, env!("CARGO_PKG_VERSION"), &mut |m| logf(stderr, m));
    logf(
        stderr,
        &format!(
            "automation: auto_unlock={} auto_hangup={} (source={flag_source})",
            on_off(automation.load_auto_unlock()),
            on_off(automation.load_auto_hangup()),
        ),
    );

    // ── startup push（§7.2 automation_state + §7.3 diagnosis；均 banner 之后、detached）──
    // automation_state：两 flag 用字符串 "true"/"false" 编码（HACS 静默重启后收敛）。
    daemon::spawn_push(
        &push_tracker,
        Arc::clone(&push_client),
        Arc::clone(&shutdown),
        "automation_state".into(),
        vec![
            (
                "auto_unlock".into(),
                bool_str(automation.load_auto_unlock()),
            ),
            (
                "auto_hangup".into(),
                bool_str(automation.load_auto_hangup()),
            ),
        ],
    );
    // diagnosis：detached 线程内先 sleep ~500ms 站稳再 push；延迟期 honor shutdown。
    daemon::spawn_diagnosis_push(
        &push_tracker,
        Arc::clone(&push_client),
        Arc::clone(&shutdown),
        std::time::Duration::from_millis(500),
    );

    logf(
        stderr,
        "dooraccess-rs up (M1.5: listeners + http + worker + automation)",
    );

    // 阻塞到 SIGTERM/SIGINT，或 HTTP 线程提前结束（§9.3：bind 失败等 fatal——HTTP 是必备控制
    // 面，其线程早退即视为 fatal，不等信号、走 shutdown 排序后非零退出码）。listener 线程提前
    // 退出**不**进此条件（listener 失败非致命，HTTP-only 模式继续，锚 Go main.go:244-273）。
    loop {
        if shutdown_requested() {
            logf(stderr, "shutdown signal received, draining");
            break;
        }
        if http_thread.is_finished() {
            logf(
                stderr,
                "http8080 thread exited (fatal: bind/serve failure), draining",
            );
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    // ── 钉死 shutdown 排序（决策 8 + Phase 5 骨架 MODIFIED delta ①v 插步）──
    // ① 置位 shutdown（listener recv / 在飞 unlock / push 线程观察）。
    shutdown.store(true, Ordering::SeqCst);

    // ①v VideoMgr shutdown（带 deadline 5s，对齐 Go main.go:327-353 三处插桩的
    //    stopCtx 5s；video 未启用 = no-op）——**先于 join listener/HTTP**：video
    //    teardown（preview stop → RTCP BYE → 尾等 → cancel session 线程 →
    //    FrameBuffer close）使 stream consumer channel 全断 → in-flight stream
    //    handler 退出 → ④ httpx shutdown（拒新连接 + 等 in-flight handler）才能在
    //    deadline 内完成（design D6：反序必卡满 timeout）。
    if let Some(mgr) = &video_mgr {
        mgr.shutdown(std::time::Duration::from_secs(5));
    }

    // ② 先 join listener 线程（各于下个 SO_RCVTIMEO ~500ms wakeup 退出）。
    //    join 完即无新 OnDetect → 无新门铃 push spawn（关 RC-F1 窗口）。
    for t in listener_threads {
        let _ = t.join();
    }

    // ②.5 join self-unlock 消费者线程（钉死排序新增插步，design 决策 1）：listener-join
    //    之后、Job::Shutdown 哨兵之前。先于哨兵 join 保证「哨兵后无新 Unlock job 入队」的
    //    关窗不变量。消费者 recv_timeout(20ms) 轮询观察 shutdown 置位（①已 store），≤一个
    //    poll + ε 退出（非裸 recv，不死锁）；退出调 sub.cancel() 清理。
    if let Some(c) = self_unlock_consumer {
        let _ = c.join();
    }

    // ③ 投 Job::Shutdown 哨兵（容忍 worker 已死 SendError，禁 unwrap）→ join worker。
    let _ = daemon::submit_job(&job_tx, Job::Shutdown);
    let _ = worker.join();

    // ④ shutdown HTTP server（拒新连接 + 等在飞 handler）→ join HTTP 线程。
    //    ④ 须先于 ⑤（spec：所有在飞 handler 结束后再 join push，否则晚到 handler push 越过 ⑤）。
    let _ = http_server.shutdown(Some(std::time::Duration::from_secs(5)));
    let http_result = http_thread.join();

    // ⑤ join 残留 detached push 线程（排空）。
    push_tracker.join_all();

    // fatal 上报（§9.3）：HTTP 线程返 Err（bind 失败 / serve 非 graceful 错）→ run 返 Err →
    // main_impl 映射非零退出码（1）。listener 失败不进 fatal（HTTP-only 继续，上面 join 即吞）。
    // HTTP 线程 panic：已 join，best-effort 不阻塞退出（视为干净，避免 panic 掩盖 shutdown）。
    match http_result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => Ok(()),
    }
}

/// 真 wire 注入（决策 5 / §4.2）：wire-worker 跑 unlock job 时单次三步握手用的
/// [`dooraccess_rs::unlock::UnlockWire`] 实现 = [`sender::WireSenderAdapter`]（底层
/// `wire_sender::Sender` 真发 18022 帧）。HTTP 路径 per-attempt cap=默认 5s、retry=1s×8、
/// 总 cap=10s 由 worker 的 [`daemon::run_worker`] 注入。
fn worker_wire(cfg: &Config) -> Arc<dyn dooraccess_rs::unlock::UnlockWire> {
    Arc::new(dooraccess_rs::sender::WireSenderAdapter::new(
        wire_sender::Sender {
            iface: cfg.iface.clone(),
            timeout: Some(std::time::Duration::from_secs(5)),
            local_ip: None,
        },
    ))
}

// ---------------------------------------------------------------------------
// DetachedPusher：control.rs Pusher → daemon spawn_push（决策 3：业务 push off-worker）
// ---------------------------------------------------------------------------

/// 把 control.rs 的同步 `Pusher::push` 适配为 detached `spawn_push`（决策 3：手动 unlock 的
/// business-result push 走 detached 线程，off wire-worker；锚 Go `asyncPush`）。
struct DetachedPusher {
    client: Arc<HaPushClient>,
    shutdown: Arc<AtomicBool>,
    tracker: PushTracker,
}

impl Pusher for DetachedPusher {
    fn push(&self, event: &str, fields: &[(&str, &str)]) {
        let owned: Vec<(String, String)> = fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        daemon::spawn_push(
            &self.tracker,
            Arc::clone(&self.client),
            Arc::clone(&self.shutdown),
            event.to_string(),
            owned,
        );
    }
}

// ---------------------------------------------------------------------------
// banner + 小工具（§7.1）
//
// 注：automation flag 加载（load_automation_flags）、OnDetect 门铃 builder
// （build_listen18022）、号码查询 callback（build_number_query_callback）、手动 unlock
// → wire-worker 派发缝（WorkerUnlockDispatch）等纯逻辑编排 helper 已上移到
// `dooraccess_rs::orchestration`，使 e2e 测真生产函数（G4 FIX 问题 B）。main.rs 只剩薄
// 入口 + 线程编排。
// ---------------------------------------------------------------------------

/// 渲染启动 banner（部署参数）到 syslog（锚 Go `info.RenderBanner`；info.rs 仅 `build`，
/// 故 banner 行在此组装——语义等价：一眼看完部署参数）。
fn render_banner(cfg: &Config, version: &str, logf: &mut dyn FnMut(&str)) {
    let sd = info::build(Some(cfg), version, true);
    logf(&format!(
        "banner: dooraccess-rs {} (brand={}, pid={})",
        sd.version, sd.brand, sd.pid
    ));
    logf(&format!(
        "banner: monitor={} stations={} iface={}",
        sd.monitor,
        sd.stations.len(),
        cfg.iface
    ));
    logf(&format!(
        "banner: listen={}:{} video.forward={}",
        cfg.listen.addr, cfg.listen.port, cfg.video.forward
    ));
    if let Some(reachable) = sd.hass_reachable {
        logf(&format!("banner: hass_reachable={reachable}"));
    }
}

/// bool → "on"/"off"（banner 用；锚 Go `onOff`）。
fn on_off(b: bool) -> &'static str {
    if b {
        "on"
    } else {
        "off"
    }
}

/// bool → "true"/"false" String（automation_state push 字段编码；锚 Go `strconv.FormatBool`）。
fn bool_str(b: bool) -> String {
    if b {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// flag 解析：默认路径 / `--config <p>` / `--config=<p>` / `--state <p>` / `--state=<p>` / 组合。
    #[test]
    fn parse_flags_default_and_explicit() {
        // 默认：config + state 均回落硬编常量（R1-R7 裸跑向后兼容）。
        assert_eq!(
            parse_flags(&["bin".into()]).unwrap(),
            (
                DEFAULT_CONFIG_PATH.to_string(),
                AUTOMATION_STATE_PATH.to_string()
            )
        );
        // --config 仅改 config，state 仍默认。
        assert_eq!(
            parse_flags(&["bin".into(), "--config".into(), "/tmp/x.ini".into()]).unwrap(),
            ("/tmp/x.ini".to_string(), AUTOMATION_STATE_PATH.to_string())
        );
        assert_eq!(
            parse_flags(&["bin".into(), "--config=/tmp/y.ini".into()]).unwrap(),
            ("/tmp/y.ini".to_string(), AUTOMATION_STATE_PATH.to_string())
        );
        // --state 仅改 state，config 仍默认（state 路径不从 config 派生）。
        assert_eq!(
            parse_flags(&["bin".into(), "--state".into(), "/tmp/s.state".into()]).unwrap(),
            (DEFAULT_CONFIG_PATH.to_string(), "/tmp/s.state".to_string())
        );
        assert_eq!(
            parse_flags(&["bin".into(), "--state=/tmp/t.state".into()]).unwrap(),
            (DEFAULT_CONFIG_PATH.to_string(), "/tmp/t.state".to_string())
        );
        // 组合：中性路径同时传两参（init.d 的形态）。
        assert_eq!(
            parse_flags(&[
                "bin".into(),
                "--config".into(),
                "/etc/dooraccess/config.ini".into(),
                "--state".into(),
                "/etc/dooraccess/automation.state".into(),
            ])
            .unwrap(),
            (
                "/etc/dooraccess/config.ini".to_string(),
                "/etc/dooraccess/automation.state".to_string()
            )
        );
    }

    /// flag 解析失败 → Err（main_impl 映射退出码 2）。
    #[test]
    fn parse_flags_errors() {
        assert!(parse_flags(&["bin".into(), "--config".into()]).is_err()); // config 缺值
        assert!(parse_flags(&["bin".into(), "--state".into()]).is_err()); // state 缺值
        assert!(parse_flags(&["bin".into(), "--bogus".into()]).is_err()); // 未知 flag
    }

    /// main_impl：flag 解析失败 → 退出码 2，不加载 config / 不启线程。
    #[test]
    fn main_impl_flag_error_returns_2() {
        let mut sink = Vec::new();
        let code = main_impl(&["bin".into(), "--unknown".into()], &mut sink);
        assert_eq!(code, 2);
    }

    /// main_impl：config 不存在 → 退出码 1（区别于 flag 错的 2）。
    #[test]
    fn main_impl_config_missing_returns_1() {
        let mut sink = Vec::new();
        let code = main_impl(
            &[
                "bin".into(),
                "--config".into(),
                "/nonexistent/zzz_no_such_config.ini".into(),
            ],
            &mut sink,
        );
        assert_eq!(code, 1);
        let out = String::from_utf8(sink).unwrap();
        // 既有断言：83 路径（load config）经 logf 收编进注入的 W sink。
        assert!(out.contains("load config"), "应 log config 加载失败: {out}");
        // 3.2 强化（F1 双保险，补门禁 grep 之外的第二道闸）：断 83 路径的输出行带
        // 时间戳前缀 shape `^dooraccess-rs: \d{4}/`，使「83 已收编」机械可验。
        // 项目禁外部 crate（无 regex），用手工 shape 检查。
        assert!(
            out.lines()
                .any(|l| l.contains("load config") && has_ts_prefix(l)),
            "load config 行须带时间戳前缀 ^dooraccess-rs: \\d{{4}}/: {out}"
        );
    }

    /// 手工时间戳前缀 shape 检查（替代 regex；项目禁外部 crate）。
    /// 形如 `dooraccess-rs: 2026/06/10 ...`——前缀 "dooraccess-rs: "（15 字节）后紧跟
    /// 4 位年 + `/`。
    fn has_ts_prefix(line: &str) -> bool {
        const TAG: &str = "dooraccess-rs: ";
        let Some(rest) = line.strip_prefix(TAG) else {
            return false;
        };
        let bytes = rest.as_bytes();
        bytes.len() >= 5 && bytes[0..4].iter().all(u8::is_ascii_digit) && bytes[4] == b'/'
    }
}
