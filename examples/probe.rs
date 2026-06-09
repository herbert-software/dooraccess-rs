//! `dooraccess-rs-probe`：hAP 真机 passive-shadow 只读探针
//! （OpenSpec `verify-rust-listeners-on-hap` 组 A）。
//!
//! 复用 `listen6672::Listener` + `listen18022::Listener` 的 PF_PACKET/recv 路径
//! （`ffi::open_packet_socket` / `bind_to_ifindex` / `set_promisc` / `attach_filter`
//! / `recv` —— 本变更要在大端 hAP 内核认证的就是它俩），并发跑两个 `run()`（6672 +
//! 18022，共 4 socket = 2 listener × 2 slave），**只 log detect，不发 wire、不 unlock、
//! 不 HA push**。
//!
//! 设计要点（spec / tasks 2.1）：
//!   - slave 列表**显式给**（`--slaves eth1,eth0.2`，默认 `eth1,eth0.2`）——`config.rs`
//!     不移植桥解析，探针不从 `iface=br-door` 解析（桥解析非本变更范围）。
//!   - 回调与 `Listener.logf` **都**接 log-only sink：listen6672 用 `Callbacks`
//!     （`on_ring` / `on_elevator_key` / `on_number_query` / `on_number_response`
//!     仅 log；**`on_number_query` 只 log，不调 `build_number_query_response`、不发
//!     wire**）；listen18022 用 `on_detect`（仅 log）；**`logf` 必接**：邻居 0x96 帧经
//!     `classify`→`EventKind::Unknown` 臂只走 `self.logf`（listen6672.rs:516）、不触
//!     callback，漏设则整管证据失效。KeepAlive(byte19=0x00) 是 silent drop 无 hook，
//!     但 idle 本单元不自发流量，整管证据靠邻居 0x96（logf 可见），不靠 keepalive。
//!   - orchestration **fail-stop**：两 listener 共享**同一** `shutdown` Arc + 错误经
//!     channel 回传；主线程监到**首个** `run()` Err 即 `exit(1)`（不朴素顺序 join
//!     两个独立 handle——健康 listener 永 loop 会阻塞主线程，做不到「任一即退」）。
//!   - **自限 timeout**：`--duration <N>s`（默认 120s）到点自 `exit(0)`，即便 SSH 断 /
//!     STA 抖断 / setsid 缺也有界自终止，不无限占 64MB RAM。

use std::process::exit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use dooraccess_rs::{listen18022, listen6672};

/// 探针默认采证窗口（秒）。邻居 6672 周期 ~61s，≥120s 给 ~2 周期窗口。
const DEFAULT_DURATION_SECS: u64 = 120;

/// 默认 slave 列表（hAP `[eth1(ifindex 3), eth0.2(ifindex 6)]`，真机基线 2026-06-09）。
const DEFAULT_SLAVES: &[&str] = &["eth1", "eth0.2"];

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cfg = match Config::parse(&args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("dooraccess-rs-probe: {e}");
            eprintln!(
                "usage: dooraccess-rs-probe [--slaves <if1,if2,...>] [--duration <N>s]\n\
                 \tpassive-shadow read-only probe: logs detect only, never sends wire/unlock/HA push.\n\
                 \tdefaults: --slaves {} --duration {}s",
                DEFAULT_SLAVES.join(","),
                DEFAULT_DURATION_SECS
            );
            exit(2);
        }
    };

    eprintln!(
        "dooraccess-rs-probe: passive-shadow start (slaves={:?}, duration={}s) — \
         read-only, no wire/unlock/HA push",
        cfg.slaves, cfg.duration_secs
    );

    // ① 两 listener 共享同一 shutdown Arc（fail-stop 的根缝）。
    let shutdown = Arc::new(AtomicBool::new(false));

    // ② 错误回传 channel：任一 run() 返回（Ok 或 Err）都把结果送回主线程。
    let (err_tx, err_rx) = mpsc::channel::<(&'static str, RunOutcome)>();

    // ③ 起两个 listener 线程，各跑一个 run()，共享 shutdown。
    let l6672 = build_6672(&cfg.slaves);
    let l18022 = build_18022(&cfg.slaves);

    let sd6 = shutdown.clone();
    let tx6 = err_tx.clone();
    let h6672 = thread::Builder::new()
        .name("listen6672".into())
        .spawn(move || {
            let outcome = match l6672.run(sd6) {
                Ok(()) => RunOutcome::Ok,
                Err(e) => RunOutcome::Err(e.to_string()),
            };
            let _ = tx6.send(("listen6672", outcome));
        })
        .expect("spawn listen6672");

    let sd18 = shutdown.clone();
    let tx18 = err_tx.clone();
    let h18022 = thread::Builder::new()
        .name("listen18022".into())
        .spawn(move || {
            let outcome = match l18022.run(sd18) {
                Ok(()) => RunOutcome::Ok,
                Err(e) => RunOutcome::Err(e.to_string()),
            };
            let _ = tx18.send(("listen18022", outcome));
        })
        .expect("spawn listen18022");

    // 丢掉主线程持有的 err_tx 原件，使 channel 仅靠两个线程克隆存活
    // （所有线程退出后 recv_timeout 才会 Disconnected）。
    drop(err_tx);

    // ④ 主线程监控：到 deadline 自杀(exit 0)；任一 run() 提前返回则处理。
    let deadline = Instant::now() + Duration::from_secs(cfg.duration_secs);
    loop {
        let now = Instant::now();
        if now >= deadline {
            // 自限 timeout 到点：干净撤离，置 shutdown 让 listener 收尾，exit(0)。
            eprintln!(
                "dooraccess-rs-probe: duration {}s elapsed — shutting down, exit(0)",
                cfg.duration_secs
            );
            shutdown.store(true, Ordering::SeqCst);
            join_quietly(h6672);
            join_quietly(h18022);
            exit(0);
        }
        let remaining = deadline - now;
        // 用短轮询等待，确保 deadline 到点能及时自杀（不被 run() 永久阻塞）。
        match err_rx.recv_timeout(remaining.min(Duration::from_millis(500))) {
            Ok((who, RunOutcome::Err(msg))) => {
                // 首个 run() Err → fail-stop：置 shared shutdown 停另一 listener，exit(1)。
                eprintln!("dooraccess-rs-probe: {who} run() failed: {msg} — fail-stop, exit(1)");
                shutdown.store(true, Ordering::SeqCst);
                exit(1);
            }
            Ok((who, RunOutcome::Ok)) => {
                // 某 listener 在 shutdown 未置位时自行 Ok 返回是异常（健康 listener 应
                // 永 loop 到 shutdown）；视作残态 → fail-stop exit(1)，避免 2/4 行残态漏网。
                if !shutdown.load(Ordering::SeqCst) {
                    eprintln!(
                        "dooraccess-rs-probe: {who} run() returned Ok before shutdown — \
                         残态, fail-stop, exit(1)"
                    );
                    shutdown.store(true, Ordering::SeqCst);
                    exit(1);
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // 正常：继续等到 deadline 或下一个事件。
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                // 两个 listener 线程都已退出且未在上面被处理（理论不可达，因 Ok/Err
                // 都会先到上面分支）；保守 fail-stop。
                eprintln!(
                    "dooraccess-rs-probe: both listeners exited unexpectedly — fail-stop, exit(1)"
                );
                shutdown.store(true, Ordering::SeqCst);
                exit(1);
            }
        }
    }
}

/// 单个 listener `run()` 的退出归类。
enum RunOutcome {
    Ok,
    Err(String),
}

/// 探针运行参数。
struct Config {
    slaves: Vec<String>,
    duration_secs: u64,
}

impl Config {
    /// 手工解析 `--slaves <csv>` / `--duration <N>[s]`（不引 clap：binary 体型是 OOM 红线）。
    fn parse(args: &[String]) -> Result<Config, String> {
        let mut slaves: Vec<String> = DEFAULT_SLAVES.iter().map(|s| s.to_string()).collect();
        let mut duration_secs = DEFAULT_DURATION_SECS;

        let mut i = 1; // args[0] = bin path
        while i < args.len() {
            match args[i].as_str() {
                "--slaves" => {
                    let v = args.get(i + 1).ok_or_else(|| {
                        "--slaves requires a value (e.g. eth1,eth0.2)".to_string()
                    })?;
                    slaves = v
                        .split(',')
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                        .collect();
                    if slaves.is_empty() {
                        return Err("--slaves value parsed to empty list".to_string());
                    }
                    i += 2;
                }
                "--duration" => {
                    let v = args
                        .get(i + 1)
                        .ok_or_else(|| "--duration requires a value (e.g. 120s)".to_string())?;
                    // 接受 "120" 或 "120s"。
                    let trimmed = v.strip_suffix('s').unwrap_or(v);
                    duration_secs = trimmed
                        .parse::<u64>()
                        .map_err(|_| format!("--duration not a number of seconds: {v}"))?;
                    if duration_secs == 0 {
                        return Err("--duration must be > 0".to_string());
                    }
                    i += 2;
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }
        Ok(Config {
            slaves,
            duration_secs,
        })
    }
}

/// 构造 6672 listener：各回调仅 log（**on_number_query 不调
/// build_number_query_response、不发 wire**），并接 logf。
fn build_6672(slaves: &[String]) -> listen6672::Listener {
    let callbacks = listen6672::Callbacks {
        on_ring: Some(Box::new(|ip, f| {
            eprintln!(
                "[6672 recv] ring src={}.{}.{}.{} subtype=0x{:02x}",
                ip[0], ip[1], ip[2], ip[3], f.subtype
            );
        })),
        on_elevator_key: Some(Box::new(|ip, f| {
            eprintln!(
                "[6672 recv] elevator_key src={}.{}.{}.{} subtype=0x{:02x} (log-only)",
                ip[0], ip[1], ip[2], ip[3], f.subtype
            );
        })),
        // log-only：**不**调 build_number_query_response、**不**发 wire response。
        on_number_query: Some(Box::new(|ip, f| {
            eprintln!(
                "[6672 recv] number_query src={}.{}.{}.{} target_bcd={:02x?} (log-only, no wire)",
                ip[0], ip[1], ip[2], ip[3], f.target_bcd
            );
        })),
        on_number_response: Some(Box::new(|ip, f| {
            eprintln!(
                "[6672 recv] number_response src={}.{}.{}.{} ip_field={:02x?}",
                ip[0], ip[1], ip[2], ip[3], f.self_or_ip
            );
        })),
    };
    let mut l = listen6672::Listener::new(slaves.to_vec(), callbacks);
    // ★ 必接 logf：邻居 0x96 走 classify→Unknown 臂只 logf（listen6672.rs:516），
    // 不触任何 callback；机会性整管证据全靠它可见。
    l.logf = Some(Box::new(|msg| eprintln!("[6672 log] {msg}")));
    l
}

/// 构造 18022 listener：on_detect 仅 log，并接 logf。
fn build_18022(slaves: &[String]) -> listen18022::Listener {
    let on_detect: listen18022::OnDetect = Box::new(|f| {
        eprintln!(
            "[18022 recv] detect req={} src={}.{}.{}.{}:{} -> dst={}.{}.{}.{}:{} body_len={}",
            f.req,
            f.src_ip[0],
            f.src_ip[1],
            f.src_ip[2],
            f.src_ip[3],
            f.src_port,
            f.dst_ip[0],
            f.dst_ip[1],
            f.dst_ip[2],
            f.dst_ip[3],
            f.dst_port,
            f.body.len()
        );
    });
    let mut l = listen18022::Listener::new(slaves.to_vec(), Some(on_detect));
    l.logf = Some(Box::new(|msg| eprintln!("[18022 log] {msg}")));
    l
}

/// join 一个 listener 线程，忽略 panic（撤离阶段尽力收尾，不让收尾失败掩盖 exit code）。
fn join_quietly(h: thread::JoinHandle<()>) {
    let _ = h.join();
}
