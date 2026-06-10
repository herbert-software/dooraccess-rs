//! ring 触发 self-unlock 消费者主体（Phase 4 ②，`port-rust-self-unlock-consumer` 组 B）。
//!
//! 对标 Go `consumeSelfUnlock`（main.go:538）/ `scheduleOutdoorHangup`（:681）/
//! `ExecuteUnlockFromRing`（handlers.go:266）。
//!
//! ## 并发模型（design 决策 1/2）
//!
//! listen18022 dispatch 是 **per-slave 多线程**（`run_platform` thread::scope），`on_detect`
//! 是 `Fn + Send + Sync` 跨 slave 并发调用——把 debounce 状态放其中既数据竞争又编译不过。故
//! self-unlock 走 `listener.subscribe(filter)`：filter（只读不可变捕获 indoor_ip/outdoor_by_ip）
//! 在多线程 dispatch 路径同步跑、无状态；命中帧经 bounded `sync_channel(8)` 汇成单流，由**独立
//! 单消费者线程** drain。debounce / flag-gate / 产 Job 全在消费者线程顺序完成（真正单线程、无锁）。
//!
//! ## per-ring 失败隔离（防御式编码，非 catch_unwind）
//!
//! release profile `panic="abort"` 下 `catch_unwind` 是 no-op，故 Go 的 inner-closure
//! `defer recover()` 不可直译。消费者用防御式编码：可失败操作返 `Result` → log + 跳过该 ring
//! 继续 drain；ring 处理路径禁 `unwrap`/`expect`/越界/`panic!`。debounce 写在可失败操作之前
//! （起步即写，对齐 main.go:645）。
//!
//! ## shutdown 集成（recv_timeout 轮询，非裸 recv）
//!
//! `Subscription.ch` 是 std `mpsc::Receiver`（阻塞 recv 听不到 `Arc<AtomicBool>`；crate gate
//! 禁 crossbeam）。消费者 drain 用 `recv_timeout(poll)`：`Timeout` 查 shutdown 置位则 break；
//! `Disconnected` clean break（不 log 为错误）；退出统一调 `sub.cancel()`（id-幂等）。钉死排序里
//! 插在 listener-join 之后、`Job::Shutdown` 哨兵之前 join。

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::indexing_slicing)]
#![deny(clippy::arithmetic_side_effects)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::codec;
use crate::config::Config;
use crate::control::AutomationState;
use crate::daemon::{self, HangupJob, Job, PushTracker, UnlockJob};
use crate::ha_push::HaPushClient;
use crate::listen18022::{DetectedFrame, SubFilter, Subscribable};
use crate::log::log_line;
use crate::unlock::{
    UnlockOutcome, SELF_UNLOCK_PROBE_INTERVAL, SELF_UNLOCK_PROBE_TIMEOUT, UNLOCK_TOTAL_CAP,
};

// ---------------------------------------------------------------------------
// test-tunable 形态（参 Go `var selfUnlockDebounceMargin` / `var selfUnlockHangupDelay`）
//
// Rust 侧 UNLOCK_TOTAL_CAP 是 const（不可运行时改），故 margin/delay 用 static AtomicU32(ms)
// + setter 让 mock-e2e 缩到 ms 级；生产值 ~2s（勿改）。
//
// **用 AtomicU32 而非 AtomicU64**：目标 32-bit MIPS（mips-unknown-linux-musl）无原生 64-bit
// 原子（`std::sync::atomic::AtomicU64` 在该 target 不存在，build-mips 会 E0432）。ms 值上限
// ~2s（远 < u32::MAX），故 u32 余量充足；公开 setter 仍收 u64（不破坏 e2e 调用方），边界 `as` 折宽窄。
// ---------------------------------------------------------------------------

/// debounce 窗 = `UNLOCK_TOTAL_CAP + margin` 中的余量（ms）。生产 ~2s。
static DEBOUNCE_MARGIN_MS: AtomicU32 = AtomicU32::new(2_000);

/// auto-hangup 自开锁成功到投 `Job::Hangup` 的延迟（ms）。生产 ~2s。
static HANGUP_DELAY_MS: AtomicU32 = AtomicU32::new(2_000);

/// 计时线程分片轮询片长（ms）；shutdown 响应 ≤ 一个分片（援引 unlock.rs 20ms 惯用法）。
const HANGUP_POLL_SLICE: Duration = Duration::from_millis(20);

/// 消费者 drain `recv_timeout` poll（~ listener SO_RCVTIMEO / unlock.rs 20ms 惯用法）。
const CONSUMER_POLL: Duration = Duration::from_millis(20);

/// debounce 窗（`UNLOCK_TOTAL_CAP + margin`，不硬编 12s）。
fn debounce_window() -> Duration {
    UNLOCK_TOTAL_CAP.saturating_add(Duration::from_millis(u64::from(
        DEBOUNCE_MARGIN_MS.load(Ordering::SeqCst),
    )))
}

/// auto-hangup 延迟（test-tunable）。
fn hangup_delay() -> Duration {
    Duration::from_millis(u64::from(HANGUP_DELAY_MS.load(Ordering::SeqCst)))
}

/// 测试专用：临时缩短 debounce margin（ms）。返回旧值（调用方负责复原）。
#[doc(hidden)]
pub fn set_debounce_margin_ms_for_test(ms: u64) -> u64 {
    u64::from(DEBOUNCE_MARGIN_MS.swap(ms as u32, Ordering::SeqCst))
}

/// 测试专用：临时缩短 auto-hangup 延迟（ms）。返回旧值（调用方负责复原）。
#[doc(hidden)]
pub fn set_hangup_delay_ms_for_test(ms: u64) -> u64 {
    u64::from(HANGUP_DELAY_MS.swap(ms as u32, Ordering::SeqCst))
}

// ---------------------------------------------------------------------------
// SelfUnlockDeps：消费者线程的注入缝（生产 orchestration 装配 / 测试 mock）
// ---------------------------------------------------------------------------

/// self-unlock 消费者运行所需依赖（构造期解析的不可变 ring 参数 + 运行时缝）。
///
/// 经 [`try_new`] 构造（构造期解析 outdoor_by_ip / callee_bcd / indoor_ip；calleeBCD 或
/// indoorIP 解析失败返 `None` = self-unlock 禁用）。`job_tx` 既投 `Job::Unlock`（worker 执行
/// 自开锁）又投 `Job::Hangup`（auto-hangup）；`push_client`/`tracker`/`shutdown` 经
/// `daemon::spawn_push` 发 `event=unlock`；`automation` 是运行时 flag gate 的 atomic 读源。
pub struct SelfUnlockDeps {
    /// IP → 外机 SIP URI 反查表（filter + 触发用，构造期解析、运行期只读）。
    outdoor_by_ip: HashMap<String, String>,
    /// 室内机 BCD（cfg.SIP，作 callee；hangup 的 monitorBCD/callerBCD 槽）。
    callee_bcd: [u8; 4],
    /// 本机室内机 IPv4（filter dst 收紧）。
    indoor_ip: [u8; 4],
    /// 室内机 URI（cfg.SIP，push event=unlock 的 to 字段）。
    indoor_uri: String,
    /// 投 `Job::Unlock` / `Job::Hangup` 给 worker。
    job_tx: Sender<Job>,
    /// event=unlock detached push 缝。
    push_client: Arc<HaPushClient>,
    tracker: PushTracker,
    /// 单 `Arc<AtomicBool>` shutdown（consumer drain / 计时线程轮询）。
    shutdown: Arc<AtomicBool>,
    /// 运行时 flag gate（atomic 读 auto_unlock / auto_hangup）。
    automation: AutomationState,
}

impl SelfUnlockDeps {
    /// 构造消费者依赖。构造期解析 outdoor_by_ip / callee_bcd / indoor_ip 各一次；
    /// calleeBCD 或 indoorIP 解析失败 → 返 `None`（self-unlock 禁用，消费者不启动），
    /// log 一行、不 panic、不部分启动（锚 Go main.go:555-576）。
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        cfg: &Config,
        outdoor_by_ip: HashMap<String, String>,
        job_tx: Sender<Job>,
        push_client: Arc<HaPushClient>,
        tracker: PushTracker,
        shutdown: Arc<AtomicBool>,
        automation: AutomationState,
        mut logf: impl FnMut(&str),
    ) -> Option<Self> {
        // callee（室内机）BCD ← cfg.SIP（失败禁用）。
        let callee_bcd = match codec::parse_uri(&cfg.sip) {
            Ok((name, _, _)) => match codec::encode_bcd(&name) {
                Ok(b) => b,
                Err(_) => {
                    logf("[auto_unlock] consumer: parse cfg.sip (callee bcd) failed; self-unlock disabled");
                    return None;
                }
            },
            Err(_) => {
                logf("[auto_unlock] consumer: parse cfg.sip (callee) failed; self-unlock disabled");
                return None;
            }
        };

        // 本机室内机 IP ← cfg.SIP（失败禁用；filter dst 收紧防多室内机误触）。
        let indoor_ip = match codec::parse_uri(&cfg.sip) {
            Ok((_, ip_str, _)) => match crate::orchestration::parse_ipv4(&ip_str) {
                Some(ip) => ip,
                None => {
                    logf("[auto_unlock] consumer: cfg.sip indoor ip unparseable; self-unlock disabled");
                    return None;
                }
            },
            Err(_) => {
                logf("[auto_unlock] consumer: parse cfg.sip (indoor ip) failed; self-unlock disabled");
                return None;
            }
        };

        Some(Self {
            outdoor_by_ip,
            callee_bcd,
            indoor_ip,
            indoor_uri: cfg.sip.clone(),
            job_tx,
            push_client,
            tracker,
            shutdown,
            automation,
        })
    }

    /// filter 闭包（只读不可变捕获 indoor_ip/outdoor_by_ip）：`req==704 && dst==本机室内机IP
    /// && src∈outdoorByIP`（锚 Go main.go:581-590）。在多线程 dispatch 路径同步跑、无状态。
    fn make_filter(&self) -> SubFilter {
        let indoor_ip = self.indoor_ip;
        // 仅捕获 IP 集合（cheap），不捕 URI map（filter 只判 src 是否在集）。
        let outdoor_ips: std::collections::HashSet<String> =
            self.outdoor_by_ip.keys().cloned().collect();
        Box::new(move |d: &DetectedFrame| {
            if d.req != 704 {
                return false;
            }
            if d.dst_ip != indoor_ip {
                return false;
            }
            outdoor_ips.contains(&ipv4_str(d.src_ip))
        })
    }
}

// ---------------------------------------------------------------------------
// 消费者线程主体（决策 1：单消费者 drain + debounce + flag gate + 产 Job）
// ---------------------------------------------------------------------------

/// 注册订阅 + 起独立单消费者线程。返回消费者线程 `Some(JoinHandle)`（钉死排序里 listener-join
/// 之后、`Job::Shutdown` 哨兵之前 join）；spawn 失败返 `None`（self-unlock 静默禁用，best-effort）。
///
/// **调用方契约**（钉死 shutdown 排序新增插步，design 决策 1）：
///   - listener 线程 join 之后 join 本 handle（保证「哨兵后无新 Unlock job 入队」关窗不变量）；
///   - `Job::Shutdown` 哨兵之前 join 本 handle；
///   - 返 `None` 时调用方无 handle 可 join，跳过（self-unlock 未启动，不影响排序）。
pub fn spawn_consumer(
    listener: &dyn Subscribable,
    deps: SelfUnlockDeps,
    mut logf: impl FnMut(&str),
) -> Option<JoinHandle<()>> {
    let filter = deps.make_filter();
    let sub = listener.subscribe(Some(filter));
    let window = debounce_window();
    let stations = deps.outdoor_by_ip.len(); // 在 deps move 进闭包前取，供成功后 log
    match std::thread::Builder::new()
        .name("self-unlock".into())
        .spawn(move || run_consumer(sub, &deps))
    {
        Ok(h) => {
            // 「started」日志只在 spawn 真成功后打（否则失败时误报已启动）。
            logf(&format!(
                "[auto_unlock] self-unlock consumer started (debounce={window:?}, stations={stations})"
            ));
            Some(h)
        }
        Err(e) => {
            // spawn 失败极罕见（线程资源耗尽）；按骨架 best-effort 纪律不 panic。返 `None`
            // 而非裸 `std::thread::spawn` 占位——后者在同一资源耗尽下也会失败并**panic**
            // （`panic=abort` 即 abort daemon），违背本模块 no-panic 防御纪律。此时 sub 随
            // 失败闭包 drop → `Subscription` 的 `Drop` guard 兜底 cancel（清 orphan SubEntry，
            // 不留空转 filter），channel Disconnected，listener 后续投递 silent。
            log_line(&format!(
                "[auto_unlock] failed to spawn consumer thread: {e} (self-unlock disabled)"
            ));
            None
        }
    }
}

/// 消费者 drain 主循环（recv_timeout 轮询 + 防御式 per-ring 处理）。
fn run_consumer(sub: crate::listen18022::Subscription, deps: &SelfUnlockDeps) {
    let window = debounce_window();
    // debounce 状态：消费者线程单线程访问，无锁（决策 1）。
    let mut last_triggered: HashMap<String, Instant> = HashMap::new();
    // channel-drop 对账（dequeue − triggered = debounce_skips，锚 Go main.go:597/604）。
    let mut dequeue_count: u64 = 0;
    let mut triggered_count: u64 = 0;

    loop {
        match sub.ch.recv_timeout(CONSUMER_POLL) {
            Ok(frame) => {
                // 取帧 Instant = t_ms 起点（锚 Go ringAt main.go:621）。recv_timeout 帧到达
                // 立即返回，t_ms 精度不受 poll 影响。
                let ring_at = Instant::now();
                dequeue_count = dequeue_count.saturating_add(1);
                handle_ring(
                    deps,
                    &frame,
                    ring_at,
                    window,
                    &mut last_triggered,
                    &mut triggered_count,
                );
            }
            Err(RecvTimeoutError::Timeout) => {
                // 查 shutdown 置位则 break（裸 recv 听不到 AtomicBool，故必须 recv_timeout）。
                if deps.shutdown.load(Ordering::SeqCst) {
                    break;
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                // listener 退出 / cancel drop tx：clean exit，不 log 为错误。
                break;
            }
        }
    }

    log_line(&format!(
        "[auto_unlock] self-unlock consumer stopping (dequeue={dequeue_count} triggered={triggered_count})"
    ));
    // 退出统一调一次 cancel（id-幂等，listen18022.rs:664 找不到即 no-op）。
    sub.cancel();
}

/// 单个 ring 的防御式处理：flag gate → debounce → ExecuteUnlockFromRing → auto-hangup。
///
/// **处理顺序钉死**（spec「flag gate」）：dequeue → 读 flag（off 即 return 不写 debounce）→
/// debounce 检查 → debounce 写（起步即写）→ 解析外机 URI（失败 log+跳过）→ 触发。
/// 所有可失败操作返 `Result`/`Option` + log + return（不 panic）。
fn handle_ring(
    deps: &SelfUnlockDeps,
    frame: &DetectedFrame,
    ring_at: Instant,
    window: Duration,
    last_triggered: &mut HashMap<String, Instant>,
    triggered_count: &mut u64,
) {
    let src_str = ipv4_str(frame.src_ip);
    let outdoor_uri = match deps.outdoor_by_ip.get(&src_str) {
        Some(u) => u.clone(),
        None => return, // filter 已挡，理论不达；防御性 skip。
    };

    // flag gate：每 ring dequeue 后读运行时 auto_unlock（atomic）。off 跳过且**不写 debounce**
    // （未触发无需占窗，否则 flag-off 的 ring 污染窗口误 debounce 后续 flag-on ring）。
    if !deps.automation.load_auto_unlock() {
        log_line(&format!("[auto_unlock] flag off, skip src={outdoor_uri}"));
        return;
    }

    // debounce：起步即写、单调、不复位、不论成败（即使后续解析失败时间戳也保留）。
    if let Some(last) = last_triggered.get(&outdoor_uri) {
        // 单调侧 duration_since（now.duration_since(earlier)，防反向 panic）。
        if ring_at.duration_since(*last) < window {
            log_line(&format!("[auto_unlock] debounce skip src={outdoor_uri}"));
            return;
        }
    }
    // 起步即写（在任何可失败操作之前，对齐 main.go:645）。
    last_triggered.insert(outdoor_uri.clone(), ring_at);

    // 解析外机 BCD/IP/port（一次；失败 log+跳过不 panic）。
    let (caller_bcd, target_ip, target_port) = match parse_outdoor_uri(&outdoor_uri) {
        Ok(v) => v,
        Err(e) => {
            log_line(&format!(
                "[auto_unlock] parse outdoor uri {outdoor_uri:?} failed: {e} (skip)"
            ));
            return;
        }
    };

    *triggered_count = triggered_count.saturating_add(1);
    log_line(&format!(
        "[auto_unlock] trigger src={outdoor_uri} (dequeue-triggered=debounce_skips)"
    ));

    // ExecuteUnlockFromRing：经 worker 跑自开锁 + t_ms 测量 + 成功 push event=unlock。
    let outcome = execute_unlock_from_ring(
        deps,
        caller_bcd,
        deps.callee_bcd,
        &target_ip,
        target_port,
        &outdoor_uri,
        ring_at,
    );

    // auto-hangup：result=0 + 运行时 auto_hangup on → detached 计时线程延迟 ~2s 投 Job::Hangup。
    if deps.automation.load_auto_hangup() && outcome.result == codec::result::OK {
        schedule_outdoor_hangup(
            deps.job_tx.clone(),
            Arc::clone(&deps.shutdown),
            caller_bcd,      // outdoor_bcd（BuildStopFrame 首参 / calleeBCD 槽）
            deps.callee_bcd, // monitor_bcd（= 室内机 BCD，callerBCD 槽，锚 Go main.go:563/697）
            target_ip,
            target_port,
        );
    }
}

// ---------------------------------------------------------------------------
// ExecuteUnlockFromRing 包装（task 3.1/3.2，锚 Go handlers.go:266）
// ---------------------------------------------------------------------------

/// ring 触发自开锁：经骨架 worker（`Job::Unlock`）跑 unlock 核心 + t_ms 测量（起点 = ring_at
/// dequeue Instant）+ 仅成功 push `event=unlock`（失败不 push，锚 Go handlers.go:288）。
///
/// 投 `Job::Unlock` + 等一次性 reply（同 `WorkerUnlockDispatch` 模式）：worker 已退/panic →
/// SendError/RecvError → 退化 wire-failure outcome（不永等、不 panic）。
fn execute_unlock_from_ring(
    deps: &SelfUnlockDeps,
    caller_bcd: [u8; 4],
    callee_bcd: [u8; 4],
    target_ip: &str,
    target_port: u16,
    from_uri: &str,
    ring_at: Instant,
) -> UnlockOutcome {
    let (reply_tx, reply_rx) = mpsc::sync_channel::<UnlockOutcome>(1);
    let job = Job::Unlock(UnlockJob {
        caller_bcd,
        callee_bcd,
        target_ip: target_ip.to_string(),
        target_port,
        reply: reply_tx,
        // ring 路径 fail-fast probe 计时（headline ~1.3s 成因）：per-attempt 800ms 切断首帧
        // 阻塞假象 + 200ms 重探（非 HTTP 1s 退避），锚 Go handlers.go:283。
        per_attempt_timeout: Some(SELF_UNLOCK_PROBE_TIMEOUT),
        retry_interval: SELF_UNLOCK_PROBE_INTERVAL,
    });
    // 投 job：容忍 SendError（worker 已退 → 退化 wire-failure，禁 unwrap）。
    if daemon::submit_job(&deps.job_tx, job).is_err() {
        return worker_unavailable_outcome();
    }
    // 等 worker 回灌（worker panic → reply sender drop → RecvError → 退化，不永等）。
    let outcome = match reply_rx.recv() {
        Ok(o) => o,
        Err(_) => worker_unavailable_outcome(),
    };

    let t_ms = ring_at.elapsed().as_millis();
    log_line(&format!(
        "[auto_unlock] result={} retries={} t_ms={t_ms}",
        outcome.result, outcome.retries
    ));

    // §5.1：仅成功 push event=unlock（失败不 push，保持 Stage 1 现状）。best-effort detached。
    if outcome.result == codec::result::OK {
        daemon::spawn_push(
            &deps.tracker,
            Arc::clone(&deps.push_client),
            Arc::clone(&deps.shutdown),
            "unlock".into(),
            vec![
                ("from".into(), from_uri.to_string()),
                ("to".into(), deps.indoor_uri.clone()),
                ("result".into(), "0".into()),
            ],
        );
    }
    outcome
}

/// worker 不可用（已退 / panic）时退化 outcome（与 orchestration::worker_unavailable_outcome 同义）。
fn worker_unavailable_outcome() -> UnlockOutcome {
    UnlockOutcome {
        result: codec::result::ERR,
        retries: 0,
        terminated_by_bye: false,
        wire_kind: Some(crate::unlock::WireKind::Other),
    }
}

// ---------------------------------------------------------------------------
// auto-hangup detached 计时线程（task 4.1/4.3，决策 2，锚 Go scheduleOutdoorHangup）
// ---------------------------------------------------------------------------

/// 起一次性 detached 计时线程：分片轮询延迟 ~2s（Instant 累计，可中断）后投**即时**
/// `Job::Hangup` 给 worker（worker 发外机目标 req=708 preview-stop 帧）。
///
/// - **可中断延迟**：`park_timeout(20ms)` 轮询 + `start.elapsed() >= delay` 单调判据（非分片
///   计数——park_timeout spurious 提前会让计数法误判早发）；shutdown 置位即放弃（不投不发）。
/// - **不发 wire**：仅 `submit_job(Job::Hangup)`，req=708 仍由 worker 唯一发（守 M1.5）。
/// - **best-effort**：投 `Job::Hangup` 的 SendError 容忍禁 unwrap；晚于哨兵到 worker 则丢弃，
///   daemon 不为它延迟 shutdown / 不绕 worker 直发（锚 Go main.go:680/691-694）。
/// - **不触碰 debounce**：按值持 BCD/IP（消费者线程的 last_triggered 够不到）。
fn schedule_outdoor_hangup(
    job_tx: Sender<Job>,
    shutdown: Arc<AtomicBool>,
    outdoor_bcd: [u8; 4],
    monitor_bcd: [u8; 4],
    outdoor_ip: String,
    outdoor_port: u16,
) {
    let delay = hangup_delay();
    // 有意 detach：不加入 shutdown 排空集；靠 shutdown AtomicBool 兜底放弃（决策 2 / 骨架
    // spec:108 非排空契约口）。spawn 失败仅 log（best-effort）。
    let spawned = std::thread::Builder::new()
        .name("self-unlock-hangup".into())
        .spawn(move || {
            // 分片轮询延迟（单调 Instant 累计；援引 unlock.rs:204 ThreadSleeper 先例）。
            let start = Instant::now();
            loop {
                if shutdown.load(Ordering::SeqCst) {
                    return; // 置位即放弃（不投不发）。
                }
                if start.elapsed() >= delay {
                    break;
                }
                std::thread::park_timeout(HANGUP_POLL_SLICE);
            }
            // 到点再查一次 shutdown（轮询窗口边界竞态）：置位则放弃。
            if shutdown.load(Ordering::SeqCst) {
                return;
            }
            // 投即时 Job::Hangup 给 worker（worker 发 req=708）。SendError 容忍禁 unwrap。
            let job = Job::Hangup(HangupJob {
                outdoor_bcd,
                monitor_bcd,
                outdoor_ip,
                outdoor_port,
            });
            if daemon::submit_job(&job_tx, job).is_err() {
                log_line("[auto_unlock] hangup Job::Hangup dropped (worker queue closed)");
            }
        });
    if spawned.is_err() {
        log_line("[auto_unlock] failed to spawn hangup timer thread (skip)");
    }
}

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

/// 解析外机 URI → (callerBCD, targetIP, targetPort)（等价 Go `ParseURIBCD`）。
fn parse_outdoor_uri(uri: &str) -> Result<([u8; 4], String, u16), String> {
    let (name, ip, port) = codec::parse_uri(uri).map_err(|_| "uri parse".to_string())?;
    let bcd = codec::encode_bcd(&name).map_err(|_| "bcd encode".to_string())?;
    Ok((bcd, ip, port))
}

/// `[u8; 4]` → 点分十进制（filter src/dst 比较用）。
fn ipv4_str(ip: [u8; 4]) -> String {
    // 借 orchestration 的同款渲染（无 unwrap/index 越界）。
    crate::orchestration::ipv4_str(ip)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debounce_window_derives_from_cap_plus_margin() {
        let old = set_debounce_margin_ms_for_test(500);
        assert_eq!(
            debounce_window(),
            UNLOCK_TOTAL_CAP.saturating_add(Duration::from_millis(500))
        );
        set_debounce_margin_ms_for_test(old);
    }

    #[test]
    fn hangup_delay_is_tunable() {
        let old = set_hangup_delay_ms_for_test(30);
        assert_eq!(hangup_delay(), Duration::from_millis(30));
        set_hangup_delay_ms_for_test(old);
    }

    #[test]
    fn parse_outdoor_uri_ok_and_err() {
        let (bcd, ip, port) = parse_outdoor_uri("06020000@172.16.106.201:18022")
            .unwrap_or_else(|_| panic!("valid uri should parse"));
        assert_eq!(ip, "172.16.106.201");
        assert_eq!(port, 18022);
        assert_eq!(bcd, codec::encode_bcd("06020000").unwrap_or([0; 4]));
        assert!(parse_outdoor_uri("garbage").is_err());
    }
}

// ===========================================================================
// 组 C — mock-e2e 测试（tasks §5；行为锚 Go selfunlock 测试集）
//
// 测试缝：消费者主体（`run_consumer`/`spawn_consumer`）的依赖经 `SelfUnlockDeps::try_new`
// 构造（公开 API），filter 经 `make_filter`（私有，本模块可见）注册到一个真 `Listener`，
// 帧经 `dispatch_for_test` 直驱（绕 PF_PACKET）。`job_tx` 接一个真 `run_worker`（注入
// 记录型 `UnlockWire` / `HangupWire`），断言 Job::Unlock 抵达 worker + 自开锁被执行、
// Job::Hangup 携 `build_stop_frame` 字节、debounce/flag-gate/失败隔离/shutdown 不死锁。
//
// 沙箱：纯进程内同步原语（mpsc / Listener / 线程），无 socket bind——沙箱内外皆可跑。
// 唯一例外是 5.8 的 push 断言（经 `daemon::spawn_push`→`HaPushClient` 需 loopback HTTP
// capture server，须沙箱外；bind 失败静默跳过，仿 daemon_e2e t10_9）。
// ===========================================================================

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod e2e {
    use super::*;
    use crate::daemon::{run_worker, HangupWire, WorkerDeps};
    use crate::listen18022::{DetectedFrame, Listener, Subscribable};
    use crate::unlock::{AttemptOutcome, Deadline, UnlockWire, WireKind};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    // ---- 测试夹具 ----------------------------------------------------------

    const INDOOR_URI: &str = "06021103@172.16.106.91:18022";
    const INDOOR_IP: [u8; 4] = [172, 16, 106, 91];
    const OUTDOOR_URI: &str = "06020000@172.16.106.152:18022";
    const OUTDOOR_IP: [u8; 4] = [172, 16, 106, 152];

    fn e2e_cfg() -> Config {
        let mut cfg = Config::default();
        cfg.sip = INDOOR_URI.into();
        cfg.stations = vec![crate::config::Station {
            sip: OUTDOOR_URI.into(),
            rtsp_url: String::new(),
        }];
        cfg
    }

    fn never() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    /// 记录每次 `try_once` 调用的 mock wire（按 outcome 折叠成 result）。
    struct RecordingWire {
        outcome: AttemptOutcome,
        calls: Arc<AtomicUsize>,
    }
    impl RecordingWire {
        fn new(outcome: AttemptOutcome) -> (Arc<Self>, Arc<AtomicUsize>) {
            let calls = Arc::new(AtomicUsize::new(0));
            (
                Arc::new(Self {
                    outcome,
                    calls: Arc::clone(&calls),
                }),
                calls,
            )
        }
    }
    impl UnlockWire for RecordingWire {
        #[allow(clippy::too_many_arguments)]
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

    /// 记录每次 `send_fire_and_forget` 收到的 (target_ip, port, frame) 的 mock HangupWire。
    #[derive(Default)]
    struct RecordingHangupWire {
        frames: Mutex<Vec<(String, u16, Vec<u8>)>>,
    }
    impl RecordingHangupWire {
        fn frames(&self) -> Vec<(String, u16, Vec<u8>)> {
            self.frames.lock().unwrap().clone()
        }
    }
    impl HangupWire for RecordingHangupWire {
        fn send_fire_and_forget(
            &self,
            _cancel: &AtomicBool,
            target_ip: &str,
            target_port: u16,
            frame: &[u8],
        ) -> Result<(), String> {
            self.frames
                .lock()
                .unwrap()
                .push((target_ip.to_string(), target_port, frame.to_vec()));
            Ok(())
        }
    }

    /// 起一个真 wire-worker（注入记录型 wire + 可选 hangup wire），返回 (job_tx, worker handle)。
    fn spawn_worker(
        wire: Arc<dyn UnlockWire>,
        hangup_wire: Option<Arc<dyn HangupWire>>,
        shutdown: Arc<AtomicBool>,
    ) -> (Sender<Job>, std::thread::JoinHandle<()>) {
        let (tx, rx) = mpsc::channel::<Job>();
        let mut deps = WorkerDeps::new(wire, None);
        if let Some(hw) = hangup_wire {
            deps = deps.with_hangup_wire(hw);
        }
        let h = std::thread::spawn(move || run_worker(deps, rx, shutdown));
        (tx, h)
    }

    /// 经 `try_new` 构造 deps（push api 空 → push 静默 NotConfigured，不触网络）。
    fn make_deps(
        cfg: &Config,
        job_tx: Sender<Job>,
        shutdown: Arc<AtomicBool>,
        automation: AutomationState,
    ) -> SelfUnlockDeps {
        let push_client = Arc::new(HaPushClient::new(cfg.clone(), None));
        SelfUnlockDeps::try_new(
            cfg,
            crate::orchestration::parse_stations_to_ip_map(cfg),
            job_tx,
            push_client,
            PushTracker::new(),
            shutdown,
            automation,
            |_| {},
        )
        .expect("deps construct")
    }

    /// 构造命中 filter 的 req=704 wire payload（外机响铃帧），经 `dispatch_for_test` 直喂。
    fn wire_704_payload() -> Vec<u8> {
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

    /// 在独立线程跑 `run_consumer`，返回 (consumer handle, shutdown)。调用方经
    /// `dispatch_for_test` 喂帧后置 shutdown + join。`sub` 经 listener 的 filter 注册。
    fn spawn_consumer_thread(
        listener: &Arc<Listener>,
        deps: SelfUnlockDeps,
    ) -> std::thread::JoinHandle<()> {
        let filter = deps.make_filter();
        let sub = listener.subscribe(Some(filter));
        std::thread::spawn(move || run_consumer(sub, &deps))
    }

    /// busy-wait 直到 `cond()` 为真或超时，返回是否在超时前满足。
    fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if cond() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        cond()
    }

    // ---- 5.1 ring 触发自开锁 e2e -------------------------------------------

    /// mock 外机 req=704 → 经 subscribe → 消费者产 Job::Unlock → worker 跑自开锁
    /// （wire 被调一次）。锚 Go `main_listen18022_ring_test.go`。
    #[test]
    fn t5_1_ring_triggers_self_unlock_through_worker() {
        let cfg = e2e_cfg();
        let shutdown = never();
        let (wire, wire_calls) = RecordingWire::new(AttemptOutcome::Ok);
        let (job_tx, worker) = spawn_worker(wire, None, shutdown.clone());

        let automation = AutomationState::new(true, false); // auto_unlock on
        let deps = make_deps(&cfg, job_tx.clone(), shutdown.clone(), automation);

        let listener = Arc::new(Listener::new(vec!["eth0".into()], None));
        let consumer = spawn_consumer_thread(&listener, deps);

        // ring 帧经真 dispatch（filter 命中 → 投消费者 sub channel）。
        listener.dispatch_for_test(OUTDOOR_IP, INDOOR_IP, 50000, 18022, &wire_704_payload());

        // 等 worker 被调一次（自开锁触发）。
        assert!(
            wait_until(Duration::from_secs(2), || wire_calls.load(Ordering::SeqCst)
                >= 1),
            "ring 应触发一次自开锁（worker wire 被调）"
        );
        assert_eq!(wire_calls.load(Ordering::SeqCst), 1, "恰一次自开锁");

        // 收尾：shutdown 消费者 + 哨兵停 worker。
        shutdown.store(true, Ordering::SeqCst);
        consumer.join().unwrap();
        daemon::submit_job(&job_tx, Job::Shutdown).ok();
        worker.join().unwrap();
    }

    // ---- 5.2 debounce e2e ---------------------------------------------------

    /// 窗口内重复 ring 仅触发首次（debounce 单线程顺序判定）。
    #[test]
    fn t5_2_debounce_window_triggers_once() {
        let old = set_debounce_margin_ms_for_test(5_000); // 窗口 = cap(10s)+5s，确保两帧同窗
        let cfg = e2e_cfg();
        let shutdown = never();
        let (wire, wire_calls) = RecordingWire::new(AttemptOutcome::Ok);
        let (job_tx, worker) = spawn_worker(wire, None, shutdown.clone());
        let automation = AutomationState::new(true, false);
        let deps = make_deps(&cfg, job_tx.clone(), shutdown.clone(), automation);
        let listener = Arc::new(Listener::new(vec!["eth0".into()], None));
        let consumer = spawn_consumer_thread(&listener, deps);

        // 同一外机连投 3 帧（窗口内）。
        for _ in 0..3 {
            listener.dispatch_for_test(OUTDOOR_IP, INDOOR_IP, 50000, 18022, &wire_704_payload());
            std::thread::sleep(Duration::from_millis(20));
        }
        // 等消费者处理完（首次触发 + 后两次 debounce skip）。
        assert!(
            wait_until(Duration::from_secs(2), || wire_calls.load(Ordering::SeqCst)
                >= 1),
            "首次 ring 应触发"
        );
        std::thread::sleep(Duration::from_millis(100)); // 给后续帧 debounce 机会
        assert_eq!(
            wire_calls.load(Ordering::SeqCst),
            1,
            "窗口内重复 ring 仅触发首次"
        );

        shutdown.store(true, Ordering::SeqCst);
        consumer.join().unwrap();
        daemon::submit_job(&job_tx, Job::Shutdown).ok();
        worker.join().unwrap();
        set_debounce_margin_ms_for_test(old);
    }

    // ---- 5.3 flag gate e2e --------------------------------------------------

    /// auto_unlock off → ring 跳过不触发、不写 debounce；随后拨 on 同外机仍能触发
    /// （证 off 未污染 debounce 窗口）。
    #[test]
    fn t5_3_flag_off_skips_and_does_not_write_debounce() {
        let old = set_debounce_margin_ms_for_test(5_000);
        let cfg = e2e_cfg();
        let shutdown = never();
        let (wire, wire_calls) = RecordingWire::new(AttemptOutcome::Ok);
        let (job_tx, worker) = spawn_worker(wire, None, shutdown.clone());
        let automation = AutomationState::new(false, false); // off
        let auto_handle = automation.share();
        let deps = make_deps(&cfg, job_tx.clone(), shutdown.clone(), automation);
        let listener = Arc::new(Listener::new(vec!["eth0".into()], None));
        let consumer = spawn_consumer_thread(&listener, deps);

        // off 时 ring：跳过不触发。
        listener.dispatch_for_test(OUTDOOR_IP, INDOOR_IP, 50000, 18022, &wire_704_payload());
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(wire_calls.load(Ordering::SeqCst), 0, "flag off 不触发");

        // 拨 on，同外机 ring：应触发（off 那次未写 debounce 窗口，故不被误 debounce）。
        auto_handle.store_auto_unlock(true);
        listener.dispatch_for_test(OUTDOOR_IP, INDOOR_IP, 50000, 18022, &wire_704_payload());
        assert!(
            wait_until(Duration::from_secs(2), || wire_calls.load(Ordering::SeqCst)
                >= 1),
            "拨 on 后同外机应触发（off 未占 debounce 窗口）"
        );
        assert_eq!(wire_calls.load(Ordering::SeqCst), 1);

        shutdown.store(true, Ordering::SeqCst);
        consumer.join().unwrap();
        daemon::submit_job(&job_tx, Job::Shutdown).ok();
        worker.join().unwrap();
        set_debounce_margin_ms_for_test(old);
    }

    /// 「在飞」语义：已 dequeue 并过 gate 正在触发的那次 MUST 跑完——用阻塞 wire 卡住
    /// 触发中、拨 off，断言该次仍完成（result 回灌、worker wire 被调）。
    #[test]
    fn t5_3_in_flight_unlock_completes_after_flag_off() {
        use std::sync::Barrier;
        let cfg = e2e_cfg();
        let shutdown = never();

        // 阻塞 wire：第一次 try_once 卡 gate（main 放行后才返回 Ok）。
        struct GatedWire {
            gate: Arc<Barrier>,
            used: AtomicBool,
            calls: Arc<AtomicUsize>,
        }
        impl UnlockWire for GatedWire {
            #[allow(clippy::too_many_arguments)]
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
                if !self.used.swap(true, Ordering::SeqCst) {
                    self.gate.wait();
                }
                AttemptOutcome::Ok
            }
        }
        let gate = Arc::new(Barrier::new(2));
        let calls = Arc::new(AtomicUsize::new(0));
        let wire: Arc<dyn UnlockWire> = Arc::new(GatedWire {
            gate: gate.clone(),
            used: AtomicBool::new(false),
            calls: calls.clone(),
        });
        let (job_tx, worker) = spawn_worker(wire, None, shutdown.clone());
        let automation = AutomationState::new(true, false); // on
        let auto_handle = automation.share();
        let deps = make_deps(&cfg, job_tx.clone(), shutdown.clone(), automation);
        let listener = Arc::new(Listener::new(vec!["eth0".into()], None));
        let consumer = spawn_consumer_thread(&listener, deps);

        // ring 过 gate 进 worker，卡在 GatedWire（已 dequeue + 过 flag gate = 在飞）。
        listener.dispatch_for_test(OUTDOOR_IP, INDOOR_IP, 50000, 18022, &wire_704_payload());
        assert!(
            wait_until(Duration::from_secs(2), || calls.load(Ordering::SeqCst) >= 1),
            "在飞 unlock 应已进 worker（卡 gate）"
        );
        // 此刻拨 off——不影响已在飞的这次。
        auto_handle.store_auto_unlock(false);
        // 放行 gate → 在飞 unlock 跑完。
        gate.wait();

        shutdown.store(true, Ordering::SeqCst);
        consumer.join().unwrap();
        daemon::submit_job(&job_tx, Job::Shutdown).ok();
        worker.join().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1, "在飞那次跑完");
    }

    // ---- 5.4 双触发消除（daemon 单边）e2e ----------------------------------

    /// 无 HACS `/unlock` 时 ring 单次自开锁（daemon 侧不引入第二触发源）。**不**声称端到端。
    /// 锚 Go `main_selfunlock_test.go`。
    #[test]
    fn t5_4_single_self_unlock_without_hacs_callback() {
        let cfg = e2e_cfg();
        let shutdown = never();
        let (wire, wire_calls) = RecordingWire::new(AttemptOutcome::Ok);
        let (job_tx, worker) = spawn_worker(wire, None, shutdown.clone());
        let automation = AutomationState::new(true, false);
        let deps = make_deps(&cfg, job_tx.clone(), shutdown.clone(), automation);
        let listener = Arc::new(Listener::new(vec!["eth0".into()], None));
        let consumer = spawn_consumer_thread(&listener, deps);

        // 单次 ring（无任何模拟 HACS /unlock 投递）→ 恰一次自开锁。
        listener.dispatch_for_test(OUTDOOR_IP, INDOOR_IP, 50000, 18022, &wire_704_payload());
        assert!(
            wait_until(Duration::from_secs(2), || wire_calls.load(Ordering::SeqCst)
                >= 1),
            "ring 应触发自开锁"
        );
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(
            wire_calls.load(Ordering::SeqCst),
            1,
            "daemon 侧单次（无第二触发源）"
        );

        shutdown.store(true, Ordering::SeqCst);
        consumer.join().unwrap();
        daemon::submit_job(&job_tx, Job::Shutdown).ok();
        worker.join().unwrap();
    }

    // ---- 5.5 auto-hangup e2e ------------------------------------------------

    /// result=0 + auto_hangup on → 计时线程延迟投 Job::Hangup → worker 发 req=708
    /// preview-stop 帧逐字节对齐 Go `BuildStopFrame(outdoorBCD, monitorBCD)`。
    #[test]
    fn t5_5_auto_hangup_sends_stop_frame_byte_exact() {
        let old = set_hangup_delay_ms_for_test(30); // 缩到 30ms
        let cfg = e2e_cfg();
        let shutdown = never();
        let (wire, _calls) = RecordingWire::new(AttemptOutcome::Ok);
        let hangup_wire = Arc::new(RecordingHangupWire::default());
        let (job_tx, worker) = spawn_worker(
            wire,
            Some(Arc::clone(&hangup_wire) as Arc<dyn HangupWire>),
            shutdown.clone(),
        );
        let automation = AutomationState::new(true, true); // auto_unlock + auto_hangup on
        let deps = make_deps(&cfg, job_tx.clone(), shutdown.clone(), automation);
        let listener = Arc::new(Listener::new(vec!["eth0".into()], None));
        let consumer = spawn_consumer_thread(&listener, deps);

        listener.dispatch_for_test(OUTDOOR_IP, INDOOR_IP, 50000, 18022, &wire_704_payload());

        // 等 hangup wire 收到 req=708 帧（计时线程 30ms 后投 Job::Hangup → worker 发）。
        assert!(
            wait_until(Duration::from_secs(3), || !hangup_wire.frames().is_empty()),
            "auto_hangup on 应发 req=708"
        );
        let frames = hangup_wire.frames();
        assert_eq!(frames.len(), 1, "恰一帧 hangup");
        let (ip, port, frame) = &frames[0];
        assert_eq!(ip, "172.16.106.152", "发往外机目标 IP");
        assert_eq!(*port, 18022);

        // 逐字节对齐 build_stop_frame(outdoorBCD, monitorBCD)（仿 Go selfunlock_test.go:246）。
        let outdoor_bcd = codec::encode_bcd("06020000").unwrap();
        let monitor_bcd = codec::encode_bcd("06021103").unwrap();
        let want = crate::wire18022::build_stop_frame(outdoor_bcd, monitor_bcd);
        assert_eq!(
            frame, &want,
            "req=708 帧须逐字节 == build_stop_frame(outdoor,monitor)"
        );

        shutdown.store(true, Ordering::SeqCst);
        consumer.join().unwrap();
        daemon::submit_job(&job_tx, Job::Shutdown).ok();
        worker.join().unwrap();
        set_hangup_delay_ms_for_test(old);
    }

    /// shutdown 期间计时线程分片轮询观察到即放弃不投 Job::Hangup（不发帧）。
    #[test]
    fn t5_5_auto_hangup_abandoned_on_shutdown_during_delay() {
        let old = set_hangup_delay_ms_for_test(500); // 长延迟，留 shutdown 窗口
        let shutdown = never();
        let hangup_wire = Arc::new(RecordingHangupWire::default());
        let (job_tx, worker) = spawn_worker(
            RecordingWire::new(AttemptOutcome::Ok).0,
            Some(Arc::clone(&hangup_wire) as Arc<dyn HangupWire>),
            shutdown.clone(),
        );

        // 直接调 schedule_outdoor_hangup（计时线程），延迟期间置 shutdown。
        let outdoor_bcd = codec::encode_bcd("06020000").unwrap();
        let monitor_bcd = codec::encode_bcd("06021103").unwrap();
        schedule_outdoor_hangup(
            job_tx.clone(),
            Arc::clone(&shutdown),
            outdoor_bcd,
            monitor_bcd,
            "172.16.106.152".into(),
            18022,
        );
        // 延迟 500ms 期间立刻置 shutdown（计时线程下一分片观察到即放弃）。
        std::thread::sleep(Duration::from_millis(20));
        shutdown.store(true, Ordering::SeqCst);
        // 等远超延迟（500ms + 余量）确认从未发帧。
        std::thread::sleep(Duration::from_millis(700));
        assert!(
            hangup_wire.frames().is_empty(),
            "shutdown 期间计时线程应放弃，不投 Job::Hangup（不发帧）"
        );

        daemon::submit_job(&job_tx, Job::Shutdown).ok();
        worker.join().unwrap();
        set_hangup_delay_ms_for_test(old);
    }

    /// hangup 延迟期间新 Unlock 不被 head-of-line 阻塞（延迟在 detached 计时线程，worker
    /// 收到的是即时 job）：起一个 hangup 计时（长延迟）+ 立刻投 Unlock，断言 Unlock 即时跑完。
    #[test]
    fn t5_5_hangup_delay_does_not_block_unlock() {
        let old = set_hangup_delay_ms_for_test(1_000); // 长延迟
        let shutdown = never();
        let (wire, wire_calls) = RecordingWire::new(AttemptOutcome::Ok);
        let hangup_wire = Arc::new(RecordingHangupWire::default());
        let (job_tx, worker) = spawn_worker(
            wire,
            Some(Arc::clone(&hangup_wire) as Arc<dyn HangupWire>),
            shutdown.clone(),
        );

        // 起 hangup 计时线程（1s 延迟，detached，不占 worker）。
        let outdoor_bcd = codec::encode_bcd("06020000").unwrap();
        let monitor_bcd = codec::encode_bcd("06021103").unwrap();
        schedule_outdoor_hangup(
            job_tx.clone(),
            Arc::clone(&shutdown),
            outdoor_bcd,
            monitor_bcd,
            "172.16.106.152".into(),
            18022,
        );

        // 立刻投一个 Unlock 并等 reply：应即时完成（不被 hangup 的 1s 延迟阻塞）。
        let (rtx, rrx) = mpsc::sync_channel::<UnlockOutcome>(1);
        daemon::submit_job(
            &job_tx,
            Job::Unlock(UnlockJob {
                caller_bcd: outdoor_bcd,
                callee_bcd: monitor_bcd,
                target_ip: "172.16.106.152".into(),
                target_port: 18022,
                reply: rtx,
                per_attempt_timeout: Some(SELF_UNLOCK_PROBE_TIMEOUT),
                retry_interval: SELF_UNLOCK_PROBE_INTERVAL,
            }),
        )
        .unwrap();
        let t0 = Instant::now();
        let outcome = rrx
            .recv_timeout(Duration::from_millis(500))
            .expect("Unlock 应即时完成不被 hangup 延迟阻塞（detached 计时线程）");
        assert_eq!(outcome.result, codec::result::OK);
        assert!(
            t0.elapsed() < Duration::from_millis(500),
            "Unlock 不应等满 hangup 的 1s 延迟"
        );
        assert_eq!(wire_calls.load(Ordering::SeqCst), 1);

        // 收尾：shutdown 让 hangup 计时线程放弃。
        shutdown.store(true, Ordering::SeqCst);
        daemon::submit_job(&job_tx, Job::Shutdown).ok();
        worker.join().unwrap();
        set_hangup_delay_ms_for_test(old);
    }

    // ---- 5.6 dst 收紧 e2e ---------------------------------------------------

    /// 非本机室内机 dst / 未配置 src 不触发（filter 收紧）。
    #[test]
    fn t5_6_dst_and_src_tightening_no_trigger() {
        let cfg = e2e_cfg();
        let shutdown = never();
        let (wire, wire_calls) = RecordingWire::new(AttemptOutcome::Ok);
        let (job_tx, worker) = spawn_worker(wire, None, shutdown.clone());
        let automation = AutomationState::new(true, false);
        let deps = make_deps(&cfg, job_tx.clone(), shutdown.clone(), automation);
        let listener = Arc::new(Listener::new(vec!["eth0".into()], None));
        let consumer = spawn_consumer_thread(&listener, deps);

        // (a) dst != 本机室内机（.92，PROMISC 看到的其他室内机呼叫）→ filter 挡，不触发。
        listener.dispatch_for_test(
            OUTDOOR_IP,
            [172, 16, 106, 92],
            50000,
            18022,
            &wire_704_payload(),
        );
        // (b) src 不在 cfg.Stations（.199 邻居/未配置外机）→ filter 挡，不触发。
        listener.dispatch_for_test(
            [172, 16, 106, 199],
            INDOOR_IP,
            50000,
            18022,
            &wire_704_payload(),
        );
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(
            wire_calls.load(Ordering::SeqCst),
            0,
            "非本机 dst / 未配置 src 均不触发"
        );

        // 对照：命中（src∈Stations dst==室内机）确应触发。
        listener.dispatch_for_test(OUTDOOR_IP, INDOOR_IP, 50000, 18022, &wire_704_payload());
        assert!(
            wait_until(Duration::from_secs(2), || wire_calls.load(Ordering::SeqCst)
                >= 1),
            "命中帧应触发（对照）"
        );

        shutdown.store(true, Ordering::SeqCst);
        consumer.join().unwrap();
        daemon::submit_job(&job_tx, Job::Shutdown).ok();
        worker.join().unwrap();
    }

    // ---- 5.7 多 slave 并发 debounce e2e ------------------------------------

    /// ≥2 std::thread 并发调 `dispatch_for_test` 共享同一 Listener → 同一外机 ring 仅一个
    /// Job::Unlock（debounce 在单消费者线程顺序判定，无 TOCTOU 双触发）。
    #[test]
    fn t5_7_concurrent_slaves_debounce_single_unlock() {
        let old = set_debounce_margin_ms_for_test(5_000);
        let cfg = e2e_cfg();
        let shutdown = never();
        let (wire, wire_calls) = RecordingWire::new(AttemptOutcome::Ok);
        let (job_tx, worker) = spawn_worker(wire, None, shutdown.clone());
        let automation = AutomationState::new(true, false);
        let deps = make_deps(&cfg, job_tx.clone(), shutdown.clone(), automation);
        // 2 slave（自动启 dedup）但用唯一 src_port 并发投不同帧绕 dedup——TOCTOU 窗口靠
        // 单消费者线程顺序 debounce 关闭（非 dedup）。这里用同一外机 IP、不同 src_port 制造
        // 「两个不同 ring 几乎同时」（dedup key 含 src_port → 不被 dedup 挡，debounce 才是关键）。
        let listener = Arc::new(Listener::new(vec!["eth0".into(), "eth1".into()], None));
        let consumer = spawn_consumer_thread(&listener, deps);

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let l1 = Arc::clone(&listener);
        let b1 = Arc::clone(&barrier);
        let t1 = std::thread::spawn(move || {
            b1.wait();
            l1.dispatch_for_test(OUTDOOR_IP, INDOOR_IP, 50001, 18022, &wire_704_payload());
        });
        let l2 = Arc::clone(&listener);
        let b2 = Arc::clone(&barrier);
        let t2 = std::thread::spawn(move || {
            b2.wait();
            l2.dispatch_for_test(OUTDOOR_IP, INDOOR_IP, 50002, 18022, &wire_704_payload());
        });
        t1.join().unwrap();
        t2.join().unwrap();

        assert!(
            wait_until(Duration::from_secs(2), || wire_calls.load(Ordering::SeqCst)
                >= 1),
            "并发 ring 应至少触发一次"
        );
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            wire_calls.load(Ordering::SeqCst),
            1,
            "同一外机并发 ring 仅一个 Job::Unlock（单消费者线程 debounce 无 TOCTOU）"
        );

        shutdown.store(true, Ordering::SeqCst);
        consumer.join().unwrap();
        daemon::submit_job(&job_tx, Job::Shutdown).ok();
        worker.join().unwrap();
        set_debounce_margin_ms_for_test(old);
    }

    // ---- 5.8 包装层 × 失败分支 e2e（push / no-push）-------------------------

    /// loopback HTTP capture server：accept 一个连接、读 body、回 200，记 body。bind 失败
    /// （沙箱）返 None。仿 daemon_e2e `start_capture_server`。
    #[allow(clippy::type_complexity)]
    fn start_capture_server() -> Option<(
        String,
        u16,
        Arc<Mutex<Option<Vec<u8>>>>,
        std::thread::JoinHandle<()>,
    )> {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(l) => l,
            Err(e) => {
                eprintln!("skip 5.8 push e2e: bind failed (sandbox?): {e}"); // TEST-ONLY
                return None;
            }
        };
        let addr = listener.local_addr().unwrap();
        let captured: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
        let cap2 = Arc::clone(&captured);
        let t = std::thread::spawn(move || {
            if let Ok((mut conn, _)) = listener.accept() {
                conn.set_read_timeout(Some(Duration::from_secs(3))).ok();
                let mut raw = Vec::new();
                let mut buf = [0u8; 1024];
                loop {
                    match conn.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            raw.extend_from_slice(&buf[..n]);
                            if let Some(hdr_end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                                let header =
                                    String::from_utf8_lossy(&raw[..hdr_end]).to_lowercase();
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
                let _ = conn.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
            }
        });
        Some((addr.ip().to_string(), addr.port(), captured, t))
    }

    /// cfg 指向 loopback capture server（push 真发）。
    fn cfg_push_to(host: &str, port: u16) -> Config {
        let mut cfg = e2e_cfg();
        cfg.hass = crate::config::Hass {
            ipaddr: host.to_string(),
            port: port as i64,
            api: "/api/webhook/doorbell".into(),
            token: "test-token".into(),
        };
        cfg
    }

    /// 成功（result=0）→ push event=unlock（from/to/result）；逐字段断言（锚 Go
    /// `TestExecuteUnlockFromRing_SuccessPushesUnlockEvent`）。
    #[test]
    fn t5_8_success_pushes_unlock_event() {
        let Some((host, port, captured, srv_thread)) = start_capture_server() else {
            return; // 沙箱跳过
        };
        let cfg = cfg_push_to(&host, port);
        let shutdown = never();
        let (wire, _calls) = RecordingWire::new(AttemptOutcome::Ok);
        let (job_tx, worker) = spawn_worker(wire, None, shutdown.clone());
        let automation = AutomationState::new(true, false);
        let deps = make_deps(&cfg, job_tx.clone(), shutdown.clone(), automation);
        let listener = Arc::new(Listener::new(vec!["eth0".into()], None));
        let consumer = spawn_consumer_thread(&listener, deps);

        listener.dispatch_for_test(OUTDOOR_IP, INDOOR_IP, 50000, 18022, &wire_704_payload());
        srv_thread.join().ok();

        let body = captured
            .lock()
            .unwrap()
            .clone()
            .expect("成功应 push event=unlock");
        let s = String::from_utf8_lossy(&body);
        assert!(s.contains("\"event\":\"unlock\""), "event=unlock: {s}");
        assert!(
            s.contains(&format!("\"from\":\"{OUTDOOR_URI}\"")),
            "from: {s}"
        );
        assert!(s.contains(&format!("\"to\":\"{INDOOR_URI}\"")), "to: {s}");
        assert!(s.contains("\"result\":\"0\""), "result: {s}");

        shutdown.store(true, Ordering::SeqCst);
        consumer.join().unwrap();
        daemon::submit_job(&job_tx, Job::Shutdown).ok();
        worker.join().unwrap();
    }

    /// 非 OK（wire-failure → ERR+Some(Other)）→ 不 push（锚 Go
    /// `TestExecuteUnlockFromRing_FailureNoPush`）。
    #[test]
    fn t5_8_failure_does_not_push() {
        let Some((host, port, captured, srv_thread)) = start_capture_server() else {
            return;
        };
        let cfg = cfg_push_to(&host, port);
        let shutdown = never();
        // wire 始终 Other（不可重试）→ unlock result=ERR，非 OK。
        let (wire, _calls) = RecordingWire::new(AttemptOutcome::WireErr {
            stage: crate::unlock::UnlockStage::UnlockA,
            err: WireKind::Other,
        });
        let (job_tx, worker) = spawn_worker(wire, None, shutdown.clone());
        let automation = AutomationState::new(true, false);
        let deps = make_deps(&cfg, job_tx.clone(), shutdown.clone(), automation);
        let listener = Arc::new(Listener::new(vec!["eth0".into()], None));
        let consumer = spawn_consumer_thread(&listener, deps);

        listener.dispatch_for_test(OUTDOOR_IP, INDOOR_IP, 50000, 18022, &wire_704_payload());
        // 等消费者处理完该帧（自开锁失败 → 不 push）。给足时间。
        std::thread::sleep(Duration::from_millis(300));
        // capture server 永不收 push → 自 connect 唤醒它退出（避免悬挂线程）。
        if let Ok(c) = TcpStream::connect((host.as_str(), port)) {
            let _ = c.shutdown(std::net::Shutdown::Both);
        }
        srv_thread.join().ok();
        assert!(
            captured.lock().unwrap().is_none(),
            "非 OK 结果 MUST 不 push event=unlock"
        );

        shutdown.store(true, Ordering::SeqCst);
        consumer.join().unwrap();
        daemon::submit_job(&job_tx, Job::Shutdown).ok();
        worker.join().unwrap();
    }

    // ---- 5.9 per-ring 失败隔离 e2e -----------------------------------------

    /// 单 ring 解析失败（注入 malformed outdoor URI）→ 消费者经 Result 分支 log+跳过，
    /// 后续 ring 仍处理（防御式编码，非 panic-recover）。
    ///
    /// 经直接调 `run_consumer` 喂帧：第一个 ring 的 outdoor_by_ip 映射到 malformed URI
    /// （parse_outdoor_uri 失败），第二个 ring 映射到合法 URI。断言仅第二个触发 worker。
    #[test]
    fn t5_9_per_ring_parse_failure_isolated() {
        let old = set_debounce_margin_ms_for_test(5_000);
        let shutdown = never();
        let (wire, wire_calls) = RecordingWire::new(AttemptOutcome::Ok);
        let (job_tx, worker) = spawn_worker(wire, None, shutdown.clone());

        // 构造 deps：outdoor_by_ip 含一个 malformed URI（.152）+ 一个合法 URI（.153）。
        let mut cfg = e2e_cfg();
        cfg.stations = vec![
            crate::config::Station {
                // .152 → malformed（无 @/port）→ parse_outdoor_uri 失败。
                sip: "garbage-no-at".into(),
                rtsp_url: String::new(),
            },
            crate::config::Station {
                sip: "06020001@172.16.106.153:18022".into(),
                rtsp_url: String::new(),
            },
        ];
        // parse_stations_to_ip_map 用 parse_uri 建表；malformed 无法入表，故手工塞入
        // 一个映射到 malformed URI 的 IP，模拟「filter 过但 outdoor URI 解析失败」。
        let push_client = Arc::new(HaPushClient::new(cfg.clone(), None));
        let mut outdoor_by_ip = crate::orchestration::parse_stations_to_ip_map(&cfg);
        outdoor_by_ip.insert("172.16.106.152".into(), "garbage-no-at".into());
        let deps = SelfUnlockDeps::try_new(
            &cfg,
            outdoor_by_ip,
            job_tx.clone(),
            push_client,
            PushTracker::new(),
            shutdown.clone(),
            AutomationState::new(true, false),
            |_| {},
        )
        .expect("deps");

        // 第一个 ring：src=.152 → malformed URI → 解析失败 → log+跳过（不触发、不 panic）。
        let f1 = DetectedFrame {
            req: 704,
            body: vec![],
            src_ip: [172, 16, 106, 152],
            dst_ip: INDOOR_IP,
            src_port: 50000,
            dst_port: 18022,
        };
        // 第二个 ring：src=.153 → 合法 URI → 触发。
        let f2 = DetectedFrame {
            req: 704,
            body: vec![],
            src_ip: [172, 16, 106, 153],
            dst_ip: INDOOR_IP,
            src_port: 50001,
            dst_port: 18022,
        };

        // 经 listener subscribe + run_consumer 喂两帧。
        let listener = Arc::new(Listener::new(vec!["eth0".into()], None));
        let sub = listener.subscribe(Some(deps.make_filter()));
        // 把两帧直接投进 sub channel 的 tx——经 dispatch_for_test 需 src∈outdoor_ips；
        // .152/.153 均在 outdoor_by_ip → filter 通过。构造对应 wire payload。
        let consumer = std::thread::spawn(move || run_consumer(sub, &deps));
        listener.dispatch_for_test(
            f1.src_ip,
            f1.dst_ip,
            f1.src_port,
            f1.dst_port,
            &wire_704_payload(),
        );
        std::thread::sleep(Duration::from_millis(50));
        listener.dispatch_for_test(
            f2.src_ip,
            f2.dst_ip,
            f2.src_port,
            f2.dst_port,
            &wire_704_payload(),
        );

        // 仅第二个 ring（合法 URI）触发 worker；第一个 malformed 被跳过、消费者未死。
        assert!(
            wait_until(Duration::from_secs(2), || wire_calls.load(Ordering::SeqCst)
                >= 1),
            "malformed ring 被跳过后，后续合法 ring 仍处理"
        );
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(
            wire_calls.load(Ordering::SeqCst),
            1,
            "仅合法 ring 触发（malformed 经 Result 分支跳过）"
        );

        shutdown.store(true, Ordering::SeqCst);
        consumer.join().unwrap();
        daemon::submit_job(&job_tx, Job::Shutdown).ok();
        worker.join().unwrap();
        set_debounce_margin_ms_for_test(old);
    }

    // ---- 5.10 shutdown 不死锁 e2e（关键）-----------------------------------

    /// 消费者 park 在 recv_timeout 等帧时触发 shutdown → 消费者在有限时间内 join 完成、
    /// 不死锁（验 recv_timeout 唤醒路径）。**关键**：裸 recv() 会死锁。
    #[test]
    fn t5_10_shutdown_does_not_deadlock_consumer() {
        let cfg = e2e_cfg();
        let shutdown = never();
        let (job_tx, _worker_rx) = mpsc::channel::<Job>();
        let deps = make_deps(
            &cfg,
            job_tx,
            shutdown.clone(),
            AutomationState::new(true, false),
        );

        // listener 持 tx（subscribe 在其内）；消费者 park 在 recv_timeout（无帧到达）。
        let listener = Arc::new(Listener::new(vec!["eth0".into()], None));
        let consumer = spawn_consumer_thread(&listener, deps);

        // 确保消费者已 park 在 recv_timeout（无帧）。
        std::thread::sleep(Duration::from_millis(50));
        // 触发 shutdown：消费者下一个 recv_timeout poll（CONSUMER_POLL=20ms）应观察到并 break。
        let t0 = Instant::now();
        shutdown.store(true, Ordering::SeqCst);

        // watchdog：join 必须在有限时间内返回（≤ poll + ε）。listener 仍存活持 tx（channel
        // 不 Disconnected），故唯一退出路径是 recv_timeout→shutdown 检查。
        let joined = Arc::new(AtomicBool::new(false));
        let j2 = Arc::clone(&joined);
        let jh = std::thread::spawn(move || {
            consumer.join().ok();
            j2.store(true, Ordering::SeqCst);
        });
        assert!(
            wait_until(Duration::from_secs(2), || joined.load(Ordering::SeqCst)),
            "shutdown 后消费者应在有限时间内 join（recv_timeout 唤醒，非裸 recv 死锁）"
        );
        let elapsed = t0.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "消费者 join 应快（≤poll+ε），实测 {elapsed:?}"
        );
        jh.join().unwrap();
        // listener 在此后才 drop（证明 channel 全程未 Disconnected，退出靠 shutdown flag）。
        drop(listener);
    }

    // ---- 5.11 Job::Hangup 晚于哨兵 e2e -------------------------------------

    /// 计时线程在 worker 已读 Job::Shutdown 后投 Job::Hangup → SendError 被容忍、daemon
    /// 不 panic/不延迟 shutdown（best-effort 丢弃）。
    #[test]
    fn t5_11_hangup_after_sentinel_tolerated() {
        let shutdown = never();
        let hangup_wire = Arc::new(RecordingHangupWire::default());
        let (job_tx, worker) = spawn_worker(
            RecordingWire::new(AttemptOutcome::Ok).0,
            Some(Arc::clone(&hangup_wire) as Arc<dyn HangupWire>),
            shutdown.clone(),
        );

        // 先停 worker（读 Job::Shutdown 哨兵 break + join）。
        daemon::submit_job(&job_tx, Job::Shutdown).unwrap();
        worker.join().unwrap();

        // 此刻 worker 已退、rx drop。计时线程到点投 Job::Hangup → submit_job 得 SendError。
        // schedule_outdoor_hangup 必须容忍（不 panic、不 unwrap）。用极短延迟立即投。
        let old = set_hangup_delay_ms_for_test(5);
        let outdoor_bcd = codec::encode_bcd("06020000").unwrap();
        let monitor_bcd = codec::encode_bcd("06021103").unwrap();
        schedule_outdoor_hangup(
            job_tx.clone(),
            Arc::clone(&shutdown),
            outdoor_bcd,
            monitor_bcd,
            "172.16.106.152".into(),
            18022,
        );
        // 等计时线程到点并尝试投递（SendError 被吞）。无 panic、无帧发出。
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            hangup_wire.frames().is_empty(),
            "worker 已退，Job::Hangup 投递得 SendError 被丢弃（不发帧）"
        );
        set_hangup_delay_ms_for_test(old);
    }
}
