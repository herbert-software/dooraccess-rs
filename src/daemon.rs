//! M1.5 并发骨架：Job 队列 + 单 wire-worker 线程 + 单 `Arc<AtomicBool>` shutdown +
//! detached push 排空（`port-rust-daemon-skeleton` G2）。
//!
//! ## 并发模型（design 决策 1/2/3/4/8）
//!
//! 保留 Phase 3 已 hAP 真机认证的 per-slave listener 线程（I/O 层零改动），把 Go 的
//! self-unlock/hangup goroutine + `wireMu` + `context` 树塌缩为：
//!   - 单 **wire-worker 线程**：跑 wire-bound 的 [`Job::Unlock`]（+ 未来 ② 的 `Hangup`）；
//!     `Job::Shutdown` 哨兵 → break。所有 18022 wire 出站收敛到此唯一执行流 = 结构性串行
//!     （决策 2：`wireMu` 删除——只有 worker 发 wire，无运行时锁）。
//!   - 单 `mpsc::Sender<Job>` 队列：HTTP `/unlock` handler / 未来 OnDetect 投 job。
//!   - 单 `Arc<AtomicBool>` shutdown：穿入 worker loop + listener recv 循环 +
//!     `execute_unlock` 的 `cancel` + detached push 线程（决策 4：**仅取消**；
//!     unlock 总 cap 超时是另一机制 [`crate::unlock::InstantDeadline`]）。
//!   - **push 保持 detached**（决策 3）：push 是 HTTP-to-HA 不发 wire，作 detached
//!     fire-and-forget 线程，`JoinHandle` 收进 [`PushTracker`]（`Arc<Mutex<Vec<JoinHandle>>>`），
//!     shutdown join-all 排空。**不**进 wire-worker 队列（否则 HA 离线的慢 push 会
//!     head-of-line 阻塞排队的 Unlock）。
//!
//! ## worker 生命周期契约（决策 8）
//!   - worker 退出 = 显式 `Job::Shutdown` 哨兵 + break（不依赖「全 clone 释放→Disconnected」）。
//!   - worker panic → 其持有的未回灌 reply `Sender` 随线程 teardown drop → 阻塞的 HTTP
//!     handler 收 `Disconnected` 而非永等。
//!   - 投 job 路径 MUST 容忍 `SendError`（worker 已退则返错，禁 unwrap）。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::ha_push::HaPushClient;
use crate::listen18022::Subscribable;
use crate::unlock::{
    self, Deadline, InstantDeadline, Sleeper, ThreadSleeper, UnlockOutcome, UnlockWire,
    UNLOCK_RETRY_INTERVAL, UNLOCK_TOTAL_CAP,
};

// ---------------------------------------------------------------------------
// Job enum（决策 3：只含 wire-bound + Shutdown 哨兵；无 Push 变体）
// ---------------------------------------------------------------------------

/// 一次手动 unlock 请求的参数 + reply 回灌缝（task 4.x 接线）。
///
/// `reply` 是一次性 `SyncSender<UnlockOutcome>`（worker 跑完经它回灌阻塞中的 HTTP
/// handler）。worker panic 时该 sender 随 `UnlockJob` drop → handler 收 `RecvError`
/// 不永等（决策 8 worker-death 解锁）。
pub struct UnlockJob {
    pub caller_bcd: [u8; 4],
    pub callee_bcd: [u8; 4],
    pub target_ip: String,
    pub target_port: u16,
    /// 一次性回灌 channel（`SyncSender(1)`）。
    pub reply: SyncSender<UnlockOutcome>,
}

/// wire-worker 队列里的工作项。
///
/// **决策 3**：只含 **wire-bound** 变体——`Unlock`（+ 未来 ② 的 `Hangup`，本骨架不实现）
/// + `Shutdown` 哨兵。**无 `Push` 变体**：push 走 detached 线程（见 [`spawn_push`]）。
pub enum Job {
    /// 手动 / （未来 ring 触发）开锁——worker 跑 [`unlock::execute_unlock`]。
    Unlock(UnlockJob),
    /// 优雅退出哨兵（决策 8）：worker 排空已入队 Unlock 后读到它 → break。
    Shutdown,
}

// ---------------------------------------------------------------------------
// PushTracker（决策 3：Arc<Mutex<Vec<JoinHandle>>> 排空 + 运行期回收）
// ---------------------------------------------------------------------------

/// detached push 线程的 `JoinHandle` 集合（std-native 的 Go `pushWG` 等价；**非** Condvar）。
///
/// graceful shutdown 时 [`PushTracker::join_all`] 排空全部 = best-effort 等所有在飞 push
/// 完成（各 honor shutdown flag、ha_push 5s 超时上限内退出）。
#[derive(Clone, Default)]
pub struct PushTracker {
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl PushTracker {
    pub fn new() -> Self {
        Self {
            handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 登记一个新 detached push 线程的 handle。
    ///
    /// **运行期回收**（RC-F6）：登记前先 `retain(|h| !h.is_finished())` 清已结束 handle，
    /// 使 Vec 大小 ≈ 并发 push 数（O(1)）而非累计 push 数——否则随每次门铃 push 单调增长
    /// = 内存泄漏，64MB 多日运行必爆。
    pub fn register(&self, handle: JoinHandle<()>) {
        let mut v = self.handles.lock().unwrap_or_else(|e| e.into_inner());
        v.retain(|h| !h.is_finished());
        v.push(handle);
    }

    /// join 全部残留 push 线程（shutdown 排空步骤 ⑤）。
    pub fn join_all(&self) {
        // 取出全部 handle（释放锁后 join，避免长持锁阻塞仍在 register 的路径——shutdown
        // 阶段已无新 push spawn，但保持不长持锁的纪律）。
        let drained: Vec<JoinHandle<()>> = {
            let mut v = self.handles.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *v)
        };
        for h in drained {
            let _ = h.join();
        }
    }

    /// 当前登记的 handle 数（含已结束未回收的；测试断言用）。
    pub fn len(&self) -> usize {
        self.handles.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// spawn_push（决策 3 / task 2.5）：detached HA push 线程 + handle 入 tracker
// ---------------------------------------------------------------------------

/// spawn 一个 detached fire-and-forget 线程发一次 HA push，handle 收进 `tracker`。
///
/// - **off worker**：push 是 HTTP-to-HA 不发 wire，不进 wire-worker 队列（决策 3）。
/// - **honor shutdown**：spawn 时若 `shutdown` 已置位则直接放弃（best-effort，不发）；
///   否则发一次 `client.push(event, fields)`（ha_push 自带 5s 超时上限）。
/// - 适用 push 种类（均骨架 IN）：startup `automation_state` / 门铃 `ring` / 手动 unlock
///   business-result。startup `diagnosis` 的 500ms 站稳延迟由 [`spawn_diagnosis_push`] 承载。
pub fn spawn_push(
    tracker: &PushTracker,
    client: Arc<HaPushClient>,
    shutdown: Arc<AtomicBool>,
    event: String,
    fields: Vec<(String, String)>,
) {
    let h = std::thread::Builder::new()
        .name(format!("push-{event}"))
        .spawn(move || {
            if shutdown.load(Ordering::SeqCst) {
                return; // 已 shutdown：放弃（best-effort）。
            }
            let map: std::collections::BTreeMap<String, String> = fields.into_iter().collect();
            let _ = client.push(&event, &map);
        });
    if let Ok(h) = h {
        tracker.register(h);
    }
}

/// startup diagnosis push：detached 线程内先 sleep ~500ms 站稳再 push；延迟期 honor
/// shutdown flag —— 尚未发出则放弃（决策 3 / task 7.3）。
///
/// 500ms 站稳延迟落在它自己的 detached 线程里（**非** worker 内 sleep），与其他 push 线程
/// 同 join 集。延迟用短轮询 + flag 检查中断（不引 Condvar）。
pub fn spawn_diagnosis_push(
    tracker: &PushTracker,
    client: Arc<HaPushClient>,
    shutdown: Arc<AtomicBool>,
    settle: Duration,
) {
    let h = std::thread::Builder::new()
        .name("push-diagnosis".into())
        .spawn(move || {
            // 500ms 站稳：每 20ms 轮询 shutdown，置位即放弃（尚未发出）。
            let deadline = std::time::Instant::now() + settle;
            loop {
                if shutdown.load(Ordering::SeqCst) {
                    return; // shutdown 时未发出 → 放弃（best-effort 非安全不变量）。
                }
                let now = std::time::Instant::now();
                if now >= deadline {
                    break;
                }
                std::thread::sleep(
                    deadline
                        .saturating_duration_since(now)
                        .min(Duration::from_millis(20)),
                );
            }
            if shutdown.load(Ordering::SeqCst) {
                return;
            }
            let _ = client.push_diagnosis();
        });
    if let Ok(h) = h {
        tracker.register(h);
    }
}

// ---------------------------------------------------------------------------
// wire-worker 线程（task 2.2 / 2.3 / 2.4）
// ---------------------------------------------------------------------------

/// wire-worker 依赖：执行 unlock 所需的注入缝（决策 2/4/5）。
///
/// - `wire`：单次握手注入（生产 `WireSenderAdapter` / 测试 mock）。
/// - `listener`：bye 早停订阅源（`None` 时不订阅，nil-channel select 永不命中）。
/// - `post_wire_mu`：Phase 3 `execute_unlock` 签名占位锁（决策 2：单 worker 下**永不竞争**，
///   保留签名以防未来加第二 worker）。
/// - `sleeper`：退避 sleep 注入（生产 [`ThreadSleeper`]）。
pub struct WorkerDeps {
    pub wire: Arc<dyn UnlockWire>,
    pub listener: Option<Arc<dyn Subscribable>>,
    pub post_wire_mu: Mutex<()>,
    pub sleeper: Box<dyn Sleeper>,
}

impl WorkerDeps {
    /// 生产 worker 依赖（真 sleeper）。
    pub fn new(wire: Arc<dyn UnlockWire>, listener: Option<Arc<dyn Subscribable>>) -> Self {
        Self {
            wire,
            listener,
            post_wire_mu: Mutex::new(()),
            sleeper: Box::new(ThreadSleeper),
        }
    }
}

/// wire-worker 主循环（决策 8）：`for job in rx { match job { Unlock => exec, Shutdown => break } }`。
///
/// - **取消**：`shutdown` AtomicBool 穿入 `execute_unlock` 的 `cancel`——在飞 unlock 在下一
///   检查点早退（决策 4）。
/// - **超时**：每个 unlock job 构造独立 [`InstantDeadline::new`]`(总cap)` 注入（决策 4：超时
///   是独立机制，不能只穿 AtomicBool；`execute_unlock` 同接 `&dyn Deadline` + `cancel`）。
/// - **reply 回灌**：worker 跑完（含 shutdown 取消早退）MUST 向 `job.reply` 回灌
///   `UnlockOutcome` 解除阻塞 handler（决策 8）。worker panic 路径靠 `reply` sender drop
///   收 `Disconnected`，二者都不让 handler 永等。
pub fn run_worker(deps: WorkerDeps, rx: Receiver<Job>, shutdown: Arc<AtomicBool>) {
    // post_wire_mu 单 worker 下永不竞争（决策 2）；execute_unlock 签名仍需 &Mutex<()>。
    for job in rx {
        match job {
            Job::Shutdown => break,
            Job::Unlock(j) => {
                // 每个 unlock job 独立总 cap（决策 4：超时独立于取消）。
                let deadline = InstantDeadline::new(UNLOCK_TOTAL_CAP);
                let outcome = execute_unlock_job(&deps, &j, &deadline, &shutdown);
                // 回灌 reply（决策 8：取消/wire-failure 也回灌，否则 HTTP join 死锁）。
                // SyncSender(1) send：handler 仍在等 → Ok；handler 已走（极少）→ Err 忽略。
                let _ = j.reply.send(outcome);
            }
        }
    }
}

/// 跑一次 [`unlock::execute_unlock`]（free fn，返 [`UnlockOutcome`]，决策 5）。
///
/// 注入 `deps.post_wire_mu`（占位锁）/ `deadline`（总 cap）/ `shutdown`（cancel）/
/// `deps.sleeper`（退避）/ `deps.listener`（bye 早停）。HTTP 路径：`per_attempt_timeout=None`
/// + `retry_interval=1s`。
fn execute_unlock_job(
    deps: &WorkerDeps,
    j: &UnlockJob,
    deadline: &dyn Deadline,
    shutdown: &AtomicBool,
) -> UnlockOutcome {
    let listener = deps.listener.as_deref();
    unlock::execute_unlock(
        &deps.post_wire_mu,
        deps.wire.as_ref(),
        listener,
        j.caller_bcd,
        j.callee_bcd,
        &j.target_ip,
        j.target_port,
        shutdown,
        deps.sleeper.as_ref(),
        deadline,
        None,
        UNLOCK_RETRY_INTERVAL,
    )
}

// ---------------------------------------------------------------------------
// 投 job helper（决策 8：容忍 SendError，禁 unwrap）
// ---------------------------------------------------------------------------

/// 向 wire-worker 投一个 `Job`，容忍 worker 已退（`SendError` → `Err`，禁 unwrap panic）。
pub fn submit_job(tx: &Sender<Job>, job: Job) -> Result<(), &'static str> {
    tx.send(job).map_err(|_| "worker queue closed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unlock::{AttemptOutcome, Deadline, UnlockStage, UnlockWire, WireKind};
    use std::sync::mpsc;
    use std::time::Duration;

    /// 永不被取消的 cancel flag。
    fn never() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    /// mock wire：固定返一个 [`AttemptOutcome`]。
    struct FixedWire(AttemptOutcome);
    impl UnlockWire for FixedWire {
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
            self.0.clone()
        }
    }

    fn unlock_job(reply: SyncSender<UnlockOutcome>) -> UnlockJob {
        UnlockJob {
            caller_bcd: [0x06, 0x02, 0x00, 0x00],
            callee_bcd: [0x06, 0x02, 0x11, 0x03],
            target_ip: "172.16.106.201".into(),
            target_port: 18022,
            reply,
        }
    }

    // --- worker 跑 Unlock 经 reply 回灌 ---

    #[test]
    fn worker_runs_unlock_and_replies() {
        let (tx, rx) = mpsc::channel::<Job>();
        let shutdown = never();
        let deps = WorkerDeps::new(Arc::new(FixedWire(AttemptOutcome::Ok)), None);
        let sd = shutdown.clone();
        let worker = std::thread::spawn(move || run_worker(deps, rx, sd));

        let (rtx, rrx) = mpsc::sync_channel::<UnlockOutcome>(1);
        submit_job(&tx, Job::Unlock(unlock_job(rtx))).unwrap();
        let outcome = rrx.recv().expect("worker 应回灌 reply");
        assert_eq!(outcome.result, crate::codec::result::OK);

        // 哨兵停 worker。
        submit_job(&tx, Job::Shutdown).unwrap();
        worker.join().unwrap();
    }

    // --- Job::Shutdown 哨兵：worker break 即便仍持 sender clone（不依赖 clone 全释放） ---

    #[test]
    fn shutdown_sentinel_breaks_worker_with_live_clone() {
        let (tx, rx) = mpsc::channel::<Job>();
        let shutdown = never();
        let deps = WorkerDeps::new(Arc::new(FixedWire(AttemptOutcome::Ok)), None);
        let worker = std::thread::spawn(move || run_worker(deps, rx, shutdown));

        // 保留一个 sender clone 存活（模拟 HTTP/OnDetect 仍持 clone）。
        let _live_clone = tx.clone();
        submit_job(&tx, Job::Shutdown).unwrap();
        // worker 应靠哨兵 break，不卡在 recv（即便 clone 未释放）。
        worker.join().unwrap();
    }

    // --- submit_job 容忍 worker 已退（SendError → Err，不 panic） ---

    #[test]
    fn submit_after_worker_exit_returns_err_not_panic() {
        let (tx, rx) = mpsc::channel::<Job>();
        let shutdown = never();
        let deps = WorkerDeps::new(Arc::new(FixedWire(AttemptOutcome::Ok)), None);
        let worker = std::thread::spawn(move || run_worker(deps, rx, shutdown));
        submit_job(&tx, Job::Shutdown).unwrap();
        worker.join().unwrap();

        // worker 已退，rx drop → send 返 Err（不 unwrap panic）。
        let (rtx, _rrx) = mpsc::sync_channel::<UnlockOutcome>(1);
        assert!(submit_job(&tx, Job::Unlock(unlock_job(rtx))).is_err());
    }

    // --- worker panic：reply sender drop → handler 收 RecvError 不永等（决策 8 worker-death） ---

    #[test]
    fn worker_panic_unblocks_handler_via_reply_drop() {
        // wire panic → worker 线程 panic → 其持有的 UnlockJob（含 reply sender）teardown drop。
        struct PanicWire;
        impl UnlockWire for PanicWire {
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
                panic!("simulated worker panic");
            }
        }
        let (tx, rx) = mpsc::channel::<Job>();
        let shutdown = never();
        let deps = WorkerDeps::new(Arc::new(PanicWire), None);
        let worker = std::thread::spawn(move || run_worker(deps, rx, shutdown));

        let (rtx, rrx) = mpsc::sync_channel::<UnlockOutcome>(1);
        submit_job(&tx, Job::Unlock(unlock_job(rtx))).unwrap();
        // handler 等 reply：worker panic → UnlockJob drop → reply sender drop → RecvError。
        assert!(
            rrx.recv().is_err(),
            "worker panic 后 handler 应收 RecvError 不永等"
        );
        let _ = worker.join(); // panic 线程 join 返 Err，忽略。
    }

    // --- PushTracker 运行期回收（retain 已结束 handle，Vec 不单调增长） ---

    #[test]
    fn push_tracker_reclaims_finished_handles() {
        let tracker = PushTracker::new();
        // 连续登记 N 个已快速结束的线程；每次 register 前 retain → 长度有界。
        for _ in 0..20 {
            let h = std::thread::spawn(|| {});
            // 等其结束以触发 is_finished()。
            // （register 内 retain 会清掉之前已结束的。）
            std::thread::sleep(Duration::from_millis(2));
            tracker.register(h);
        }
        // join 残留确保不泄漏线程。
        tracker.join_all();
        assert!(tracker.is_empty(), "join_all 后应排空");
    }

    // --- spawn_push honor shutdown：已置位 shutdown 时放弃不发（线程仍可 join） ---

    #[test]
    fn spawn_push_skips_when_shutdown_set() {
        use crate::config::Config;
        let tracker = PushTracker::new();
        let shutdown = Arc::new(AtomicBool::new(true)); // 已 shutdown
        let client = Arc::new(HaPushClient::new(Config::default(), None));
        spawn_push(
            &tracker,
            client,
            shutdown,
            "ring".into(),
            vec![("from".into(), "x".into())],
        );
        tracker.join_all();
        // 线程已 spawn 并立即返回（push 被跳过，无 panic、无 hang）。
        assert!(tracker.is_empty());
    }

    // --- worker 取消（shutdown 置位）：在飞 unlock 早退并回灌 reply（不死锁） ---

    #[test]
    fn worker_cancel_replies_outcome() {
        // wire 始终 silent-FIN（会进退避）；shutdown 在第一次退避被置位 → cancel 早退。
        let (tx, rx) = mpsc::channel::<Job>();
        let shutdown = never();
        let deps = WorkerDeps::new(
            Arc::new(FixedWire(AttemptOutcome::WireErr {
                stage: UnlockStage::UnlockA,
                err: WireKind::SilentFin,
            })),
            None,
        );
        let sd = shutdown.clone();
        let worker = std::thread::spawn(move || run_worker(deps, rx, sd));

        // 投 unlock，随后立即置 shutdown（worker 在退避 sleep 中观察到 cancel 早退）。
        let (rtx, rrx) = mpsc::sync_channel::<UnlockOutcome>(1);
        submit_job(&tx, Job::Unlock(unlock_job(rtx))).unwrap();
        shutdown.store(true, Ordering::SeqCst);
        // 仍应在有限时间内收到回灌（取消 → -103），不永等。
        let outcome = rrx
            .recv_timeout(Duration::from_secs(3))
            .expect("取消应回灌 reply 不死锁");
        assert_eq!(outcome.result, crate::codec::result::NO_RING);

        submit_job(&tx, Job::Shutdown).unwrap();
        worker.join().unwrap();
    }
}
