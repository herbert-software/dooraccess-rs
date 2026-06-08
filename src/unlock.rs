//! `unlock`：executeUnlock 开锁核心（组 F，D-C 切点）。
//!
//! 逐行移植 Go `http8080.executeUnlock`（`handlers.go:301-372`）+ `runUnlockWithRetry`
//! （390-447）+ `tryUnlockOnce`（467-492）+ `classifyWireErr`（714）+
//! `filterReq708ForTarget`（508）的**核心逻辑**：
//!   - [`execute_unlock`] 串行核心：持 `Mutex`（postMu+wireMutex 等价）守 wire 出站
//!     （HTTP /unlock 与 self-unlock 不交错）；retry → 结果分类
//!   - [`run_unlock_with_retry`]：1s×8/cap10s 退避 + `per_attempt_timeout>0` fail-fast
//!     probe 分支（per-attempt deadline + attempt_expired 重探，锚 handlers.go:399-413）
//!     + unlock-B 不重试（handlers.go:423）
//!   - bye 早停：经 [`crate::listen18022::Subscribable`] 订阅 req=708；`None` 时
//!     nil-channel select 永不命中（可无 listener mock 测）；命中 → `terminated_by_bye=true`
//!   - [`classify_wire_err`]（wire 错误分类，与业务 body mismatch 分流）
//!
//! **范围红线**：`ExecuteUnlockFromRing` 入口包装（asyncPush + 测量）与 ring consumer
//! **不在本变更**（Phase 4）。本模块只做 `executeUnlock` 核心 + retry/probe/bye/串行/classify。
//!
//! ## 可测性设计（D-C：mock-e2e 不触真 socket / 真 sleep）
//!
//! Go `tryUnlockOnce` 直接调 `s.Sender.SendContext`（两次：710→711 / 518→519）。为让
//! mock-e2e 不触真网络，本模块把"单次三步握手"抽象成 [`UnlockWire`] trait（一次
//! `try_once` 返回 [`AttemptOutcome`]，对齐 Go `(codec.Result, unlockStage, error)`）。
//! 真实现 [`crate::sender::WireSenderAdapter`] 用 `wire_sender::Sender` 实现它；测试用
//! [`MockWire`] 注入 canned 序列。
//!
//! 退避 sleep 经 [`Sleeper`] trait 注入：生产 [`ThreadSleeper`]（真 sleep）；测试
//! [`NoopSleeper`]（不 sleep，记录调用次数）避免真等 8 秒。总 cap 用 [`Deadline`] 注入
//! （生产真 `Instant`；测试可控）。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::codec::result as result_code;
use crate::listen18022::{DetectedFrame, SubFilter, Subscribable};

// ---------------------------------------------------------------------------
// 退避 / cap 常量（锚 Go handlers.go:46-65）
// ---------------------------------------------------------------------------

/// 最大重试次数（不含首次；总 9 次尝试，锚 Go `unlockMaxRetries`）。
pub const UNLOCK_MAX_RETRIES: u32 = 8;

/// HTTP 路径退避间隔（锚 Go `unlockRetryInterval` = 1s）。
pub const UNLOCK_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// 总时长 cap（覆盖 9 次尝试，锚 Go `unlockTotalCap` = 10s）。
pub const UNLOCK_TOTAL_CAP: Duration = Duration::from_secs(10);

/// ring 路径 fail-fast per-attempt 超时（锚 Go `selfUnlockProbeTimeout` = 800ms）。
pub const SELF_UNLOCK_PROBE_TIMEOUT: Duration = Duration::from_millis(800);

/// ring 路径 fail-fast 退避间隔（锚 Go `selfUnlockProbeInterval` = 200ms）。
pub const SELF_UNLOCK_PROBE_INTERVAL: Duration = Duration::from_millis(200);

// ---------------------------------------------------------------------------
// 握手阶段 + 单次尝试结果（锚 Go unlockStage + tryUnlockOnce 返回三元组）
// ---------------------------------------------------------------------------

/// 三步握手阶段（锚 Go `unlockStage`：`stageUnlockA` / `stageUnlockB`）。
///
/// unlock-A = 710 send / 711 ack；unlock-B = 518 send / 519 ack。unlock-B wire 失败
/// **禁止**重试（710 已 ack，协议级不应重发整个握手，锚 handlers.go:423）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnlockStage {
    /// 710 send / 711 ack 阶段。
    UnlockA,
    /// 518 send / 519 ack 阶段。
    UnlockB,
}

/// 单次三步握手（[`UnlockWire::try_once`]）的结果，对齐 Go `tryUnlockOnce` 返回的
/// `(codec.Result, unlockStage, error)`：
///
///   - [`AttemptOutcome::Ok`]：710+711+518+519 完整成功（result=0）。
///   - [`AttemptOutcome::BusinessErr`]：外机响应了但 body 不符 spec（业务错 result=-1，
///     **不重试**）；`stage` 标识哪步 body 校验失败（锚 Go `(ResultErr, stage, nil)`）。
///   - [`AttemptOutcome::WireErr`]：wire 层失败（connect/send/recv），`stage` 标阶段、
///     `err` 是分类好的 [`WireKind`]（锚 Go `(ResultErr, stage, wireErr)`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// 完整成功。
    Ok,
    /// 外机响应了但 body 不符 spec（业务错，不重试）。
    BusinessErr { stage: UnlockStage },
    /// wire 层失败（可能可重试，由 [`WireKind::is_retryable`] 决定）。
    WireErr { stage: UnlockStage, err: WireKind },
}

/// wire 错误分类（锚 Go `wire18022` 的 `ErrSilentFIN` / `ErrTimeout` + 其它）。
///
/// 抽成独立 enum 让 mock-e2e 不依赖真 `wire_sender::WireError`（后者携 `io::Error`
/// 不便构造）；真实现 [`crate::sender::WireSenderAdapter`] 把 `WireError` 映射到此。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireKind {
    /// 外机收帧后 silent FIN（ring 状态机拒绝）。锚 Go `ErrSilentFIN`。**可重试**。
    SilentFin,
    /// connect/send/recv 任一阶段超时。锚 Go `ErrTimeout`。**可重试**。
    Timeout,
    /// 错误串含 "connection reset" / "broken pipe" 或底层 timeout。**可重试**。
    Retryable,
    /// 其它 wire 错误（如 connect refused / 解析失败）。**不可重试**。
    Other,
}

impl WireKind {
    /// 是否可重试（锚 Go `wire18022.IsRetryableError`）。
    ///
    /// SilentFin / Timeout / Retryable → true；Other → false。
    /// 业务 body mismatch 不走此分类（那条路径是 [`AttemptOutcome::BusinessErr`]，不进重试）。
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            WireKind::SilentFin | WireKind::Timeout | WireKind::Retryable
        )
    }
}

// ---------------------------------------------------------------------------
// classify_wire_err（锚 Go classifyWireErr，handlers.go:714）
// ---------------------------------------------------------------------------

/// 把 wire 错误分类翻译成 result code（锚 Go `classifyWireErr`）。
///
///   - [`WireKind::SilentFin`] → `result_code::NO_RING`（-103，外机 ring 状态机拒绝，daemon 健康）
///   - [`WireKind::Timeout`] → `result_code::TIMEOUT`（-5，下游故障）
///   - 其它 → `result_code::ERR`（-1）
///
/// 与 Go 一致：业务级 body mismatch **不**经此函数（上层直接判 [`result_code::ERR`]）。
pub fn classify_wire_err(kind: WireKind) -> i32 {
    match kind {
        WireKind::SilentFin => result_code::NO_RING,
        WireKind::Timeout => result_code::TIMEOUT,
        WireKind::Retryable | WireKind::Other => result_code::ERR,
    }
}

// ---------------------------------------------------------------------------
// 注入缝：UnlockWire（单次握手）/ Sleeper（退避）/ Deadline（总 cap）
// ---------------------------------------------------------------------------

/// 单次三步握手抽象（锚 Go `tryUnlockOnce`）。
///
/// `per_attempt_cap`：`Some(d)` 时（ring fail-fast probe）每步上限为 `d`（到点切断重探）；
/// `None`（HTTP 路径）每步用 sender 自身的 5s 默认。`deadline`：总 cap（锚 Go `tryUnlockOnce`
/// 收到的 `ctx`=attemptCtx=HTTP 路径 tctx 总 cap）——710/518 两步**共享**这同一个递减的剩余
/// cap，故每步实际超时 = `min(per_attempt_cap_or_default, deadline.remaining())`，两步合计 ≤
/// 剩余 cap（对齐 Go 两次 `SendContext(ctx,...)` 传同一 ctx）。`cancel`：父 ctx 取消
/// （SIGTERM/HACS 断开）时立即放弃。返回 [`AttemptOutcome`]。
pub trait UnlockWire: Send + Sync {
    /// 执行一次 710→711→518→519 握手。
    #[allow(clippy::too_many_arguments)]
    fn try_once(
        &self,
        caller_bcd: [u8; 4],
        callee_bcd: [u8; 4],
        target_ip: &str,
        target_port: u16,
        per_attempt_cap: Option<Duration>,
        deadline: &dyn Deadline,
        cancel: &AtomicBool,
    ) -> AttemptOutcome;
}

/// 退避 sleep 抽象（让测试不真等 8 秒）。
///
/// 返回值表示"sleep 是否被中断"——`true` 表示被 cancel 中断（对齐 Go select 命中
/// `<-ctx.Done()`），调用方应停止重试；`false` 表示睡满（继续重试）。
pub trait Sleeper: Send + Sync {
    /// 睡 `dur`，期间监视 `cancel` 与 `bye`：
    ///   - cancel 置位 → 返 [`SleepWake::Canceled`]
    ///   - bye channel 收到匹配 frame → 返 [`SleepWake::Bye`]
    ///   - 睡满 → 返 [`SleepWake::Elapsed`]
    fn sleep(
        &self,
        dur: Duration,
        cancel: &AtomicBool,
        bye: Option<&Receiver<DetectedFrame>>,
    ) -> SleepWake;
}

/// [`Sleeper::sleep`] 的唤醒原因（对齐 Go retry select 三分支）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SleepWake {
    /// 睡满退避间隔（继续下一次重试）。锚 Go `case <-time.After(retryInterval)`。
    Elapsed,
    /// 被 cancel 中断（父 ctx Done）。锚 Go `case <-ctx.Done()`。
    Canceled,
    /// bye channel 收到帧（早停）。锚 Go `case <-byeCh`。
    Bye,
}

/// 生产退避：真 `std::thread::sleep`，期间每 20ms 轮询 cancel / bye channel。
pub struct ThreadSleeper;

impl Sleeper for ThreadSleeper {
    fn sleep(
        &self,
        dur: Duration,
        cancel: &AtomicBool,
        bye: Option<&Receiver<DetectedFrame>>,
    ) -> SleepWake {
        let deadline = Instant::now() + dur;
        loop {
            if cancel.load(Ordering::SeqCst) {
                return SleepWake::Canceled;
            }
            if let Some(rx) = bye {
                // try_recv 非阻塞探一帧（bye 早停）。Disconnected 视为无 bye 继续睡。
                match rx.try_recv() {
                    Ok(_) => return SleepWake::Bye,
                    Err(std::sync::mpsc::TryRecvError::Empty) => {}
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {}
                }
            }
            let now = Instant::now();
            if now >= deadline {
                return SleepWake::Elapsed;
            }
            let remaining = deadline.saturating_duration_since(now);
            std::thread::sleep(remaining.min(Duration::from_millis(20)));
        }
    }
}

/// 总时长 cap 抽象（锚 Go `context.WithTimeout(ctx, unlockTotalCap)`）。
///
/// 生产 [`InstantDeadline`]（真 `Instant`）；测试可注入"永不过期"或固定剩余。
pub trait Deadline: Send + Sync {
    /// 总 cap 是否已到（true → 应停止重试，对齐 Go `ctx.Err() != nil`）。
    fn expired(&self) -> bool;

    /// 距总 cap 还剩多少时长（对齐 Go `attemptCtx = ctx`：每次 attempt 受**剩余总 cap**
    /// 约束）。`None` 表示无 cap（永不过期，如生产 self-unlock 无总 cap 注入或测试
    /// `NeverExpire`）；`Some(ZERO)` 表示已到点（等价 [`Deadline::expired`] 为 true）。
    fn remaining(&self) -> Option<Duration>;
}

/// 生产总 cap：以构造时刻 + [`UNLOCK_TOTAL_CAP`] 派生绝对 deadline。
pub struct InstantDeadline {
    deadline: Instant,
}

impl InstantDeadline {
    /// 以 `now + cap` 为绝对 deadline。
    pub fn new(cap: Duration) -> Self {
        Self {
            deadline: Instant::now() + cap,
        }
    }
}

impl Deadline for InstantDeadline {
    fn expired(&self) -> bool {
        Instant::now() >= self.deadline
    }

    fn remaining(&self) -> Option<Duration> {
        // 到点返回 Some(ZERO)（saturating_duration_since 已到点即为 0）。
        Some(self.deadline.saturating_duration_since(Instant::now()))
    }
}

// ---------------------------------------------------------------------------
// UnlockOutcome（锚 Go UnlockOutcome）
// ---------------------------------------------------------------------------

/// `execute_unlock` 的结果（锚 Go `UnlockOutcome`）。
///
/// `result` 是 i32 result code（0 / -1 / -103 / -5 等）；`retries` 重试次数（不含首次）；
/// `terminated_by_bye` true 表示被 bye 早停（result 必然 -103）；`wire_kind` 是最后一次
/// wire 失败的分类（成功 / 业务错时 `None`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnlockOutcome {
    /// 最终 result code。
    pub result: i32,
    /// 重试次数（不含首次）。
    pub retries: u32,
    /// 是否被 bye 早停。
    pub terminated_by_bye: bool,
    /// 最后一次 wire 失败分类（成功/业务错时 `None`）。
    pub wire_kind: Option<WireKind>,
}

// ---------------------------------------------------------------------------
// filter_req708_for_target（锚 Go filterReq708ForTarget，handlers.go:508）
// ---------------------------------------------------------------------------

/// 构造 bye 订阅 filter：匹配 req=708 且 src/dst 任一为 `target_ip` 的 frame
/// （锚 Go `filterReq708ForTarget`）。
///
/// `target_ip == None`（解析失败 fallback）时退化为匹配所有 req=708（保持 Go
/// `targetIP == nil` 旧行为）。匹配 src/dst 任一方向：两个方向的 bye 都标志 ring
/// session 已结束（design.md 待解 #1 默认决策）。
pub fn filter_req708_for_target(target_ip: Option<[u8; 4]>) -> SubFilter {
    match target_ip {
        None => Box::new(|d: &DetectedFrame| d.req == 708),
        Some(ip) => {
            Box::new(move |d: &DetectedFrame| d.req == 708 && (d.src_ip == ip || d.dst_ip == ip))
        }
    }
}

// ---------------------------------------------------------------------------
// run_unlock_with_retry（锚 Go runUnlockWithRetry，handlers.go:390-447）
// ---------------------------------------------------------------------------

/// 执行带退避重试的 unlock 三步握手（锚 Go `runUnlockWithRetry`）。
///
/// 返回 `(result_code, retries, terminated_by_bye, last_wire_kind)`。
///
/// 参数：
///   - `wire`：单次握手注入缝（生产真 sender / 测试 mock）。
///   - `bye`：bye 早停 channel；`None`（无 listener）时等价 Go nil-channel select 永不命中。
///   - `cancel`：父 ctx 取消信号（SIGTERM/HACS 断开）。
///   - `deadline`：总 cap（锚 Go `ctx.WithTimeout(unlockTotalCap)`）。
///   - `sleeper`：退避 sleep 注入缝。
///   - `per_attempt_timeout`：`Some` → ring fail-fast probe；`None` → HTTP 路径。
///   - `retry_interval`：attempt 间退避。
#[allow(clippy::too_many_arguments)]
pub fn run_unlock_with_retry(
    wire: &dyn UnlockWire,
    bye: Option<&Receiver<DetectedFrame>>,
    caller_bcd: [u8; 4],
    callee_bcd: [u8; 4],
    target_ip: &str,
    target_port: u16,
    cancel: &AtomicBool,
    deadline: &dyn Deadline,
    sleeper: &dyn Sleeper,
    per_attempt_timeout: Option<Duration>,
    retry_interval: Duration,
) -> (i32, u32, bool, Option<WireKind>) {
    // 跨 attempt 保留最后一次 wire 失败分类（对齐 Go `lastWireErr`）：cap 在退避后命中时
    // top-of-loop bail 须把它带出去（等价 Go retry `select` 的 `<-ctx.Done()` 分支返回
    // wireErr，handlers.go:436-437），让 execute_unlock 归一分支正确分类。首次 attempt 前
    // 就过期则仍为 None（等价 Go 循环未设 lastWireErr → 返回 nil，handlers.go:446）。
    let mut last_wire_kind: Option<WireKind> = None;

    // attempt 0 = 首次；attempt 1..=UNLOCK_MAX_RETRIES = 重试。
    for attempt in 0..=UNLOCK_MAX_RETRIES {
        // 总 cap 已到 → 停（对齐 Go ctx.Done() 在 attempt 前/select 中命中）。
        if deadline.expired() || cancel.load(Ordering::SeqCst) {
            // 中断路径：无新尝试，按上一次失败分类（首次就被取消则无 wire_kind）。
            return (result_code::NO_RING, attempt, false, last_wire_kind);
        }

        // 每次 attempt 的单帧超时对齐 Go 两层：sender 固定 5s SO_*TIMEO（`SendContext` 内
        // `conn.SetDeadline(now+5s)`）∧ 剩余总 cap（attemptCtx=tctx 在 cap 命中时 `conn.Close()`）。
        // 关键（修复 bugbot #1）：一次 attempt 内 710+518 两步**共享**递减的剩余总 cap——Go 把
        // **同一个 ctx** 传给两次 `SendContext`，第二步的 cap 预算 = 第一步消耗后的剩余。故不在此
        // 预算单个 effective 传下去（那样每步各起新 5s 预算 → 合计可达 2×effective、超剩余 cap），
        // 而是把 `per_attempt_cap`（本路径单帧上限：ring probe=800ms / HTTP=None→sender 默认 5s）
        // 与 `deadline`（总 cap，可重读递减剩余）一并传进 try_once，由其对 710/518 **各自**在调
        // send 前重算 `min(per_attempt_cap_or_default, deadline.remaining())`。
        let outcome = wire.try_once(
            caller_bcd,
            callee_bcd,
            target_ip,
            target_port,
            per_attempt_timeout,
            deadline,
            cancel,
        );

        match outcome {
            // 业务结果（OK / -1 unexpected response）—— 直接返回不重试（锚 handlers.go:414-417）。
            AttemptOutcome::Ok => return (result_code::OK, attempt, false, None),
            AttemptOutcome::BusinessErr { .. } => return (result_code::ERR, attempt, false, None),
            AttemptOutcome::WireErr { stage, err } => {
                // 记录本次 wire 失败分类（top-of-loop cap bail 据此带出，对齐 Go lastWireErr）。
                last_wire_kind = Some(err);
                // per-attempt deadline 命中（探到未就绪外机挂住连接）视为可重试——但总 cap
                // 已 Done 不算（锚 handlers.go:410）。这里以 WireKind::Timeout 在 ring 路径
                // 近似 attemptExpired（per_attempt_timeout 击中表现为 Timeout）。
                let attempt_expired = per_attempt_timeout.is_some()
                    && err == WireKind::Timeout
                    && !deadline.expired();

                // unlock-B 阶段 wire 失败 → **禁止**重试（710 已 ack，锚 handlers.go:423）。
                if stage == UnlockStage::UnlockB {
                    return (result_code::NO_RING, attempt, false, Some(err));
                }

                // 不可重试 且 非 attempt_expired → 直接返回（锚 handlers.go:427）。
                if !err.is_retryable() && !attempt_expired {
                    return (result_code::NO_RING, attempt, false, Some(err));
                }

                // 还有重试配额 → 退避；否则 quota 耗尽返回（锚 handlers.go:431-444）。
                if attempt < UNLOCK_MAX_RETRIES {
                    match sleeper.sleep(retry_interval, cancel, bye) {
                        SleepWake::Bye => {
                            // bye 早停（result 必然 -103，锚 handlers.go:434-435）。
                            return (result_code::NO_RING, attempt + 1, true, Some(err));
                        }
                        SleepWake::Canceled => {
                            return (result_code::NO_RING, attempt + 1, false, Some(err));
                        }
                        SleepWake::Elapsed => {
                            continue;
                        }
                    }
                }
                // quota 耗尽（锚 handlers.go:443-444）。
                return (result_code::NO_RING, attempt, false, Some(err));
            }
        }
    }
    // 不可达（循环必在内部 return）；保留对齐 Go 末尾 return。
    (result_code::NO_RING, UNLOCK_MAX_RETRIES, false, None)
}

// ---------------------------------------------------------------------------
// execute_unlock（锚 Go executeUnlock，handlers.go:301-372）
// ---------------------------------------------------------------------------

/// `execute_unlock` 串行核心（锚 Go `executeUnlock`）。
///
/// 持 `post_wire_mu`（postMu+wireMutex 等价）守 wire 出站全程——HTTP /unlock 与
/// self-unlock 经同一 [`Mutex`] 串行，不交错。订阅 req=708 bye 早停（`listener==None`
/// 时不订阅，等价 Go nil-channel select 永不命中）→ [`run_unlock_with_retry`] →
/// 结果分类（锚 handlers.go:345-364：cap 击中且非可重试 → 翻译成 Timeout 走 -5）。
///
/// 返回 [`UnlockOutcome`]（result code 已分类好）。
#[allow(clippy::too_many_arguments)]
pub fn execute_unlock(
    post_wire_mu: &Mutex<()>,
    wire: &dyn UnlockWire,
    listener: Option<&dyn Subscribable>,
    caller_bcd: [u8; 4],
    callee_bcd: [u8; 4],
    target_ip: &str,
    target_port: u16,
    cancel: &AtomicBool,
    sleeper: &dyn Sleeper,
    deadline: &dyn Deadline,
    per_attempt_timeout: Option<Duration>,
    retry_interval: Duration,
) -> UnlockOutcome {
    // postMu+wireMutex 串行：守 wire 出站全程（锚 handlers.go:309-314）。
    let _guard = post_wire_mu.lock().unwrap_or_else(|e| e.into_inner());

    // bye watcher：订阅 req=708（src/dst 任一为 target）。listener==None → 不订阅，
    // bye=None，run_unlock_with_retry 的 select 永不命中（锚 handlers.go:336-341 +
    // Listener==nil → byeCh nil channel）。
    let target_ip_bytes = parse_ipv4(target_ip);
    let sub = listener.map(|l| l.subscribe(Some(filter_req708_for_target(target_ip_bytes))));
    let bye_ref = sub.as_ref().map(|s| &s.ch);

    let (mut result, retries, terminated_by_bye, wire_kind) = run_unlock_with_retry(
        wire,
        bye_ref,
        caller_bcd,
        callee_bcd,
        target_ip,
        target_port,
        cancel,
        deadline,
        sleeper,
        per_attempt_timeout,
        retry_interval,
    );

    // 结果分类（锚 handlers.go:345-364）。归一后的 wire_kind 回写到返回值（修复 bugbot #3：
    // cap 击中翻 Timeout 后须同步 wire_kind，使 result 与 wire_kind 一致——对齐 Go
    // `UnlockOutcome.WireErr` 是已归一值的契约）。
    let mut wire_kind = wire_kind;
    if result == result_code::OK || result == result_code::ERR {
        // OK / 业务错：result 已是最终值。
    } else if terminated_by_bye {
        // bye 早停：result 必然 -103（NO_RING），保持。
        result = result_code::NO_RING;
    } else if let Some(kind) = wire_kind {
        // 重试耗尽路径：用真实 wire 分类。cap 击中（deadline 到）且非可重试 →
        // 翻译成 Timeout 让上层走 -5（锚 handlers.go:360-362）。
        //
        // `deadline.expired() && !kind.is_retryable()` 的可达性：
        //   - cap-bail（top-of-loop）/ quota 耗尽 / 普通退避路径**不会**带出非可重试错——
        //     非可重试错（WireKind::Other）在 run_unlock_with_retry 的 unlock-A 分支
        //     （`!err.is_retryable() && !attempt_expired`）立即 return，走不到 sleep+continue，
        //     故这些路径的 last_wire_kind 恒为可重试类。
        //   - **但 unlock-B 分支例外**：unlock-B wire 失败无论可否重试都直接返 `Some(err)`
        //     （710 已 ack 禁重试），若该 err 非可重试（如二次 connect refused→Other）且总 cap
        //     恰在此刻过期，则本翻转条件可达 → 翻 Timeout → -5，与 Go handlers.go:360-362
        //     （`tctx.Err()!=nil && !IsRetryableError(err)` 同条件）行为一致，故保留。
        let kind = if deadline.expired() && !kind.is_retryable() {
            WireKind::Timeout
        } else {
            kind
        };
        // 回写归一后的 kind（与 result 一致，对齐 Go WireErr 已归一契约）。
        wire_kind = Some(kind);
        result = classify_wire_err(kind);
    } else {
        // 无 wire_kind 的中断路径（cancel/cap 在首次尝试前命中）→ NO_RING（保持 Go
        // runUnlockWithRetry 返回 ResultNoRing）。
        result = result_code::NO_RING;
    }

    // 显式 cancel 订阅（per-invocation，不累积泄漏，锚 handlers.go:339 defer sub.Cancel()）。
    if let Some(s) = &sub {
        s.cancel();
    }

    UnlockOutcome {
        result,
        retries,
        terminated_by_bye,
        wire_kind,
    }
}

/// 解析点分十进制 IPv4 为 `[u8;4]`；失败返 `None`（锚 Go `net.ParseIP` fallback 到
/// 匹配所有 req=708）。
fn parse_ipv4(s: &str) -> Option<[u8; 4]> {
    s.parse::<std::net::Ipv4Addr>().ok().map(|ip| ip.octets())
}

// ===========================================================================
// tests（mock-e2e：mock UnlockWire + mock Subscribable + Noop sleeper；锚 8.3）
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::listen18022::Listener;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    // --- mock UnlockWire：注入 canned attempt 序列 ---

    /// 按调用次序返回 canned [`AttemptOutcome`]；超出末尾重复最后一个。
    struct MockWire {
        seq: Vec<AttemptOutcome>,
        calls: AtomicUsize,
    }

    impl MockWire {
        fn new(seq: Vec<AttemptOutcome>) -> Self {
            Self {
                seq,
                calls: AtomicUsize::new(0),
            }
        }
        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl UnlockWire for MockWire {
        #[allow(clippy::too_many_arguments)]
        fn try_once(
            &self,
            _caller_bcd: [u8; 4],
            _callee_bcd: [u8; 4],
            _target_ip: &str,
            _target_port: u16,
            _per_attempt_cap: Option<Duration>,
            _deadline: &dyn Deadline,
            _cancel: &AtomicBool,
        ) -> AttemptOutcome {
            let i = self.calls.fetch_add(1, Ordering::SeqCst);
            let idx = i.min(self.seq.len() - 1);
            self.seq[idx].clone()
        }
    }

    // --- Noop sleeper：不真 sleep，但仍遵守 cancel/bye 早停语义（用于退避计数） ---

    struct NoopSleeper {
        slept: AtomicUsize,
    }
    impl NoopSleeper {
        fn new() -> Self {
            Self {
                slept: AtomicUsize::new(0),
            }
        }
        fn count(&self) -> usize {
            self.slept.load(Ordering::SeqCst)
        }
    }
    impl Sleeper for NoopSleeper {
        fn sleep(
            &self,
            _dur: Duration,
            cancel: &AtomicBool,
            bye: Option<&Receiver<DetectedFrame>>,
        ) -> SleepWake {
            self.slept.fetch_add(1, Ordering::SeqCst);
            // 优先 cancel，再 bye，否则视为睡满（不真等）。
            if cancel.load(Ordering::SeqCst) {
                return SleepWake::Canceled;
            }
            if let Some(rx) = bye {
                if rx.try_recv().is_ok() {
                    return SleepWake::Bye;
                }
            }
            SleepWake::Elapsed
        }
    }

    // --- Deadline mock：永不过期 / 立即过期 ---

    struct NeverExpire;
    impl Deadline for NeverExpire {
        fn expired(&self) -> bool {
            false
        }
        fn remaining(&self) -> Option<Duration> {
            None
        }
    }
    struct AlwaysExpire;
    impl Deadline for AlwaysExpire {
        fn expired(&self) -> bool {
            true
        }
        fn remaining(&self) -> Option<Duration> {
            Some(Duration::ZERO)
        }
    }

    const CALLER: [u8; 4] = [0x06, 0x02, 0x00, 0x00];
    const CALLEE: [u8; 4] = [0x06, 0x02, 0x11, 0x03];
    const TARGET_IP: &str = "172.16.106.201";
    const TARGET_PORT: u16 = 18022;

    fn run<W: UnlockWire>(
        wire: &W,
        listener: Option<&dyn Subscribable>,
        cancel: &AtomicBool,
        sleeper: &dyn Sleeper,
        deadline: &dyn Deadline,
        per_attempt: Option<Duration>,
    ) -> UnlockOutcome {
        let mu = Mutex::new(());
        execute_unlock(
            &mu,
            wire,
            listener,
            CALLER,
            CALLEE,
            TARGET_IP,
            TARGET_PORT,
            cancel,
            sleeper,
            deadline,
            per_attempt,
            UNLOCK_RETRY_INTERVAL,
        )
    }

    // --- 场景：unlock success（首次即成功） ---

    #[test]
    fn unlock_success_first_try() {
        let wire = MockWire::new(vec![AttemptOutcome::Ok]);
        let cancel = AtomicBool::new(false);
        let sleeper = NoopSleeper::new();
        let out = run(&wire, None, &cancel, &sleeper, &NeverExpire, None);
        assert_eq!(out.result, result_code::OK);
        assert_eq!(out.retries, 0);
        assert!(!out.terminated_by_bye);
        assert_eq!(wire.call_count(), 1);
        assert_eq!(sleeper.count(), 0, "成功无退避");
    }

    // --- 场景：silent first N conns 重试成功 ---

    #[test]
    fn unlock_retry_then_success() {
        // 前 3 次 silent-FIN（unlock-A，可重试），第 4 次成功。
        let wire = MockWire::new(vec![
            AttemptOutcome::WireErr {
                stage: UnlockStage::UnlockA,
                err: WireKind::SilentFin,
            },
            AttemptOutcome::WireErr {
                stage: UnlockStage::UnlockA,
                err: WireKind::SilentFin,
            },
            AttemptOutcome::WireErr {
                stage: UnlockStage::UnlockA,
                err: WireKind::SilentFin,
            },
            AttemptOutcome::Ok,
        ]);
        let cancel = AtomicBool::new(false);
        let sleeper = NoopSleeper::new();
        let out = run(&wire, None, &cancel, &sleeper, &NeverExpire, None);
        assert_eq!(out.result, result_code::OK);
        assert_eq!(out.retries, 3, "3 次退避后第 4 次成功");
        assert_eq!(wire.call_count(), 4);
        assert_eq!(sleeper.count(), 3, "3 次退避 sleep");
    }

    // --- 场景：body mismatch（result=-1 不重试） ---

    #[test]
    fn unlock_body_mismatch_no_retry() {
        let wire = MockWire::new(vec![AttemptOutcome::BusinessErr {
            stage: UnlockStage::UnlockA,
        }]);
        let cancel = AtomicBool::new(false);
        let sleeper = NoopSleeper::new();
        let out = run(&wire, None, &cancel, &sleeper, &NeverExpire, None);
        assert_eq!(out.result, result_code::ERR, "body mismatch → -1");
        assert_eq!(out.retries, 0);
        assert_eq!(wire.call_count(), 1, "业务错不重试");
        assert_eq!(sleeper.count(), 0);
    }

    // --- 场景：retry 耗尽（全 silent-FIN）→ -103 ---

    #[test]
    fn unlock_exhausted_returns_no_ring() {
        let wire = MockWire::new(vec![AttemptOutcome::WireErr {
            stage: UnlockStage::UnlockA,
            err: WireKind::SilentFin,
        }]);
        let cancel = AtomicBool::new(false);
        let sleeper = NoopSleeper::new();
        let out = run(&wire, None, &cancel, &sleeper, &NeverExpire, None);
        assert_eq!(out.result, result_code::NO_RING, "silent-FIN 耗尽 → -103");
        // 首次 + 8 重试 = 9 次尝试。
        assert_eq!(wire.call_count(), 9);
        assert_eq!(sleeper.count(), 8, "8 次退避");
        assert_eq!(out.retries, 8);
    }

    // --- 场景：timeout 耗尽 → -5 ---

    #[test]
    fn unlock_timeout_exhausted_returns_timeout() {
        let wire = MockWire::new(vec![AttemptOutcome::WireErr {
            stage: UnlockStage::UnlockA,
            err: WireKind::Timeout,
        }]);
        let cancel = AtomicBool::new(false);
        let sleeper = NoopSleeper::new();
        let out = run(&wire, None, &cancel, &sleeper, &NeverExpire, None);
        assert_eq!(out.result, result_code::TIMEOUT, "timeout 耗尽 → -5");
    }

    // --- 场景：bye early-stop（注入 708 验 terminated_by_bye） ---

    #[test]
    fn unlock_bye_early_stop() {
        // 真 Listener 作 Subscribable；retry 期投递匹配 target 的 req=708。
        let listener = Listener::new(vec!["eth0".into()], None);
        // wire 始终 silent-FIN（会进退避）。
        let wire = MockWire::new(vec![AttemptOutcome::WireErr {
            stage: UnlockStage::UnlockA,
            err: WireKind::SilentFin,
        }]);
        let cancel = AtomicBool::new(false);

        // 用一个在第一次 sleep 时注入 708 的 sleeper。它持 &Listener 引用以 dispatch。
        struct ByeInjectingSleeper<'a> {
            listener: &'a Listener,
            injected: AtomicBool,
        }
        impl Sleeper for ByeInjectingSleeper<'_> {
            fn sleep(
                &self,
                _dur: Duration,
                cancel: &AtomicBool,
                bye: Option<&Receiver<DetectedFrame>>,
            ) -> SleepWake {
                if cancel.load(Ordering::SeqCst) {
                    return SleepWake::Canceled;
                }
                // 首次 sleep：dispatch 一个匹配 target 的 req=708 到订阅者。
                if !self.injected.swap(true, Ordering::SeqCst) {
                    // 构造 wire 帧 req=708；src_ip = target（172.16.106.201）。
                    let mut fb = Vec::new();
                    fb.extend_from_slice(b"req=708&query=");
                    fb.extend_from_slice(b"bye");
                    let mut frame = vec![0x07u8, 0xb8];
                    frame.extend_from_slice(&(fb.len() as u16).to_le_bytes());
                    frame.push(0x00);
                    frame.push(0x00);
                    frame.extend_from_slice(&fb);
                    self.listener.dispatch_for_test(
                        [172, 16, 106, 201],
                        [172, 16, 106, 202],
                        50000,
                        18022,
                        &frame,
                    );
                }
                // 投递后立即探 bye channel。
                if let Some(rx) = bye {
                    if rx.try_recv().is_ok() {
                        return SleepWake::Bye;
                    }
                }
                SleepWake::Elapsed
            }
        }

        let sleeper = ByeInjectingSleeper {
            listener: &listener,
            injected: AtomicBool::new(false),
        };
        let out = run(
            &wire,
            Some(&listener as &dyn Subscribable),
            &cancel,
            &sleeper,
            &NeverExpire,
            None,
        );
        assert!(out.terminated_by_bye, "应被 bye 早停");
        assert_eq!(
            out.result,
            result_code::NO_RING,
            "bye 早停 result 必然 -103"
        );
        // 早停在第一次退避：1 次尝试 + bye → retries=1。
        assert_eq!(out.retries, 1);
    }

    // --- 场景：bye 帧不匹配 target → 不早停（IP 维度过滤） ---

    #[test]
    fn unlock_bye_wrong_target_no_stop() {
        let listener = Listener::new(vec!["eth0".into()], None);
        let wire = MockWire::new(vec![AttemptOutcome::WireErr {
            stage: UnlockStage::UnlockA,
            err: WireKind::SilentFin,
        }]);
        let cancel = AtomicBool::new(false);

        // sleeper 投递一个 src/dst 都不是 target 的 req=708（邻居家 bye）。
        struct WrongByeSleeper<'a> {
            listener: &'a Listener,
            injected: AtomicBool,
        }
        impl Sleeper for WrongByeSleeper<'_> {
            fn sleep(
                &self,
                _dur: Duration,
                cancel: &AtomicBool,
                bye: Option<&Receiver<DetectedFrame>>,
            ) -> SleepWake {
                if cancel.load(Ordering::SeqCst) {
                    return SleepWake::Canceled;
                }
                if !self.injected.swap(true, Ordering::SeqCst) {
                    let mut fb = Vec::new();
                    fb.extend_from_slice(b"req=708&query=");
                    fb.extend_from_slice(b"bye");
                    let mut frame = vec![0x07u8, 0xb8];
                    frame.extend_from_slice(&(fb.len() as u16).to_le_bytes());
                    frame.push(0x00);
                    frame.push(0x00);
                    frame.extend_from_slice(&fb);
                    // src/dst = 9.9.9.x（非 target）→ filter 不通过。
                    self.listener.dispatch_for_test(
                        [9, 9, 9, 1],
                        [9, 9, 9, 2],
                        50000,
                        18022,
                        &frame,
                    );
                }
                if let Some(rx) = bye {
                    if rx.try_recv().is_ok() {
                        return SleepWake::Bye;
                    }
                }
                SleepWake::Elapsed
            }
        }

        let sleeper = WrongByeSleeper {
            listener: &listener,
            injected: AtomicBool::new(false),
        };
        let out = run(
            &wire,
            Some(&listener as &dyn Subscribable),
            &cancel,
            &sleeper,
            &NeverExpire,
            None,
        );
        assert!(!out.terminated_by_bye, "邻居家 bye 不应早停");
        assert_eq!(out.result, result_code::NO_RING);
    }

    // --- 场景：ctx cancel（退避期间 cancel → 停） ---

    #[test]
    fn unlock_ctx_cancel_stops() {
        let wire = MockWire::new(vec![AttemptOutcome::WireErr {
            stage: UnlockStage::UnlockA,
            err: WireKind::SilentFin,
        }]);
        let cancel = Arc::new(AtomicBool::new(false));

        // sleeper 第一次被调用即置 cancel（模拟 SIGTERM/HACS 断开）。
        struct CancelOnSleep {
            cancel: Arc<AtomicBool>,
        }
        impl Sleeper for CancelOnSleep {
            fn sleep(
                &self,
                _dur: Duration,
                cancel: &AtomicBool,
                _bye: Option<&Receiver<DetectedFrame>>,
            ) -> SleepWake {
                self.cancel.store(true, Ordering::SeqCst);
                if cancel.load(Ordering::SeqCst) {
                    return SleepWake::Canceled;
                }
                SleepWake::Elapsed
            }
        }
        let sleeper = CancelOnSleep {
            cancel: cancel.clone(),
        };
        let out = run(&wire, None, &cancel, &sleeper, &NeverExpire, None);
        assert!(!out.terminated_by_bye);
        assert_eq!(out.result, result_code::NO_RING, "cancel 中断 → -103");
        // 1 次尝试 + 退避中 cancel → retries=1。
        assert_eq!(out.retries, 1);
        assert_eq!(wire.call_count(), 1, "cancel 后不再尝试");
    }

    // --- 场景：cap 立即过期（首次尝试前）→ 不尝试 ---

    #[test]
    fn unlock_cap_expired_before_first_attempt() {
        let wire = MockWire::new(vec![AttemptOutcome::Ok]);
        let cancel = AtomicBool::new(false);
        let sleeper = NoopSleeper::new();
        let out = run(&wire, None, &cancel, &sleeper, &AlwaysExpire, None);
        assert_eq!(out.result, result_code::NO_RING);
        assert_eq!(wire.call_count(), 0, "cap 过期不尝试");
    }

    // --- 场景：probe per-attempt 超时重探（ring 路径 attempt_expired 视为可重试） ---

    #[test]
    fn unlock_probe_per_attempt_timeout_reprobe() {
        // ring fail-fast：per_attempt_timeout=Some。前 2 次 attempt 超 per-attempt
        // deadline（表现为 Timeout），第 3 次成功。验证 Timeout 在 ring 路径触发重探。
        let wire = MockWire::new(vec![
            AttemptOutcome::WireErr {
                stage: UnlockStage::UnlockA,
                err: WireKind::Timeout,
            },
            AttemptOutcome::WireErr {
                stage: UnlockStage::UnlockA,
                err: WireKind::Timeout,
            },
            AttemptOutcome::Ok,
        ]);
        let cancel = AtomicBool::new(false);
        let sleeper = NoopSleeper::new();
        let out = run(
            &wire,
            None,
            &cancel,
            &sleeper,
            &NeverExpire,
            Some(SELF_UNLOCK_PROBE_TIMEOUT),
        );
        assert_eq!(out.result, result_code::OK);
        assert_eq!(out.retries, 2, "2 次 per-attempt 超时重探后成功");
        assert_eq!(wire.call_count(), 3);
    }

    // --- 场景：unlock-B 不重试（510 已 ack，B 阶段 wire 失败直接返回） ---

    #[test]
    fn unlock_b_no_retry() {
        // 即使 unlock-B 是可重试的 SilentFin，也不应重试（stage==UnlockB）。
        let wire = MockWire::new(vec![AttemptOutcome::WireErr {
            stage: UnlockStage::UnlockB,
            err: WireKind::SilentFin,
        }]);
        let cancel = AtomicBool::new(false);
        let sleeper = NoopSleeper::new();
        let out = run(&wire, None, &cancel, &sleeper, &NeverExpire, None);
        assert_eq!(
            out.result,
            result_code::NO_RING,
            "unlock-B silent-FIN → -103"
        );
        assert_eq!(out.retries, 0, "unlock-B 不重试");
        assert_eq!(wire.call_count(), 1, "B 阶段失败仅尝试一次");
        assert_eq!(sleeper.count(), 0);
    }

    // --- 场景：unlock-B 业务错（519 body 不符）不重试 → -1 ---

    #[test]
    fn unlock_b_business_err() {
        let wire = MockWire::new(vec![AttemptOutcome::BusinessErr {
            stage: UnlockStage::UnlockB,
        }]);
        let cancel = AtomicBool::new(false);
        let sleeper = NoopSleeper::new();
        let out = run(&wire, None, &cancel, &sleeper, &NeverExpire, None);
        assert_eq!(out.result, result_code::ERR);
        assert_eq!(out.retries, 0);
    }

    // --- 场景：unlock-A 不可重试 wire 错（如 connect refused）→ 立即返回 -1 ---

    #[test]
    fn unlock_a_non_retryable_stops() {
        let wire = MockWire::new(vec![AttemptOutcome::WireErr {
            stage: UnlockStage::UnlockA,
            err: WireKind::Other,
        }]);
        let cancel = AtomicBool::new(false);
        let sleeper = NoopSleeper::new();
        let out = run(&wire, None, &cancel, &sleeper, &NeverExpire, None);
        assert_eq!(out.result, result_code::ERR, "不可重试 → -1");
        assert_eq!(wire.call_count(), 1, "不重试");
        assert_eq!(sleeper.count(), 0);
    }

    // --- 场景：总 cap 中途命中、最后一次是 timeout 类 → Timeout 分类(-5) 而非 -103 ---

    #[test]
    fn unlock_cap_mid_attempt_timeout_classified_as_timeout() {
        // HTTP 路径（per_attempt=None）：wire 始终 Timeout（可重试）。Deadline 在前几次
        // attempt 返 remaining>0（cap 未到，attempt 受剩余 cap 约束），随后 expired。
        // 验：最后一次是 timeout 类 → execute_unlock 归一分支把 result 分类成 -5
        // （对齐 Go：cap 中途命中 wire 返 Timeout(Some) → classifyWireErr → -5）。
        struct CapAfter {
            // remaining() 调用次数计数；超过阈值返回 ZERO（cap 到）。
            calls: AtomicUsize,
            // 前 `live` 次 remaining 返回非空剩余，之后返回 ZERO。
            live: usize,
        }
        impl Deadline for CapAfter {
            fn expired(&self) -> bool {
                self.calls.load(Ordering::SeqCst) >= self.live
            }
            fn remaining(&self) -> Option<Duration> {
                let n = self.calls.fetch_add(1, Ordering::SeqCst);
                if n < self.live {
                    Some(Duration::from_secs(2))
                } else {
                    Some(Duration::ZERO)
                }
            }
        }
        // wire 始终 Timeout（unlock-A，可重试）→ 退避循环跑到 cap 命中。
        let wire = MockWire::new(vec![AttemptOutcome::WireErr {
            stage: UnlockStage::UnlockA,
            err: WireKind::Timeout,
        }]);
        let cancel = AtomicBool::new(false);
        let sleeper = NoopSleeper::new();
        let deadline = CapAfter {
            calls: AtomicUsize::new(0),
            live: 2,
        };
        let out = run(&wire, None, &cancel, &sleeper, &deadline, None);
        // 最后一次 wire_kind=Timeout（可重试），但 cap 已到——execute_unlock 归一分支保留
        // Timeout 分类 → -5（而非误判 -103）。
        assert_eq!(
            out.result,
            result_code::TIMEOUT,
            "cap 中途命中且末次 timeout 类 → -5"
        );
        assert!(out.wire_kind.is_some(), "应保留末次 wire_kind 供归一分类");
    }

    // --- 场景（修复 bugbot #3）：cap 击中 + unlock-B 非可重试错 → wire_kind 回写归一为
    //     Timeout，与 result(-5) 一致 ---

    #[test]
    fn unlock_b_non_retryable_cap_hit_remaps_wire_kind() {
        // unlock-B wire 失败（Other，非可重试）直接返 Some(Other)（710 已 ack 禁重试）。
        // Deadline 首次 top-of-loop 检查未过期（让 attempt 跑），之后过期 → execute_unlock
        // 归一分支 `expired && !retryable` 命中：kind 翻成 Timeout → result=-5，wire_kind 须
        // 同步回写为 Timeout（不留陈旧 Other）。
        struct ExpireAfterFirstCheck {
            checks: AtomicUsize,
        }
        impl Deadline for ExpireAfterFirstCheck {
            fn expired(&self) -> bool {
                // 第 0 次（run_unlock_with_retry top-of-loop）未过期；之后（execute_unlock
                // 归一分支）过期。
                self.checks.fetch_add(1, Ordering::SeqCst) >= 1
            }
            fn remaining(&self) -> Option<Duration> {
                Some(Duration::from_secs(2))
            }
        }
        let wire = MockWire::new(vec![AttemptOutcome::WireErr {
            stage: UnlockStage::UnlockB,
            err: WireKind::Other,
        }]);
        let cancel = AtomicBool::new(false);
        let sleeper = NoopSleeper::new();
        let deadline = ExpireAfterFirstCheck {
            checks: AtomicUsize::new(0),
        };
        let out = run(&wire, None, &cancel, &sleeper, &deadline, None);
        assert_eq!(out.result, result_code::TIMEOUT, "cap 击中非可重试 → -5");
        assert_eq!(
            out.wire_kind,
            Some(WireKind::Timeout),
            "wire_kind 须回写为归一后的 Timeout，与 result 一致（不留陈旧 Other）"
        );
    }

    // --- classify_wire_err 直测 ---

    #[test]
    fn classify_wire_err_table() {
        assert_eq!(classify_wire_err(WireKind::SilentFin), result_code::NO_RING);
        assert_eq!(classify_wire_err(WireKind::Timeout), result_code::TIMEOUT);
        assert_eq!(classify_wire_err(WireKind::Retryable), result_code::ERR);
        assert_eq!(classify_wire_err(WireKind::Other), result_code::ERR);
    }

    // --- filter_req708_for_target 直测 ---

    #[test]
    fn filter_708_matches_target_either_direction() {
        let f = filter_req708_for_target(Some([172, 16, 106, 201]));
        let mk = |req: i64, src: [u8; 4], dst: [u8; 4]| DetectedFrame {
            req,
            body: vec![],
            src_ip: src,
            dst_ip: dst,
            src_port: 0,
            dst_port: 0,
        };
        // req=708 src=target → 通过。
        assert!(f(&mk(708, [172, 16, 106, 201], [9, 9, 9, 9])));
        // req=708 dst=target → 通过。
        assert!(f(&mk(708, [9, 9, 9, 9], [172, 16, 106, 201])));
        // req=708 都不是 target → 不通过。
        assert!(!f(&mk(708, [9, 9, 9, 9], [8, 8, 8, 8])));
        // req!=708 → 不通过。
        assert!(!f(&mk(704, [172, 16, 106, 201], [9, 9, 9, 9])));
    }

    #[test]
    fn filter_708_none_target_matches_all_708() {
        let f = filter_req708_for_target(None);
        let mk = |req: i64| DetectedFrame {
            req,
            body: vec![],
            src_ip: [9, 9, 9, 9],
            dst_ip: [8, 8, 8, 8],
            src_port: 0,
            dst_port: 0,
        };
        assert!(f(&mk(708)));
        assert!(!f(&mk(704)));
    }

    // --- listener==None：bye-watch nil channel select 永不命中（可无 listener 测） ---

    #[test]
    fn unlock_no_listener_no_bye() {
        // listener=None → 不订阅；retry 耗尽走正常 -103，不因 bye 早停。
        let wire = MockWire::new(vec![AttemptOutcome::WireErr {
            stage: UnlockStage::UnlockA,
            err: WireKind::SilentFin,
        }]);
        let cancel = AtomicBool::new(false);
        let sleeper = NoopSleeper::new();
        let out = run(&wire, None, &cancel, &sleeper, &NeverExpire, None);
        assert!(!out.terminated_by_bye, "无 listener 不会 bye 早停");
        assert_eq!(out.result, result_code::NO_RING);
    }

    // --- wire 出站串行：两个并发 execute_unlock 经同一 Mutex 不交错 ---

    #[test]
    fn wire_serial_mutex() {
        use std::sync::atomic::AtomicI32;
        // 一个共享 mutex；wire try_once 进入时 +1 并断言并发计数始终 ≤1。
        let mu = Arc::new(Mutex::new(()));
        let in_flight = Arc::new(AtomicI32::new(0));
        let max_seen = Arc::new(AtomicI32::new(0));

        struct SerialWire {
            in_flight: Arc<AtomicI32>,
            max_seen: Arc<AtomicI32>,
        }
        impl UnlockWire for SerialWire {
            #[allow(clippy::too_many_arguments)]
            fn try_once(
                &self,
                _c: [u8; 4],
                _ca: [u8; 4],
                _ip: &str,
                _p: u16,
                _t: Option<Duration>,
                _deadline: &dyn Deadline,
                _cancel: &AtomicBool,
            ) -> AttemptOutcome {
                let n = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                let mut prev = self.max_seen.load(Ordering::SeqCst);
                while n > prev {
                    match self.max_seen.compare_exchange(
                        prev,
                        n,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => break,
                        Err(p) => prev = p,
                    }
                }
                std::thread::sleep(Duration::from_millis(20));
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                AttemptOutcome::Ok
            }
        }

        let mut handles = vec![];
        for _ in 0..4 {
            let mu = mu.clone();
            let inf = in_flight.clone();
            let ms = max_seen.clone();
            handles.push(std::thread::spawn(move || {
                let wire = SerialWire {
                    in_flight: inf,
                    max_seen: ms,
                };
                let cancel = AtomicBool::new(false);
                let sleeper = NoopSleeper::new();
                execute_unlock(
                    &mu,
                    &wire,
                    None,
                    CALLER,
                    CALLEE,
                    TARGET_IP,
                    TARGET_PORT,
                    &cancel,
                    &sleeper,
                    &NeverExpire,
                    None,
                    UNLOCK_RETRY_INTERVAL,
                );
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            max_seen.load(Ordering::SeqCst),
            1,
            "wire 出站必须串行（并发 in-flight ≤ 1）"
        );
    }
}
