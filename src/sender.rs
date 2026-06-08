//! Phase2 最小 `Sender` trait + mock impl（`/unlock` 薄切，ExecuteUnlock 边界）。
//!
//! 语义对齐 Go `http8080.Server.ExecuteUnlock`——**不是**叶子 `wire18022.Sender.SendContext`。
//! retry / bye / 错误分类属 Phase 3，不在此 trait 暴露。

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// unlock 调用参数快照（单测断言用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnlockCall {
    pub caller_bcd: [u8; 4],
    pub callee_bcd: [u8; 4],
    pub target_ip: String,
    pub target_port: u16,
}

/// Phase 2 最小 sender 错误；Phase 3 真 wire impl 可扩展变体。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SenderError {
    Wire(String),
}

impl core::fmt::Display for SenderError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            SenderError::Wire(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for SenderError {}

/// Go `ExecuteUnlock` 方法边界：caller=外机(to BCD)，callee=室内机(from BCD)。
pub trait Sender: Send + Sync {
    /// `cancel` 预留 Phase 3 ctx 取消；Phase 2 mock 忽略。
    fn execute_unlock(
        &self,
        caller_bcd: [u8; 4],
        callee_bcd: [u8; 4],
        target_ip: &str,
        target_port: u16,
        cancel: &AtomicBool,
    ) -> Result<i32, SenderError>;
}

/// 可注入 canned 结果的 mock Sender；不触真 wire、不发网络。
pub struct MockSender {
    outcome: Result<i32, SenderError>,
    last_call: Arc<Mutex<Option<UnlockCall>>>,
}

impl MockSender {
    /// 成功路径：返回 `result=0`。
    pub fn success() -> Arc<Self> {
        Arc::new(Self {
            outcome: Ok(0),
            last_call: Arc::new(Mutex::new(None)),
        })
    }

    /// 可注入业务 result 码（仍 HTTP 200 + `{result:<code>}`）。
    pub fn with_result(result: i32) -> Arc<Self> {
        Arc::new(Self {
            outcome: Ok(result),
            last_call: Arc::new(Mutex::new(None)),
        })
    }

    /// 可注入 wire 失败变体；handler 薄切层映射为 `result=-1`（无 Phase 3 错误分类）。
    pub fn wire_err(msg: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            outcome: Err(SenderError::Wire(msg.into())),
            last_call: Arc::new(Mutex::new(None)),
        })
    }

    pub fn last_call(&self) -> Option<UnlockCall> {
        self.last_call.lock().ok().and_then(|g| g.clone())
    }
}

impl Sender for MockSender {
    fn execute_unlock(
        &self,
        caller_bcd: [u8; 4],
        callee_bcd: [u8; 4],
        target_ip: &str,
        target_port: u16,
        _cancel: &AtomicBool,
    ) -> Result<i32, SenderError> {
        if let Ok(mut guard) = self.last_call.lock() {
            *guard = Some(UnlockCall {
                caller_bcd,
                callee_bcd,
                target_ip: target_ip.to_string(),
                target_port,
            });
        }
        match &self.outcome {
            Ok(code) => Ok(*code),
            Err(e) => Err(e.clone()),
        }
    }
}

// ===========================================================================
// WireSenderAdapter（Phase 3 真 impl，组 F）：把 `trait Sender::execute_unlock`
// 接到 unlock 核心（`unlock::execute_unlock`）+ wire_sender::Sender 真发帧。
// ===========================================================================

use crate::listen18022::Subscribable;
use crate::unlock::{
    self, AttemptOutcome, Deadline, InstantDeadline, ThreadSleeper, UnlockStage, UnlockWire,
    WireKind, UNLOCK_RETRY_INTERVAL, UNLOCK_TOTAL_CAP,
};
use crate::wire18022;
use crate::wire_sender::{self, WireError};

/// `trait Sender` 的真实现（替换 Phase2 [`MockSender`]，组 F）。
///
/// 把 Phase2 `ExecuteUnlock` 边界接到 unlock 核心：HTTP `/unlock` 路径
/// （`per_attempt_timeout=None` / `retry_interval=1s` / 总 cap=10s）。持
/// `post_wire_mu`（postMu+wireMutex 等价）守 wire 出站串行（HTTP /unlock 与
/// self-unlock 不交错）。bye 早停经可选 `listener`（`trait Subscribable`）。
///
/// **范围红线**：本 adapter 只暴露 HTTP `/unlock` 入口（`ExecuteUnlock` 边界）。ring
/// 触发的 self-unlock（`per_attempt_timeout=800ms` fail-fast probe + asyncPush + 测量）
/// 是 Phase 4——其 per-attempt probe 机制已由 unlock 核心 [`unlock::run_unlock_with_retry`]
/// 实现，Phase 4 只需以 `Some(SELF_UNLOCK_PROBE_TIMEOUT)` 调它。
pub struct WireSenderAdapter {
    wire: wire_sender::Sender,
    post_wire_mu: Mutex<()>,
    listener: Option<Arc<dyn Subscribable>>,
}

impl WireSenderAdapter {
    /// 用底层 [`wire_sender::Sender`] 构造（无 bye listener）。
    pub fn new(wire: wire_sender::Sender) -> Self {
        Self {
            wire,
            post_wire_mu: Mutex::new(()),
            listener: None,
        }
    }

    /// 注入 bye 早停 listener（`trait Subscribable`，通常是 `listen18022::Listener`）。
    pub fn with_listener(mut self, listener: Arc<dyn Subscribable>) -> Self {
        self.listener = Some(listener);
        self
    }
}

/// 把 [`wire_sender::WireError`] 映射为 unlock 核心的 [`WireKind`] 分类
/// （锚 Go `wire18022.IsRetryableError` + `classifyWireErr` 的输入分类）。
fn map_wire_kind(err: &WireError) -> WireKind {
    match err {
        WireError::SilentFin => WireKind::SilentFin,
        WireError::Timeout { .. } => WireKind::Timeout,
        _ => {
            // 其它（Io / Canceled / Iface）：用 is_retryable_error 判 connection reset /
            // broken pipe / 底层 timeout → Retryable；否则 Other。
            if wire_sender::is_retryable_error(err) {
                WireKind::Retryable
            } else {
                WireKind::Other
            }
        }
    }
}

impl UnlockWire for WireSenderAdapter {
    /// 单次三步握手 710→711→518→519（锚 Go `tryUnlockOnce`）。
    ///
    /// `per_attempt_timeout`：`Some` 时（ring fail-fast probe）以该值覆盖 sender 每阶段
    /// timeout（让未就绪外机挂住的连接到点切断）；`None` 用 sender 配置的 5s。
    fn try_once(
        &self,
        caller_bcd: [u8; 4],
        callee_bcd: [u8; 4],
        target_ip: &str,
        target_port: u16,
        per_attempt_timeout: Option<Duration>,
        cancel: &AtomicBool,
    ) -> AttemptOutcome {
        // per-attempt timeout 覆盖：clone sender 改 timeout（短连接每帧独立 socket，
        // clone 廉价）。None → 用原 sender 5s。
        let sender = match per_attempt_timeout {
            Some(t) => {
                let mut s = self.wire.clone();
                s.timeout = Some(t);
                s
            }
            None => self.wire.clone(),
        };

        // 1. unlock-A（req=710），期望 req=711。
        let resp_a = match sender.send_context(
            cancel,
            target_ip,
            target_port,
            &wire18022::build_unlock_a_frame(caller_bcd, callee_bcd),
            true,
        ) {
            Ok(r) => r,
            Err(e) => {
                return AttemptOutcome::WireErr {
                    stage: UnlockStage::UnlockA,
                    err: map_wire_kind(&e),
                }
            }
        };
        let resp_a = resp_a.unwrap_or_default();
        if !wire18022::validate_response(&resp_a, 711) {
            return AttemptOutcome::BusinessErr {
                stage: UnlockStage::UnlockA,
            };
        }

        // 2. unlock-B（req=518 标准 + 校验和），期望 req=519。
        let resp_b = match sender.send_context(
            cancel,
            target_ip,
            target_port,
            &wire18022::build_unlock_b_frame(caller_bcd, callee_bcd),
            true,
        ) {
            Ok(r) => r,
            Err(e) => {
                return AttemptOutcome::WireErr {
                    stage: UnlockStage::UnlockB,
                    err: map_wire_kind(&e),
                }
            }
        };
        let resp_b = resp_b.unwrap_or_default();
        if !wire18022::validate_response(&resp_b, 519) {
            return AttemptOutcome::BusinessErr {
                stage: UnlockStage::UnlockB,
            };
        }

        AttemptOutcome::Ok
    }
}

impl Sender for WireSenderAdapter {
    /// HTTP `/unlock` 边界（锚 Go `ExecuteUnlock`）：调 unlock 核心走 retry/bye/分类，
    /// 返回最终 result code。`per_attempt_timeout=None`、`retry_interval=1s`、cap=10s。
    ///
    /// 与 Go `ExecuteUnlock` 一致：无论成功/业务错/wire 失败都返 `Ok(result_code)`
    /// （HTTP 200 + `{result:<code>}`），不返 `Err`——wire 失败已由 unlock 核心
    /// classify 成 -103/-5/-1。`SenderError` 仅保留给将来真正无法编码的入参错（不发生）。
    fn execute_unlock(
        &self,
        caller_bcd: [u8; 4],
        callee_bcd: [u8; 4],
        target_ip: &str,
        target_port: u16,
        cancel: &AtomicBool,
    ) -> Result<i32, SenderError> {
        let deadline = InstantDeadline::new(UNLOCK_TOTAL_CAP);
        let sleeper = ThreadSleeper;
        let listener = self.listener.as_deref();
        let out = unlock::execute_unlock(
            &self.post_wire_mu,
            self,
            listener,
            caller_bcd,
            callee_bcd,
            target_ip,
            target_port,
            cancel,
            &sleeper,
            &deadline as &dyn Deadline,
            None,
            UNLOCK_RETRY_INTERVAL,
        );
        Ok(out.result)
    }
}

// ===========================================================================
// tests（组 E，task 8.2）：map_wire_kind 私有，golden errclass 的 result_code 列
// 在此覆盖——验 WireError → WireKind → classify_wire_err(i32) 的端到端映射。
// （tests/golden_listeners.rs 只能验公开 is_retryable_error 的 retryable bool。）
// ===========================================================================

#[cfg(test)]
mod golden_errclass_tests {
    use super::map_wire_kind;
    use crate::codec::result as result_code;
    use crate::unlock::classify_wire_err;
    use crate::wire_sender::WireError;
    use std::fs;
    use std::io;
    use std::path::PathBuf;

    /// 与 tests/golden_listeners.rs::make_wire_err 对齐：golden errclass 名 → WireError。
    fn make_wire_err(kind: &str) -> WireError {
        match kind {
            "silent_fin" => WireError::SilentFin,
            "timeout" => WireError::Timeout { stage: "connect" },
            "connection_reset" => WireError::Io {
                stage: "write",
                source: io::Error::new(io::ErrorKind::ConnectionReset, "connection reset by peer"),
            },
            "broken_pipe" => WireError::Io {
                stage: "write",
                source: io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe"),
            },
            "other_connrefused" => WireError::Io {
                stage: "connect",
                source: io::Error::new(io::ErrorKind::ConnectionRefused, "connection refused"),
            },
            other => panic!("unknown errclass kind {:?}", other),
        }
    }

    /// 验 wire_sender.txt 的 errclass result_code 列：
    /// WireError → map_wire_kind → classify_wire_err 必等于 Go classifyWireErr 导出值
    /// （silent_fin→-103 / timeout→-5 / 其它→-1）。
    #[test]
    fn golden_errclass_result_code() {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/golden/wire_sender.txt");
        let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {:?}: {}", path, e));
        let mut n = 0;
        for raw in text.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let f: Vec<&str> = line.split('|').collect();
            if f[0] != "errclass" {
                continue;
            }
            // errclass|<kind>|<retryable>|<result_code>
            assert_eq!(f.len(), 4, "errclass line: {:?}", line);
            let err = make_wire_err(f[1]);
            let want: i32 = f[3]
                .parse()
                .unwrap_or_else(|_| panic!("bad result_code: {:?}", line));
            let got = classify_wire_err(map_wire_kind(&err));
            assert_eq!(
                got, want,
                "errclass [{}] result_code: got {} want {}",
                f[1], got, want
            );
            n += 1;
        }
        assert!(n >= 5, "too few errclass result_code vectors: {}", n);
        // 钉死语义常量（防 golden 与 codec 同时漂移不被察觉）。
        assert_eq!(result_code::NO_RING, -103);
        assert_eq!(result_code::TIMEOUT, -5);
        assert_eq!(result_code::ERR, -1);
    }
}
