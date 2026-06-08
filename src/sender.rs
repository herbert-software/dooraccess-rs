//! Phase2 最小 `Sender` trait + mock impl（`/unlock` 薄切，ExecuteUnlock 边界）。
//!
//! 语义对齐 Go `http8080.Server.ExecuteUnlock`——**不是**叶子 `wire18022.Sender.SendContext`。
//! retry / bye / 错误分类属 Phase 3，不在此 trait 暴露。

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

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
