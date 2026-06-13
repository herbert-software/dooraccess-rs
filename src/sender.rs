//! 最小 `Sender` trait + mock impl（`/unlock` 薄切，execute_unlock 边界）。
//!
//! 这是 `/unlock` HTTP 入口的方法边界——**不是**叶子 wire send（`wire_sender::Sender::send_context`）。
//! retry / bye / 错误分类由真实现承担，不在此 trait 暴露。

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

/// 最小 sender 错误；真 wire impl 可扩展变体。
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

/// `execute_unlock` 方法边界：caller=外机(to BCD)，callee=室内机(from BCD)。
pub trait Sender: Send + Sync {
    /// `cancel` 预留 ctx 取消；mock 忽略。
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

    /// 可注入 wire 失败变体；handler 薄切层映射为 `result=-1`（mock 路径无错误分类）。
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
// WireSenderAdapter（真 impl）：把 `trait Sender::execute_unlock`
// 接到 unlock 核心（`unlock::execute_unlock`）+ wire_sender::Sender 真发帧。
// ===========================================================================

use crate::listen18022::Subscribable;
use crate::unlock::{
    self, AttemptOutcome, Deadline, InstantDeadline, ThreadSleeper, UnlockStage, UnlockWire,
    WireKind, UNLOCK_RETRY_INTERVAL, UNLOCK_TOTAL_CAP,
};
use crate::wire18022;
use crate::wire_sender::{self, WireError};

/// `trait Sender` 的真实现（替换 [`MockSender`]）。
///
/// 把 `execute_unlock` 边界接到 unlock 核心：HTTP `/unlock` 路径
/// （`per_attempt_timeout=None` / `retry_interval=1s` / 总 cap=10s）。持
/// `post_wire_mu` 守 wire 出站串行（HTTP /unlock 与 self-unlock 不交错）。
/// bye 早停经可选 `listener`（`trait Subscribable`）。
///
/// **范围红线**：本 adapter 只暴露 HTTP `/unlock` 入口（`execute_unlock` 边界）。ring
/// 触发的 self-unlock（`per_attempt_timeout=800ms` fail-fast probe + async push + 测量）
/// 的 per-attempt probe 机制已由 unlock 核心 [`unlock::run_unlock_with_retry`]
/// 实现，self-unlock 路径只需以 `Some(SELF_UNLOCK_PROBE_TIMEOUT)` 调它。
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
/// （wire 错误分类 → 重试/超时/其它的输入分类）。
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
    /// 单次三步握手 710→711→518→519。
    ///
    /// `per_attempt_cap`：`Some` 时（ring fail-fast probe）每步上限为该值（让未就绪外机挂住的
    /// 连接到点切断）；`None` 用 sender 默认 5s（[`wire_sender::DEFAULT_TIMEOUT`]）。
    /// `deadline`：总 cap——710/518 两步**共享**这同一个递减的剩余 cap，故每步在 **send 前**重算
    /// `op_timeout = min(per_attempt_cap_or_default, deadline.remaining())`，使第二步预算 =
    /// `min(cap, 第一步消耗后剩余)`，两步合计 ≤ 剩余 cap、不超 2×。`deadline.remaining()` 为 None
    /// （无 cap）时每步仅用 per_attempt_cap_or_default，不施 cap 层。
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
    ) -> AttemptOutcome {
        // 每步 send 前重算 op_timeout = min(本路径单帧上限, 那一刻的剩余总 cap)。
        // 本路径单帧上限 = per_attempt_cap（ring probe）或 sender 默认 5s（HTTP 路径）。
        // 两步分别取此刻 deadline.remaining()——故 cap 预算在 710→518 间递减、共享。
        let cap = per_attempt_cap.unwrap_or(wire_sender::DEFAULT_TIMEOUT);
        let op_timeout = || match deadline.remaining() {
            Some(remaining) => cap.min(remaining),
            None => cap,
        };

        // 1. unlock-A（req=710），期望 req=711。
        let mut sender = self.wire.clone();
        sender.timeout = Some(op_timeout());
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
        // op_timeout() 此刻重算 → 拿第一步消耗后的剩余 cap（两步共享递减预算）。
        let mut sender = self.wire.clone();
        sender.timeout = Some(op_timeout());
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
    /// HTTP `/unlock` 边界：调 unlock 核心走 retry/bye/分类，
    /// 返回最终 result code。`per_attempt_timeout=None`、`retry_interval=1s`、cap=10s。
    ///
    /// 无论成功/业务错/wire 失败都返 `Ok(result_code)`
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
// tests：map_wire_kind 私有，golden errclass 的 result_code 列
// 在此覆盖——验 WireError → WireKind → classify_wire_err(i32) 的端到端映射。
// （tests/golden_listeners.rs 只能验公开 is_retryable_error 的 retryable bool。）
// ===========================================================================

// ===========================================================================
// tests：try_once 两步共享递减剩余 cap，合计不超 2×单步上限。
// ===========================================================================

#[cfg(test)]
mod shared_budget_tests {
    use super::*;
    use crate::unlock::{Deadline, UnlockStage, UnlockWire};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    /// 记录每次 `remaining()` 返回值的 fake Deadline：按调用次序吐出 `seq` 中的值
    /// （超出末尾重复末值）；`expired()` 在剩余为 ZERO 时为 true。
    struct ScriptedDeadline {
        seq: Vec<Option<Duration>>,
        calls: AtomicUsize,
    }
    impl Deadline for ScriptedDeadline {
        fn expired(&self) -> bool {
            // 与本测无关（run_unlock_with_retry 才调）；保守按当前应返值是否 ZERO。
            let i = self.calls.load(Ordering::SeqCst).min(self.seq.len() - 1);
            matches!(self.seq[i], Some(d) if d.is_zero())
        }
        fn remaining(&self) -> Option<Duration> {
            let i = self.calls.fetch_add(1, Ordering::SeqCst);
            self.seq[i.min(self.seq.len() - 1)]
        }
    }

    /// 构造一个合法响应帧：`07 b8` magic + LE 长度 + 00 00 reserved + `req=N&query=` + 10 字节 body。
    fn build_resp(req: &str, body10: [u8; 10]) -> Vec<u8> {
        let mut frame_body = Vec::new();
        frame_body.extend_from_slice(format!("req={req}&query=").as_bytes());
        frame_body.extend_from_slice(&body10);
        let mut frame = vec![0x07u8, 0xb8];
        frame.extend_from_slice(&(frame_body.len() as u16).to_le_bytes());
        frame.push(0x00);
        frame.push(0x00);
        frame.extend_from_slice(&frame_body);
        frame
    }

    /// 711 标准 body 模板（与 wire18022::RESP_711_BODY 一致，validate_response 严校）。
    fn build_711() -> Vec<u8> {
        build_resp(
            "711",
            [0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00],
        )
    }

    /// 第一步（710）正常回 711，但服务器先 sleep 消耗预算；第二步（518）连上后
    /// 永不响应。Deadline 在第二步 remaining 返回一个**比单步 cap 小**的剩余 →
    /// 第二步必须按那个更小的剩余超时（证明两步共享递减 cap，而非各起新 cap）。
    #[test]
    fn try_once_second_step_bounded_by_remaining_cap() {
        let conn_idx = Arc::new(AtomicUsize::new(0));
        let r711 = build_711();
        let conn_idx_c = conn_idx.clone();

        let ln = TcpListener::bind("127.0.0.1:0").expect("listen");
        let port = ln.local_addr().unwrap().port();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_c = stop.clone();
        let server = thread::spawn(move || {
            for conn in ln.incoming() {
                if stop_c.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(mut c) = conn else { return };
                let idx = conn_idx_c.fetch_add(1, Ordering::SeqCst);
                let r711 = r711.clone();
                thread::spawn(move || {
                    let _ = c.set_read_timeout(Some(Duration::from_secs(2)));
                    let mut buf = [0u8; 256];
                    let _ = c.read(&mut buf);
                    if idx == 0 {
                        // 第一步：回 711（让握手进入第二步）。
                        let _ = c.write_all(&r711);
                    } else {
                        // 第二步：永不响应，长 sleep 让客户端按其 read timeout 超时。
                        thread::sleep(Duration::from_secs(3));
                    }
                    drop(c);
                });
            }
        });

        // 单步 cap=5s（HTTP 默认）；但 Deadline 第二步剩余=400ms → 第二步 read 应在 ~400ms
        // 超时返回 Timeout，而非 5s。第一步剩余给足（2s）让 711 能回。
        let deadline = ScriptedDeadline {
            seq: vec![
                Some(Duration::from_secs(2)),     // 710 步剩余
                Some(Duration::from_millis(400)), // 518 步剩余（被收紧）
            ],
            calls: AtomicUsize::new(0),
        };
        let adapter = WireSenderAdapter::new(wire_sender::Sender::default());
        let cancel = AtomicBool::new(false);

        let start = Instant::now();
        let outcome = adapter.try_once(
            [0, 0, 0, 0],
            [0, 0, 0, 0],
            "127.0.0.1",
            port,
            None, // HTTP 路径：单步 cap=sender 默认 5s
            &deadline,
            &cancel,
        );
        let elapsed = start.elapsed();

        stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(("127.0.0.1", port));
        let _ = server.join();

        // 第二步应因收紧的 400ms 剩余 cap 超时（而非 5s 单步上限）。
        assert!(
            matches!(
                outcome,
                AttemptOutcome::WireErr {
                    stage: UnlockStage::UnlockB,
                    ..
                }
            ),
            "第二步应 wire 超时（UnlockB），got {outcome:?}"
        );
        // 合计耗时受递减剩余约束：远小于 2×5s，更小于单步 5s 上限的两倍。
        assert!(
            elapsed < Duration::from_secs(2),
            "两步共享递减 cap，合计应远小于 2×5s，实际 {elapsed:?}"
        );
    }

    /// remaining()==None（无 cap）时每步仅用 per_attempt_cap（不施 cap 层）——
    /// 验 None 路径不 panic 且能正常握手成功。
    #[test]
    fn try_once_no_cap_uses_per_attempt_only() {
        let r711 = build_711();
        let r519 = build_resp("519", [0u8; 10]);

        let conn_idx = Arc::new(AtomicUsize::new(0));
        let conn_idx_c = conn_idx.clone();
        let ln = TcpListener::bind("127.0.0.1:0").expect("listen");
        let port = ln.local_addr().unwrap().port();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_c = stop.clone();
        let server = thread::spawn(move || {
            for conn in ln.incoming() {
                if stop_c.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(mut c) = conn else { return };
                let idx = conn_idx_c.fetch_add(1, Ordering::SeqCst);
                let (r711, r519) = (r711.clone(), r519.clone());
                thread::spawn(move || {
                    let _ = c.set_read_timeout(Some(Duration::from_secs(2)));
                    let mut buf = [0u8; 256];
                    let _ = c.read(&mut buf);
                    let _ = c.write_all(if idx == 0 { &r711 } else { &r519 });
                    drop(c);
                });
            }
        });

        struct NoCap;
        impl Deadline for NoCap {
            fn expired(&self) -> bool {
                false
            }
            fn remaining(&self) -> Option<Duration> {
                None
            }
        }

        let adapter = WireSenderAdapter::new(wire_sender::Sender {
            timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        });
        let cancel = AtomicBool::new(false);
        let outcome = adapter.try_once(
            [0, 0, 0, 0],
            [0, 0, 0, 0],
            "127.0.0.1",
            port,
            None,
            &NoCap,
            &cancel,
        );

        stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(("127.0.0.1", port));
        let _ = server.join();

        assert_eq!(outcome, AttemptOutcome::Ok, "无 cap 路径应正常握手成功");
    }
}

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
    /// WireError → map_wire_kind → classify_wire_err 必等于 committed golden 值
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
