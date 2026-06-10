//! frame_buffer：RTP receiver 写入 / 多 stream consumer 读出的中央缓冲
//! （移植 Go `internal/video/rtp.go` 的 `FrameBuffer`）。
//!
//! 双重职责（锚 Go）：
//!   - 缓存最近 SPS/PPS/IDR 三件套（新 consumer 经 [`FrameBuffer::latest_idr`] 取种子）
//!   - fan-out NAL stream 给所有 active consumers（chunked 发到 HTTP client）
//!
//! 等价纪律（spec「FrameBuffer fan-out 与 backpressure 等价」/ design D3）：
//!   - per-consumer `std::sync::mpsc::sync_channel(64)` + `try_send` 非阻塞 fan-out：
//!     channel 满时**丢弃新到的 NAL（drop-newest）**，缓冲内旧 NAL 保留，
//!     永不阻塞 RTP receiver；丢帧与 Go 一致**静默**（无计数无日志——stats 的
//!     dropped_frames 是 reassembler 层另一概念，禁混淆、禁顺手加非 parity 计数器）
//!   - 种子获取走 [`FrameBuffer::latest_idr`]（订阅 channel 本身**不投递**种子；
//!     Go 另有 `SeedNALs` 但生产无调用方——死代码不移植，同 SnapshotFLV 档处理；
//!     种子快照与订阅起点之间存在 NAL 间隙是 Go 已知行为，等价保持）
//!   - IDR-ready 用 `Mutex + Condvar` 广播（Go `close(idrReady)` 的等价物）；
//!     [`FrameBuffer::wait_idr`] 支持 deadline，且 **closed 错误与 timeout 分型**
//!     （[`WaitIdrError`]；Go 是 `WaitIDR` 返 nil + 调用方 `Closed()` 自判两段式，
//!     Rust 合并为带分型的单返回是等价重构——HTTP 层靠它区分 503「session closed」
//!     与 504 timeout）
//!   - `close` 语义：close 后 Push 静默丢、所有 consumer channel 断开、close 后新
//!     `subscribe` 返回已断开的 channel
//!
//! MIPS32 红线：本模块无任何原子（Mutex/Condvar 足够），天然合规。

use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use super::rtp::{NalUnit, NAL_TYPE_IDR, NAL_TYPE_PPS, NAL_TYPE_SPS};

/// per-consumer channel 缓冲深度（锚 Go `make(chan nalUnit, 64)`：
/// 外机 ~99 包/s × 不到 1s burst）。
pub const CONSUMER_CHAN_DEPTH: usize = 64;

/// [`FrameBuffer::wait_idr`] 错误分型（spec 场景「close 与 wait_idr 竞态分型」）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitIdrError {
    /// buffer 已 close（session teardown）→ HTTP 层映射 503「session closed」。
    Closed,
    /// deadline 内无 IDR → HTTP 层映射 504（no-keyframe）。
    Timeout,
}

impl core::fmt::Display for WaitIdrError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            WaitIdrError::Closed => write!(f, "video: frame buffer closed"),
            WaitIdrError::Timeout => write!(f, "video: no keyframe within deadline"),
        }
    }
}

impl std::error::Error for WaitIdrError {}

struct Inner {
    // 关键 NAL 缓存（新 consumer 种子）。
    sps: Option<NalUnit>,
    pps: Option<NalUnit>,
    idr: Option<NalUnit>,

    // 首个 IDR 已出现（Go `idrReadyHit`；信号经 Condvar 广播）。
    idr_ready: bool,

    // closed 标志位：close 后 Push 静默丢弃。
    closed: bool,

    // fan-out：每个 consumer 一个 sync_channel 发送端（id 供退订摘除）。
    consumers: Vec<(u64, SyncSender<NalUnit>)>,
    next_id: u64,
}

/// RTP receiver 写入 / 多 consumer 读出的中央缓冲。锚 Go `FrameBuffer`。
pub struct FrameBuffer {
    inner: Mutex<Inner>,
    idr_cond: Condvar,
}

impl Default for FrameBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameBuffer {
    /// 构造空 buffer（锚 Go `NewFrameBuffer`）。
    pub fn new() -> FrameBuffer {
        FrameBuffer {
            inner: Mutex::new(Inner {
                sps: None,
                pps: None,
                idr: None,
                idr_ready: false,
                closed: false,
                consumers: Vec::new(),
                next_id: 0,
            }),
            idr_cond: Condvar::new(),
        }
    }

    /// RTP receiver 的写入路径（锚 Go `Push`）：
    ///   - SPS/PPS/IDR：更新种子缓存 + 首个 IDR 时 Condvar 广播
    ///   - 所有 NAL：`try_send` fan-out 给所有 consumer（满 → drop-newest，静默）
    ///   - closed 后静默丢弃
    pub fn push(&self, n: NalUnit) {
        let senders: Vec<SyncSender<NalUnit>> = {
            let mut inner = self.inner.lock().unwrap();
            if inner.closed {
                return;
            }
            match n.nal_type {
                NAL_TYPE_SPS => inner.sps = Some(n.clone()),
                NAL_TYPE_PPS => inner.pps = Some(n.clone()),
                NAL_TYPE_IDR => {
                    inner.idr = Some(n.clone());
                    if !inner.idr_ready {
                        inner.idr_ready = true;
                        self.idr_cond.notify_all();
                    }
                }
                _ => {}
            }
            inner.consumers.iter().map(|(_, tx)| tx.clone()).collect()
        };
        // 锁外 fan-out（锚 Go 先 snapshot consumers 再 unlock send）。
        // try_send 满 → Err(Full) 丢新 NAL（drop-newest）；断开 → 忽略（退订路径清理）。
        for tx in senders {
            let _ = tx.try_send(n.clone());
        }
    }

    /// 注册一个 consumer，返 (接收端, 退订句柄)。锚 Go `Subscribe`。
    ///
    /// close 后调用：返回**已断开**的 channel（接收端立刻 `Err(Disconnected)`），
    /// 退订句柄为 no-op——对齐 Go `closed` 分支返已 close 的 chan。
    ///
    /// 退订：[`Subscription`] drop（或显式 [`Subscription::unsubscribe`]）即摘除
    /// 发送端 → 接收端 drain 完缓冲后断开（Go `delete + close(ch)` 等价）。
    pub fn subscribe(self: &Arc<Self>) -> (Receiver<NalUnit>, Subscription) {
        let (tx, rx) = sync_channel::<NalUnit>(CONSUMER_CHAN_DEPTH);
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            drop(tx); // 发送端即刻丢弃 → rx 断开。
            return (
                rx,
                Subscription {
                    buf: Arc::clone(self),
                    id: None,
                },
            );
        }
        let id = inner.next_id;
        inner.next_id += 1;
        inner.consumers.push((id, tx));
        (
            rx,
            Subscription {
                buf: Arc::clone(self),
                id: Some(id),
            },
        )
    }

    /// 返回最近 (SPS, PPS, IDR) 三件套种子（任一缺失返 `None`）。锚 Go `LatestIDR`。
    pub fn latest_idr(&self) -> Option<(NalUnit, NalUnit, NalUnit)> {
        let inner = self.inner.lock().unwrap();
        match (&inner.sps, &inner.pps, &inner.idr) {
            (Some(sps), Some(pps), Some(idr)) => Some((sps.clone(), pps.clone(), idr.clone())),
            _ => None,
        }
    }

    /// 阻塞等首个 IDR，支持 deadline。锚 Go `WaitIDR(ctx)` + 调用方 `Closed()`
    /// 两段式的合并分型版：
    ///   - close（含等待中被 close 的竞态）→ [`WaitIdrError::Closed`]（→ 503）
    ///   - deadline 内无 IDR → [`WaitIdrError::Timeout`]（→ 504）
    pub fn wait_idr(&self, timeout: Duration) -> Result<(), WaitIdrError> {
        let deadline = Instant::now() + timeout;
        let mut inner = self.inner.lock().unwrap();
        loop {
            if inner.closed {
                return Err(WaitIdrError::Closed);
            }
            if inner.idr_ready {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(WaitIdrError::Timeout);
            }
            let (guard, _wait) = self.idr_cond.wait_timeout(inner, remaining).unwrap();
            inner = guard;
            // 醒来（信号 / 超时 / spurious）回环以单调 deadline 重判。
        }
    }

    /// 标记 buffer 关闭：后续 Push 静默丢；现有 consumer channel 全断开；
    /// 等待中的 [`FrameBuffer::wait_idr`] 立即返 [`WaitIdrError::Closed`]。
    /// 锚 Go `Close`。
    pub fn close(&self) {
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            return;
        }
        inner.closed = true;
        // 摘除全部发送端（drop SyncSender → 接收端 drain 完后 Disconnected）。
        inner.consumers.clear();
        // 唤醒所有 wait_idr（醒后见 closed → Err(Closed)；Go 用 close(idrReady)
        // + caller Closed() 自判，Rust 分型单返回等价）。
        self.idr_cond.notify_all();
    }

    /// 返 buffer 是否已 close。锚 Go `Closed`。
    pub fn closed(&self) -> bool {
        self.inner.lock().unwrap().closed
    }

    /// 摘除指定 consumer（退订句柄内部用）。
    fn remove_consumer(&self, id: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.consumers.retain(|(cid, _)| *cid != id);
    }
}

/// 退订句柄（Go `Subscribe` 返回的 unsubscribe 闭包的等价物）。
///
/// drop 即退订（摘除发送端 → 接收端 drain 完缓冲后断开）；显式
/// [`Subscription::unsubscribe`] 仅是带语义名的 drop。
pub struct Subscription {
    buf: Arc<FrameBuffer>,
    /// `None` = close 后订阅的 no-op 句柄。
    id: Option<u64>,
}

impl Subscription {
    /// 显式退订（等价直接 drop）。
    pub fn unsubscribe(self) {}
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            self.buf.remove_consumer(id);
        }
    }
}

// ── 单测（移植 Go rtp_test.go FrameBuffer 用例 + spec 场景）─────────────────────

#[cfg(test)]
mod tests {
    use super::super::rtp::NAL_TYPE_NON_IDR;
    use super::*;
    use std::sync::mpsc::{RecvTimeoutError, TryRecvError};
    use std::thread;

    fn nal(nal_type: u8, data: &[u8], ts: u32) -> NalUnit {
        NalUnit {
            nal_type,
            data: data.to_vec(),
            timestamp: ts,
        }
    }

    fn seed_all(b: &FrameBuffer) {
        b.push(nal(NAL_TYPE_SPS, &[0x67, 0xaa], 100));
        b.push(nal(NAL_TYPE_PPS, &[0x68, 0xbb], 100));
        b.push(nal(NAL_TYPE_IDR, &[0x65, 0xcc], 100));
    }

    /// 移植 Go `TestFrameBuffer_PushAndSeed`（种子走 latest_idr API，SeedNALs 不移植）。
    #[test]
    fn push_and_seed() {
        let b = FrameBuffer::new();
        seed_all(&b);
        let (sps, pps, idr) = b.latest_idr().expect("latest_idr after pushing all 3");
        assert_eq!(sps.nal_type, NAL_TYPE_SPS);
        assert_eq!(pps.nal_type, NAL_TYPE_PPS);
        assert_eq!(idr.nal_type, NAL_TYPE_IDR);
        assert_eq!(idr.data, vec![0x65, 0xcc]);
    }

    /// 三件套任一缺失 → None（Go LatestIDR ok=false）。
    #[test]
    fn latest_idr_requires_all_three() {
        let b = FrameBuffer::new();
        b.push(nal(NAL_TYPE_SPS, &[0x67], 0));
        b.push(nal(NAL_TYPE_IDR, &[0x65], 0));
        assert!(b.latest_idr().is_none(), "missing PPS must yield None");
    }

    /// 移植 Go `TestFrameBuffer_WaitIDR_ReturnsImmediatelyAfterIDR`。
    #[test]
    fn wait_idr_returns_immediately_after_idr() {
        let b = FrameBuffer::new();
        b.push(nal(NAL_TYPE_IDR, &[0x65], 1));
        b.wait_idr(Duration::from_millis(100))
            .expect("wait_idr after IDR push");
    }

    /// 移植 Go `TestFrameBuffer_WaitIDR_TimeoutNoIDR`：无 IDR → Timeout 分型。
    #[test]
    fn wait_idr_timeout_no_idr() {
        let b = FrameBuffer::new();
        let err = b
            .wait_idr(Duration::from_millis(50))
            .expect_err("expected timeout");
        assert_eq!(err, WaitIdrError::Timeout);
    }

    /// 移植 Go `TestFrameBuffer_FanOutToConsumer`。
    #[test]
    fn fan_out_to_consumer() {
        let b = Arc::new(FrameBuffer::new());
        let (rx, _sub) = b.subscribe();
        b.push(nal(NAL_TYPE_NON_IDR, &[0x61, 0x01], 100));
        b.push(nal(NAL_TYPE_NON_IDR, &[0x61, 0x02], 200));
        let n1 = rx.recv_timeout(Duration::from_millis(500)).expect("NAL 1");
        let n2 = rx.recv_timeout(Duration::from_millis(500)).expect("NAL 2");
        assert_eq!(n1.data, vec![0x61, 0x01]);
        assert_eq!(n2.data, vec![0x61, 0x02]);
    }

    /// 慢消费者丢帧不阻塞（spec 场景）：consumer A 不读，积满 64 后丢新 NAL
    /// （drop-newest——缓冲内旧 NAL 保留）；push 全程不阻塞、其它 consumer 不受影响。
    #[test]
    fn slow_consumer_drops_newest_without_blocking() {
        let b = Arc::new(FrameBuffer::new());
        let (rx_slow, _sub_slow) = b.subscribe();
        let (rx_fast, _sub_fast) = b.subscribe();

        // 推 CONSUMER_CHAN_DEPTH + 10 个 NAL，两个 consumer 都不读。
        // push 必须全程立即返回（try_send 永不阻塞）——若阻塞本测试会挂死。
        let total = CONSUMER_CHAN_DEPTH + 10;
        for i in 0..total {
            b.push(nal(NAL_TYPE_NON_IDR, &[0x61, i as u8], i as u32));
        }

        // fast consumer drain 自己的缓冲（64 个最早的）后接收新 NAL——
        // 证明慢消费者（slow 始终不读）不影响其它 consumer 与 receiver。
        for i in 0..CONSUMER_CHAN_DEPTH {
            let n = rx_fast.try_recv().expect("fast buffered NAL");
            assert_eq!(n.data[1], i as u8);
        }
        b.push(nal(NAL_TYPE_NON_IDR, &[0x61, 0xfe], 9999));
        let fresh = rx_fast
            .recv_timeout(Duration::from_millis(500))
            .expect("drained fast consumer must receive new NAL");
        assert_eq!(fresh.data, vec![0x61, 0xfe]);

        // slow consumer 缓冲恰好保留**最早的 64 个**（drop-newest：旧 NAL 保留，
        // 后续含 0xfe 在内的新 NAL 全部被丢）。
        let mut slow_got = Vec::new();
        while let Ok(n) = rx_slow.try_recv() {
            slow_got.push(n);
        }
        assert_eq!(slow_got.len(), CONSUMER_CHAN_DEPTH, "slow buffered count");
        for (i, n) in slow_got.iter().enumerate() {
            assert_eq!(
                n.data[1], i as u8,
                "drop-newest must preserve oldest NALs in order"
            );
        }
    }

    /// 新消费者种子与间隙（spec 场景「新消费者种子」）：latest_idr 取三件套；
    /// 种子快照与订阅首 NAL 之间的 NAL 不补投（Go 已知间隙行为）。
    #[test]
    fn new_consumer_seed_and_gap() {
        let b = Arc::new(FrameBuffer::new());
        seed_all(&b);
        // 间隙 NAL：种子之后、订阅之前推送——新 consumer 收不到（已知行为）。
        b.push(nal(NAL_TYPE_NON_IDR, &[0x61, 0xee], 190));

        let (sps, pps, idr) = b.latest_idr().expect("seed");
        assert_eq!(
            (sps.nal_type, pps.nal_type, idr.nal_type),
            (NAL_TYPE_SPS, NAL_TYPE_PPS, NAL_TYPE_IDR)
        );

        let (rx, _sub) = b.subscribe();
        b.push(nal(NAL_TYPE_NON_IDR, &[0x61, 0xff], 280));
        let first = rx.recv_timeout(Duration::from_millis(500)).expect("NAL");
        assert_eq!(first.data, vec![0x61, 0xff], "订阅起点之前的间隙 NAL 不补投");
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
    }

    /// 移植 Go `TestFrameBuffer_CloseShutsDownConsumers`。
    #[test]
    fn close_shuts_down_consumers() {
        let b = Arc::new(FrameBuffer::new());
        let (rx, _sub) = b.subscribe();
        b.close();
        match rx.recv_timeout(Duration::from_millis(100)) {
            Err(RecvTimeoutError::Disconnected) => {}
            other => panic!("consumer channel should disconnect after close, got {other:?}"),
        }
    }

    /// 移植 Go `TestFrameBuffer_PushAfterCloseIsNoop`。
    #[test]
    fn push_after_close_is_noop() {
        let b = FrameBuffer::new();
        b.close();
        b.push(nal(NAL_TYPE_IDR, &[0x65], 0)); // 不 panic、静默丢。
        assert!(b.latest_idr().is_none(), "close 后种子不更新");
        assert!(b.closed());
    }

    /// close 后新 subscribe 返已断开 channel（spec close 语义）。
    #[test]
    fn subscribe_after_close_returns_disconnected() {
        let b = Arc::new(FrameBuffer::new());
        b.close();
        let (rx, sub) = b.subscribe();
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Disconnected);
        sub.unsubscribe(); // no-op 句柄不 panic。
    }

    /// close 竞态分型（spec 场景「close 与 wait_idr 竞态分型」）：等待中 close →
    /// 立即返 Closed（非 Timeout），上层据此 503 而非 504。
    #[test]
    fn wait_idr_close_race_returns_closed() {
        let b = Arc::new(FrameBuffer::new());
        let b2 = Arc::clone(&b);
        let waiter = thread::spawn(move || b2.wait_idr(Duration::from_secs(5)));
        thread::sleep(Duration::from_millis(50));
        let t0 = Instant::now();
        b.close();
        let res = waiter.join().unwrap();
        assert_eq!(res, Err(WaitIdrError::Closed), "close 必须分型为 Closed");
        assert!(
            t0.elapsed() < Duration::from_secs(1),
            "close 必须立即唤醒 wait_idr，took {:?}",
            t0.elapsed()
        );
    }

    /// 已 close 再 wait_idr → Closed（即便 IDR 曾出现过——对齐 Go handler
    /// WaitIDR-nil 后 Closed() 判 503 的两段式语义）。
    #[test]
    fn wait_idr_after_close_returns_closed_even_with_idr() {
        let b = FrameBuffer::new();
        b.push(nal(NAL_TYPE_IDR, &[0x65], 0));
        b.close();
        assert_eq!(
            b.wait_idr(Duration::from_millis(50)),
            Err(WaitIdrError::Closed)
        );
    }

    /// 退订：Subscription drop → 接收端 drain 完缓冲后断开；后续 push 不再投递。
    #[test]
    fn unsubscribe_disconnects_receiver() {
        let b = Arc::new(FrameBuffer::new());
        let (rx, sub) = b.subscribe();
        b.push(nal(NAL_TYPE_NON_IDR, &[0x61, 0x01], 0));
        sub.unsubscribe();
        b.push(nal(NAL_TYPE_NON_IDR, &[0x61, 0x02], 0));
        // 退订前已入缓冲的仍可 drain。
        assert_eq!(rx.recv().expect("buffered NAL").data, vec![0x61, 0x01]);
        // 之后断开（退订后的 push 不可见）。
        assert!(rx.recv().is_err(), "receiver must disconnect after unsub");
    }
}
