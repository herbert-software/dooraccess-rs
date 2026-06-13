//! video：视频转发栈。
//!
//! 协议真相锚 `openspec/specs/anjubao-video-stream/` 与
//! `samples/anjubao-video-stream/observations.md`。crate gate 仅 std + libc；
//! MIPS32 红线禁 `AtomicU64`/`AtomicI64`。
//!
//! 子模块：
//!   - [`rtp`]：RTP 头解析（V=2/X=0/CC=0 校验，P 位不检、PT 仅解析）+ Annex-B
//!     NAL 提取 + receiver 主循环（`bind_udp` 同步 bind / 1s read-timeout 轮询
//!     stop / expectSrcIP 过滤 / SSRC 首包锁存 / stats ticker 内联）
//!   - [`reassembler`]：专有分片 frame 重组器（按 RTP seq 重组字节流再切 NAL，
//!     禁逐包提取——实测逐包会静默丢 81%）
//!   - [`frame_buffer`]：SPS/PPS/IDR 种子缓存 + `sync_channel(64)` `try_send`
//!     fan-out（drop-newest）+ Condvar `wait_idr`（closed/timeout 分型）
//!   - [`preview`]：preview 信令客户端（TCP 短连接直拨 18022 + ack 校验；
//!     不经骨架 wire-worker）
//!   - [`rtcp`]：RR/SDES/BYE 构造 + 5s 周期 keepalive sender（CNAME 字面
//!     `"dooraccess-go"`，字节 golden 纪律禁改名）
//!   - [`transmux`]：FLV 静态构造（header / AVCDecoderConfigurationRecord /
//!     seq-header tag / NALU tag）+ FLV 流写出器（订阅循环 / ts 换算 /
//!     per-tag flush）
//!
//!   - [`session`]：session 生命周期 Manager（单 active 互斥 / TTL
//!     `Mutex<Instant>` + 1s monitor / detached cleanup 对账 / teardown 序列 /
//!     UUID v4 + 随机 SSRC via `/dev/urandom`）
//!
//! req=704/708 preview 帧 builder 在 [`crate::wire18022`]（与
//! `build_stop_frame` 同居，共用 `assemble_frame` 与 golden 基建）。

pub mod frame_buffer;
pub mod preview;
pub mod reassembler;
pub mod rtcp;
pub mod rtp;
pub mod session;
pub mod transmux;

/// 可选 log hook（对齐仓内 listen6672/listen18022/ha_push 的 `LogFn` 约定）。
pub type LogFn = Box<dyn Fn(&str) + Send + Sync>;

/// 可共享 log hook（Arc 形态）：[`rtp::RtpReceiver`] 需把同一 sink 同时派生给
/// 内部重组器（drop 即时行从重组器内部打）与 receiver 自身行，Box 不可 clone
/// 故此处用 Arc。其余单持有方仍用 [`LogFn`]。
pub type SharedLogFn = std::sync::Arc<dyn Fn(&str) + Send + Sync>;
