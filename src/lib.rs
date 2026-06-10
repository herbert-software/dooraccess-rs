//! dooraccess-rs lib。
//!
//! Phase1：四个确定性协议核心模块（与 Go `dooraccess-go` golden parity）。
//! Phase2：httpx bare HTTP/1.1 + JSON encoder + 控制面/元信息占位（见 `port-rust-http-control`）。
//! Phase3：监听器（listen6672 / listen18022 PF_PACKET 抓帧）+ wire TCP sender + unlock 核心
//!   （见 `port-rust-listeners-unlock`）。
//!
//! ## crate gate 破口边界（D-A，spec「crate gate 放宽边界」）
//!
//! Phase 1/2 守 0-crate（仅 std）。Phase 3 把依赖**放宽为 std + libc only**——
//! `AF_PACKET` / `bind(sockaddr_ll)` / `setsockopt(SO_ATTACH_FILTER / PACKET_ADD_MEMBERSHIP)` /
//! `recvfrom` 无 std 等价物，必须引 `libc`。破口**严格框死为三处用途**：
//!   ① `listen6672` PF_PACKET（经 `ffi`）
//!   ② `listen18022` PF_PACKET（经 `ffi`）
//!   ③ `wire_sender` 的 `SO_BINDTODEVICE`（`wire_sender` 自持）
//! **拒绝** nix / pnet / socket2 / tokio 或任何其他外部 crate（`cargo tree` 必须仅 `libc`）。
//! 任何超出此边界须新 OpenSpec change。

// --- Phase 1/2 既有模块 ---
pub mod automation_state;
pub mod codec;
pub mod config;
pub mod control;
/// Phase 4 M1.5 并发骨架：Job 队列 / 单 wire-worker / shutdown / detached push 排空。
pub mod daemon;
pub mod ha_push;
pub mod httpx;
pub mod info;
/// daemon 编排接线 helper（automation flag 加载 / OnDetect 门铃 builder / 号码查询 callback /
/// 手动 unlock → wire-worker 派发缝）；从 main.rs 上移以便 e2e 测真生产函数。
pub mod orchestration;
/// ring 触发 self-unlock 消费者主体（Subscribable 消费者线程 + debounce + flag gate +
/// ExecuteUnlockFromRing + auto-hangup detached 计时线程；Phase 4 ②）。
pub mod self_unlock;
pub mod sender;
/// 视频转发栈（Phase 5 `port-rust-video-forward`）：RTP 解析 / 专有分片重组 /
/// FLV transmux 等（对 Go `internal/video/` golden parity）。
pub mod video;
pub mod wire18022;

// --- Phase 3 新增：FFI / BPF 地基（组 A）---
/// 烤好的 BPF filter 常量（对 Go `bpf.Assemble` golden）。
pub mod bpf;
/// PF_PACKET raw-socket FFI 缝 + htons + int 宽度断言（libc 破口承载）。
pub mod ffi;

// --- Phase 3 新增：监听器 / sender / unlock（组 B/C/D/F 填充）---
/// TCP 18022 wire 帧监听 + Subscribe 订阅（PF_PACKET + BPF + parser/dedup）。
pub mod listen18022;
/// UDP 6672 事件信号监听（PF_PACKET + BPF + parser/dedup/numquery/dispatch）。
pub mod listen6672;
/// executeUnlock 开锁核心（retry / probe / bye 早停 / 错误分类 / wire 串行锁）。
pub mod unlock;
/// 18022 wire 帧 TCP sender（短连接 + deadline + silent-FIN + SO_BINDTODEVICE）。
pub mod wire_sender;
