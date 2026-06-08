//! dooraccess-rs Phase1 协议核心 lib。
//!
//! 四个确定性纯函数模块的 Rust 等价实现（与 Go `dooraccess-go` golden parity）。
//! 网络 / socket / daemon 编排不在 Phase1 范围内（见 design D3）。

pub mod automation_state;
pub mod codec;
pub mod config;
pub mod wire18022;
