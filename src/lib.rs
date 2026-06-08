//! dooraccess-rs lib。
//!
//! Phase1：四个确定性协议核心模块（与 Go `dooraccess-go` golden parity）。
//! Phase2：httpx bare HTTP/1.1 + JSON encoder + 控制面/元信息占位（见 `port-rust-http-control`）。

pub mod automation_state;
pub mod codec;
pub mod config;
pub mod control;
pub mod sender;
pub mod ha_push;
pub mod httpx;
pub mod info;
pub mod wire18022;
