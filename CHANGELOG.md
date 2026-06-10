# Changelog

本文件记录 `dooraccess-rs` 的显著改动。格式参考 [Keep a Changelog](https://keepachangelog.com/)。

仓库暂无 git tag；在此之前改动归入 `[Unreleased]`。

## [Unreleased]

### Added

- **生产日志带墙钟时间戳**（OpenSpec `add-rust-log-timestamps`）：新增中央带戳日志层 `src/log.rs`——
  - `render(tm, millis, msg)`（纯函数，手工 `format!` 拼接，不用 `strftime`）+ `format_log_line(now, msg)`（经 `libc::localtime_r` 取本地分解时间，不用 `tzset`）+ `log_line(msg)`（单次加锁写 stderr，多线程不交错）。
  - 每条生产日志行前缀 `dooraccess-rs: YYYY/MM/DD HH:MM:SS.mmm`（进程内自带戳，不依赖 syslog/procd；毫秒精度）。
  - 健壮性零 panic（`panic=abort`）：时钟早于 epoch（`duration_since` `Err`）或 `localtime_r` 返 null 时降级占位戳，仍输出行、不 `unwrap`。

### Changed

- 收编全部生产日志 sink 经统一带戳入口：`main.rs:logf<W>`（保留注入 `W` sink）、两处视频 `SharedLogFn`、以及 `daemon.rs`/`orchestration.rs`/`self_unlock.rs`/`main.rs` 散落的裸 `eprintln!`。`main.rs` 的 `load config`/`run` 错误行改走 `logf` 得时间戳。

### Tooling

- 新增机械门禁 `scripts/check-log-gate.sh`：多行感知扫描所有直写 stderr 形式（含 rustfmt 拆行 `writeln!(\n stderr,`），断言每命中带 `// EARLY-STAGE` / `// TEST-ONLY` / `// CENTRAL-SINK` 标记之一（`CENTRAL-SINK` ≤ 2），并断言 `src/log.rs` 零 `unwrap`/`expect` + `log_line` 单次加锁 `write_all`。

### Notes

- 时区：`TZ` 未设时渲染为 UTC（可接受；如需本地时区，部署设 `TZ`）。
- MIPS binary 字节数核对（`make dist`/`verify-mips`）留待 PR CI（Linux + nightly + build-std + OpenWrt SDK；macOS host 不具备交叉工具链）。
