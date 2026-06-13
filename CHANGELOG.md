# Changelog

本文件记录 dooraccess-rs 的版本变更。pre-1.0 阶段不维护 CHANGELOG（git 历史为准）；
`v1.0.0` 是首个 git tag、转正生产基线。

> 版本闸门：`v1.0.0` MUST 在真机 P-SOP 转正验证 + 转正后 procd burn-in（≥72h）通过**之后**才打
> （见 `dooraccess-go/DEPLOY.md` §Rust 生产部署 + OpenSpec `promote-rust-to-production`）。
> 下列实测占位符在打 tag 前据真机结果填实。

## [Unreleased] → v1.0.0（转正生产基线，待真机验证后定版）

功能等价 Go v0.10.0；转正为 hAP ac lite 上 procd 托管的正式生产 daemon，取代灰度 setsid 形态。

### 新增
- `--state <path>` flag：automation.state 路径可显式指定（支撑 config/state 中性化到
  `/etc/dooraccess/`；state 路径不从 config 派生）。binary 默认常量仍 `-go` 作 R1-R7 临时 swap /
  手动 setsid 裸跑向后兼容。
- `package/files/etc/init.d/dooraccess-rs`：OpenWrt procd init script——`respawn 3600 5 5`、
  开机自启、SIGTERM graceful、config 变更 reload。**无 GOMEMLIMIT/GOGC/GOMAXPROCS**（Rust 无 GC；
  稳态 VmRSS ~600KB）。`command` 显式传 `--config /etc/dooraccess/config.ini --state
  /etc/dooraccess/automation.state`。
- `package/README.md`：直接安装 / 共存回滚（Go-as-rollback）/ 回滚双模式说明。
- `.github/workflows/release-on-tag.yml`：交叉构建 MIPS BE softfloat binary + 上传**可下载
  workflow artifact**（CI-canonical 溯源——cutover `gh run download` 取此被测产物部署）；tag
  （`v*`）另创 GitHub Release（release asset MUST == burn-in 被测产物，nightly 漂移则
  `gh release upload --clobber` 替换）。`workflow_dispatch` 供 dry-run / 出 cutover 产物。

### 生产形态
- procd init.d 托管（取代灰度 `/tmp` + setsid）；开机自启；Go binary/init.d 转 disabled 紧急回滚根。
- config/state 迁中性路径 `/etc/dooraccess/`（cutover 从 `/etc/dooraccess-go/` 拷贝迁移）。

### 实测（2026-06-13 真机转正，见 `D4_DECISION.md`）
- MIPS binary 字节数：**574,540**（CI 产物，对照 Go v0.10.0 MIPS 3,997,853 字节 ≈ 1/7）
- 稳态 VmRSS：**500–604 KB**（对照 Go ~4.6MB ≈ 1/9）
- reboot 存活：procd 自启 Rust、Go disabled、`/info` 就绪、0 respawn
- state 迁移生效：daemon `automation: auto_unlock=on auto_hangup=on (source=state)`（中性路径）
- 真实响铃 self-unlock 物理门开：灰度 t_ms=1398（功能等价）；转正后新鲜复测按可用 deferred
- procd burn-in（≥72h，**v1.0.0 tag 前置**）：进行中，到期前不打 tag
