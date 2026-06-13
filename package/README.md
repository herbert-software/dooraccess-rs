# dooraccess-rs 安装 / 回滚

hAP ac lite（MIPS 大端 / softfloat / OpenWrt musl / 64MB RAM）上把 dooraccess-rs 作为
**procd 托管的正式生产 daemon** 安装。单设备直接安装（scp binary + init.d），**不构建 OpenWrt
`.ipk`**——全静态单文件 binary（`DEPENDS` 为空），ipk 的依赖管理价值有限；分发到多设备时再评估。

> ⚠️ **真机部署的权威 SOP 是 `dooraccess-go/DEPLOY.md` §Rust 生产部署 P0-P13（单一来源）。**
> 本文只给概览，**任何 hAP scp / restart / 替换 binary / 翻 enable 位 MUST 按 DEPLOY.md
> 逐步执行**（OOM 红线 / SSH 死机处理 / 命令交人工）。禁自创流程。本仓 `Makefile` **不提供**
> 任何自动 scp/ssh 到 hAP 的 target——部署命令交人工在自己终端跑。

## 产物

| 文件 | 说明 |
|---|---|
| `/usr/bin/dooraccess-rs` | 全静态 MIPS BE softfloat binary（`make dist` → `dist/dooraccess-rs-mips`）|
| `/etc/init.d/dooraccess-rs` | procd init script（本目录 `files/etc/init.d/dooraccess-rs`）|
| `/etc/dooraccess/config.ini` | 主配置（中性路径；cutover 时从 `/etc/dooraccess-go/` 迁移）|
| `/etc/dooraccess/automation.state` | automation flag 持久态（中性路径；init.d 经 `--state` 显式传入）|

## 构建

```sh
make build-mips     # nightly + build-std + OpenWrt SDK musl sysroot + 全静态
make dist           # = verify-mips + 拷 dist/dooraccess-rs-mips（记 ls -l 字节数）
```

## 共存回滚模型（Go 作冻结紧急回滚根）

- `/usr/bin/dooraccess-go` + 其 init.d + 冻结的 `/etc/dooraccess-go/` **全程不覆盖/不删**，转正后
  转为 **disabled 的紧急回滚根**。
- 转正（cutover）= 装 Rust binary + init.d → 迁移 config/state 到 `/etc/dooraccess/` → `start`
  Rust（先不 enable）→ 验健康（Go 仍 enabled-stopped 兜底）→ `disable dooraccess-go` **先于**
  `enable dooraccess-rs`（中断落「两 disabled」安全侧）→ reboot 存活 + 真实响铃功能 smoke。
- **回滚双模式**：① 紧急冻结 = `enable`+`start` Go 读 `-go` 冻结快照（止血优先）；② 状态保留 =
  先 `cp /etc/dooraccess/automation.state /etc/dooraccess-go/automation.state` 灌回再启 Go
  （避免长期回滚倒退住户最近设置）。回滚序首步 stop+disable rust → **确认 8080/PF_PACKET 端口
  释放**（`/proc/net/tcp` 无 8080 LISTEN + `cat /proc/net/packet` 无 dead daemon 行，kill-9 后
  轮询）→ 再启 Go。

完整步骤、命令模板（含 `sshpass`/`PubkeyAuthentication=no`/`ConnectTimeout`/scp `-O`）、OOM 红线、
SSH 死机救援、源缺失分支、reboot 存活判据（身份用 `pgrep` 非 `/info`——`/info.daemon` 对 Rust/Go
不可区分）见 **`dooraccess-go/DEPLOY.md` §Rust 生产部署 P0-P13**。
