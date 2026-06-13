# D4 决策记录 —— dooraccess-rs 转正生产

日期：2026-06-13。规程：`dooraccess-go/DEPLOY.md` §Rust 生产部署 P0-P13（OpenSpec `promote-rust-to-production`）。

## 转正执行结果（P0-P12 完成）

hAP ac lite（192.168.2.89）真机 cutover：把灰度 `/tmp`+setsid 形态提升为 procd 托管的正式生产 daemon。

| 项 | 结果 |
|---|---|
| 权威 binary | **CI 产物 574,540 字节**（MIPS BE softfloat static，`release-on-tag.yml` run 27467786859；CI-canonical 溯源 = 部署=被测=待发布同一 binary）|
| 生产 daemon | `/usr/bin/dooraccess-rs`，procd `S90dooraccess-rs` 自启，pid `ppid=1`，cmdline `--config /etc/dooraccess/config.ini --state /etc/dooraccess/automation.state` |
| Go 回滚根 | `/usr/bin/dooraccess-go` **3,997,853 字节全程未动**、init.d disabled（冻结紧急回滚根）|
| config/state | 迁中性路径 `/etc/dooraccess/`（config 874B + state 34B）；`/etc/dooraccess-go/` 冻结 |
| **state 迁移生效铁证** | daemon 日志 `automation: auto_unlock=on auto_hangup=on (source=state)` —— 从中性路径 state 文件读到迁移的 flag，`--state` 全链打通 |
| 稳态 RSS | **500–604 KB**（对照 Go ~4.6MB，≈1/9）|
| reboot 存活（P12） | **PASS**——reboot 后 procd 自启 Rust、Go disabled 不起、`/info` 就绪 |
| 无 respawn 风暴 | 自启后**同 pid 全程未变**、稳定运行（boot+53s 起，0 崩溃）|
| listener 活 | 实时检测门禁网 18022 帧（req=564/565…）|
| P13 物理门开 | **user 按可用算、不重测**（灰度期已证 self-unlock 物理门开 t_ms=1398、功能等价；当前 daemon 检测响铃帧 + auto_unlock=on）|

## roadmap §D4 六判据

| # | 判据 | 状态 | 证据 |
|---|---|---|---|
| ① | 功能覆盖 Go v0.10.x | ✅ | Phase 0-5 功能等价 + 同功能 procd 运行中 |
| ② | size/RSS/CPU 不劣 | ✅ | binary 574,540B vs Go 3,997,853B（1/7）；RSS 500–604KB vs ~4.6MB（~1/9）|
| ③ | 真实响铃 self-unlock 物理门开 | ✅(accept) | 灰度 t_ms=1398 物理门开（功能等价同款代码）；cutover P13 user 按可用算、新鲜复测 deferred |
| ④ | 视频稳定 | ✅(accept) | 灰度端到端 HACS 视频流；BE②(RTCP send)/60s+ 长流为已登记边界 |
| ⑤ | (a) 灰度 liveness 3 天 + (b) 转正后 procd burn-in ≥72h 无 OOM/respawn/分叉 | (a)✅ /(b)🔄进行中 | (a) 灰度 3 天+ 稳定（user 确认）；(b) procd 形态已稳跑、0 respawn、RSS 500KB——**≥72h 累积后正式签 + 打 v1.0.0** |
| ⑥ | rollback binary/config 就绪 | ✅ | Go 冻结根 3,997,853B 未动 + disabled；DEPLOY §Rust 永久回滚双模式；`--state` 隔离 state |

## 结论

**D4 = 转正生产已执行、Rust 为 canonical 正线、Go 降级冻结紧急回滚根。** 判据①②③④⑥满足，⑤(a)满足、⑤(b) procd burn-in ≥72h 累积中。

**待续**：⑤(b) 满 ≥72h（procd 形态无 OOM/respawn 风暴/HA 状态分叉）→ 打首个 git tag `v1.0.0` + 校验 release asset 字节 == 574,540（被测产物）。

## 已知边界（accept）
- BE②(RTCP send) 端到端不可验；60s+ 长视频流受外机单次推流限制；auto-hangup 物理 teardown 同 Go 待解——均 Phase 5 / DEPLOY 登记。
- P13 物理门开新鲜复测 deferred（user 按可用算；下次真实访客自然触发即覆盖）。
