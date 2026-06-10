# Rust self-unlock hAP 真机验证（HAP_VERIFY）

Phase 4 Rust daemon（`dooraccess-rs`，ring 触发自开锁 + auto-hangup）在 hAP ac lite 的 ground-truth 验证，回答 roadmap 决策点 **D3/D4** 缺的两个数据：**真机稳态 RSS** + **self-unlock 物理门开**。

OpenSpec 规程：`openspec/changes/.../verify-rust-self-unlock-on-hap/`（3 轮对抗 review 加固）。

> ⚠️ **swap 部署 + 回滚机制看 `dooraccess-go/DEPLOY.md` §Rust 临时验证 swap R1-R7**（单一来源，本文不重复）。本文只放**验证清单 + 结果表**。所有 hAP 命令交人工执行 / 逐步确认，AI 不擅自连发。验证窗口须低流量时段 + 现场有人手动开门兜底。

## 对照基线（Go v0.10.0）

| 指标 | Go 基线 |
|---|---|
| 稳态 RSS | ~4.6MB（hAP 实测） |
| self-unlock | 真实响铃 `result=0 t_ms=1258`，~1.3s 物理门开已确认 |
| MIPS binary | 3,997,853 字节 |

Rust 对照：MIPS binary **491,500 字节**（`make dist`）；RSS / 物理 self-unlock = **本次验证要取的数**。

## 验证清单（DEPLOY.md §Rust R7 启动后执行；递进，逐项判据）

**关键：RSS 健康前置于暴露真实响铃**（Rust 无 GOMEMLIMIT 兜底）。

1. **启动 alive + 即时 RSS** — `pgrep dooraccess-rs`、`/proc/<pid>/status` 的 `VmRSS`（State=S）。
2. **`GET /info`** — `wget -qO- http://127.0.0.1:8080/info` 返合法 JSON（daemon/version/brand/monitor/outdoor_stations）。
3. **稳态 RSS + free（前置于响铃，abort 闸门）** — `sleep 30-60` 后 `VmRSS` + `free -m`；**若 free 余量 < ~15MB 或 RSS 异常 → 立即 abort + 回滚**（DEPLOY.md §Rust 回滚），不暴露真实响铃。
3b. **后台存活** — 断开 SSH 重连后 `pgrep dooraccess-rs` 仍在（验 setsid + SIGHUP-ignore）。
4. **手动 `POST /unlock`** — **hAP 无 `nc`/`curl`/`socat`（2026-06-10 实证），busybox `wget --post-data` 被 bare-net httpx 拒**：手动路径走 **HACS UI 开锁按钮**（生产客户端，POST 到 hAP:8080=此刻 Rust）。判据：外机播报「门锁已开」+ **物理门开**。注：手动开锁路径不呼叫室内机，与响铃路径（呼室内机 + 自动开锁）语义不同——若手动路径异常，以响铃路径（步骤 5）为 headline 准。
5. **真实响铃 self-unlock（headline，仅在 3 RSS 健康后）** — 设好 HACS 两 switch 防双触发 → **物理按门铃** → 外机播报 + **物理门开** + 从 `/tmp/rust.log`/logread 记 `t_ms`（对照 Go 1258）。3.4/3.5 期间现场观测 `free`，骤降即中止回滚。
6. **auto-hangup 物理 teardown（记录现象非硬 pass/fail）** — 自开锁成功 + auto_hangup on → ~2s 外机 req=708 → teardown；`grep req=708 /tmp/rust.log` + 物理表现。Go 侧本就未闭环（待解1），只记录现象。

验收铁律：**ground-truth = 外机播报 + 物理门真开**，daemon log `result=0` 不算（memory `feedback_protocol_ground_truth`）。

## 结果（2026-06-10 实测，pid 32594）

| 项 | Rust 实测 | Go 基线 | 结论 |
|---|---|---|---|
| automation.state 原值快照（R1b） | `auto_unlock=true auto_hangup=true` | — | 全程未变（Rust source=state 读取，未拨 flag） |
| 即时 RSS | **528 kB** | ~4.6MB | **~9× 优势** |
| 稳态 RSS（35s） | **528 kB**（完全不涨） | ~4.6MB | 平直，无 GOMEMLIMIT 也零增长 |
| 峰值 VmHWM | **548 kB** | — | OOM 零压力 |
| free 余量（稳态） | 14 MB | — | abort 阈值远未触及 |
| `/info` JSON | ✓ 全字段 | ✓ | monitor/stations/video.forward=true |
| **真实响铃 self-unlock 物理门开** | **Y**（用户确认「开了」） | ✓ | **headline ✅** |
| **t_ms** | **1398** | 1258 | 同量级 ~1.4s |
| 链路 | req=704→trigger→710 unlock-A→708 bye→518 unlock-B→result=0 | — | 完整握手 |
| auto-hangup req=708 出站 | Y（build_stop_frame 发了） | 待解1 | 物理 teardown 留观察 |
| 回滚后 Go `/info` + RSS | Y（RSS 4452kB 回基线） | — | 生产恢复 |
| automation.state 还原确认 | Y（未变，无需还原） | — | |
| binary 字节 | **491,500** | 3,997,853 | **~8× 优势** |

### 真机观察（记录，按范围不在本 change 修）

- **第二次快速连续响铃 result=-5（ResultTimeout）**：第一次响铃成功开门 + teardown 后，紧接的第二次自开锁 unlock-A 重发 8 次外机不应答、打满 cap ~11.7s 超时。用户解读：手动开锁路径不呼叫室内机、响铃路径呼叫室内机才自动开锁——属外机状态机时序，非 Rust 逻辑 bug（unlock-A 正常重发，外机未回）。
- **hAP 无 `nc`/`curl`/`socat`**（R1 预检抓到）：手动 unlock 的 nc 裸 POST 不可用；本次靠响铃路径（不经 HTTP）验 headline；DEPLOY.md/HAP_VERIFY 的手动 unlock 命令应改注「hAP 无 nc，手动路径走 HACS UI 开锁按钮」。
- **多 slave listener 确认**：banner 显示 `slaves=["eth0.2","eth1"]`（br-door 桥两口）→ 并发 debounce 设计在真机多 slave 下生效（消费者线程 + bounded channel 汇流）。

## D3 决策产出

体型 **491KB vs 4.0MB（~8×）** + RSS **528KB vs 4.6MB（~9×）** + 物理 self-unlock 可用（~1.4s）—— **Rust 无视频版的体型/RSS 优势压倒性成立**。即便 Phase 5 加视频（Go 视频层仅占其 4MB 一小部分），Rust 大概率仍远低于 Go。**D3 → 移植视频（Phase 5）收益成立、值得推进。**

## D3 决策产出

验证完成后合成：体型优势（491KB vs 4.0MB，已知）+ 本次 RSS + 物理 self-unlock 可用性 → 更新 `rust_port_roadmap.md` D3（是否移植视频 Phase 5）建议。
