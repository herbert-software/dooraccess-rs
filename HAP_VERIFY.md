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
4. **手动 `POST /unlock`**（**用 `nc` 裸 POST 非 `wget`**——bare-net httpx 拒 wget 请求体静默失败；Content-Length 自动 `wc -c` 算不手填）：
   ```bash
   sshpass -p 'gaohaobo' ssh -o ConnectTimeout=8 -o ServerAliveInterval=15 -o PubkeyAuthentication=no root@192.168.2.89 \
     'B='\''{"from":"<室内机BCD>","to":"<外机BCD@IP>"}'\''; printf '\''POST /unlock HTTP/1.0\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s'\'' "$(printf %s "$B" | wc -c)" "$B" | nc 127.0.0.1 8080'
   ```
   判据：外机播报「门锁已开」+ **物理门开**（`result=0` ≠ 物理门开）。
5. **真实响铃 self-unlock（headline，仅在 3 RSS 健康后）** — 设好 HACS 两 switch 防双触发 → **物理按门铃** → 外机播报 + **物理门开** + 从 `/tmp/rust.log`/logread 记 `t_ms`（对照 Go 1258）。3.4/3.5 期间现场观测 `free`，骤降即中止回滚。
6. **auto-hangup 物理 teardown（记录现象非硬 pass/fail）** — 自开锁成功 + auto_hangup on → ~2s 外机 req=708 → teardown；`grep req=708 /tmp/rust.log` + 物理表现。Go 侧本就未闭环（待解1），只记录现象。

验收铁律：**ground-truth = 外机播报 + 物理门真开**，daemon log `result=0` 不算（memory `feedback_protocol_ground_truth`）。

## 结果表（待填）

| 项 | Rust 实测 | Go 基线 | 结论 |
|---|---|---|---|
| automation.state 原值快照（R1b） | `auto_unlock=? auto_hangup=?` | — | （回滚还原参照） |
| HACS 两 switch 测前态 | `auto_unlock=? auto_hangup=?` | — | （回滚还原参照） |
| 即时 RSS | | — | |
| 稳态 RSS（30-60s） | | ~4.6MB | |
| free 余量（稳态） | | — | abort 阈值 ~15MB |
| `/info` JSON | | ✓ | |
| 手动 unlock 物理门开 | Y / N | ✓ | |
| **真实响铃 self-unlock 物理门开** | Y / N | ✓ | headline |
| **t_ms** | | 1258 | |
| auto-hangup teardown 现象 | （记录） | 待解1 | 非硬判 |
| 回滚后 Go `/info` 恢复 | Y / N | — | |
| automation.state 还原确认 | Y / N | — | |
| HACS 两 switch 还原确认 | Y / N | — | |
| 暴露的真机 bug（如有） | （记现象，不在本 change 修） | — | |

## D3 决策产出

验证完成后合成：体型优势（491KB vs 4.0MB，已知）+ 本次 RSS + 物理 self-unlock 可用性 → 更新 `rust_port_roadmap.md` D3（是否移植视频 Phase 5）建议。
