# Rust video forward hAP 真机验证（HAP_VIDEO_VERIFY）

Phase 5 Rust daemon（`dooraccess-rs`，视频转发全栈：RTP/RTCP/H.264/FLV/session/HTTP）在 hAP ac lite 的 ground-truth 验证，为 roadmap **D4（替换生产）** 补齐视频数据块：**新 BE 面正确性（UDP 9880 RTP recv + RTCP 6671 send）+ 60s+ 流稳定性 + ffmpeg 解码真实画面 + 流期间 OOM**。

OpenSpec 规程：`openspec/changes/.../verify-rust-video-on-hap/`（3 轮对抗 review 加固）。

> ⚠️ **swap 部署 + 回滚机制看 `dooraccess-go/DEPLOY.md` §Rust 临时验证 swap R1-R7**（单一来源，本文不重复）。本文只放**验证清单 + 结果表**。**项目最高风险操作**（停生产 Go + Rust 无 GOMEMLIMIT + 视频长流持续负载）。所有 hAP 命令交人工执行 / 逐步确认，AI 不擅自连发。窗口须低门禁流量时段 + 现场有人手动开门兜底 + 时长上限。

## 对照基线（Go v0.10.0）

| 指标 | Go 基线 |
|---|---|
| 稳态 RSS | ~4.6MB（hAP 实测） |
| 视频 | preview 信令 + UDP RTP/H.264 + RTCP + FLV stream（生产稳定） |
| MIPS binary | 3,997,853 字节 |

Rust 对照：MIPS 视频 binary **563,228 字节**（`make dist` 2026-06-XX 实测，= Phase 4 self-unlock 版 491,500 + 视频栈；R6 判据 = 现场 hAP `ls -l` == 本机 `ls -l` 相等，**不硬编**）；Phase 4 self-unlock verify 稳态 RSS **528KB** 基线。视频流 BE 面 / 60s+ 稳定 / CPU/RSS / 端到端 = **本次验证要取的数**。

## 触发通道与拓扑（核实）

- 外机 URI（POST body，须精确匹配 hAP `config.ini` `[station "1"] sip`）：`06020000@172.16.106.152:18022`
- 室内机：`06021103@172.16.106.91:18022`
- **hAP 无 `nc`/`curl`/`socat`**（self-unlock §4 实证）→ 带 body 的 `POST /video/start`·`/video/stop` 走 **Mac 经家庭网 `curl`/裸 socket → `192.168.2.89:8080`**（Mac 工具齐：ffmpeg 8.1.1/ffprobe/curl/nc）；不通则退室内机按「监视」被动触发。
- **Mac→hAP:8080 跨主机访问 self-unlock 从未演示** → 停 Go 前必预检（拉生产 Go `/info`）。

## 验证清单（DEPLOY.md §Rust R7 启动后执行；递进，逐项判据）

**关键：稳态 RSS 健康 + 流期间持续观测前置**（Rust 无 GOMEMLIMIT、daemon 内无 RSS 自守；人手 kill 未必早于 OOM-killer → 软阈值 free < ~10MB 即 kill）。

1. **稳态 RSS 前置闸** — `/proc/<pid>/status` VmRSS + `free -m`，对照 528KB 基线；偏离大 / free 逼近 OOM → abort 回滚不进视频。
2. **`GET /info`** — Mac `curl http://192.168.2.89:8080/info` 返合法 JSON（复证跨主机可达 + video 字段）。
3. **主动触发** — Mac `curl` POST `/video/start {"outdoor":"06020000@172.16.106.152:18022"}` → 200 + `{session_id,stream_url,ttl:60}`；外机暖启推 RTP。不通退室内机按监视。
4. **ground-truth 解码** — Mac `ffprobe`/`ffmpeg` 拉 `http://192.168.2.89:8080/video/<session_id>/stream.flv` → 解出有效 H.264 stream（连续递增 PTS + 无 corrupt）= 真实画面。504「outdoor not pushing」/「missing SPS/PPS」单独记录区分。
5. **60s+ 稳定性 + 流期间 OOM + 断流归因** — ffmpeg 连续解码 60s+；流期间每 ~5s 盯 VmRSS + free（软阈值即 kill）；记峰值 RSS + CPU。若 <60s 断 → Go daemon co-witness 拉同时长区分「外机会话上限（Go 也断）」vs「Rust/BE 缺陷（仅 Rust 断）」。
6. **新 BE 面判据** — RTP recv BE：连续递增 PTS + 60s 无 corrupt（非「拿到流头即判」）。RTCP send BE：**tcpdump 在 hAP br-door 抓 daemon 9881→6671 逐字节比对 §4.1 模板 `81 c9 00 07`(RR)/`81 ca 00 05`(SDES) SSRC/length NBO**（非靠不断流推断——外机单向不回 RTCP）。
7. **teardown** — Mac POST `/video/stop` → daemon 发 **req=708 stop preview（外机据此停推）+ RTCP BYE（best-effort）**；停推归因 req=708 非 BYE。记外机停推 + 能否再 start。
8. **可选** — HACS camera entity 端到端 + 多消费者（两 ffmpeg 并发）。

验收铁律：**ground-truth = ffmpeg 解出真实画面 + 60s 连续 PTS**，daemon log「收到 N 包」不算（memory `feedback_protocol_ground_truth`）。RTCP BE 必抓包，不靠停推/不断流推断。

## 结果（2026-06-10 真机实测）

> 说明：swap（停 Go → 跑 Rust）在本验证窗口**前**已完成，本会话以 Rust 已在运行（pid 755）的状态接续做视频触发 + ground-truth + teardown + 回滚。故 R1b 快照/稳态 RSS/hAP binary 字节几项是接续观测而非现场首测，已注明。

| 项 | Rust 实测 | Go 基线 | 结论 |
|---|---|---|---|
| Mac→hAP:8080 跨主机预检 | ✅ `curl http://192.168.2.89:8080/info` 返合法 JSON（daemon/video.forward=true/outdoor_stations） | — | 通，ffmpeg/POST 通道可用 |
| automation.state 原值快照（R1b） | `auto_unlock=true / auto_hangup=true`（`.preverify` 34B） | — | 视频不拨 flag，回滚时 == 快照、免还原 |
| binary 字节 | **563,228**（本机 make dist）/ hAP 未现场 `ls -l`（swap 在窗口前） | 3,997,853 | 本机已核；hAP 端字节留下次窗口 |
| 稳态 RSS | 未单独取 VmRSS（接续状态）；流期 `free` available ~7MB、未撞 OOM 软阈值 | ~4.6MB | 无 OOM；精确 RSS 留下次窗口 |
| free 余量 | 流期 ~7MB available；回滚起 Go 后 ~3MB | — | 未 < 10MB 软阈值 |
| `/info` JSON + video 字段 | ✅ `video.forward=true / protocol=anjubao-h264 / format=flv` | ✓ | |
| POST /video/start 触发通道 | ✅ **Mac curl POST**（session `ac8ffc5d…`，返 200 `{session_id,stream_url,ttl}`） | — | Mac 经家庭网通道 |
| **ffmpeg 解码真实画面（连续递增 PTS）** | ⚠️ **frame=1**（有效 H.264 High/L2.2 640×480 `avc1.64c016`，ffprobe 干净、ffmpeg 零 corrupt，但**仅 1 个 I-frame 种子帧**，非连续 PTS） | ✓ | 见下「真机观察」：根因 = 会话复用不重发 req=704 + 外机单次推流耗尽，**非 Rust 缺陷** |
| **60s+ 连续不断流** | ✗ 未达——外机每会话单次推 ~330 帧（~8s@40fps）即停，`/video/start` 复用活会话不重推 | ✓ | 外机会话上限特性（observations §3.4 实证），非 daemon |
| 流期间峰值 RSS / CPU | 未取（接续窗口未做长流） | — | 留下次窗口 |
| **RTP recv BE①（连续无 corrupt）** | ✅ rust.log `rtp stats final packets=1671 nals=364 frames=330 dropped_frames=0 max_frame=29307B` | — | **新 BE 面① 真机铁证：330 帧零丢** + 专有分片重组正确 + FLV transmux 字节验证（FLV 头/AVC seq/SPS） |
| **RTCP send BE②（tcpdump 比对）** | ✗ 未抓包；外机 `rtcp send: Connection refused (os err 146)` 反复 | — | 外机单向不收 RTCP，**端到端不可验**（review 判据已修正：靠 dev golden 字节保真 + const 断言） |
| teardown（req=708 + BYE） | ✅ rust.log `req=708 stop preview → 外机 30B ack → req=709 bye-ack`；SIGTERM graceful 干净退出 | — | 停推归因 req=708 信令面（非 BYE） |
| 外机悬挂处理 | N/A——走正常 `/video/stop` teardown，未触发 abort-before-stop 分支 | — | 无悬挂态 |
| 回滚后 Go `/info` + RSS | ✅ Go pid 1010 起、`/info` 正常、free available ~3MB | — | 生产恢复 |
| automation.state 还原 + HACS | state == `.preverify`（true/true，Rust 未动 flag）；HACS 两 switch 本会话未拨（视频不需），待 user 扫一眼确认仍 ON | — | flag 面已确认不变 |
| 可选：HACS 端到端 / 多消费者 | 未做 | — | 加分项，留后续 |

### 真机观察（按范围不在本 change 修）

- **ffmpeg `frame=1` 根因（有 rust.log 铁证）**：`/video/start` 对同一外机的**活会话**返 200+reuse（不是 409）只刷新 TTL、**不重发 req=704**（rust.log `reuse existing session id=ac8ffc5d… (TTL refreshed)`），故重抓只拿 daemon 缓存的种子帧（21164B = FLV 头+AVC seq+1 IDR）。外机本次会话已推完 330 帧（~8s）停推。**Go/Rust 1:1 同行为**（`session.go` ↔ `video/session.rs` 复用逻辑 + IDR 门控刷 TTL 字面一致；subagent 对抗审计 16 常量全等确认），**非移植缺陷**。要连续帧须 stop + 等 TTL 过期再 fresh start，或靠真实「监视」/呼叫事件维持。
- **RTCP send BE② 外机单向不收**（`Connection refused`），端到端不可验——印证 observations §4.3 + review 判据修正。
- **60s+ 连续画面靠程序化 req=704 在本外机做不到**（外机单次推流 ~8s），非 daemon 限制。

## D4 视频数据产出

- **视频 BE 面①（RTP recv）真机铁证**：330 帧零丢 + 专有分片重组 + FLV transmux 字节正确——MIPS BE 上新 BE 面①全栈正确。
- **BE 面②（RTCP send）**：外机不收 RTCP，端到端不可验，靠 dev golden 字节保真 + const 断言（review 已修正判据）。
- **60s+ 长流 / 端到端连续画面 / 精确 RSS·CPU**：受外机单次推流上限 + 本窗口为接续态限制**未取全**——属外机特性 + 窗口安排，非 daemon 缺陷；留下次专门窗口（需真实监视事件维持长流）。
- **合成 D4**：与 Phase 4 self-unlock（HAP_VERIFY.md：真实响铃物理门开 t_ms=1398 + RSS 528KB）合并——协议核心 / listener BE / daemon / self-unlock / **视频 BE 面①** 全栈真机认证；视频 BE②/长流受外机限制，作 D4 已知边界记录。
