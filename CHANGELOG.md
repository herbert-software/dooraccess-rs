# Changelog

本文件记录 dooraccess-rs 的版本变更。pre-1.0 阶段不维护 CHANGELOG（git 历史为准）；
`v1.0.0` 是首个 git tag、转正生产基线。

> 版本闸门（原定）：`v1.0.0` 在真机 P-SOP 转正验证 + 转正后 procd burn-in（≥72h）通过后才打。
> **实际：2026-06-13 打 v1.0.0 时 procd burn-in 仅 ~1h（0 respawn）；≥72h 门槛由 user 豁免**
> （依据：灰度同款功能码已稳跑 3 天 + procd reboot 自启/0-respawn）。详 `D4_DECISION.md`。
> （见 `dooraccess-go/DEPLOY.md` §Rust 生产部署 + OpenSpec `promote-rust-to-production`）。
> 下列实测占位符在打 tag 前据真机结果填实。

## Unreleased — 视频转发推流维持 + 消费端断开 teardown（`harden-video-sustain-and-teardown`）

修两个共享根因的视频转发缺陷（实证 `samples/anjubao-video-stream/observations.md` §10）：
① 外机对单 `req=704` 仅推 ≈16.3s/~330 帧自停，daemon 不重发 → HA/HomeKit ~16s 后冻帧；
② `StreamWriter` RTP 停后纯阻塞、永不检测消费端 TCP FIN → 连接堆 `CLOSE_WAIT`、session 僵尸、线程泄漏。

### 新增
- **按需 re-invite 维持连续画面**：在既有 session TTL monitor 线程内（**不新增线程**）
  检出「有活跃消费者 + RTP 停 >1s」→ 重发 `req=704` 取下一波推流（session 级单触发）。
  re-invite dial 超时收紧 2s、两次最小间隔 5s 防 thrash、连续 3 次无 RTP → 主动 teardown。
- **FLV 时间轴跨 re-invite 连续**：外机每波换新 SSRC + 新随机 RTP ts，receiver re-lock SSRC
  并打「流分段世代（stream epoch）」标在该波 NAL 上；transmux 检出 epoch 变化即重基准 FLV
  timestamp（新段 = 上段高水位 + ~40ms 一帧间隙），保证输出**单调递增不跳变**，并每段重发
  AVC sequence header 让解码器重初始化。RTCP RR 的 `ssrc_source` 自动跟随新 SSRC。
  换段重发的 seq-header 推迟到本段首 IDR 之前发出，确保用**本段**刷新后的 SPS/PPS
  而非上段旧缓存（Bugbot Low；与 Go 对称）。
- **消费端断开检测 + teardown**：
  - transmux `rx.recv()` → `recv_timeout(500ms)` + socket 写探测（RTP 停时周期探测，broken-pipe → 退出）；
  - httpx 层暴露连接只读克隆（`Request::peek_conn`）；stream handler 起 FIN peek 线程，
    对端半关（`peek` 返 `Ok(0)`）→ close FrameBuffer → StreamWriter 即时退出；
  - 断开后主动 teardown（复用 `Manager::stop` 的 CAS 守卫，解 `CLOSE_WAIT` / 僵尸 session / 线程泄漏）。

### 不变 / 明确不做
- 不碰 RTCP 维持逻辑（实测外机从不开 6671、RR 被 ICMP 拒、原生室内机同样，与维持推流无关）；
  仅按既有 spec 继续 best-effort 发 RR+SDES、终结发 BYE。
- 不改已闭合的显式 `/video/stop` 路径（teardown 序列复用，触发入口不动）。
- MIPS32 红线遵守：`last_rtp_at` 用 `Mutex<Instant>`，包计数用 `AtomicU32`（**禁 64 位原子**）。

### Migration
- 行为变化：`GET /video/<id>/stream.flv` 从「~16s 后冻帧 + 不可收敛僵尸 session」改为
  「连续画面（跨 re-invite 无缝续播）+ 消费端关窗即 teardown」。`POST /video/start`、
  `POST /video/stop` 的请求/响应字节契约不变。无配置项变更，无状态迁移。

### 加固（review 发现）
- **re-invite 竞态守卫**：`tick_reinvite` 发 `req=704` 前复查 session 仍是 `current`
  （`Arc::ptr_eq`），teardown 已摘走 current 则禁发——防晚到的 704 在 708 之后重启外机。
- **消费端断开按 session 身份匹配 teardown**：新增 `Manager::stop_by_id`（按 per-session
  UUID 匹配），stream handler 断开退出改调它——避免同 outdoor URI 会话快速更替时误拆后继会话。
  `Manager::stop`（URI 版）保留供 `/video/stop` 等入口。
- **注释/spec 诚实化**：明确停流期断开检测主路径是 FIN-peek watcher；`recv_timeout`/flush
  写探测探不到半关（向半关 peer 写不报错），仅周期观测 FrameBuffer 关闭；peek 不可用时
  退化为有界兜底（有帧流靠 write-error、停流靠 re-invite-fail/TTL）。
- **种子三元组 epoch 一致性**：`latest_idr` 仅当 SPS/PPS/IDR 三槽 `stream_epoch` 全相等才返
  `Some`，否则 `None`——消除 re-invite 换流首帧三次 push 间 µs 窗里凑出混 epoch 三元组
  （新参数集解旧 IDR slice）导致首关键帧损坏；`None` 复用既有「504 + 不刷 TTL」健康闸。

### Known limitations
- 段间约 ~165ms 起播缝（外机新波 ready 延迟）可接受，不视为流中断。
- 外机预算是时间制还是包数制无法从抓包区分（恒 ~101 pkt/s）；以「RTP 停即 re-invite」修法不依赖该区分。
- 真机 ground-truth 验收（连续画面 ≥60s 无冻帧 / 关窗 `netstat` 无 `CLOSE_WAIT` 堆积 / re-invite 失败兜底）按 DEPLOY.md Rust swap R1-R7 SOP 另行执行。

## v1.0.0 — 2026-06-13（转正生产基线，首个 git tag）

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
- procd burn-in：打 tag 时 procd ~1h、pid 全程未变（0 respawn）、RSS 500–644KB、free 18MB；**≥72h 门槛 user 豁免**
- ⚠️ 版本串：v1.0.0 binary 与转正部署的 0.0.0 binary（574,540B）仅差版本串、功能等价；生产 `/info` 现报 0.0.0（可选重部署 v1.0.0 对齐，见 `D4_DECISION.md`）
