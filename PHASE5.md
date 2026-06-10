# dooraccess-rs Phase5 视频转发栈移植记录

> 变更：`port-rust-video-forward`（OpenSpec，spec-driven）
> 日期：2026-06-10
> 范围：把 Go `internal/video/` 六件视频转发栈 + `http8080/video_handlers.go` 三路由 + main.rs
> 编排 ①v shutdown 插步移植到 Rust，对标 Go v0.10.0 视频层（行为级 + 字节级等价基线）。
> **不**部署 hAP（Gate = dev mock-e2e + 本地 MIPS 体型测量，Phase 7 才灰度真机）；新 BE 面
> （UDP 9880 recv + RTCP 6671 send）登记并 defer Phase 7。crate gate 仅 std + libc；MIPS32
> 红线禁 `AtomicU64`/`AtomicI64`（TTL `Mutex<Instant>`、tunable `AtomicU32` 毫秒）。
>
> 移植决策门：D3「是否移植视频」经 2026-06-10 hAP 真机 self-unlock 验证后定为**移植**
> （证据 `HAP_VERIFY.md`）——无视频 daemon 已证 Rust 体型/RSS 优势，加视频后大概率仍远低于 Go。

## 1. 交付模块清单

| 文件 | 类型 | 内容 |
|---|---|---|
| `src/wire18022.rs` | 扩（纯加 `build_start_frame`） | video 信令 req=704 start 帧（复用 `assemble_frame`，与 `build_stop_frame` 同居共用 golden 基建；与 stop 帧恰差 2 处——req 号 + 尾，golden 断言） |
| `src/video/mod.rs` | 新增 | 模块根 + `LogFn`/`SharedLogFn` 别名（对齐仓内 listener/ha_push log hook 约定） |
| `src/video/preview.rs` | 新增 | `PreviewClient`：TCP 短连接直拨 outdoor:18022、dial 5s + 连接独立 5s deadline（最坏 ~10s，禁单一总闸）、ack 单次 read（64B buf 不循环）、read 0 字节按 silent-FIN 错、`validate_start_ack`/`validate_stop_ack`（magic + req 文本、长度宽松）|
| `src/video/rtp.rs` | 新增 | RTP 头解析（仅校验 V=2/X=0/CC=0，P 位不检、PT 仅解析；`from_be_bytes`）+ Annex-B NAL 提取 + `RtpReceiver` 主循环（`bind_udp` 同步 bind / 1s read-timeout 轮询 stop / expectSrcIP 过滤 / SSRC 首包锁存 / stats ticker 内联）|
| `src/video/reassembler.rs` | 新增 | 专有分片 frame 重组器：8 包重排窗口（int16 差值回绕比较）/ marker 切 frame / 256KB cap / seq gap 丢弃重置 / NAL 字节 move 出 buffer / stats（周期 10s 5 项）。**禁逐包提取**——expA.pcap 实证逐包会静默丢 81% |
| `src/video/frame_buffer.rs` | 新增 | SPS/PPS/IDR 种子缓存 + `latest_idr`/种子 API（订阅 channel 不投种子）+ `subscribe`（`sync_channel(64)` + 退订）+ Push `try_send` 丢帧 fan-out（drop-newest）+ Condvar `wait_idr`（deadline + closed/timeout 分型）+ `close` 语义 |
| `src/video/rtcp.rs` | 新增 | RR(32B，length=7)/SDES(CNAME 字面 `"dooraccess-go"`)/BYE 构造 + 5s 周期 sender 线程（分片 park_timeout、首包 t≈5s 无 0 时刻 tick、SSRC 未知跳过、dial 失败线程退出 session 照常活）+ `send_bye`；golden 逐字节 |
| `src/video/transmux.rs` | 新增 | FLV 静态构造（9B header + PreviousTagSize0 / `pack_avc_decoder_config`（SPS<4B 或 PPS 空返 None）/ seq-header tag / NALU tag（keyframe 判定 + 4B BE length prefix + PreviousTagSize 链））+ `StreamWriter`（header→等首 IDR→SPS/PPS/IDR 校验→seq-header+首 IDR ts=0→订阅增量**仅跳过 SPS/PPS，后续每 IDR 以 keyframe tag 0x17 写出**→逐 tag 写+flush；ts `wrapping_sub`/90）|
| `src/video/session.rs` | 新增 | Outdoor/Caller 解析（复用 codec）+ UUID v4 + 随机 SSRC（`/dev/urandom`，读失败返错不 panic，SSRC 循环重抽至非零）+ Manager（单 active 互斥**持锁跨 bind+preview 信令全程** / 同 outdoor 幂等 / 异 outdoor conflict / 回滚链两段 / 起 RTP+RTCP+TTL 三线程 per-session `Arc<AtomicBool>`）+ TTL `Mutex<Instant>` deadline + 1s monitor + 过期 detached cleanup（monitor 禁自 join，`Mutex<Vec<JoinHandle>>` retain）+ teardown 序列（preview stop→BYE→~50ms 尾等→cancel 三线程 2s 兜底→FrameBuffer close）+ **双驱动竞态「摘 current 比对」CAS 守卫** |
| `src/control.rs` | 扩 | `/video/start`、`/video/stop` **无条件注册**（method_guard_post + require_json_content_type 中间件链 → 405/415）+ `/video/` 前缀路由；start/stop handler（**nil-guard 先于一切 body 解析** / allowlist 精确匹配 `cfg.Stations[*].SIP` / 400×4 分支 / 409 / start 503=preview 超时/其它/nil-guard 三类而 stop 503 仅 nil-guard / 动态路由 `/video/<uuid>/...`（nil-guard 503→malformed 404→UUID 400→405+Allow→资源）/ stream.flv handler（等首 IDR tunable `AtomicU32` 毫秒→504 no-keyframe / 503 session-closed / 504 missing-SPS-PPS，失败分支禁刷 TTL→就绪刷 TTL→`video/x-flv`+`no-store`+chunked per-tag flush→30s TTL refresh 循环）/ snapshot.jpg 503 stub）|
| `src/main.rs` | 扩 | `cfg.video.forward` 门控**构造 Manager**（路由始终注册；false 时 Manager 缺位 → 三入口 503 nil-guard）；shutdown 序列插 ①v 步（**VideoMgr shutdown 先于 join listener/HTTP**，对齐 Go main.go:327-353）；骨架既有不变量不动 |
| `examples/size_probe.rs` | 扩链 | 链入 Phase 5 video 代表函数（`build_start_frame` / `flv_header` / `pack_avc_decoder_config` / `build_avc_sequence_header_tag` / `parse_rtp` / `extract_nals_annexb` / `bind_udp` fp / `FrameReassembler::new` / `FrameBuffer::new` / `build_rr_sdes` / `build_bye` / `parse_outdoor` / `parse_caller`），`black_box` 防 DCE |
| `tests/video_e2e.rs` | 新增 | 3 个 mock 外机端到端（见 §3）|
| `tests/golden_video.rs` / `golden_video_http.rs` / `expa_parity.rs` | 新增 | 跨语言 golden + expA fixture parity（见 §4）|
| `.github/workflows/ci.yml` | 扩 | golden-drift 加 `video` 包 export + `video_{wire,rtcp,flv}.txt` / `http/video_endpoints.txt` 双侧 diff + expA fixture 双侧 `cmp` |

## 2. Go 侧顺手修复（移植对照时发现，仅测试代码 + 注释，不改业务逻辑）

- **5 处陈旧注释修正**（抄代码不抄注释纪律）：`transmux.go:246`（实际仅跳 SPS/PPS 非「SPS/PPS/IDR」）、
  `transmux.go:188-189`（base 实为种子 IDR ts 非首 NAL）、`session.go:487`（50ms 尾等实证在 observations
  §3.4 非 §2.2）、`video_handlers.go:257`（ctx.Canceled 真实触发是 daemon shutdown 非客户端断开，bare httpx
  无 disconnect 检测）、`rtp.go:133`（实为 drop-newest 非丢老 NAL）；可选第六处 `httpx/server.go:157`
  （「可检测 client disconnect」表述陈旧，watcher 仅看 closing）。
- **golden export 测试新增**：`internal/video/export_golden_test.go`（`BuildStartFrame`/`BuildStopFrame` 含 2
  字节差、RTCP `buildRR`/`buildSDES`/`buildRRSDES`/`buildBYE`、FLV header / `PackAVCDecoderConfig` /
  seq-header tag / NALU keyframe+inter tag）+ `internal/http8080/export_golden_video_test.go`（HTTP 错误体全集，
  错误消息字面即契约：start 200 + 400×4 + 409 + 503×3 + 405+Allow + 415，stop 同集，动态路由 404×3 + UUID 400，
  stream.flv 504×2 + 503，snapshot 503 stub）。
- **`tests/integration` 两处 pre-existing `httptest`→`httpxtest` 编译错修复**（v0.8.0 `slim-http-stack-bare-net`
  漏改的遗留——`video_e2e_test.go` / `video_lifecycle_test.go` 引旧 stdlib `httptest`，bare-net 迁移后该路径已不存在，
  导致 `go test -tags fixtures ./tests/integration/` 编译失败；改用仓内 `httpxtest` 后整包编译通过——验证：
  `go test -tags fixtures -run xNONEx ./tests/integration/` = `ok [no tests to run]`）。

## 3. mock 外机 e2e（dev 认证，行为锚 Go video e2e 测试集）

`tests/video_e2e.rs` 3 个端到端（mock 18022 ack server + UDP replay `testdata/rtp-fragmentation/expA.packets.tsv`
子集 + mock UDP 6671 RTCP receiver，全 tunable 压缩 TTL/monitor/tail-wait，**禁真实 60s/5s 等待**）：

- **`e2e_start_stream_fanout_stop_bye`**：`Manager::start`（真 PreviewClient 指向 mock、`rtp_listen_addr`
  随机端口）→ HTTP `GET /video/<id>/stream.flv`（经 control server，最接近 HACS 真实拉流）→ 断言：
  - **FLV 字节流前缀**（13B 头部块 + seq-header tag ts=0）逐字节 byte-exact
  - **输出含 ≥2 个 keyframe tag（0x17 且 AVCPacketType=0x01）+ tag 计数**符合 replay 内容（种子 IDR + stage 2 第 2 个 IDR）
  - **多消费者 fan-out**：消费者 A 先入场起播、消费者 B 后入场经 `latest_idr` 缓存立即种子起播，双订阅同流（A/B FLV 字节流逐字节相等）
  - **stop/bye teardown**：POST `/video/stop` → 200 `{"result":0}` + 发出 req=708 + mock UDP 6671 收到 RTCP **BYE 复合包**（逐子包按 length 步进确认 PT=203）
- **`e2e_slow_consumer_does_not_block_fast`**：fast 贪读 + slow 发请求后**一字节不读**（handler 写满 socket
  缓冲后阻塞/丢帧）→ 全量 replay（1.4MB > loopback 缓冲）→ 断言 fast 持续进流（≥2 keyframe + ≥150 tag，慢消费者
  不拖死 fan-out）→ 收口先掐 slow 解阻塞再 stop teardown（BYE 到达）。
- **`e2e_ttl_expiry_detached_cleanup`**：TTL 压缩到百 ms 级 → session 不被显式 stop → TTL 过期 monitor 触发
  **detached cleanup 自动收场**（Manager 对账确认 session 摘除、线程回收、BYE 发出）。

## 4. golden + parity + 全量测试计数

`cargo test`（**非沙箱**环境——沙箱拦 loopback TCP/UDP bind 致网络测试假失败）全通过：

| 测试集 | 计数 | 内容 |
|---|---|---|
| lib 单测 | **308 passed** | Phase 1-4 全不回归 + Phase 5 video（rtp/reassembler/frame_buffer/preview/rtcp/transmux/session + wire `build_start_frame` + control video handlers）|
| `src/main.rs` 单测 | **4 passed** | `main_impl` flag/config 退出码包装（不回归）|
| `tests/daemon_e2e.rs` | **19 passed** | Phase 4 ①+② daemon mock-e2e（不回归）|
| `tests/video_e2e.rs` | **3 passed** | §3 mock 外机端到端 |
| `tests/golden_video.rs` | **3 passed** | wire start/stop 帧 + RTCP RR/SDES/RRSDES/BYE + FLV header/seq-header/NALU tag 逐字节对 Go export |
| `tests/golden_video_http.rs` | **1 passed** | HTTP 错误体全集逐字面对 Go export（`video_endpoints.txt`）|
| `tests/expa_parity.rs` | **1 passed** | 读 `testdata/expA.packets.tsv` 灌重组器，对 `expected.json` 11 项 Go 断言（frames_complete=296 / nals_total=326 等）逐项断言 |
| 其余跨语言 golden（codec/config/http/wire/state/bpf/listeners）| 不回归全过 | Phase 1-4 集成 |

**golden 文件清单**（新增）：`testdata/golden/video_wire.txt`（start/stop 帧）、`video_rtcp.txt`（RR/SDES/BYE）、
`video_flv.txt`（FLV header/seq-header/NALU tag）、`http/video_endpoints.txt`（HTTP 错误体全集）、
`testdata/rtp-fragmentation/expA.packets.tsv` + `expected.json`（从 Go 仓静态复制的语言无关 fixture，CI 双侧 `cmp`）。

**`cargo clippy --all-targets` 零警告**。**`cargo tree -e normal` 外部 crate 仅 `libc`**（dev-dep 不算）；
拒 tokio/crossbeam/mio/nix/pnet/socket2/ffmpeg 绑定（video 全栈纯 std + 手写 FLV/RTP/RTCP）。

## 5. MIPS 体型（Phase5 gate 数据）

构建路线沿用 Phase 0-4（nightly + `-Z build-std=std,panic_abort` + OpenWrt ath79 SDK musl sysroot +
`rust-lld` 经 `scripts/link-mips.sh`，全静态非 PIE，`panic=immediate-abort`）。

| 产物 | ② self-unlock 基线 | **Phase 5 video 链入** | 增量 | `file` / FP ABI |
|---|---|---|---|---|
| size_probe（代表函数链入体型测量载体） | 341,292 | **347,516** | +6,224 | ELF 32-bit MSB MIPS, static, stripped, **FP ABI Soft float (0x3)** |
| 真 daemon bin `dooraccess-rs`（实际 ship 物） | 492,284 | **563,228** | **+70,944** | ELF 32-bit MSB MIPS, static, stripped, **FP ABI Soft float (0x3)** |

`make verify-mips` 通过（断言 ELF 32-bit MSB MIPS / statically linked / FP ABI Soft float）。

**体型说明**：真 daemon bin 从 492,284（② 无视频）→ **563,228**（+70,944 = 视频全栈 RTP/重组/FLV transmux/
RTCP/session/preview + 三 HTTP 路由），仍**远在 `< 4.0MB` 优先目标内**（roadmap 上限 < 5.0MB）。对照 Go
v0.10.0 MIPS daemon = 3,997,853 字节（含全部网络/HTTP/video）——Rust 全功能 daemon 563KB ≈ Go 的 **1/7.1**，
D3 决策门「即便加视频 Rust 仍远低于 Go」实测成立。size_probe 增量小（+6,224）因其只链代表函数指针/纯函数
（不含 control handler/session Manager 全展开），daemon bin 是实际 ship 物的权威体型数。

**CI qemu-mips（BE）单测**：本机 macOS 无 qemu-user（结构性已知，见 memory `rust_be_testing_no_qemu_user_macos`），
本地不可跑。BE 第二层防御 = `#[cfg(target_endian="big")] const _: () = assert!(...)`（ffi.rs htons/from_be_bytes/
struct 宽度），由 `make build-mips` const-eval 按 mips BE 目标端序强制——**本地 `make build-mips` 通过即认证
BE layer-2**（编译期失败即写错端序）。video 层 RTP `from_be_bytes` / RTCP NBO 写入在 BE 下正确性同受此门约束。
CI `mips-build` job 在 push 后复跑 `make build-mips` + `make verify-mips` + golden-drift（含 video 包）——
**push 后 CI 复验，本地 best-effort 已过**。

## 6. 范围红线确认（均守住）

- **不部署 hAP**：Gate 止于 dev mock-e2e + 本地 MIPS 体型测量（Phase 7 灰度按 DEPLOY.md S1-S12）。
- **crate gate 守住**：`cargo tree` 仅 `libc`；video 全栈零新外部依赖（FLV/RTP/RTCP/H.264 分片重组全手写）。
- **骨架不变量不动**：`git diff` 自查 listener I/O 零触碰；main.rs shutdown 仅插 ①v VideoMgr 步（先于 join
  listener/HTTP），骨架既有 ①-⑤ 排序不变。

## 7. 已知差异登记（移植等价边界，逐条对齐 Go）

1. **bad-JSON 错误字面残余差异**（golden 33 行集**外**）：unmarshal type-error 消息、非 ASCII quoting、
   `/unlock` 等旧端点仍用 Phase 2 校验器字面——两套校验器并存（video 端点用 Phase 5 校验器、旧端点用 Phase 2），
   **统一留后续 change**。不影响 video golden 集逐字节一致。
2. **start 无 handler 级 10s 总闸**：结构性 ~10s 等价（preview dial 5s + conn 5s）；stop 已有 8s teardown
   总预算（stop_budget）、TTL cleanup 5s、shutdown 共享 caller deadline。已合入一个 teardown deadline 预算修复。
3. **Start 回滚路径 StopPreview 3s 为 per-segment**（最坏 ~6s vs Go 3s 总 cap）——UUID/SSRC 失败回滚链分两段，
   best-effort StopPreview 每段独立 3s。
4. **RTP final stats 行退出必打**（Go 仅取消路径打且 racy）——spec 已登记为确定性超集（Rust 退出统一打 final
   stats，比 Go racy 行为更确定）。
5. **SnapshotFLV / SeedNALs 不移植**（Go 生产死代码，有意省略）——snapshot.jpg 端点仅返 503 stub（body golden 字面），
   与 Go 一致（Go 侧 snapshot 也走 HA stream-source ffmpeg fallback，daemon 不产 JPEG）。
6. **TTL deadline `Mutex<Instant>` 单调时钟**（vs Go wall-clock `atomic.Int64`）——spec 登记的有意差异
   （MIPS32 禁 `AtomicI64` + 单调时钟抗 wall-clock 跳变）。
7. **reassembler stats 无 Mutex**（ticker 内联同线程）——design 登记的有意简化（单消费线程无并发竞争）。

## 8. 新 BE 面登记（defer Phase 7 真机验）

本 change 引入 listener I/O 零改动伞**外**的新 socket/字节序路径，dev[LE] 测不出 BE 正确性，须 defer
Phase 7 真机验（与 BE-listener / `send_udp_response` / `resolve_iface_list` 一并）：

- **UDP 9880 recv（`video::rtp::RtpReceiver`）**：新 UDP socket 接收 + RTP 头 `from_be_bytes` 解析。std UDP
  非 PF_PACKET，但接收路径 + 网络字节序解析 dev 测不出 BE。
- **RTCP 6671 send（`video::rtcp::RtcpSender`）**：新 UDP socket 发送 RR/SDES/BYE 复合包 + NBO 字段写入。
  std UDP 发送路径 + 网络字节序，dev 测不出 BE。

两处均 **std UDP（非 PF_PACKET 新 socket-level BE 面）**，BE 正确性由 §5 编译期 const 断言（`from_be_bytes`/
NBO 表达式）+ Phase 7 真机视频流验证双重保障。**SnapshotFLV 有意省略登记**（Go 生产死代码，不移植，见 §7.5）。

## 9. 结论

`port-rust-video-forward`（Phase 5）**通过**：video 全栈移植（wire18022 start 帧 / PreviewClient 信令 / RTP
receiver + 专有分片重组器 / FrameBuffer 种子缓存 + fan-out / RTCP keepalive+BYE / FLV transmux + StreamWriter /
session Manager 生命周期 + TTL detached cleanup / 三 HTTP 路由 / main.rs ①v shutdown 插步）。**cargo test 全过
（308 lib + 4 main + 19 daemon_e2e + 3 video_e2e + 跨语言 golden video 5 + expA parity 1，0 失败）+ clippy
--all-targets 零警告 + crate gate 守住（仅 `libc`，video 全栈零新依赖）+ MIPS 交叉编译（真 daemon bin
563,228 字节 / size_probe 347,516 字节，均 BE(MSB)/softfloat/全静态，daemon bin ≈ Go 1/7.1）+ 范围红线守住
（不部署 hAP / 骨架不变量不动 / listener I/O 零触碰）**。Go 侧顺手修复 6 处陈旧注释 + golden export 测试 +
`tests/integration` 两处 pre-existing `httptest`→`httpxtest` 编译错（v0.8.0 漏改）。已知差异 7 条逐条登记
（bad-JSON 字面残余 / start 无总闸结构性等价 / 回滚 per-segment / RTP final stats 超集 / SnapshotFLV 不移植 /
TTL 单调时钟 / reassembler 无 Mutex）；新 BE 面（UDP 9880 recv + RTCP 6671 send）登记 defer Phase 7。

剩余收尾：

- `dooraccess-rs` 子仓 Phase 5 git commit + tag（git 写操作留人工）。建议 tag：`rs-v0.5.0`（或仓内 PR 合并后
  按 `Phase 5: video forward (#N)` commit 题打），时机 = 用户 review 本组交付 + commit 后。
- CI `mips-build` + golden-drift（含 video 包）push 后复验（本地 best-effort 已过：`make build-mips` +
  `make verify-mips` + 本地 golden 全过）。
- 视频层 BE 面（UDP 9880 recv + RTCP 6671 send）+ 真实视频流稳定性（stream 60s+ 长连接、多消费者）留 Phase 7
  真机验（与 BE-listener / self-unlock 物理门开一并；ground-truth = 外机播报 + 物理视频流，非 daemon log）。
