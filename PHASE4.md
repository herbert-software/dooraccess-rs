# dooraccess-rs Phase4 daemon 编排骨架移植记录（①）

> 变更：`port-rust-daemon-skeleton`（OpenSpec，spec-driven）
> 日期：2026-06-09
> 范围：把 Phase 1-3 已交付并认证的协议/IO 积木接成一个**能起、能手动开锁、能 push、能持久化
> automation flag** 的可运行 daemon，对标 Go `cmd/dooraccess-go/main.go`（800 行）。
> **不**接 ring 触发的 self-unlock 消费者（filter+debounce→Unlock job）/ `ExecuteUnlockFromRing`
> 测量 / 双触发消除 / auto-hangup 外机 req=708 延迟发送（均隔到姊妹 change ②
> `port-rust-self-unlock-consumer`）；**不**接 video（Phase 5）；**不**部署 hAP（Gate = dev mock-e2e
> + 本地 MIPS 体型测量，Phase 7 才灰度）。
>
> Phase 4 拆 2 与 Phase 3 同思路：在「self-unlock 触发」这条正交边界切一刀，让骨架主体先合、不被
> self-unlock 的 debounce/双触发消除/channel-drop 对账阻塞。

## 1. M1.5 并发模型落地

并发模型经 Software Architect subagent 权衡（M1/M2/M3/M4/M1.5 五选项）+ 用户拍板 **M1.5**：保留已
hAP 真机认证的 per-slave listener 线程（I/O 层零改动、零重新认证），把 Go 的 self-unlock/hangup
goroutine + `wireMu` + `context` 树**塌缩为单 wire-worker 线程 + `mpsc::Sender<Job>` 队列 + 一个
`Arc<AtomicBool>`**；push 保持 detached（off-worker）。

| Go main.go 原语 | Rust 落地 | 说明 |
|---|---|---|
| self-unlock/hangup goroutine + handler 直发 wire | **单 wire-worker 线程**（`daemon::run_worker`）| 所有 18022 wire 出站收敛到唯一执行流 = 结构性串行 |
| `wireMu` 互斥锁 | **删除** | 单 worker 发 wire，互斥从「运行时保证」降级成「结构性事实」；`execute_unlock` 仍接 `&Mutex<()>` 签名占位（worker 跑时 `lock()` 零竞争），防未来加第二 worker |
| `context` 取消对象树 | **单 `Arc<AtomicBool>`**（仅取消）| 复用为 worker loop 退出 + listener recv 循环 + `execute_unlock` 的 `cancel` + detached push honor。**超时另由** `execute_unlock` 的 `deadline: &dyn Deadline` 承载（注入 `InstantDeadline::new(总cap)`），AtomicBool 只取代取消那一半 |
| `go func(){ pusher.Push() }()` | **detached push 线程**（`daemon::spawn_push` / `spawn_diagnosis_push`）| off wire-worker：HA 离线的慢 push 不 head-of-line 阻塞排队 Unlock（破坏 ② self-unlock ~1.3s 关键路径） |
| `pushWG sync.WaitGroup` | **`PushTracker`（`Arc<Mutex<Vec<JoinHandle>>>`）**| std-native，非 Condvar。**运行期回收**：每次 register 前 `retain(\|h\| !h.is_finished())`，Vec 大小 ≈ 并发 push 数（O(1)）非累计 push 数，防 64MB 多日运行内存泄漏（RC-F6） |
| `fatalErrCh chan error` + `drainFatal` | `Mutex<Option<Error>>` / 线程 join 返回 Err | 跨线程 fatal 上报不需 channel，一个 Mutex 槽够 |
| `videoMgr` 构造 + 三处 Shutdown | **省略** | video = Phase 5 OUT；故本骨架 shutdown 序列比 Go 有意更短（无 video teardown 步），非遗漏 |
| `var wg`（listener/HTTP 线程 join） | `Vec<JoinHandle>` join | 线程 join，非 push 跟踪 |

**`Job` enum 只含 wire-bound 变体**（决策 3，spec gate）：`Unlock(UnlockJob)` + `Shutdown` 哨兵。
**无 `Push` 变体**（push 走 detached 线程）、**无 `Hangup`**（留 ②，骨架仅为其留排空契约口）。worker
退出取**显式 `Job::Shutdown` 哨兵 + break**（决策 8），不依赖「drop 全 sender → Disconnected」——HTTP/
号码查询/OnDetect 都持 `Sender<Job>` clone，靠全 clone 释放易随 ② 增长漏 clone。

## 2. 交付模块清单

| 文件 | 类型 | 内容 |
|---|---|---|
| `src/main.rs` | 改写（探针 → daemon 编排，+559/−236） | `--config` flag 解析（默认 `/etc/dooraccess-go/config.ini`）+ `main_impl(args, stderr) -> exitcode` 可测包装；config 加载 + 缺失/废弃字段告警；退出码（flag 失败 **2** / config 失败 **1**）；`hass.token` 配但 `hass.api` 缺 → log 继续；SIGHUP 忽略 + SIGTERM/SIGINT 注册；依赖构造 + 线程编排 + banner + startup push + graceful shutdown 钉死排序 |
| `src/daemon.rs` | 新增 | M1.5 骨架：`Job`/`UnlockJob` enum、`PushTracker`（运行期回收）、`spawn_push`/`spawn_diagnosis_push`（detached + honor shutdown + 500ms 站稳）、`WorkerDeps`、`run_worker`（`for job in rx { Unlock => execute_unlock, Shutdown => break }`）、`submit_job`（容忍 `SendError` 禁 unwrap） |
| `src/orchestration.rs` | 新增（从 main.rs 上移以便 e2e 测真生产函数） | `load_automation_flags`（优先级 state>config>env）、`load_state_file`（损坏整文件丢弃降级）、`build_listen18022`（OnDetect 安装：每帧 FormatLog + req=704 门铃 push）、`build_number_query_callback`（号码查询命中响应/未命中 drop）、`WorkerUnlockDispatch`（手动 unlock → wire-worker 派发缝 + reply channel）、`worker_unavailable_outcome`、`parse_stations_to_ip_map`、`ipv4_str`/`parse_ipv4` |
| `src/automation_state.rs` | +138/−4 | 补 Phase 1 defer 的 **`Persister`**：`persist()` 读运行时 atomic 真值（非快照）+ 内部锁串行化（HTTP 线程并发拨动）+ **纯值去重**（同值 no-op、翻转即写，逐行对齐 Go `Persist`，**无时间窗节流**）；`write_atomic`（写 `.tmp` 同目录 → `rename`，断言同目录跨 fs 非原子）；落盘失败 best-effort log 不阻塞不 crash |
| `src/control.rs` | +211/−31 | `handle_unlock`/`respond_business_result` **改写**（非平凡）：从「handler 局部直调 Sender + `AtomicBool::new(false)`」改为「产 `Job::Unlock` 投 worker + 一次性 reply channel 等 `UnlockOutcome` + 4 分支 HTTP 映射」（OK / Err result / bye→silent-FIN→-103 / wire-failure），worker panic 时 reply Sender drop → handler 收 Disconnected 不永等；business-result push 经 `spawn_push` detached 发 |
| `src/listen18022.rs` | +301/−0（**纯加 helper，I/O 零改动**） | `format_log`(req/src/dst/body/dir) + `infer_direction` + `req_note` + `Direction` Display（OnDetect 诊断纯函数，golden 可测）；PF_PACKET socket/recv/BPF 路径**字节级未触碰** |
| `src/listen6672.rs` | +60/−0（**纯加 helper，I/O 零改动**） | `send_udp_response`/`send_udp_response_to`（号码查询 UDP 单播发送原语，**新 BE 面**——UDP socket 发送 + NBO 写入不在 listener I/O 零改动伞下，BE 正确留 Phase 7）；PF_PACKET I/O 路径未触碰 |
| `src/httpx/server.rs` | +61/−0 | Phase 2 遗留的 server graceful shutdown（accept 持锁）死锁修：使 HTTP 线程能在 shutdown 时退出被 join |
| `examples/probe.rs` | 新增（从旧 `src/main.rs` 迁） | Phase 0-3 passive-shadow 探针逻辑零回归迁出（决策 7：探针是 hAP BE 认证复现工具，保留为 example 零成本，`cargo build --example probe` 仍构建）|
| `examples/size_probe.rs` | 扩链 | 链入 Phase 4 骨架代表函数（`run_worker`/`spawn_push`/`submit_job`/`PushTracker` / `load_automation_flags` / `build_number_query_callback` / `build_listen18022` / `worker_unavailable_outcome` / `Persister::new`+`persist` / `write_atomic`），`black_box` 防 DCE |
| `tests/daemon_e2e.rs` | 新增 | 16 个 daemon mock-e2e（见 §3）|

## 3. mock-e2e 结果（dev 认证，行为锚 Go 测试集）

`cargo test`（非沙箱环境，沙箱拦 TCP bind 致网络测试假失败）全通过：

- **lib 单测 180 passed**（Phase 1/2/3 全部不回归 + Phase 4：daemon worker/PushTracker/spawn_push、
  orchestration flag 加载/OnDetect builder/号码查询 callback、automation_state Persister 去重/原子写）
- **main.rs 单测 4 passed**（`main_impl` flag/config 退出码包装）
- **`tests/daemon_e2e.rs` 16 passed**：
  - `t10_1` daemon 启动/停止 + GET `/info` + `/automation` 正确 + graceful shutdown 排空
  - `t10_2` 手动 unlock 经 worker 跑通 + result 回灌（+ reply 多路径）
  - `t10_3` automation flag 三级优先级（state 优先 / state 损坏降级 config / config 压 env）
  - `t10_4` Persister 拨动→原子落盘（temp+rename 无半写）→重启回读一致 + 落盘失败不阻塞
  - `t10_5` startup push（flag 加载后才 push automation_state + 字符串编码 / diagnosis 500ms 延迟期 shutdown 放弃）
  - `t10_6` 号码查询（命中响应 / 未命中 drop）
  - `t10_7` worker 忙时第二个 `/unlock` 排队 + 期间 `/info`/`/automation` 仍响应
  - `t10_8` shutdown 不死锁 + worker panic 经 reply Sender drop 解除阻塞 handler
  - `t10_9` 门铃 push（req=704 src∈cfg.Stations dst==室内机 → detached `event=ring`；src 不在 cfg.Stations → log dropped 不 push）
  - `t10_10` push 不阻塞 unlock（mock HA 离线 push 挂 5s，排队 Unlock 不被 head-of-line 阻塞）
  - `t10_11` `PushTracker` JoinHandle 回收有界（`retain(!is_finished)` 不泄漏）
- **跨语言 golden + Phase 1/2/3 集成测试 35 passed**（golden_bpf 2 / codec 4 / config 5 / http 8 /
  listeners 5 / state 8 / wire 3，含新增 `persist_*` / `write_atomic_*` 状态 golden），全部不回归
- **`cargo clippy --all-targets` 零警告**（附带修了 config.rs test helper 的 `field_reassign_with_default`）

## 4. MIPS 体型（Phase4 gate 数据）

构建路线沿用 Phase 0-3（nightly + `-Z build-std=std,panic_abort` + OpenWrt ath79 SDK musl sysroot +
`rust-lld` 经 `scripts/link-mips.sh`，全静态非 PIE）。**Makefile `MIPS_BIN` 从旧 `dooraccess-rs-probe`
更正为 `dooraccess-rs`**（Phase 4 bin 名 = daemon，探针迁 example）。

| 产物 | 字节数 | `file` / FP ABI |
|---|---|---|
| Phase 3 size_probe（基线） | 104,892 | ELF 32-bit MSB MIPS, static, stripped, Soft float |
| **Phase 4 size_probe（链入骨架全栈）** | **312,028** | ELF 32-bit MSB MIPS, static, stripped, **FP ABI Soft float (0x3)** |
| **Phase 4 真 daemon bin `dooraccess-rs`（实际 ship 物）** | **457,004** | ELF 32-bit MSB MIPS, static, stripped, **FP ABI Soft float (0x3)** |

`make verify-mips` 通过（断言 ELF 32-bit MSB MIPS / statically linked / FP ABI Soft float）。

**体型说明**：size_probe 从 104,892 → 312,028 的增量来自骨架把 httpx server + control handlers +
ha_push client + orchestration 全链入为**活代码路径**（Phase 3 size_probe 只链 Phase 1-3 纯函数）。
真 daemon bin 457,004 字节是实际 ship 物，仍远在 `< 4.0MB` 优先目标内（Go v0.10.0 MIPS daemon =
3,997,853 字节，含全部网络/HTTP/video，非同类对比）。两数分别记录（size_probe 是体型测量载体、
daemon bin 是 ship 物），对齐 design 风险登记要求。

## 5. 范围红线确认（均守住）

- **self-unlock 触发 OUT**：`grep` 核实 `src/main.rs`/`src/orchestration.rs` **无** filter+debounce→Unlock
  job 自动产出路径。`Job` enum 仅 `Unlock`+`Shutdown`（**无 `Hangup`**）；`build_listen18022` 的 OnDetect
  回调只做 ① 每帧 `format_log` syslog ② req=704 门铃 `event=ring` push（detached），**无任何 Unlock job
  submission**。`event=ring` 门铃 push 是 **IN**（HACS 门铃通知，否则相对 Go 回归），与 ② 的 ring 触发
  self-unlock 正交。
- **auto-hangup OUT**：无 `Job::Hangup`、无 req=708 send、无延迟发送（骨架仅为 ② 留排空契约口）。
- **video OUT**（Phase 5）：main.rs 仅 banner 显示 `cfg.video.forward` 配置值，无 RTP/H.264/转发实现。
- **crate gate 守住**：`cargo tree` 仅 `libc` 一个外部 crate（破口框死三处：listen6672/listen18022
  PF_PACKET + wire_sender SO_BINDTODEVICE）；拒 tokio/crossbeam/mio/nix/pnet/socket2。
- **不部署 hAP**：Gate 止于 dev mock-e2e + 本地 MIPS 体型测量（Phase 7 灰度按 DEPLOY.md S1-S12）。

## 6. listener I/O 零改动 + BE 三层 + 新 BE 面

### 6.1 listener I/O 字节级零改动（「免 BE 重认证」论据落地）

M1.5 决策 1/3 否决 M3 的核心理由是「listener I/O 零改动 → 免重跑 BE 真机认证」。dev（LE、无真
PF_PACKET）测不出 BE socket 行为，故「零改动」本身须 `git diff` 字节级断言：

```
$ git diff HEAD --numstat -- src/listen6672.rs src/listen18022.rs
301	0	src/listen18022.rs
60	0	src/listen6672.rs
```

**361 insertions / 0 deletions**，全为纯加 helper（listen18022：`format_log`/`infer_direction`/`req_note`/
`Direction` Display + 测试；listen6672：`send_udp_response`/`send_udp_response_to` + 测试），diff hunk
context 落在 `build_number_query_response` 之后 / `extract_tcp_payload` 之后 / `mod tests` 内——**无任何
I/O 函数（`open_packet_socket`/`bind`/`recv`/`run_slave`/`run_platform`/`attach_filter`/`set_promisc`/
`recvfrom`）出现在 diff 行**。Phase 3 已 hAP 真机认证的 PF_PACKET socket/recv/BPF 路径字节级未触碰。

### 6.2 BE 三层防御状态

Phase 3 已闭合 BE 三层防御（① golden 表达式 ② `#[cfg(target_endian="big")]` const 断言 ③ hAP 真机
socket Proto=0003 + 真帧 recv，`verify-rust-listeners-on-hap` 2026-06-09）。本 change 复用已认证 listener
I/O（§6.1 证零改动），listener 层 BE 性质继承 Phase 3 认证。

### 6.3 新 BE 面（登记 + defer Phase 7）

本 change 引入 listener I/O 零改动伞**外**的新 socket/字节序路径，dev[LE] 测不出 BE 正确性，须与
BE-listener 一并 defer Phase 7 真机验：

- **`send_udp_response`（listen6672）**：号码查询 UDP 单播发送 + NBO 写入。新 UDP socket 发送路径 +
  网络字节序，dev 测不出 BE。
- **`resolve_iface_list`（config，桥成员枚举）**：为空 iface_list 时的桥自动解析碰 BE ifindex；骨架取
  「显式 `iface_list` 优先」绕开，mock-e2e 用显式 iface_list。

故本 change **不声称「无新 BE 面」**——这两处使「免 BE 重认证」仅覆盖 listener I/O，新发送/解析面 Phase 7
一并真机验。

## 7. graceful shutdown 钉死排序（决策 8 契约）

主线程（非 signal handler 内，收 SIGTERM/SIGINT 后）按钉死排序：① 置位 `Arc<AtomicBool>` → ② **先 join
listener 线程**（各于下个 SO_RCVTIMEO ~500ms wakeup 退出；join 完即无新 OnDetect → 无新门铃 push spawn，
关 RC-F1 窗口）→ ③ 投 `Job::Shutdown` 哨兵（容忍 worker 已死 `SendError`）→ worker 排空已入队 Unlock 后
break（在飞 unlock 经 cancel 早退 + 向 reply channel 回灌结果解除阻塞 handler）→ join worker → ④ join HTTP
线程（保证在飞 handler 结束，不再有 handler 向 push Vec spawn）→ ⑤ join 残留 detached push 线程
（`PushTracker::join_all`）。④<⑤ 不变量：手动 unlock 的 business-result push、Persister 都从 HTTP handler
触发，故 ④ 先于 ⑤ 是排空正确性前提。

**worker-death 解锁**：worker panic 提前死时其持有未回灌 reply `Sender` 随 teardown drop → 阻塞 handler 收
`Disconnected` 而非永等。**job-send 容错**：投 `Job` 路径容忍 `SendError`（禁 `unwrap`/`expect`）。

## 8. 结论

`port-rust-daemon-skeleton`（①）**通过**：M1.5 并发模型落地（保留真机认证 listener 线程 + 单 wire-worker +
job 队列 + 单 `Arc<AtomicBool>` + detached push 运行期回收）+ 手动 unlock 经 worker（free fn
`execute_unlock` 4 分支）+ automation flag 三级启动优先级 + Persister temp+rename 原子写（纯值去重，逐行
对齐 Go）+ startup push/banner + graceful shutdown 钉死排序 + fatal error 上报 + 号码查询响应 + listen18022
OnDetect（FormatLog + event=ring 门铃 push）。**cargo test 全过（180 lib + 4 main + 16 daemon_e2e + 35
golden，0 失败）+ clippy --all-targets 零警告 + crate gate 守住（仅 libc）+ MIPS 交叉编译（daemon bin
457KB / size_probe 312KB，均 BE/softfloat/静态）+ listener I/O 字节级零改动可证（361 insertions/0
deletions，无 I/O fn 触碰）+ 范围红线守住（无 self-unlock 触发/auto-hangup/video）**。

历程注记（文档记录，不入代码注释）：经 5 轮对抗 review 加固 spec + 代码（M1.5 worker/job/shutdown 契约
钉死、push detached 决策、Persister 去重逐行对齐 Go 删幻觉节流窗、新 BE 面登记、listener I/O 零改动可证）+
httpx server graceful shutdown 死锁修（Phase 2 遗留）+ orchestration helper 从 main.rs 上移 lib（便 e2e
测真生产函数）。具备进入姊妹 change ② `port-rust-self-unlock-consumer`（ring 触发 self-unlock + auto-hangup）
条件。

剩余收尾：

- `dooraccess-rs` 子仓 Phase 4 骨架 git commit + tag（git 写操作留人工，task 12.3）。
- 姊妹 change ② `port-rust-self-unlock-consumer`：见下文 §9-§14（已落地）。
- `send_udp_response` / `resolve_iface_list` 的新 BE 面 Phase 7 真机验（与 BE-listener 一并）。

---

# Phase4 ② ring 触发 self-unlock 消费者移植记录

> 变更：`port-rust-self-unlock-consumer`（OpenSpec，spec-driven，姊妹 change ②）
> 日期：2026-06-09
> 范围：在 ① 已交付的 daemon 骨架（listener 线程 + 单 wire-worker + job 队列 + 单 `Arc<AtomicBool>` +
> detached push）之上，接 **ring → self-unlock 消费者**（filter+debounce→`Job::Unlock`）/
> `ExecuteUnlockFromRing` 测量 + event=unlock 回灌 / 双触发消除（daemon 侧单边）/ auto-hangup 外机
> req=708 延迟发送（`Job::Hangup`）。对标 Go `consumeSelfUnlock`（main.go:538）/
> `scheduleOutdoorHangup`（:681）/ `ExecuteUnlockFromRing`（handlers.go:266）。
> **不**接 video（Phase 5）；**不**部署 hAP（Gate = dev mock-e2e + 本地 MIPS 体型测量，Phase 7 才灰度）。

## 9. 并发模型落地（Subscribable 消费者线程，决策 1）

listen18022 dispatch 是 **per-slave 多线程**（`run_platform` `thread::scope`），`on_detect` 是
`Fn + Send + Sync` 跨 slave 并发调用——把可变 debounce map 放其中既数据竞争又编译不过（`Fn` 不可改捕获）。
故 self-unlock 走 `listener.subscribe(filter)`：

| 关注点 | 落地 |
|---|---|
| filter（`req==704 && dst==本机室内机IP && src∈outdoorByIP`，锚 Go main.go:581-590） | 只读不可变捕获 indoor_ip / outdoor_ips 集合，在多线程 dispatch 路径同步跑、**无状态** |
| N 个 per-slave dispatch 汇成单流 | bounded `sync_channel(8)`（满则 `try_send` silent drop，防慢消费者卡 dispatch 主路径） |
| debounce / flag-gate / 产 Job | 全在**独立单消费者线程**顺序完成（真单线程、`HashMap<outdoorURI, Instant>` 无锁） |
| drain + shutdown 观察 | `recv_timeout(20ms)`：`Ok` 处理 / `Timeout` 查 `shutdown` 置位则 break / `Disconnected` clean break（不 log 错误）；退出统一调一次 `sub.cancel()`（id-幂等）。**禁裸 `recv()`**——Listener `thread::scope` 借 `&self`，listener-join 后仍持 tx→不 Disconnected→主线程 join 死锁 |

**构造期解析失败禁用**（`SelfUnlockDeps::try_new`，锚 Go main.go:555-576）：构造期解析 outdoor_by_ip /
calleeBCD（cfg.SIP）/ 本机室内机 indoorIP 各一次；calleeBCD 或 indoorIP 失败 → 返 `None`（消费者不启动、
self-unlock 禁用），log 一行、**不 panic、不部分启动**；daemon 其余模块（手动 unlock / 门铃 push）继续运行。

## 10. debounce + flag gate + 失败隔离（决策 1 续）

- **处理顺序钉死**（spec「flag gate」）：dequeue → 读运行时 `auto_unlock`（atomic，off 即 return **不写
  debounce**）→ debounce 检查 → debounce 写（**起步即写**，在任何可失败操作之前，对齐 main.go:645）→
  解析外机 URI（失败 log+跳过）→ 触发。flag off 不占 debounce 窗（否则污染窗口误 debounce 后续 flag-on ring）。
- **debounce 窗** = `unlock::UNLOCK_TOTAL_CAP + margin`（不硬编 12s）。`UNLOCK_TOTAL_CAP` 是 const；
  margin 用 test-tunable static（`set_debounce_margin_ms_for_test` 让 mock-e2e 缩 ms 级），生产 ~2s。
  「在飞」= 已 dequeue 并过 gate 之后；拨 off 只影响后续 dequeue。
- **per-ring 失败隔离 = 防御式编码（非 catch_unwind）**：release profile `panic="abort"` 下 `catch_unwind`
  是 no-op，故 Go inner-closure `defer recover()` 不可直译。消费者用可失败操作返 `Result` → log + 跳过该
  ring 继续 drain；ring 处理路径 + 计时线程**无** `unwrap`/`expect`/越界/`panic!`/整数溢出源。诚实边界：
  test profile 默认 unwind，5.9 e2e 只证「Result 跳过逻辑正确」，**不证** release abort 不发生——后者靠
  §13 静态 gate（模块级 clippy deny + grep）。
- **channel-drop 对账**：`dequeueCount`/`triggeredCount`（dequeue−triggered=debounce_skips），stop 时 log
  （锚 main.go:597/604）；向 worker 投 job 的 `SendError` 容忍禁 unwrap。丢帧只可能在 listener→consumer
  的 bounded `sync_channel(8)`，**非** consumer→worker 的 Job mpsc。

## 11. ExecuteUnlockFromRing + event=unlock + ring 路径 fail-fast probe 计时

`execute_unlock_from_ring`（锚 Go handlers.go:266）：投 `Job::Unlock` 给骨架 worker（M1.5 wire 单点）+
一次性 reply channel 等 `UnlockOutcome`（worker 已退/panic → SendError/RecvError → 退化 wire-failure，**不
永等、不 panic**）+ t_ms 测量（起点 = 消费者从 sub channel **取帧的 `Instant`**，等价 Go `ringAt`
main.go:621；`recv_timeout` 帧到达立即返回，t_ms 精度不受 poll 影响）。

- **仅成功 push event=unlock**（result=0 时 from=外机URI / to=室内机URI / result=0，经骨架 detached push
  线程 best-effort）；**非 OK 结果**（silent-FIN→-103 / wire-failure）**不 push**，5.8 e2e 双支断言（锚 Go
  `TestExecuteUnlockFromRing_SuccessPushesUnlockEvent` + `_FailureNoPush`）。
- **ring 路径 fail-fast probe 计时**（headline ground-truth「ring→开门 ~1.3s」成因）：`UnlockJob` 新增计时
  字段 `per_attempt_timeout: Option<Duration>` + `retry_interval: Duration`；ring 路径投
  `Some(SELF_UNLOCK_PROBE_TIMEOUT=800ms)` + `SELF_UNLOCK_PROBE_INTERVAL=200ms`（每步到点切断重探，揭穿
  首帧阻塞 ~3.7s 假象——见 memory `retry_unlock_v0_8_1_baseline`），手动 `/unlock` 路径仍投 `None` +
  `UNLOCK_RETRY_INTERVAL=1s`（HTTP 计时不变）。worker arm 用 job 携带值而非硬编。锚 Go handlers.go:283。

## 12. auto-hangup 外机目标 bye（detached 计时 → 即时 Job::Hangup → worker，决策 2）

result=0 + 运行时 `auto_hangup` on → `schedule_outdoor_hangup` 起 **detached 计时线程**：

- **可中断分片轮询延迟**（~2s test-tunable）：`let start=Instant::now(); loop{ if shutdown {return} if
  start.elapsed()>=delay {break} park_timeout(20ms) }`——到点判据用单调 `Instant::elapsed()` 累计（**非**
  分片计数：park_timeout spurious 提前会让计数法误判早发；援引 unlock.rs:204 ThreadSleeper 先例）；
  shutdown 置位即放弃（不投不发）。**禁单次 `sleep(2s)`**（拖慢 teardown 至 2s）。
- **worker 唯一发 wire**：计时线程到点投**即时** `Job::Hangup{outdoor_bcd, monitor_bcd, outdoor_ip,
  outdoor_port}`，由 **worker** 调新增 `wire18022::build_stop_frame(outdoorBCD, monitorBCD)` 发**外机目标**
  req=708 preview-stop 帧（守 M1.5 wire 出站单点；计时线程不发 wire）。
- **MUST 用 preview-stop 帧、禁自指 bye**：Go 实证（handlers.go:615-621）室内机自指 `build_bye_frame`
  对推流外机**无终止作用**，用错帧 = auto-hangup 真机不工作。`build_stop_frame` 字节由 golden 单测逐字节
  对齐 Go `video.BuildStopFrame`。
- **best-effort 边界**：投 `Job::Hangup` 的 SendError 容忍禁 unwrap；若晚于 `Job::Shutdown` 哨兵到 worker
  则丢弃，daemon **不**为它延迟 shutdown、**不**绕 worker 直发 wire（锚 Go main.go:680/691-694）。延迟在
  detached 计时线程，**不** head-of-line 阻塞排队的 Unlock。不触碰 debounce（按值持 BCD/IP）。

**双触发消除（daemon 侧单边契约）**：self-unlock 完全由 daemon 内部 ring→消费者→job 驱动，不依赖 HACS
`/unlock` 回调；本 change 只保证 daemon 侧不引入第二触发源，端到端「ring 只开一次门」还依赖 HACS v0.4.0
遥控器重写（删 `_on_ring`），**非本 change 单方达成**（与 Go 同构）。

## 13. 范围红线核查 + 静态 gate + 新 BE 面登记

### 13.1 范围红线（task 6.3，`git diff HEAD` 佐证）

- **未改骨架核心机制**：并发模型 / 串行 / 钉死 shutdown 排序不变量 / Persister 原子写均未触碰。
  `src/self_unlock.rs` 是**新文件**（untracked，不在 `git diff HEAD` 里），故对骨架的改动仅落在下列已 track 文件：
- **(a) `Job::Hangup` match arm = 授权扩展**（骨架 spec:23 **显式命名**预留「未来 ② 的 `Hangup`」）：
  `daemon.rs` 仅**新增** `Hangup` arm + `HangupWire` trait + `WorkerDeps::with_hangup_wire` 注入缝；worker
  loop 核心（`for job in rx`，`Unlock => execute_unlock`，`Shutdown => break`）**未改**。
- **(b) 消费者线程 join 插步 = spec:104 未命名新增步**：`main.rs` 在钉死排序里 listener-join **之后**、
  `Job::Shutdown` 哨兵**之前** join 消费者 handle。正当性靠「不破坏 spec:104 既有 ①-⑤ 不变量（仍是
  set-flag→关窗→哨兵→排空）+ 5.10 e2e 证消费者 join 不死锁」论证，**非**援引 spec:104 既有授权。
- `wire18022.rs` diff = **86 insertions / 0 deletions**（纯加 `build_stop_frame` + golden，无既有 wire 代码触碰）。
- `orchestration.rs`：手动 `/unlock` 路径保持 HTTP 计时（`None` + `UNLOCK_RETRY_INTERVAL`），行为不变。
- **无 video**（Phase 5 OUT）；**无 hAP 部署**（认证止于 dev mock-e2e + 本地 MIPS 体型测量，真实物理门开
  留 Phase 7：ground-truth = 外机播报 + 物理门开，非 daemon log）。

### 13.2 静态 gate（task 6.5，scope 限 self_unlock.rs 新模块）

`src/self_unlock.rs` 模块级 `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing,
clippy::arithmetic_side_effects)]`（lines 28-31）。grep 核实**生产区（line 28-503，首个 `#[cfg(test)]`
在 504）无** `panic!`/`unreachable!`/`.unwrap()`/`.expect(`/`.lock().unwrap()`——核 ring 处理路径
（`handle_ring`）+ 计时线程（`schedule_outdoor_hangup`）无 abort 源。模块用 lock-free atomic + 单线程
消费者状态，**无 internal-mutex `.lock().unwrap()`** 需窄 `#[allow]`。test mod（`mod tests` / `mod e2e`）
有窄 `#[allow]` 四件套 + 理由注释（test profile unwind，可接受）。`cargo clippy --all-targets` 零警告。

### 13.3 BE 面登记（task 6.4，对齐骨架 spec:178-180）

req=708 hangup 经 worker 调**新增** `wire18022::build_stop_frame`（移植 Go `video.BuildStopFrame`：
preview-shape req=708 / 分隔符 `=` / 尾 `80 00 00 00 02 00 00 01` / 36B）发出。该新增是 **wire-encode 代码**
（纯 body 字节组装、无 socket/NBO，须 golden 单测逐字节对齐 Go——`wire18022.rs` `build_stop_frame_golden`
已守），但**发送经 worker 已认证 wire 出站路径**（`wire_sender::Sender::send_context`），**非新 socket/字节序
BE-socket 面**。故 ② 的 BE 风险面**仍零**，defer-Phase-7 结论不变。**不**把 `build_stop_frame` 误称
「骨架已交付/复用既有」——它是 ② 新增的 wire-encode 代码，BE-socket 面才是「零新增」。

## 14. mock-e2e + 构建 gate 结果（dev 认证，行为锚 Go selfunlock 测试集）

`cargo test`（非沙箱）全通过：

- **lib 单测 200 passed**（① 180 + ② self_unlock：debounce 窗派生 / hangup_delay tunable / parse_outdoor_uri）
- **`tests/daemon_e2e.rs` 19 passed**（① 16 + ② 3 整合）；self_unlock 模块内 18 个 mock-e2e（5.1-5.11）：
  - `t5_1` ring 触发自开锁经 subscribe→消费者→worker（wire 恰调一次）
  - `t5_2` debounce 窗内重复 ring 仅触发首次 / `t5_3` flag off 跳过不写 debounce + 在飞那次跑完
  - `t5_4` 无 HACS 回调仍单次自开锁（daemon 单边，不声称端到端）
  - `t5_5` auto-hangup 发 req=708 preview-stop 帧逐字节对齐 `build_stop_frame` + shutdown 期间计时线程
    放弃不投 + hangup 延迟不阻塞新 Unlock；`t5_5b` build_stop_frame golden
  - `t5_6` dst/src 收紧（非本机室内机 dst / 未配置 src 不触发）
  - `t5_7` 多 slave 并发 debounce（≥2 thread 并发 dispatch 共享 Listener → 仅一个 Job::Unlock，无 TOCTOU）
  - `t5_8` 包装层 × 失败分支（成功 push event=unlock / 非 OK 不 push，双支断言；push 断言须沙箱外 loopback）
  - `t5_9` per-ring 失败隔离（malformed URI → Result 跳过、后续 ring 仍处理）
  - `t5_10` **shutdown 不死锁**（消费者 park 在 recv_timeout 时 shutdown → ≤poll+ε join 完成）
  - `t5_11` Job::Hangup 晚于哨兵 → SendError 容忍、不 panic / 不延迟 shutdown
- **跨语言 golden + Phase 1/2/3 集成 36 passed**（含 wire `build_stop_frame_golden`），全不回归
- **`cargo clippy --all-targets` 零警告**

**构建 gate**：

- **crate gate（task 6.1）**：`cargo tree -e normal --prefix none | sort -u` → 外部 crate **仅 `libc` v0.2.186**
  （dev-dep 不算）；拒 tokio/crossbeam/mio/nix/pnet/socket2。
- **MIPS 交叉编译（task 6.2）**：`make build-mips` + `make verify-mips` 通过——ELF 32-bit **MSB** MIPS /
  **statically linked** / **FP ABI Soft float (0x3)**。**MIPS 可移植性修**：self_unlock 的 test-tunable
  static 从 `AtomicU64` 改 **`AtomicU32`**（32-bit MIPS 无原生 64-bit 原子，`AtomicU64` 在该 target 不存在 →
  E0432；ms 值 ~2s 远 < u32::MAX，公开 setter 仍收 u64 边界 `as` 折宽窄）——这正是 build-gate 该抓的可移植性坑。

| 产物 | ① 骨架基线 | **② self-unlock 链入** | `file` / FP ABI |
|---|---|---|---|
| size_probe（代表函数链入体型测量载体） | 312,028 | **341,292**（+29,264） | ELF 32-bit MSB MIPS, static, stripped, **Soft float (0x3)** |
| 真 daemon bin `dooraccess-rs`（实际 ship 物） | 457,004 | **492,284**（+35,280） | ELF 32-bit MSB MIPS, static, stripped, **Soft float (0x3)** |

② size_probe 扩链 `build_stop_frame`（新 wire-encode）/ `spawn_consumer`（消费者线程入口函数指针）/
`Job::Hangup` 构造，`black_box` 防 DCE。daemon bin 492KB 仍远在 `< 4.0MB` 优先目标内（Go v0.10.0 MIPS
daemon = 3,997,853 字节，含全部网络/HTTP/video，非同类对比）。

## 15. 结论（②）

`port-rust-self-unlock-consumer`（②）**通过**：Subscribable 消费者线程（filter+debounce+flag-gate+产
`Job::Unlock`，真单线程无锁）+ 构造期解析失败禁用 + per-ring 防御式失败隔离 + `ExecuteUnlockFromRing`
（worker 跑 + t_ms 测量 + 仅成功 push event=unlock）+ ring 路径 fail-fast probe 计时（800ms/200ms，
headline ~1.3s 成因）+ auto-hangup（detached 可中断计时线程 → 即时 `Job::Hangup` → worker 发外机目标
req=708 preview-stop 帧）+ 双触发消除（daemon 单边）。**cargo test 全过（200 lib + 4 main + 19 daemon_e2e +
36 golden/集成，0 失败）+ clippy --all-targets 零警告 + crate gate 守住（仅 libc）+ MIPS 交叉编译
（daemon bin 492KB / size_probe 341KB，均 BE/softfloat/静态）+ 范围红线守住（骨架核心机制未改 / Job::Hangup
授权扩展 + join 插步 spec:104 新增 / wire18022 纯加 86-0 / 无 video / 无 hAP 部署）+ 新 wire-encode
`build_stop_frame` 登记为非新 BE-socket 面（BE 风险面仍零，defer Phase 7）+ self_unlock 模块静态 gate
（模块级 deny + grep 净）**。

剩余收尾：

- `dooraccess-rs` 子仓 ② git commit + tag（git 写操作留人工，task 7.3）。
- auto-hangup 物理 teardown（req=708 对推流外机的真实终止作用）+ ring→物理门开 ground-truth 留 Phase 7
  真机验（与 BE-listener / `send_udp_response` 一并；认证 = 外机播报 + 物理门开，非 daemon log）。
