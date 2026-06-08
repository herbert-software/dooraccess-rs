# dooraccess-rs Phase3 监听器 + wire sender + unlock 核心移植记录

> 变更：`port-rust-listeners-unlock`（OpenSpec，spec-driven）
> 日期：2026-06-08
> 范围：复刻 Go `internal/wire18022`（sender 子集）+ `internal/listen6672` + `internal/listen18022`
> + `internal/http8080` 的 `executeUnlock` 核心（retry / probe / bye 早停 / 错误分类）的**外部行为**。
> **不**接 daemon main 编排 / ring consumer / self-unlock 触发（Phase 4）；**不**部署生产 Go daemon。
> PF_PACKET socket-level BE 真机认证拆出**姊妹变更** `verify-rust-listeners-on-hap`。

## 1. 交付模块清单

| Go 模块 | Rust 模块 | 内容 |
|---|---|---|
| （新增 FFI 地基） | `src/ffi.rs` | PF_PACKET raw-socket libc 缝（`socket(AF_PACKET)` / `bind(sockaddr_ll)` / `setsockopt(PACKET_ADD_MEMBERSHIP / SO_ATTACH_FILTER / SO_RCVTIMEO)` / `recvfrom` / `if_nametoindex`）；平台无关 `htons`（`from_ne_bytes(to_be_bytes)`，禁 `v<<8\|v>>8`）；`SockFilter`/`SockFprog` 类型（Linux alias `libc`，非 Linux repr(C) mirror）；int 宽度 + BE 编译期断言；非 Linux stub |
| `internal/listen6672`（listener/parser/dedup/numquery） | `src/listen6672.rs` | `extract_udp_payload`（L2→ethertype 0x0800→IP ihl/proto17→UDP→payload）、`parse_frame`/`classify`（响铃 `EventRing` = byte19 ∈ {0x8c,0x94,0x95} 多值；呼梯 0x90 仅 log）、`build_number_query_response`、dedup ringbuffer（时钟注入缝）、`dispatch_with_dedup`、PF_PACKET recv 循环（per slave + SO_RCVTIMEO=500ms wakeup + shutdown flag）；非 Linux stub |
| `internal/listen18022`（listener/parser/dedup/Subscribe） | `src/listen18022.rs` | `parse_frame`（req+body）、`extract_tcp_payload`、dedup（同模式 + 时钟注入）、`trait Subscribable` + `Subscription{ch,cancel}`（buffered 8 / filter panic 隔离 / chan 满 silent drop / cancel 移除 + 多次 cancel no-op）、`dispatch_for_test` hook、PF_PACKET recv 循环（BPF 不同）；非 Linux stub |
| `internal/wire18022`（sender 子集；**Rust 拆为独立模块**） | `src/wire_sender.rs` | `Sender::send`/`send_context`（TCP 短连接、connect/write/read 各 5s deadline、cancel-aware）、silent-FIN 检测（`WireError::SilentFin`）+ timeout 归一（`Timeout{stage}`）、`is_retryable_error`、`get_iface_ip`/`pick_ipv4_from_addrs`、`SO_BINDTODEVICE`（手动 `socket+setsockopt+connect`，破口③）；非 Linux fallback |
| `internal/http8080`（executeUnlock 核心切片） | `src/unlock.rs` | `execute_unlock`（串行核心，持 `Mutex` 守 wire 出站）、`run_unlock_with_retry`（1s×8/cap10s 退避 + `per_attempt_timeout` fail-fast probe + unlock-B 不重试）、bye 早停（经 `trait Subscribable` 订阅 req=708，`filter_req708_for_target`）、`classify_wire_err`；`trait UnlockWire`/`Sleeper`/`Deadline` 注入缝 |
| `internal/listen{6672,18022}`（BPF assemble） | `src/bpf.rs` | `const BPF_6672: [SockFilter; 11]` / `const BPF_18022: [SockFilter; 13]`（`code:u16,jt:u8,jf:u8,k:u32`）+ `as_fprog`，对 Go `bpf.Assemble` golden |

**Go 单包 → Rust 双模块的心智偏移（Phase 4 import 须知）**：`IsRetryableError` / `ErrSilentFIN` / `ErrTimeout`
在 Go 属 `wire18022` 包，Rust 归 **`wire_sender`**（不在 `wire18022`）。理由是依赖隔离——sender 的
`SO_BINDTODEVICE` 引 `libc` 破 0-crate gate，若并入 `wire18022` 会污染纯函数 frame builder 的依赖面，
使后者无法保持「纯 std、可 golden/qemu 独立验」。`wire18022`（frame builder）继续 0-crate，
`wire_sender`（TCP + libc）单独承载 gate 破口③。

## 2. crate gate 破口边界（D-A，守住）

Phase 1/2 守 0-crate（仅 std）。Phase 3 把依赖**放宽为 std + libc only**，破口**严格框死三处**：
① `listen6672` PF_PACKET（经 `ffi`）② `listen18022` PF_PACKET（经 `ffi`）③ `wire_sender` 的 `SO_BINDTODEVICE`。
`AF_PACKET` / `bind(sockaddr_ll)` / `setsockopt(SO_ATTACH_FILTER / PACKET_ADD_MEMBERSHIP)` / `recvfrom`
无 std 等价物。**拒绝** nix / pnet / socket2 / tokio。

`cargo tree` 实测（2026-06-08）：

```
dooraccess-rs v0.0.0
└── libc v0.2.186
```

**外部 crate 仅 `libc` 一个**，0-crate 放宽边界守住。

## 3. golden parity 结果

**机制**：组 E 在 Go `internal/listen6672` / `internal/listen18022` 包**新建** `export_golden_test.go`
（`-tags export`，两包此前无此文件）导出 BPF（`buildBPFFilter()` → `bpf.Assemble` → 每行
`<name>|<idx>|<op_hex>|<jt>|<jf>|<k_hex>`）+ parser 向量（含 6672 响铃 byte19=0x8c/0x94/0x95 三值各一帧
+ 呼梯 0x90 一帧）+ wire sender 帧字节 + 错误分类向量到 `testdata/golden/`。Rust 端 `tests/golden_*.rs`
对 committed 向量逐字段/逐字节断言。

committed Phase3 golden 向量：

| 文件 | 内容 |
|---|---|
| `testdata/golden/bpf.txt` | 6672+18022 BPF 程序（双写互验：Rust const 对 Go assemble）|
| `testdata/golden/listen6672.txt` | 6672 parser / classify / numquery 向量 |
| `testdata/golden/listen6672_extract.txt` | 6672 L2→UDP payload extract（linux-only export）|
| `testdata/golden/listen18022.txt` | 18022 parser 向量 |
| `testdata/golden/listen18022_extract.txt` | 18022 L2→TCP payload extract（linux-only export）|
| `testdata/golden/wire_sender.txt` | sender 帧字节 + 错误分类输入→分类结果 |

**结果**：`cargo test` 全通过（非沙箱环境，沙箱会拦 TCP bind 致 wire_sender/httpx 网络测试假失败）——

- **lib 单测 150 passed**（含 Phase1 codec/wire18022/config/automation_state + Phase2 httpx/control/info/ha_push/sender
  全部不回归 + Phase3：ffi 3 / bpf 4 / listen6672 25 / listen18022 24 / wire_sender 20 / unlock 18）
- 跨语言 golden 集成测试：`golden_bpf` 2 / `golden_listeners` 5 / `golden_wire` 3 通过（外加 Phase1/2 的
  golden_codec/golden_config/golden_state/golden_http 不回归）

**mock-e2e（task 8.3，超出 Go 现有测试集）**：self-unlock 用 mock `UnlockWire` + mock `Subscribable` 覆盖
unlock success / silent first N conns 重试成功 / body mismatch（result=-1 不重试）/ bye early-stop（注入 708
验 `terminated_by_bye`）/ ctx cancel / probe per-attempt 超时重探 / unlock-B 不重试。Go `selfunlock_test.go`
仅 3 个 unlock 测试，bye/probe/cancel/unlock-B 锚的是 Go 源行为（handlers.go retry/probe/bye 逻辑）非对照测试。

**drift gate**：`.github/workflows/ci.yml` 的 `golden-drift` job 已扩覆盖 `listen6672` / `listen18022` 包导出
（跨仓 checkout dooraccess-go + `go test -tags export` 重生成 + diff committed `bpf.txt` /
`listen{6672,18022}.txt` / `listen{6672,18022}_extract.txt` / `wire_sender.txt`）。Go 改 parser/BPF/sender
而 Rust 向量未同步 → 主分支 CI fail。

## 4. MIPS 体型（Phase3 gate 数据，D2 续证）

构建路线沿用 Phase0/1/2（nightly + `-Z build-std=std,panic_abort` + OpenWrt ath79 SDK musl sysroot +
`rust-lld` 经 `scripts/link-mips.sh`，全静态非 PIE）。`examples/size_probe.rs` 扩链 Phase3 全模块代表函数
（`ffi::htons` / `bpf::as_fprog` 双 filter / listen6672 parse_frame+classify+numquery+extract /
listen18022 parse_frame+extract+dedup_key / wire_sender is_retryable_error+pick_ipv4 /
unlock classify_wire_err+filter_req708），`black_box` 防 DCE。

| 产物 | 字节数 | `file` |
|---|---|---|
| Phase0 hello（基线） | 66,876 | ELF 32-bit MSB MIPS, static, stripped |
| Phase1 size_probe（4 纯函数模块） | 71,580 | 同上 |
| Phase2 size_probe（链入 httpx/control/info/ha_push 全栈） | 100,572 | 同上 + FP ABI Soft float |
| **Phase3 size_probe（链入 ffi/bpf/listen6672/listen18022/wire_sender/unlock + libc）** | **104,892** | **ELF 32-bit MSB MIPS, static, stripped, FP ABI Soft float** |

**Phase3 模块 + libc 引入增量 = 104,892 − 100,572 = +4,320 字节（~4.2KB）**，可忽略。
`libc` 首次引入（Phase 1/2 为 0-crate）只多 4.2KB——因为绝大多数 libc 项是 FFI 声明 + 常量（零代码），
实际调用的 syscall 封装代码量极小。仍远在 `< 4.0MB` 优先目标内（Go v0.10.0 MIPS daemon = 3,997,853 字节，
含全部网络/HTTP/video，非同类对比）。

构建后 `file` + `llvm-readobj` 实测仍 **BE（MSB）/ softfloat（FP ABI: Soft float 0x3）/ 全静态（statically linked）**。
（生产 probe bin `make build-mips` = 66,876 字节，因 `main.rs` 是 hello-style probe 不 use lib；
体型测量以 `size_probe` example 为权威载体。）

## 5. BE 大端字节序三层防御（D-B）

| 层 | 落点 | 本变更状态 | 能验 / 不能验 |
|---|---|---|---|
| **Tier 1 golden** | 本机 macOS（LE）+ CI | ✅ 已认证 | `htons`/`to_be`/`from_be_bytes` 表达式正确性（golden 逐字节，输入是固定 BE 字节，与 host 端序无关）；golden-drift 跨仓重导 Go diff |
| **Tier 2 编译期 `#[cfg(target_endian="big")]` const 断言** | `cross-build MIPS BE soft-float` job（已通过） | ✅ 已认证 | `ffi.rs` 的 `const _: () = assert!(htons(0x0003)==0x0003 / htons(0x0800)==0x0800)` + int 宽度断言（c_int=4 / mips time_t=4 / sock_filter=8）。`make build-mips` 编译 ffi.rs 时由 **const-eval 按 mips BE 目标端序求值**——htons 写错（`v<<8\|v>>8`）或 struct 宽度分叉即**编译期失败**，故该 job 通过即认证 BE 寄存器/端序 + struct 宽度。**曾尝试 qemu-mips runtime 单测 job 作 Tier 2,但 cross-test 需静态 musl sysroot（默认链接在 CI 失败）+ qemu 下线程/TCP 单测不稳；其核心保证已由编译期 const 断言覆盖,故移除 qemu job 采编译期断言（spec 明列的 fallback）** |
| **Tier 3 PF_PACKET socket-level** | 真 hAP（QCA9533） | ⏸ **交接姊妹变更 `verify-rust-listeners-on-hap`** | qemu-user 把 syscall 透传宿主 LE 内核，**抓不到 `sll_protocol` NBO bug**（AF_PACKET 是内核网络栈）。须按 `dooraccess-go/DEPLOY.md` S1-S12 上真 hAP，确认 `/proc/net/packet` Proto=0003（非 0300）+ 真收帧 |

**本变更认证到 Tier 2**（golden 表达式 + 编译期 BE const 断言在通过的 cross-build job 强制）。
PF_PACKET socket-level BE 是结构性不可在无真机/无 BE 内核下认证的局限，spec 已 gate 化、交接姊妹变更，**非假完成**。

注：macOS brew 只给 `qemu-system-*` 无 `qemu-mips` user-mode（已知限制）；`qemu-system-mips`（大端全系统，
带真 BE 内核）可作姊妹变更上 hAP 前的**可选 de-risk smoke**，但内核 ≠ 生产 QCA9533，**不替代 hAP 认证**。

## 6. trait Subscribable 缝形状（Phase 4 契约）

```rust
pub trait Subscribable: Send + Sync {
    fn subscribe(&self, filter: Option<SubFilter>) -> Subscription;
}

pub struct Subscription {
    pub ch: Receiver<DetectedFrame>,           // buffered 8
    cancel: Box<dyn Fn() + Send + Sync>,        // 多次 cancel no-op
}
// SubFilter = Box<dyn Fn(&DetectedFrame) -> bool + Send + Sync>
```

语义 = Go `listen18022.Subscribe(filter) *Subscription`。`filter == None` = 全通过（锚 Go `filter == nil`）。
`unlock::execute_unlock` 的 bye 早停经此 trait 注入：`Listener == None` 时不订阅，等价 Go `Listener==nil`
→ `byeCh` nil-channel select 永不命中，使无真 PF_PACKET listener 也能 mock e2e 测早停。

**Phase 4 import 契约**：daemon ring consumer + self-unlock 触发把真 `Listener`（impl `Subscribable`）注入
`execute_unlock`；`subscribe` 投递 buffered 8 / chan 满 silent drop / filter panic 隔离 / cancel 移除的语义
已在本变更 frozen。`UnlockWire` / `Sleeper` / `Deadline` 三注入缝同样面向 Phase 4 真 impl 替换。

## 7. 范围红线确认（均守住）

不接 daemon main 编排 / ring subscription consumer / self-unlock fail-fast 触发入口（`ExecuteUnlockFromRing`，
Phase 4）；不移植 auto-hangup 外机 bye（Phase 4）；不移植 `/video/*`（Phase 5）；不移植真原子写 `Persister`
（Phase 4）；不引入 TLS/HTTP2/keepalive/tokio；外部 crate **仅 `libc`**（破口框死三处）；
**不部署 hAP**（仅本地 `cargo build --target mips-unknown-linux-musl` 量体型）；PF_PACKET socket-level BE
真机认证交接姊妹变更 `verify-rust-listeners-on-hap`。

## 8. 结论

Phase3 **通过**：ffi（PF_PACKET libc 缝）/ bpf / listen6672 / listen18022 / wire_sender / unlock 核心 Rust
等价实现 + golden 逐字段验证（150 lib 单测 + 跨语言 golden 全过，含 Phase1/2 不回归）+ mock-e2e 覆盖
unlock retry/probe/bye/cancel/unlock-B + 0-crate 放宽边界守住（`cargo tree` 仅 `libc`）+ MIPS 交叉编译验证
（+4.2KB 增量、仍 BE/softfloat/静态）+ BE 三层防御前两层接通（golden 表达式 + 编译期 `#[cfg(target_endian="big")]`
const 断言由通过的 cross-build job 强制），第三层 PF_PACKET socket-level BE 显式交接姊妹变更。具备进入 Phase4（daemon 编排 +
self-unlock）条件。

剩余收尾：
- `dooraccess-rs` 子仓 Phase3 git tag（git 写操作留人工）。
- 姊妹变更 `verify-rust-listeners-on-hap`：PF_PACKET socket-level BE 真机认证（须按 `dooraccess-go/DEPLOY.md`
  S1-S12 上真 hAP；memory 记 DoorLink 已物理下线，hAP 可达性须确认）。
