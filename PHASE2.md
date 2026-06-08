# dooraccess-rs Phase2 bare HTTP + HA push 移植记录

> 变更：`port-rust-http-control`（OpenSpec，spec-driven）
> 日期：2026-06-08
> 范围：复刻 Go `internal/httpx` + `http8080` + `hapush` + `info`(no-probe 子集) 的**外部行为**，
> 不接真实 wire sender / listener / PF_PACKET / video / daemon 编排。**不替换生产 Go daemon**。

## 1. 交付模块清单

| Go 模块 | Rust 模块 | 内容 |
|---|---|---|
| `internal/httpx`（server/parse/response/mux/client/types） | `src/httpx/{mod,parse,response,mux,server,client,deadline_io,json}.rs` | bare HTTP/1.1 server（thread-per-conn + blocking socket + 三 deadline）、parser（拒 HEAD/h2c/chunked-req/无 Host、413）、response writer（chunked/Connection:close/幂等）、ServeMux（精确优先+最长前缀、404 字节契约）、client（POST/Bearer/timeout/cancel、chunked 解码）、手写 JSON encoder |
| `internal/http8080`（中间件 + endpoint） | `src/control.rs` | 中间件链（method_guard/require_json_content_type/require_stations_configured + write_json/error_json/parse_json_body）、控制面 endpoint（/auto_unlock、/auto_hangup、/automation）、元信息（/info、/playback）、/unlock 薄切、AutomationState、可选 Pusher/Persist hook |
| `internal/hapush` | `src/ha_push.rs` | Push(event,fields)（字母序 body + Bearer + 5s timeout + ErrNotConfigured）、discover_ha_facing_ip、PushDiagnosis |
| `internal/info`（no-probe 子集） | `src/info.rs` | SelfDesc/StationDesc/VideoDesc、build(probe_ha=false 仅喂 jsonInfo 6 字段)、render_json |
| （新增 trait） | `src/sender.rs` | 最小 `trait Sender`（ExecuteUnlock 边界缝）+ MockSender |

## 2. JSON 字节保真（encoding/json 隐式行为复刻）

Go HTTP JSON 出/入站**全部走 `encoding/json`**（非手写）。Rust 手写 encoder 复刻其 6 项隐式字节行为，
均经 cargo 单测 + 跨语言 golden 实证（机制见 §3）：

1. **key 序**：map（hapush）字母序（`BTreeMap`）vs struct（writeJSON/RenderJSON）声明序（`Vec<(k,v)>`）——**不一刀切**
2. **空 slice → `[]` 非 `null`**：outdoor_stations 空时输出 `[]`
3. **`/info` 尾随 `\n`**（`json.Encoder.Encode`）vs writeJSON 无 `\n`（`json.Marshal`）
4. **HTML 转义 per-endpoint**：`/info` 关（`SetEscapeHTML(false)`，原样 `<>&`）vs writeJSON/hapush 开（`<>&`）
5. **framing**：`/info` 无 Content-Length 走 chunked vs writeJSON fixed-length
6. **404 字节契约**：`Not Found`(9B) / `text/plain; charset=utf-8` / `Content-Length: 9`（非 JSON）

## 3. golden parity 结果

**机制**：扩展 Go `internal/http8080/export_golden_test.go`（`//go:build export` 隔离）导出 HTTP fixture 到
`testdata/golden/http/`（parser / endpoint_body(裸 body) / endpoint_socket(带框架) / middleware / hapush / mux404），
Rust `tests/golden_http.rs` 对 committed 向量逐字节/逐字段断言。

**结果**：`cargo test` 全通过——
- lib 单测 **51 passed**（httpx json/parse/response/mux/client/server deadline + control 中间件/endpoint + info + ha_push + sender），含 Phase1 golden(codec 4/config 3/state 4/wire 2) 无回归
- 跨语言 golden `tests/golden_http.rs` **7 passed**（parser 边界含 413、endpoint 裸 body 两类、endpoint socket framing、中间件 405/415/200-{result:-100}、mux404 字节、hapush 字母序）
- deadline 语义**时序测试** 3 passed（整请求绝对 deadline / header timeout 紧于 read timeout / write 不设 deadline）

**实证捕获**：跨语言 golden 抓出一处真实差异——初版 Rust endpoint_body 测试误把 writeJSON 框架（status+headers）算进裸 body，
经 fixture 对账揭露并修正（裸 body = `encode_struct(MARSHAL)`，framing 验证在 endpoint_socket fixture）。

**drift gate**：`.github/workflows/ci.yml` 的 `golden-drift` job 已扩覆盖 `http8080` 包导出（跨仓 checkout dooraccess-go
+ `go test -tags export` 重生成 + diff committed 向量）。Go 改 endpoint/parser 而 Rust 向量未同步 → 主分支 CI fail。

## 4. MIPS 体型（Phase2 gate 数据）

构建路线沿用 Phase0/1（nightly + `-Z build-std=std,panic_abort` + OpenWrt ath79 SDK musl sysroot + `rust-lld` 经
`scripts/link-mips.sh`，全静态非 PIE）。`examples/size_probe.rs` 扩链 Phase2 全模块（httpx/control/info/ha_push）。

| 产物 | 字节数 | `file` |
|---|---|---|
| Phase0 hello（基线） | 66,876 | ELF 32-bit MSB MIPS, static, stripped |
| Phase1 size_probe（4 纯函数模块） | 71,580 | 同上 |
| **Phase2 size_probe（链入 httpx/control/info/ha_push 全栈）** | **100,572** | **ELF 32-bit MSB MIPS, static, stripped, FP ABI Soft float** |

**Phase2 模块增量 = 100,572 − 71,580 = +28,992 字节（~29KB）**，可忽略，远在 `< 4.0MB` 优先目标内
（Go v0.10.0 MIPS daemon = 3,997,853 字节，含全部网络/HTTP/video，非同类对比）。仍 BE/softfloat/全静态。

## 5. D2 决策证据（是否接受 Rust 依赖集）

roadmap §363 把 D2 钉在 Phase2 后。本变更默认**手写 JSON encoder（0-crate）**，并量了一次 `serde_json` 对照：

| 产物（MIPS） | 字节数 | 说明 |
|---|---|---|
| Phase0 hello | 66,876 | 基线 |
| serde_probe（仅 serde_json JSON 编码，无 Phase2 模块） | 83,324 | serde_json+serde_core+itoa+memchr |
| **serde_json 单 JSON 增量** | **+16,448** | 83,324 − 66,876 |

对比：**整个手写 Phase2（全模块含手写 JSON）才 +33,696**（100,572 − 66,876）。serde_json **仅 JSON 编码**就吃掉
手写全 Phase2 增量的近一半。手写 encoder 已 golden 验证字节等价（§3），体型仅零头。

**D2 裁决：保持手写 0-crate**。`Cargo.toml [dependencies]` 空，`cargo tree` 仅本 crate，0 外部依赖。
（测量后已移除 serde dev-dependency + serde_probe，还原 0-crate。）

## 6. trait Sender 缝形状（Phase 3 契约）

```rust
pub trait Sender: Send + Sync {
    fn execute_unlock(&self, caller_bcd: [u8;4], callee_bcd: [u8;4],
                      target_ip: &str, target_port: u16, cancel: &AtomicBool) -> Result<i32, SenderError>;
}
```

语义 = Go `ExecuteUnlock` **方法边界**（不是叶子 `Sender.SendContext`）。`/unlock` 薄切只做 body→BCD→target→调
trait→shape JSON；retry 退避 / bye 早停 / executeUnlock 内部 / silent-FIN 错误分类整块留 **Phase 3**
（`port-rust-listeners-unlock`）。`cancel` 参数预留 Phase 3 ctx 取消，Phase 2 mock 忽略。trait 方法面**最小化**。

## 7. 范围红线确认（均守住）

不接真实 wire `Sender`/`listen18022`/`listen6672`/PF_PACKET（Phase 3）；不移植 `/video/*`（Phase 5）；
不移植真原子写 `Persister`/`writeAtomic`（Phase 4，Phase 2 用 no-op/可注入失败 hook）；不移植 daemon main 编排与
`asyncPush` 的 PushWG/handlerWG/diagWG shutdown 编舞（Phase 4，Phase 2 push 留同步可选 hook + nil no-op）；
不引入 TLS/HTTP2/keepalive/tokio；不部署 hAP（仅本地 `cargo build --target mips-unknown-linux-musl` 量体型）。

## 8. 已知局限 / 留 Phase 4

- **server graceful shutdown**：`httpx::Server::serve` 的 accept-loop 在阻塞 `accept()` 期间持有 listener 锁，
  无 pending 连接时 `shutdown()` 取锁会阻塞——graceful shutdown 编舞本属 Phase 4 daemon 范畴（spec 已推迟）。
  Phase 2 endpoint e2e（服务路径）由 `endpoint_socket_golden`（全 ControlServer 过真 socket 跑所有 endpoint）覆盖；
  shutdown lifecycle 留 Phase 4 与 daemon 编排一并解决（需 accept 非阻塞化或 lock 不跨 accept）。
- `/info` 字段集对齐 HACS v0.4.0：已记 `testdata/golden/http/`（apply 期对照，差异留 EXCEPTIONS.md）。

## 9. 结论

Phase2 **通过**：httpx/control/info/ha_push/sender Rust 等价实现 + 6 项 JSON 字节保真 golden 验证（51 单测 + 7 跨语言
golden + 3 时序测试全过）+ 0-crate gate 守住（D2 裁决保持手写：serde_json 单 JSON +16KB vs 手写零头）+ MIPS 交叉编译
验证（+29KB 增量、仍 BE/softfloat/静态）。具备进入 Phase3（listeners + unlock）条件。

剩余收尾：`dooraccess-rs` 子仓 Phase2 tag（git 写操作留人工）。
