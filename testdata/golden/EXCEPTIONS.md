# Golden 向量例外表（实测优先于 spec / 反编译残留）

本表登记 golden 向量中**真相来源不是生效 spec / 不是反编译模板**的条目。Rust parity 实现
遇 spec 与本表冲突时**必须以本表（实测）为准**。详见 `design.md` D2、`spec.md`
「需求:golden parity 机制与 SoT 治理」。

所有 wire / parse / config / state 向量由 `dooraccess-go` 的 `export_golden_test.go`
（`//go:build export` 隔离）调真实 Go 函数导出：

```
cd dooraccess-go && go test -tags export -run ExportGolden ./...
```

> ⚠️ **本批向量当前为「按 export 测试逻辑模拟推导」**（导出环境 `go` 不可执行，见仓库
> apply 记录的 blocker）。重叠 case 的值取自 `dooraccess-go` 既有**通过的** `*_test.go`
> 断言（Go-verified），非重叠 case（spec 锚点 / 参数化点）按确定性 Go builder/formula 推导
> 并已对 byte-pinned 锚点交叉核对（如 unlock-B `from=12345678/to=12340000` checksum=`0x7c`
> 与 `frames_test.go` 一致）。**首次接通 `go test -tags export` 后必须用真实导出覆盖比对**，
> 任何差异以 Go 真实输出为准。

---

## §wire — wire18022 实测例外（SoT = Go 代码 + pcap，**比生效 spec 新**）

### ① ParseResponseFrame 双分隔符 `&query=` / `&query*`

- **现象**：外机 req=705/709/711 ack 帧的分隔符实测为 `&query*`（`*` = 0x2a），不是生效
  spec 写的 `&query=`。
- **SoT**：Go `internal/wire18022/frames.go` `ParseResponseFrame`（双 sep 兼容循环）
  + pcap 实证 `samples/anjubao-video-stream/observations.md §2.2`
  + `samples/anjubao-doorbell/`（v0.3.3 `fix-doorbell-pipeline` 引入）。
- **风险**：Rust 若严格照 spec 只认 `&query=`，会重踩「unlock-A OK ack 解析失败 →
  daemon 报 unexpected response」。Rust `parse_response_frame` **必须**同时接受两种 sep。
- **golden 锚点**：`wire.txt`
  - `parse_ok|...71756572793d...|711|...`（`&query=`，sep 末字节 `3d`）
  - `parse_ok|...71756572792a...|711|...`（`&query*`，sep 末字节 `2a`）
  - `validate|...792a...|711|true`（`*` 分隔的 711 ack 仍 ValidateResponse=true）

### ② ValidateResponse 711 模板 byte0 = `0x00`（v0.3.3 实测，**非反编译残留 `0x2a`**）

- **现象**：req=711 OK ack body 实测首字节为 `0x00`，其余 9 字节 `00 01 00 00 00 03 00 00 00`。
- **SoT**：Go `frames.go` 变量 `resp711Body = {0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00}`
  （实际比对值）+ `samples/anjubao-doorbell/observations.md §4` ring-unlock.pcap t=6.06s。
- **陷阱**：`frames.go` 的 **doc 注释**残留旧反编译解读 `byte 0 = 0x2a`（注释 stale，疑 off-by-one
  误读）。**勿照抄注释**——以 `resp711Body` 的实际值 `0x00` 为准。Rust `validate_response`
  的 711 模板 byte0 **必须** = `0x00`。
- **golden 锚点**：`wire.txt`
  - `validate|...793d00000100000003000000|711|true`（byte0=0x00 → 通过）
  - `validate|...793d2a00010000000300...|711|false`（旧反编译 byte0=0x2a → **拒绝**）
  - `validate|...793dff0001...|711|false`（byte0 任意非 0 → 拒绝）
- **519 模板**：req=519 body 为 10 字节全 `0x00`（`resp519Body`），无反编译歧义，作常规锚点
  （`validate|...3531392671756572793d00000000000000000000|519|true`）。

### spec-anchor 覆盖度诚实声明（D2）

`openspec/specs/anjubao-tcp18022/messages/tcp18022/` 的 spec hex **每模板只钉一个示例输入的
整帧**，仅作**单点交叉锚**。`wire.txt` 中 spec 锚点（如 unlock-A/B/bye/appoint 的
`from=06020000`/`callee=06021103` 组）用于确认 Go builder 在该点命中 spec；**其余参数化点**
（`12345678` / 全零 / `01020304` 等）无 spec hex 可读，de-facto SoT = Go builder 输出。
> 本次因 `go` 不可执行，spec hex 单点交叉核对（task 2.2）**未实机完成**——已按既有 Go test
> 锚定值推导，须在接通导出后补做 spec hex 比对。

---

## §URI — IPv4 接受面分叉（D5 实测厘清，标注于 `uri.txt`）

实测厘清两类 IPv4 拒绝的**不同性质**（design D5；`uri.txt` 头注释亦载）：

### ① 前导零 `01.2.3.4` —— 【双语言一致拒绝】（非分叉）

- **两语言本就同拒**：Go `net.ParseIP` 自 1.17 / CVE-2021-29923 起拒前导零八位组；Go
  `config.validateURI` 手写校验器（`leadingZero` 标志）亦拒；Rust `std::net::Ipv4Addr` 亦拒。
- 故 golden 作**双语言一致拒绝回归断言**，不是 Go↔Rust 分叉。
- **锚点**：`uri.txt` `err|12340000@01.2.3.4:18022|IPv4Only`；
  `config.txt` `validateuri|12340000@001.002.003.004:18022|reject`（及 `10.0.0.04` / `10.00.0.4`）。

### ② IPv4-mapped IPv6 `::ffff:1.2.3.4` —— 【唯一真 Go↔Rust 分叉，本变更已改 Go 对齐】

- **分叉根因**：Go `codec.ParseURI` 旧实现经 `net.ParseIP().To4()` **接受** v6-mapped（To4 非 nil）；
  Go `config.validateURI` 手写校验器（只收 `[0-9.]`）**本就拒**；Rust `Ipv4Addr` **拒**。
  即唯一分叉点是 `codec.ParseURI`，且 Go 内部两校验器原已不一致。
- **本变更处置（task 7.2，用户确认改 Go）**：收紧 `dooraccess-go/internal/codec/uri.go`
  IPv4 接受面——含 `:` 的输入一律拒（规范点分十进制永不含冒号），使 `ParseURI` 与
  `validateURI` 内部一致 + 与 Rust 对齐。故 v6-mapped 现作**跨语言共享「拒绝」向量**。
- **锚点**：`uri.txt` `err|12340000@::ffff:1.2.3.4:18022|IPv4Only`（Go codec 改后拒）；
  `config.txt` `validateuri|12340000@::ffff:1.2.3.4:18022|reject`（Go config 本就拒）。
- **Rust 实现**：用 `std::net::Ipv4Addr::from_str` 统一收紧（拒前导零 + 拒 v6-mapped），
  四 sentinel 级错误 `Format/NameLen/IPv4Only/Port`（D1：不复刻 Go message 子类）。

## §int-width — 平台 int 宽度有意分叉（部署目标 GOARCH=mips → int32）

代码 review 差分测试发现：Go 平台 `int` 在 hAP（QCA9533 32-bit）= int32，Rust 用 i64/u32。
Rust 已对齐**部署目标 int32 语义**（非 64-bit host）：

- `parse_req_num` / `conv_int`：超 i32 范围拒（对齐 Go-MIPS `strconv.Atoi` / `OverflowInt`）。
- `validate_uri` IPv4 octet：≥10 位 octet，Go-MIPS int32 累加**回绕误「接受」**（int32 wrap bug，IP 之后
  `net.Dial` 仍失败）；Rust 数字循环内 `cur>255` 提前拒（对齐 Go-host int64 拒绝 + 避免 Rust debug panic）。
  **有意分叉**：两 daemon 最终都拒绝以此 malformed IP 运行，仅失败点不同。系统级可观测行为等价。

> golden 向量（64-bit host 抓取）结构盲区，靠差分测试发现；回归测试见 tests/golden_config.rs
> (validate_uri_overlong_octet / conv_int_int32_range) + tests/golden_wire.rs (parse_response_req_over_i32)，
> 经 revert 实证非 vacuous。

---

## §HTTP /info — HACS v0.4.0 字段核对（G3 硬核对，2026-06-08）

**来源**：本地 `hacs-dooraccess/` grep（`config_flow.py`、`__init__.py`、`button.py`、`lock.py`、`camera.py`、`api.py`）。

### jsonInfo 6 字段 vs HACS 实际读取

| `/info` 字段 | HACS 是否读取 | 读取位置 / 用途 | 与 Rust `info::render_json` 对齐 |
|--------------|---------------|-----------------|----------------------------------|
| `daemon` | ✅ | `config_flow.py`：`info.get("daemon") != "dooraccess-go"` | ✅ 常量 `"dooraccess-go"` |
| `version` | ⚠️ 间接 | `config_flow` 仅校验 daemon；版本 prefix check 在 CHANGELOG 提及，当前 `config_flow.py` 未硬编码 | ✅ 输出 `version` 字段 |
| `brand` | ⚠️ 未直接读 | v0.1.6 后 `config_flow` 改查 `daemon` 而非 `brand`；字段仍输出供 sanity | ✅ 常量 `"anjubao"` |
| `monitor` | ✅ | `button.py` / `lock.py` setup：`info.get("monitor")` | ✅ `cfg.sip` |
| `outdoor_stations` | ✅ | `button.py` / `lock.py` / `__init__.py`：`info.get("outdoor_stations")`，元素 `["sip"]` | ✅ 空 `[]` |
| `video` | ✅ | `__init__.py`：`info.get("video")` 整块 | ✅ 嵌套 struct 声明序 |

### `video` 子字段 vs HACS

| 子字段 | HACS 是否读取 | 读取位置 | 对齐 |
|--------|---------------|----------|------|
| `forward` | ✅ fallback | `__init__.py`：`video_block.get("forward_supported", video_block.get("forward"))` | ✅ |
| `forward_supported` | ✅ 主路径 | `__init__.py` / `camera.py` `available` | ✅ = `cfg.video.forward` |
| `protocol` | ❌ | 无 grep 命中 | ✅ 仍输出（Go 同源） |
| `format` | ✅ | `__init__.py`：`video_block.get("format")` 默认 `"flv"` | ✅ |
| `cache_path` | ❌ | 无 grep 命中 | ✅ 仍输出（Go 同源） |

### 差异 / 备注

- **无字段集分叉**：HACS v0.4.0 消费的 `/info` 字段 ⊆ Go `jsonInfo` 6 字段；Rust 按 Go `render_json.go` 声明序输出即可。
- **`version` / `brand`**：HACS 探活主门禁是 `daemon`，非 `brand`；保留输出与 Go 字节等价，非多余字段。
- **`lookup_wwan` 平台**：Go `net.InterfaceByName` 跨平台；Rust Linux 用 `SIOCGIFADDR` ioctl，macOS 开发机构造返空（`PushDiagnosis` fallback 行为与 Go「失败返空串」一致）。部署目标 OpenWrt/Linux 走 ioctl 路径。

### Phase 2 review-loop 厘清的 Go 对齐 / 已知降级（2026-06-08）

- **`/unlock` 失败状态码 = Phase-2 占位**：Go `handleUnlock` 经 `respondWireFailure` 把 sender 失败分类为
  200/-103(SilentFIN)、503/-5(Timeout)、503/-1(其它)。Rust Phase-2 一律 **200 + result=-1(ERR)**——这是 spec §133
  显式推迟到 **Phase 3** 的「错误分类」的占位行为（`trait Sender` 缝在 ExecuteUnlock 边界，分类属 Phase-3
  `port-rust-listeners-unlock`）。**非假绿**：成功路径（result=0）字节等价；失败路径状态码/result 分类待 Phase-3
  真 sender + 错误分类一并实现。HACS 若依据 HTTP 503 判 daemon 下游故障，须知 Phase-2 不发 503（已记此差异）。
- **auto_toggle / unlock body 解析 = Go json.Unmarshal 语义**（R1 + R2 修正，已逐边界对齐 go run 实测）：
  RAW `len==0` 空 body / `{}` / 缺字段 / `null` / `{"on":null}` → 零值（auto_*=false / To=""），非 400；
  **纯空白 body "   "（raw 非空）→ 报错 400**（Go 查 RAW 字节 len，非 trim）；key **大小写不敏感**；
  **重复 key last-wins**（`{"on":true,"on":false}`→false）；`{"on":1}` 等非 bool 值 / 非法 JSON → 400。
  已加单测锁定（`auto_toggle_body_matches_go_unmarshal_semantics` 覆盖全部上述边界 / `unlock_empty_body_falls_to_missing_to`）。
- **JSON 转义表 = Go encoding/json**（review-loop R1 修正）：U+2028/U+2029 两模式无条件转 ` / `；
  仅 C0(`< 0x20`) 转义，DEL(0x7F)/C1 不转。已加单测锁定（`escape_edge_cases_match_go`，go run 实测基线）。
- **body deadline = whole-request**（review-loop R1 修正）：body 读取在 headers 后切到 `read_timeout`（30s）而非更紧的
  `read_header_timeout`（5s），复刻 Go lazy-body 时序。诚实时序测试 `read_timeout_is_whole_request_deadline`
  断言 body 落在 header-deadline 与 req-deadline 之间时仍 200 OK。

---

## §listeners — listen6672 / listen18022 / BPF / wire_sender 向量（组 E，port-rust-listeners-unlock）

向量文件 `bpf.txt` / `listen6672.txt` / `listen6672_extract.txt` / `listen18022.txt` /
`listen18022_extract.txt` / `wire_sender.txt` 由 `dooraccess-go` 的
`internal/listen6672` + `internal/listen18022` 的 `export_golden_test.go`（跨平台）
+ `export_golden_linux_test.go`（`//go:build linux && export`）**真实 Go 导出**
（2026-06-08 实跑：跨平台部分 host `go test`、linux-only extract 部分 Docker `golang:1.25`
`go test -tags export`；本节向量**非模拟推导**，已用真实导出落盘）。

### ① BPF const 双写互验 + 端口 k 值（D-D：烤 const 不移植汇编器）

- **SoT**：`golang.org/x/net/bpf.Assemble`（Go `buildBPFFilter` 的 `[]bpf.Instruction`）。
- `bpf.txt` 同时含 `bpf6672`（11 条）+ `bpf18022`（13 条）两段；listen6672 与 listen18022
  两个 export test **各自写完整 bpf.txt**（互镜像，幂等内容一致）——改任一 filter 须同步两处
  镜像，否则两包写出的 bpf.txt 不一致、drift gate diff 即 fail（双写互验）。
- 端口烤为 const：6672=`0x1a10`（`bpf6672[8].k`）、18022=`0x4666`（`bpf18022[8].k` 源端口 +
  `bpf18022[10].k` 目的端口，src 或 dst = 18022 都接受）。Rust `bpf::BPF_6672`/`BPF_18022`
  对 `bpf.txt` 逐字段（op/jt/jf/k）断言相等（`tests/golden_bpf.rs`）；若不一致即组 A 烤错。

### ② 响铃 byte19 多值识别（0x8c / 0x94 / 0x95 全判 ring）★ E9 锚点

- **SoT**：Go `listen6672/parser.go:88` `case 0x8c, 0x94, 0x95:`（v0.3.3 fix-doorbell-pipeline）。
- byte19（subtype）疑似 session counter / event token——同一呼叫事件内 8 帧一致、跨事件取不同值
  （实测 0x94 / 0x95 / 0x8c）。只认单值 0x94 会漏判 0x8c/0x95 的真实响铃帧（即 Go v0.3.3 已修
  的漏分类 bug）。`listen6672.txt` 三值各一帧 + 呼梯 0x90 一帧（→ elevator_key），Rust `classify`
  对每帧的 `EventKind.as_str()` 断言等于 Go `kind.String()`（`tests/golden_listeners.rs`）。

### ③ extract 向量是 linux-only Go 导出（Rust extract 跨平台）

- Go `extractUDPPayload` / `extractTCPPayload` 定义在 `listener_linux.go`（`//go:build linux`），
  故 `*_extract.txt` 经 Docker linux 容器导出；CI `golden-drift` job 在 ubuntu(linux) runner
  上原生重导。Rust `extract_udp_payload` / `extract_tcp_payload` 是跨平台 `pub fn`（无 cfg 门），
  host `cargo test` 读已 committed 的静态 fixture 即可断言（无需 linux）。

### ④ wire_sender 错误分类（classifyWireErr 语义复刻）

- **SoT**：Go `wire18022.IsRetryableError` + `http8080.classifyWireErr`（handlers.go:714）。
- `wire_sender.txt` 的 `errclass` 行含 `retryable bool`（公开 API）+ `result_code`：
  silent_fin→`-103`(NO_RING) / timeout→`-5`(TIMEOUT) / 其它(connection_reset/broken_pipe/
  connrefused)→`-1`(ERR)。Go export **逐字复刻** `classifyWireErr` 的 `errors.Is` 链 + codec 常量
  （classifyWireErr 是 http8080 包私有，无法跨包直调）。
- Rust 侧：`tests/golden_listeners.rs` 验 `is_retryable_error`（公开）的 bool 列；result_code 列
  因 `sender::map_wire_kind` 私有，在 `src/sender.rs` 内 `#[cfg(test)]`（`golden_errclass_result_code`）
  验 `WireError → map_wire_kind → classify_wire_err(i32)` 端到端等于 golden。
- sender 帧（`frame` 行）= `wire18022.Build*Frame`（与 §wire 的 `wire.txt` 同源 builder，此处覆盖
  sender 实际发送的 5 帧 kind：unlock_a/unlock_b/bye/appoint/preview）。
