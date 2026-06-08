# dooraccess-rs Phase1 纯函数协议核心移植记录

> 变更：`port-rust-protocol-core`（OpenSpec，spec-driven）
> 日期：2026-06-08
> 范围：4 个确定性模块（codec / wire18022 / config / automation_state parse+render）的
> Rust 等价实现 + golden parity 回归 + MIPS 体型测量。**不含**网络/socket/HTTP/video/daemon 编排。

## 1. 交付模块清单

| Go 模块 | Rust 模块 | 内容 | golden 测试 |
|---|---|---|---|
| `internal/codec`（bcd/uri/results） | `src/codec.rs` | `encode_bcd`/`decode_bcd`（伪 BCD，2 sentinel 错误）、`parse_uri`（4 sentinel，`Ipv4Addr` 拒前导零+v6-mapped）、result 码常量集 | `tests/golden_codec.rs`（4 tests） |
| `internal/wire18022`（frames） | `src/wire18022.rs` | 5 模板 builder（705/710/518/708/518-appoint）、req=518 校验和、`parse_response_frame`（双分隔符）、`validate_response`（711 byte0=0x00） | `tests/golden_wire.rs`（2 tests + 覆盖下限） |
| `internal/config`（config/iniparser） | `src/config.rs` | 去反射 INI parser（`parse_ini<S:IniSink>` + 显式 match）、Config schema、deprecated 检测、`validate`/`load_config`/`validate_uri` | `tests/golden_config.rs`（3 tests，13 case） |
| `internal/automationstate`（parse/render） | `src/automation_state.rs` | `parse`（严格 2-key 整文件丢弃）、`render`（固定两行） | `tests/golden_state.rs`（4 tests） |

**排除**（按 spec D3 边界，留后续 Phase）：`wire18022.sender`（TCP 发送）、`config.ResolveIfaceList`（系统调用）、`automationstate.writeAtomic`/`Persister`（有状态写盘）。

## 2. golden parity 结果

**机制**：`dooraccess-go` 侧 `export_golden_test.go`（`//go:build export` 标签隔离，仅
`go test -tags export -run ExportGolden` 触发，`make test`（`-tags=fixtures` 无 `-run`）完全排除）
调真实 Go 函数产出 byte-exact 向量到 `dooraccess-rs/testdata/golden/`；Rust 测试各自对其断言。

**结果**：`cargo test` 全通过——codec 4 / config 3 / state 4 / wire 2，均对真实 Go 向量逐字节/逐字段断言。

**SoT 交叉验证**：主 agent 独立重跑 Go 导出，与 committed 向量逐字节 diff = 完全一致；
`wire.txt` 的 req=518 校验和示例点（`06020000`/`06021103`→`0x46`）与 `openspec/specs/anjubao-tcp18022/`
spec hex 吻合。`validate` 向量证实 711 模板 byte0=`0x00`（v0.3.3 实测）、拒 `0x2a`（frames.go doc 注释 stale）。

**实测例外表**（`testdata/golden/EXCEPTIONS.md`，SoT=Go 代码+pcap 而非 spec）：
① `parse_response_frame` 双分隔符 `&query=`/`&query*`；② `validate_response` 711 byte0=`0x00`。

**D5 IPv4 收紧（用户确认改 Go）**：`dooraccess-go/internal/codec/uri.go` 一处接受面收紧拒
IPv4-mapped IPv6（`::ffff:1.2.3.4`），与 `config.validateURI`（手写本就拒）对齐，消除两 Go 校验器
既存不一致。`go test ./...` codec/config/wire/automationstate 全过（仅 `tests/integration` 有
pre-existing `httptest` 构建债 + 沙箱网络 bind 失败，均与本改动无关）。前导零 `01.2.3.4` 两语言本就同拒。

## 3. MIPS 体型测量（Phase1 gate 数据，D2 输入）

构建路线沿用 Phase0 固定路线（nightly + `-Z build-std=std,panic_abort` + OpenWrt ath79 SDK
`mips_24kc_gcc-13.3.0_musl` 静态 sysroot + macOS `rust-lld` 经 `scripts/link-mips.sh`，全静态非 PIE）。

| 产物 | 字节数 | `file` | 说明 |
|---|---|---|---|
| Phase0 hello（基线） | 66,876 | ELF 32-bit MSB MIPS32, static, stripped | 空依赖 hello |
| `dooraccess-rs-probe`（bin，未 use lib） | 66,876 | 同上 | **不变**——main 仍 Phase0 probe，DCE 剥除未引用的 lib |
| `examples/size_probe`（链入全 4 模块） | **71,580** | ELF 32-bit MSB MIPS32, **static, stripped** | 用 `black_box` 实际调 codec/wire/config/automation_state，阻止 DCE |
| `libdooraccess_rs.rlib` | 340,854 | rlib（含元数据，非公平 binary 对比） | 编译 lib 产物 |

**结论**：Phase1 四个纯函数模块真正链入 MIPS binary 的增量 = **+4,704 字节**（71,580 − 66,876），
**可忽略**，远在 `< 4.0MB` 优先目标内。Go v0.10.0 MIPS daemon = 3,997,853 字节（含全部网络/HTTP/video，
非同类对比，Phase4+ daemon 成形后再横比）。`size_probe` 仍是 MIPS BE / softfloat / 全静态，确认
Phase1 代码不破坏 Phase0 交叉链路。

> `examples/size_probe.rs` 是体型测量辅助（非生产），因 `dooraccess-rs-probe` bin 不 use lib、
> DCE 会剥除 lib 导致测不出增量，故用 example 强制链入。

## 4. 0-crate gate（守住）

`Cargo.toml` `[dependencies]` 为空；`cargo tree` 仅输出 `dooraccess-rs v0.0.0` 本 crate，
**0 外部 crate**。4 模块全用 `std`（含 `std::net::Ipv4Addr`）。serde/tokio/libc 等取舍留后续 Phase
各自量体型决策（D2）。crate 重构为 lib+bin：`src/lib.rs` 暴露 4 模块，`src/main.rs` 保持 Phase0 probe 不变。

## 5. 向量漂移门（诚实状态）

`.github/workflows/ci.yml` 新增 `golden-drift` job（方案②）：跨仓 checkout 私有 `dooraccess-go`
+ `setup-go` + 重跑 export + 与 committed 向量逐字节 diff，不一致即 fail。

**⚠ 已知缺口（未接通）**：该 job 需 secret `DOORACCESS_GO_TOKEN`（对私有 `HerbertGao/dooraccess-go`
有 read 权限的 PAT/deploy-key）才能 checkout。**secret 未配置前，漂移门未生效**——committed 向量
可静默漂移于 Go，Rust 可能对旧向量假绿。配置该 secret 后方为真机械门。（此标注本身是 prose 自律，
按 spec tasks 2.5 元诚实要求显式记录；接通该 secret 是 apply 者收尾动作。）

## 6. 结论

Phase1 **通过**：4 个确定性模块 Rust 等价实现 + 100% golden parity（对真实 Go 向量）+ 0-crate gate 守住
+ MIPS 交叉编译验证（+4.7KB 可忽略增量、仍 BE/softfloat/静态）。具备进入 Phase2（bare HTTP + HA push）条件。

剩余收尾：配置 `DOORACCESS_GO_TOKEN` secret 激活漂移门；`dooraccess-rs` 子仓 commit + Phase1 tag（task 9.3，git 写操作留人工）。
