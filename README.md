# dooraccess-rs

把**安居宝**品牌的可视门禁接入 HomeAssistant 的 Rust daemon，跑在 **MikroTik hAP ac lite**（QCA9533，MIPS 24Kc 大端，无 FPU，OpenWrt musl，64 MB RAM）上，替换原有闭源门禁固件。

整个 daemon 只依赖 `std + libc`——安居宝二进制协议、PF_PACKET 抓帧、RTP/H.264 分片重组、FLV 封装、裸 HTTP/1.1 全部手写，不引入任何重型 crate。最终产物是一个 **全静态、MIPS 大端、softfloat、~573 KB** 的单文件二进制。

## 特性

- **安居宝 TCP 18022 协议**：`req=NNN&query=…` 二进制 wire 帧——开锁（518 三步握手 + 响铃状态不匹配重试）、视频预览启停（704 / 708）、电梯呼叫（710 appoint）等。
- **UDP 6672 按键事件监听**：按 byte19 trailer 区分响铃（`0x94`）/ 呼梯（`0x90`）；号码查询应答。
- **PF_PACKET PROMISC + BPF**：在门禁网卡上抓 6672 / 18022 帧，大端平台正确（平台无关 `htons`）。
- **裸 HTTP/1.1 控制接口**：无 Web 框架，自带 mux + 中间件链（见下表）。
- **HomeAssistant 反向 push**：`event=ring / unlock / automation_state`，用 HA Long-Lived Token。
- **响铃自开锁**：ring → 自动开门；`automation.state` 持久 flag 驱动（加载优先级 `state > config > env`），运行时可经 HTTP 拨动并写盘。
- **视频转发**：外机 RTP/RTCP/H.264（UDP 9880）→ 专有分片重组成帧 → 切 NAL → FLV 封装 → HTTP chunked 流；会话 TTL + 多消费者非阻塞 fan-out。
- **带时间戳日志**：每条日志行进程内自带墙钟时间戳 `dooraccess-rs: YYYY/MM/DD HH:MM:SS.mmm <msg>`，不依赖 syslog。

## 架构

并发模型：保留 PF_PACKET listener 线程（18022 / 6672）+ 单 worker + job 队列 + 单个 `Arc<AtomicBool>` 状态位；wire 出站串行经 worker，避免锁竞争。

| 范围 | 模块 |
|---|---|
| 协议核心 | `codec`（BCD / req 编号 / result 表）、`wire18022`（wire 模板字节级）、`listen18022` / `listen6672`（PF_PACKET + BPF）、`bpf`、`ffi`（libc 封装 + const-assert 守门） |
| HTTP / 控制 | `httpx`（bare HTTP/1.1）、`control`（控制面 endpoint + automation state）、`info`（自描述） |
| daemon 编排 | `daemon`（线程 / 关闭）、`orchestration`（接线）、`self_unlock`（响铃自开锁消费者）、`automation_state`（持久 flag 原子写） |
| 开锁 | `unlock`（710 + 518 三步 + 重试）、`wire_sender` / `sender`（SO_BINDTODEVICE 出站） |
| 视频 | `video/`：`preview`（704/708 信令）、`rtp` / `rtcp`、`reassembler`（专有分片重组）、`frame_buffer`（fan-out）、`transmux`（FLV）、`session`（TTL / 生命周期） |
| 日志 | `log`（中央带戳日志层 + 机械门禁） |

## HTTP 控制接口

监听 `0.0.0.0:8080`（裸 HTTP/1.1）。

| 方法 / 路径 | 用途 |
|---|---|
| `GET /info` | daemon 自描述 / 探活（brand / monitor / stations / video 字段） |
| `POST /unlock` | 开锁（710 + 518 三步握手，含响铃状态不匹配重试） |
| `POST /auto_unlock` | 拨动「响铃自开锁」flag（body `{"on":bool}`），写盘持久 |
| `POST /auto_hangup` | 拨动「自动挂断」flag，写盘持久 |
| `GET /automation` | 返回当前 automation 状态 |
| `POST /video/start` | 启动视频会话（body `{"outdoor":"<外机URI>"}`，须在 `stations` allowlist 内）→ `{session_id, stream_url, ttl}` |
| `GET /video/<id>/stream.flv` | 拉 FLV 视频流（chunked） |
| `POST /video/stop` | 停止视频会话（发 req=708 + RTCP BYE） |
| `GET /playback` | 回放（预留 stub） |

## 配置

INI 格式（默认 `/etc/dooraccess-go/config.ini`，可经 `--config <path>` 覆盖）：

```ini
sip = 06021103@172.16.106.91:18022     ; 本机室内机（monitor）URI
iface = br-door                         ; 门禁网卡（PF_PACKET 抓帧 + 出站绑定）

[listen]
addr = 0.0.0.0
port = 8080

[station "1"]                           ; 外机（可多个 station "N"）
sip = 06020000@172.16.106.152:18022

[hass]
api = http://172.16.106.x:8123          ; HomeAssistant 地址
token = <HA Long-Lived Token>           ; 反向 push 用

[video]
forward = true
format = flv

[automation]
auto_unlock = false
auto_hangup = false
```

运行时 automation flag 优先读 `/etc/dooraccess-go/automation.state`（持久），其次 `[automation]` 出厂默认。

## 构建

```bash
# host（stable）—— 自检构建 / 测试
make build
make test

# MIPS 交叉构建（一次性环境准备）
rustup toolchain install nightly --profile minimal
rustup component add rust-src llvm-tools-preview --toolchain nightly
make fetch-sdk        # 下载 + 解压 OpenWrt mips_24kc musl sysroot 到 .openwrt-sdk/

# 交叉构建 + 校验 + 产物
make build-mips       # → target/mips-unknown-linux-musl/release/dooraccess-rs
make verify-mips      # 断言 MIPS 大端 + softfloat + 全静态
make dist             # 校验并拷到 dist/dooraccess-rs-mips（注：dist 不触发 build-mips，改源后先 build-mips）
```

`mips-unknown-linux-musl` 是 Rust **Tier 3** 目标：rustup 不带预编译 std，须 `-Z build-std` 从 `rust-src` 现编 std，并链外部 musl sysroot。`scripts/link-mips.sh` 用宿主机原生的 `rust-lld` + SDK 静态库做全静态、非 PIE 链接，因此 **macOS arm64 也能直接出 MIPS 二进制**（不跑 SDK 自带的 Linux gcc）。

> macOS 上若 `cargo` 解析到 Homebrew 版（不懂 `+nightly`），把 rustup proxy 提前：`PATH="$HOME/.cargo/bin:$PATH" make build-mips`。

## 测试与质量门禁

```bash
make test                       # cargo test（含协议 golden / 字节保真 / 降级路径单测）
make clippy                     # cargo clippy --all-targets，零警告
bash scripts/check-log-gate.sh  # 日志门禁：无残留无戳直写 stderr + log.rs 零 unwrap/expect + 单次加锁写
```

仓库装有 `.git/hooks/pre-commit`（`cargo fmt --check`），未格式化禁止提交。

## 部署到 hAP

交叉构建产物是单文件全静态二进制，部署即「上传 + 运行」：

1. `make dist` 得到 `dist/dooraccess-rs-mips`；
2. `scp -O` 到 hAP `/tmp/dooraccess-rs`，`ls -l` 核字节数（**禁 `sha256sum`/`md5sum`**——一次读全文件进 RAM 会在 64 MB 设备上触发 OOM）；
3. `chmod +x` 后用 `setsid /tmp/dooraccess-rs --config /etc/dooraccess-go/config.ini >/tmp/rust.log 2>&1 &` 脱控制终端后台运行（hAP busybox 有 `setsid`、无 `nohup`）。

> ⚠️ hAP ac lite 64 MB RAM 极紧：SSH 必加 `-o ConnectTimeout=8 -o ServerAliveInterval=15`，失败立即停手不 retry（多 session 累积耗 RAM）。

## 开发约定

- **crate gate = `std + libc` only**：`libc` 是唯一外部依赖，破口框死在三处 PF_PACKET / SO_BINDTODEVICE FFI；拒 nix / pnet / socket2 / tokio。要放宽须走 OpenSpec change。
- **MIPS32 红线**：禁 64 位原子（TTL 用 `Mutex<Instant>`、tunable 用 `AtomicU32`）。
- **规格驱动**：行为变更走 OpenSpec change（提案 → 实现 → 归档生效）。
- **改动走 PR + Bugbot 复审**。
- `release` profile：`panic=abort` + LTO + `opt-level=z` + strip（体型优先）。

## 项目状态

完整 daemon 已实现并在 hAP 上**灰度运行**：协议核心 / HTTP 控制面 / PF_PACKET listener / 响铃自开锁 / 视频转发 / 带戳日志全栈就位。真机验证：响铃自开锁**物理门开**、HACS 端到端**收到视频流**、PF_PACKET 大端取帧、日志带时间戳。
