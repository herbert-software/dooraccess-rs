# Phase2 httpx 行为台账（G1 golden 来源）

对照 Go `dooraccess-go/internal/httpx/{server,mux,parse,response,types,client}.go` 及各 `_test.go`。
本文件列 Rust 须逐字节/逐语义复刻的精确行为；后续 golden 向量与 Rust 测试以此为验收清单。

## 1. Request parser（`parse.go` / `parse_test.go`）

### 1.1 协议常量

| 常量 | 值 | 用途 |
|------|-----|------|
| `maxRequestLineBytes` | 8192 | request line 单行上限 |
| `maxHeaderBytes` | 16384 | header section 累计字节上限 |
| `maxHeaderCount` | 64 | 单请求 header 行数上限 |
| `maxBodyBytes` | 1<<20 (1 MB) | body 声明/读取上限 |

### 1.2 接受与拒绝项

| 场景 | 行为 | 锚测试 |
|------|------|--------|
| GET / POST happy path | 解析 method、path、raw_query、Host、headers、Content-Length body | `TestReadRequest_HappyPath_GET/POST` |
| Query string | `splitPathQuery`：`/api?a=1&b=2` → path=`/api`, query=`a=1&b=2` | `TestReadRequest_QueryString` |
| 仅 GET/POST | 其它 method（含 HEAD、DELETE、PUT）→ **405** `*httpProtocolError` | `TestReadRequest_BadMethod_Rejects`, `TestReadRequest_HEAD_Rejects` |
| HEAD | 拒绝（405） | `TestReadRequest_HEAD_Rejects` |
| HTTP/1.1 无 Host | **400** `"HTTP/1.1 requires Host header"` | `TestReadRequest_NoHostHeader_HTTP11_Rejects` |
| HTTP/1.0 无 Host | 接受（不强制 Host） | `parse.go:56-58` |
| 非 HTTP/1.0/1.1 版本 | **426** Upgrade Required | `parse.go:44-46` |
| Transfer-Encoding 含 `chunked`（大小写/列表/空白） | **400** `"chunked request body not supported"` | `TestReadRequest_TransferEncodingChunkedInList_Rejects` |
| Transfer-Encoding 非 chunked（identity/gzip/compress/列表） | **接受** | `TestReadRequest_TransferEncodingNonChunked_Accepted` |
| Upgrade: h2c | **426** `"HTTP/2 cleartext upgrade not supported"` | `TestReadRequest_H2CUpgrade_Rejects` |
| 非法 Content-Length（负值/非数字） | **400** | `TestReadRequest_BadContentLength` |
| Content-Length > 1MB | **413 PayloadTooLarge**（parser 层，非 400） | `TestReadRequest_BodyTooLarge_Rejects` |
| 无 Content-Length | body 长度 0，空 body reader | `parse.go:73-77` |
| Header 名大小写不敏感 | canonicalize 后 Get 等价（`host`/`CONTENT-TYPE`） | `TestReadRequest_HeaderCaseInsensitive` |
| 空连接 EOF | 返回 `io.EOF`（干净 close） | `TestReadRequest_EOFOnEmpty_ReturnsEOF` |
| 畸形 request line / header | **400** 或 **414** URI Too Long | `TestParseRequestLine` |

### 1.3 Header 存储

- key 经 `textproto.CanonicalMIMEHeaderKey` 规范化（`Content-Type` 形式）
- `Header.Get/Set/Add/Del/Values` 大小写不敏感
- body 用 `LimitReader(br, n)` 包裹，handler 读完即停

## 2. Response writer（`response.go` / `chunked_test.go`）

| 场景 | 行为 | 锚测试 |
|------|------|--------|
| 无 Content-Length | 自动 `Transfer-Encoding: chunked`，每 Write 一 chunk frame | `TestResponseWriter_ChunkedAuto_OnNoContentLength` |
| 有 Content-Length | fixed-length 模式，无 chunked | `TestResponseWriter_FixedLength_OnContentLength` |
| Connection | **恒设** `Connection: close`（WriteHeader 时强制） | `TestResponseWriter_ConnectionCloseAlwaysSet` |
| 首次 Write 未调 WriteHeader | 自动 `WriteHeader(200)` | `TestResponseWriter_DefaultStatus200_OnFirstWrite` |
| WriteHeader 幂等 | 仅第一次 status 生效，后续 silent ignore | `TestResponseWriter_WriteHeaderIdempotent` |
| handler 无 write 即返回 | `finish()` 仍发 **200** + chunked 终止 `0\r\n\r\n` | `TestResponseWriter_FinishWithoutWrite_StillEmits200` |
| chunked frame 格式 | `hex_size\r\n` + body + `\r\n`；终止 `0\r\n\r\n` | `TestWriteChunk_SingleFrame`, `TestWriteChunk_HexSize` |
| Flush | 未写 header 时先 WriteHeader(200)，再 flush bufio | `response.go:101-105` |
| 空 Write | 返回 (0, nil)，不写 chunk | `response.go:87-89` |

## 3. ServeMux（`mux.go` / `mux_test.go`）

| 场景 | 行为 | 锚测试 |
|------|------|--------|
| 精确匹配 | pattern 无尾 `/` → 仅精确 path 命中 | `TestMux_ExactMatch` |
| 前缀匹配 | pattern 尾 `/` → `strings.HasPrefix` | `TestMux_PrefixMatch` |
| 最长前缀优先 | prefix 列表按长度倒序，第一个匹配即最长 | `TestMux_LongestPrefixWins` |
| 精确优先于前缀 | `/video/start` 精确注册时优先于 `/video/` 前缀 | spec + `mux.go:78-88` |
| 无匹配 | **404** + body `"Not Found"`（9 字节）+ `Content-Type: text/plain; charset=utf-8` + `Content-Length: 9`（**非 JSON**） | `TestMux_404_NoMatch`, `TestMux_404_BodyHasContentLength` |
| 重复 pattern | 构造期 **panic** `"duplicate pattern"` | `TestMux_DuplicatePattern_Panics` |
| nil handler | 构造期 **panic** `"nil handler"` | `TestMux_NilHandler_Panics` |
| 空 pattern | panic `"empty pattern"` | `mux.go:35-37` |
| path-only | 非 method-aware；GET/POST 同 path 会 duplicate panic | design + `mux.go` 注释 |

## 4. Server（`server.go` / `server_test.go`）— G2 任务，本组仅台账

| 场景 | 行为 | 锚测试 |
|------|------|--------|
| 模型 | accept-loop + goroutine-per-conn + blocking socket | `server.go` |
| 禁 keep-alive | 每连接 1 请求后 close | `server.go:93-98` |
| 协议错 | `writeProtocolError` 直写 status + `Connection: close` + `Content-Length: 0` | `server.go:197-209` |
| handler panic | recover → 尝试写 500，server 不挂 | `TestServer_HandlerPanic_WritesGenericError` |
| ReadHeaderTimeout + ReadTimeout | headers 阶段用较紧 deadline；headers 完后切 whole-request 绝对 deadline `connStart+ReadTimeout` | `TestServer_ReadHeaderTimeoutTighterThanReadTimeout`, `TestServer_ReadTimeoutIsWholeRequestDeadline` |
| WriteTimeout=0 | 不设写 deadline | spec + `server.go:132-134` |
| Shutdown | 拒新连接 + 等 in-flight handler 完成 | `TestServer_Shutdown_StopsAccepting` |
| request ctx | shutdown 时 cancel handler ctx | `TestServer_RequestContextCanceledOnShutdown` |

## 5. Client（`client.go` / `client_test.go`）— G3 任务，本组仅台账

| 场景 | 行为 | 锚测试 |
|------|------|--------|
| 仅 `http://` URL | 拒 https/ftp/裸 host | `TestClient_NewRequest_RejectsHTTPS` |
| 仅 GET/POST | 拒其它 method | `TestClient_NewRequest_RejectsBadMethod` |
| 每请求新 dial | `Connection: close`，无连接池 | `client.go` |
| Timeout | dial+读写总上限 `SetDeadline` | `TestClient_Timeout_FailsFast` |
| ctx cancel | 主动 close conn | `TestClient_ContextCancel` |
| 响应 chunked 解码 | `readChunkedBody` | `TestClient_ChunkedResponseDecoded` |
| splitURL | 补默认 `:80` | `TestSplitURL` |

## 6. JSON encoder 五项隐式行为（`json_helpers.go` / `render_json.go` / spec 9.1）

| # | 行为 | Go 来源 | Rust 须复刻 |
|---|------|---------|-------------|
| ① | map 路径 key **字母序** | `json.Marshal(map[string]any)` hapush | `encode_map` + BTreeMap |
| ① | struct 路径 key **声明序** | `json.Marshal(struct)` writeJSON | `encode_struct_fields` 按传入序 |
| ② | 空 slice → `[]` 非 `null` | `render_json.go:41-43` | outdoor_stations 空数组 |
| ③ | Encoder 尾随 `\n` | `json.NewEncoder.Encode` /info | `trailing_newline: true` |
| ③ | Marshal 无尾随 `\n` | `json.Marshal` writeJSON | `trailing_newline: false` |
| ④ | HTML 转义开 | 默认 Marshal（`\u003c` 等） | `escape_html: true` |
| ④ | HTML 转义关 | `SetEscapeHTML(false)` /info | `escape_html: false` |
| ⑤ | framing 由 response writer 控制 | writeJSON 设 CL；/info 不设 CL→chunked | encoder 只产 body 字节 |

### 机械基线（Go 已验证）

- map `{event,video_forward,video_format,wwan}` → `{"event":...,"video_format":...,"video_forward":...,"wwan":...}`
- struct `{Result;AutoUnlock}` → `{"result":0,"auto_unlock":true}`（result 在前，非字母序）

## 7. HTTP 信封 vs wire 编排边界（G4，对照 Go `http8080`）

### 7.1 分层原则（`handlers.go` 包注释 + `server.go` `Handler()`）

| 层 | 职责 | 典型状态码 / body |
|----|------|-------------------|
| **中间件链** | method / Content-Type / stations guard | 405+`Allow`+`{"error":...}`；415+`{"error":...}`；200+`{"result":-100}` |
| **HTTP handler（信封）** | 解析 JSON body → 字段校验 → 调下层抽象 | 400+`{"error":...}`；200+`{"result":int,...}`；503 wire 下游失败 |
| **wire 编排（Phase 3）** | `ExecuteUnlock` / retry / bye 早停 / `Sender.SendContext` | 不进 HTTP 层；结果映射回 `result` int |

**关键切点（design D1）**：Phase 2 `/unlock` 薄切在 Go `ExecuteUnlock` 边界——`handleUnlock` 只做 body→BCD→target，然后调 `trait Sender`（mock），**不**移植 retry/bye/`runUnlockWithRetry`。

### 7.2 mux 注册与中间件链（`server.go:181-207`）

```
业务 POST（/unlock /elev/* /bye /permit /ack）:
  methodGuardPOST → requireJSONContentType → requireStationsConfigured → handler

控制面 POST（/auto_unlock /auto_hangup）:
  methodGuardPOST → requireJSONContentType → handler   （无 stations guard）

元信息 GET（/info /playback /automation）:
  methodGuard("GET", handler)   （无 JSON / stations guard）
```

- `/info` `/playback` **不走** `requireStationsConfigured`（stations 空仍 200）。
- `httpx.ServeMux` path-only → GET `/automation` 必须独立 path，不能与 POST 同 path。

### 7.3 `/unlock` 薄切要点（`handlers.go:151-210` 信封部分）

1. **入参**：`POST /unlock` JSON `{"from":"<室内机 URI>","to":"<外机 URI>"}`（`from` 可空串）。
2. **中间件**：完整业务链（POST + JSON + stations）；stations 空 → 200/`result:-100`，**不发 wire**。
3. **信封内校验**：`parseJSONBody` 失败 → 400；`to` 缺失 → 400；URI/BCD 解析失败 → 400。
4. **BCD 颠倒**：`to`(外机)→wire caller；`from`(室内机)→wire callee（spec req-710-login）。
5. **下层调用**：`ExecuteUnlock(ctx, callerBCD, calleeBCD, targetIP, targetPort, "http")`——Phase 2 换 `trait Sender` mock。
6. **响应映射（信封层）**：`result=0` 成功；`-103` silent FIN；`-1` 协议偏移；503 wire timeout（Phase 3 分类，G6 薄切 mock 只返 canned result）。
7. **HTTP vs 业务错分层**：415 = Content-Type 客户端错（中间件）；400 = body/字段客户端错（handler）；200+负 `result` = 业务/wire 语义（非 HTTP 4xx）。

### 7.4 JSON helper 契约（`json_helpers.go`）

- `writeJSON`：`json.Marshal` 等价 → struct 声明序 key、HTML 转义开、**无尾随 `\n`**、设 `Content-Length`。
- `errorJSON`：统一 `{"error":"<msg>"}`。
- `parseJSONBody`：`LimitReader(1MB)` 读 body；空 body 通过；非 JSON → error（handler 写 400）。
- `isJSONMediaType`：第一个 `;` 前 trim + `EqualFold("application/json")`。

Rust 对应：`src/control.rs`（G4 中间件 + helper）；endpoint handler 留 G5/G6。

## 8. 本组范围外（后续组）

- `server.go` accept-loop / shutdown / deadline（2.4、2.5）— G2 已完成
- `client.go` 完整实现（3.x）— G3 已完成
- `http8080` endpoint handler（G5/G6）
- `hapush` / `info` 逻辑（G3/G5）— 已完成

## 8. `internal/info` + `hapush` 出站契约（G3 台账）

对照 Go `internal/info/{selfdesc,render_json}.go` 与 `internal/hapush/client.go`。

### 8.1 `GET /info` — `jsonInfo` 6 字段

| 字段 | Go 来源 | HACS 消费点 | Rust `info::build` + `render_json` |
|------|---------|-------------|-------------------------------------|
| `daemon` | 常量 `"dooraccess-go"` | `config_flow.py` 探活身份校验（`!= dooraccess-go` → wrong_daemon） | 同常量 |
| `version` | ldflags / 默认 `"dev"` | 兼容性 check（v0.2.0+ prefix） | `safe_or_default(version, "dev")` |
| `brand` | `config.SupportedBrand` = `"anjubao"` | sanity check | `SUPPORTED_BRAND` |
| `monitor` | `cfg.SIP` | `button.py` / `lock.py` wire URI `from` | `cfg.sip` |
| `outdoor_stations` | `[]StationDesc{SIP}` from `cfg.Stations` | `button.py` / `lock.py` / `__init__.py` camera 列表；元素仅 `sip` | 空 → `[]` 非 `null` |
| `video` | `VideoDesc` 嵌套对象 | `__init__.py` / `camera.py` | 见下表 |

`video` 子字段（声明序）：

| 子字段 | Go 来源 | HACS 消费 |
|--------|---------|-----------|
| `forward` | `cfg.Video.Forward` | fallback（`forward_supported` 缺失时） |
| `forward_supported` | `cfg.Video.Forward` 回声 | `camera` entity `available` |
| `protocol` | 常量 `"anjubao-h264"` | 未直接读（文档/探活） |
| `format` | `cfg.Video.Format` | `stream_source` URL 后缀（flv/mjpeg） |
| `cache_path` | `cfg.Video.CachePath` | 未直接读 |

**不进 `/info` body**（Go `Build(probeHA=false)` 仍可能调但结果丢弃）：`IfaceIP`、`HassFacing`、`HassReachable`、token/endpoint/banner 字段。Rust **跳过** `resolveIfaceIP` / `discoverHassFacingIP` 网络调用。

**JSON 编码**：`json.NewEncoder` + `SetEscapeHTML(false)` + `Encode` → struct 声明序 + 尾随 `\n`（`JsonOptions::ENCODE`）。

### 8.2 HA 反向 push — `hapush.Client.Push`

| 项 | Go 行为 | Rust `HaPushClient::push` |
|----|---------|---------------------------|
| URL | `http://<hass.ipaddr>:<hass.port><cfg.Hass.API>` | `build_url` |
| Method | POST | `METHOD_POST` |
| Header | `Content-Type: application/json`；`Authorization: Bearer <cfg.Hass.Token>` | 同 |
| Body | `json.Marshal(map)`：`{"event":<n>, ...fields}` **key 字母序** | `encode_map` + `BTreeMap` + `JsonOptions::MARSHAL` |
| Timeout | 5s | `Client { timeout: Some(5s) }` |
| `api==""` | `ErrNotConfigured`（log debug，静默跳过） | `PushError::NotConfigured` |
| HTTP 200 | `nil` | `Ok(())` |
| 其它 status / 网络错 | 包装 error + log warning | `PushError::Status` / `PushError::Do` |

### 8.3 `PushDiagnosis` body 基线（字母序）

`event=diagnosis` + 展平字段（map marshal 字母序）：

1. `event`（经 `Push` 注入）
2. `video_format` ← `cfg.Video.Format`
3. `video_forward` ← `"1"` / `"0"`（`cfg.Video.Forward`）
4. `wwan` ← `discoverHAFacingIP()` 优先；空则 `lookupWWAN(cfg.Iface)`

`wwan` 语义：**家庭网（HA-facing）**出口 IP，非门禁网 `cfg.Iface` IP（除非 fallback）。

`discoverHAFacingIP`：UDP `connect` 到 `hass.ipaddr:hass.port` 后读 `local_addr`（不真发包）。

`lookupWWAN(iface)`：`InterfaceByName` → 第一个非 loopback IPv4；Rust Linux 用 `SIOCGIFADDR` ioctl，非 Linux 返空。
