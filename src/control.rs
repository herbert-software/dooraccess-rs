//! 控制面 HTTP 中间件链 + JSON 响应 helper（Phase2 G4）。
//!
//! 复刻 Go `internal/http8080/json_helpers.go` 中间件字节契约；endpoint handler 由 G5/G6 填充。

#[allow(unused_imports)]
use std::io::{self, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::codec::{self, result};
use crate::config::Config;
use crate::httpx::json::{encode_struct, JsonOptions, JsonValue};
use crate::httpx::mux::ServeMux;
#[allow(unused_imports)]
use crate::httpx::{
    Handler, HandlerFunc, Header, Request, ResponseWriter, METHOD_GET, METHOD_POST,
    STATUS_BAD_REQUEST, STATUS_METHOD_NOT_ALLOWED, STATUS_OK,
};
use crate::info;
use crate::sender::Sender;
use crate::unlock::{UnlockOutcome, WireKind};

/// Go `httpx.StatusUnsupportedMedia`（`types.go`）；`httpx/mod.rs` 未导出，本模块自用。
const STATUS_UNSUPPORTED_MEDIA: u16 = 415;

/// Go `httpx.StatusServiceUnavailable`（503）；wire-failure 默认分支（Timeout/其它下游故障）
/// 用之（锚 Go `respondWireFailure` handlers.go:708）。`httpx/mod.rs` 未导出，本模块自用。
const STATUS_SERVICE_UNAVAILABLE: u16 = 503;

/// handler 层 JSON body 读取上限（Go `parseJSONBody` `LimitReader(1MB)`）。
pub const MAX_JSON_BODY_BYTES: u64 = 1024 * 1024;

// ---------------------------------------------------------------------------
// JSON 响应 helper（Go `json_helpers.go` writeJSON / errorJSON / parseJSONBody）
// ---------------------------------------------------------------------------

/// 写 HTTP JSON 响应：`Content-Type: application/json` + `Content-Length` + struct marshal body。
///
/// `body` 为 `None` 时只写 status（`Content-Length: 0`）。encode 失败回退 500 + error JSON。
pub fn write_json(w: &mut dyn ResponseWriter, status: u16, body: Option<&[(&str, JsonValue)]>) {
    w.header().set("Content-Type", "application/json");
    let Some(fields) = body else {
        w.header().set("Content-Length", "0");
        w.write_header(status);
        return;
    };
    let buf = encode_struct(fields, JsonOptions::MARSHAL);
    w.header().set("Content-Length", &buf.len().to_string());
    w.write_header(status);
    let _ = w.write(&buf);
}

/// 写 4xx/5xx 统一错误体 `{"error":"<msg>"}`。
pub fn error_json(w: &mut dyn ResponseWriter, status: u16, msg: &str) {
    write_json(
        w,
        status,
        Some(&[("error", JsonValue::String(msg.to_string()))]),
    );
}

/// 从 reader 读取 JSON body（1MB `take` 限）；空 body 返回空 vec；非空须通过最小 JSON 语法校验。
///
/// 失败返回 error 但不写响应——调用方决定 `error_json` 状态码（通常 400）。
/// Content-Type 校验由 `require_json_content_type` 中间件负责。
///
/// **注**：`httpx::Request.body` 为已缓冲的 `Vec<u8>`（parser 按 Content-Length 读入内存）。
/// handler 仅持 `&Request` 时经 `read_request_body` 以 `&[u8]`（impl Read）读取，无需 unsafe。
/// 本函数仍接受 `&mut dyn Read` 与 Go `parseJSONBody(r.Body)` 对齐（reader 抽象保留）。
pub fn parse_json_body(reader: &mut dyn Read) -> Result<Vec<u8>, String> {
    let raw = read_json_body(reader)?;
    if raw.is_empty() {
        return Ok(raw);
    }
    validate_json_syntax(&raw)?;
    Ok(raw)
}

/// 仅读取 body 字节（1MB 限），不做 JSON 校验。
pub fn read_json_body(reader: &mut dyn Read) -> Result<Vec<u8>, String> {
    let mut limited = reader.take(MAX_JSON_BODY_BYTES);
    let mut buf = Vec::new();
    limited
        .read_to_end(&mut buf)
        .map_err(|e| format!("read request body: {e}"))?;
    Ok(buf)
}

fn validate_json_syntax(raw: &[u8]) -> Result<(), String> {
    let trimmed = trim_ascii_whitespace(raw);
    if trimmed.is_empty() {
        return Ok(());
    }
    match parse_json_value(trimmed) {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("parse JSON body: {e}")),
    }
}

fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map(|i| i + 1)
        .unwrap_or(start);
    &bytes[start..end]
}

/// 最小 JSON 值解析器（0-crate；覆盖 endpoint 用到的 object/array/scalar）。
fn parse_json_value(input: &[u8]) -> Result<(), &'static str> {
    let mut p = JsonParser::new(input);
    p.parse_value()?;
    p.skip_ws();
    if !p.at_end() {
        return Err("trailing data");
    }
    Ok(())
}

struct JsonParser<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, pos: 0 }
    }

    fn at_end(&self) -> bool {
        self.pos >= self.input.len()
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let b = self.peek()?;
        self.pos += 1;
        Some(b)
    }

    fn skip_ws(&mut self) {
        while let Some(b) = self.peek() {
            if b.is_ascii_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn parse_value(&mut self) -> Result<(), &'static str> {
        self.skip_ws();
        match self.peek() {
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b'"') => self.parse_string(),
            Some(b't') => self.expect_literal("true"),
            Some(b'f') => self.expect_literal("false"),
            Some(b'n') => self.expect_literal("null"),
            Some(b'0'..=b'9') | Some(b'-') => {
                self.parse_number();
                Ok(())
            }
            _ => Err("invalid JSON value"),
        }
    }

    fn parse_object(&mut self) -> Result<(), &'static str> {
        self.bump(); // {
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.bump();
            return Ok(());
        }
        loop {
            self.skip_ws();
            if self.peek() != Some(b'"') {
                return Err("expected object key");
            }
            self.parse_string()?;
            self.skip_ws();
            if self.bump() != Some(b':') {
                return Err("expected colon");
            }
            self.parse_value()?;
            self.skip_ws();
            match self.bump() {
                Some(b'}') => return Ok(()),
                Some(b',') => continue,
                _ => return Err("expected comma or closing brace"),
            }
        }
    }

    fn parse_array(&mut self) -> Result<(), &'static str> {
        self.bump(); // [
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.bump();
            return Ok(());
        }
        loop {
            self.parse_value()?;
            self.skip_ws();
            match self.bump() {
                Some(b']') => return Ok(()),
                Some(b',') => continue,
                _ => return Err("expected comma or closing bracket"),
            }
        }
    }

    fn parse_string(&mut self) -> Result<(), &'static str> {
        self.bump(); // opening "
        while let Some(b) = self.bump() {
            match b {
                b'"' => return Ok(()),
                b'\\' => {
                    let esc = self.bump().ok_or("truncated escape")?;
                    match esc {
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {}
                        b'u' => {
                            for _ in 0..4 {
                                let h = self.bump().ok_or("truncated unicode escape")?;
                                if !h.is_ascii_hexdigit() {
                                    return Err("invalid unicode escape");
                                }
                            }
                        }
                        _ => return Err("invalid escape"),
                    }
                }
                b if b < 0x20 => return Err("control char in string"),
                _ => {}
            }
        }
        Err("unterminated string")
    }

    fn parse_number(&mut self) {
        if self.peek() == Some(b'-') {
            self.bump();
        }
        if self.peek() == Some(b'0') {
            self.bump();
        } else {
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.bump();
            }
        }
        if self.peek() == Some(b'.') {
            self.bump();
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.bump();
            }
        }
        if matches!(self.peek(), Some(b'e') | Some(b'E')) {
            self.bump();
            if matches!(self.peek(), Some(b'+') | Some(b'-')) {
                self.bump();
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.bump();
            }
        }
    }

    fn expect_literal(&mut self, lit: &str) -> Result<(), &'static str> {
        for expected in lit.bytes() {
            if self.bump() != Some(expected) {
                return Err("invalid literal");
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 中间件（Go `json_helpers.go` methodGuard / requireJSONContentType /
// requireStationsConfigured）
// ---------------------------------------------------------------------------

/// method 校验中间件：不匹配 → 405 + `Allow` + JSON error body。
pub fn method_guard<H: Handler>(allowed: &str, inner: H) -> MethodGuard<H> {
    MethodGuard {
        allowed: allowed.to_string(),
        inner,
    }
}

/// `method_guard(METHOD_POST, ...)` 便捷别名。
pub fn method_guard_post<H: Handler>(inner: H) -> MethodGuard<H> {
    method_guard(METHOD_POST, inner)
}

pub struct MethodGuard<H> {
    allowed: String,
    inner: H,
}

impl<H: Handler> Handler for MethodGuard<H> {
    fn serve_http(&self, w: &mut dyn ResponseWriter, r: &Request) {
        if r.method != self.allowed {
            w.header().set("Allow", &self.allowed);
            error_json(
                w,
                STATUS_METHOD_NOT_ALLOWED,
                &format!("method {} not allowed; use {}", r.method, self.allowed),
            );
            return;
        }
        self.inner.serve_http(w, r);
    }
}

/// Content-Type 须为 `application/json`（大小写不敏感；可选 `; charset=utf-8` 参数）。
pub fn require_json_content_type<H: Handler>(inner: H) -> RequireJsonContentType<H> {
    RequireJsonContentType { inner }
}

pub struct RequireJsonContentType<H> {
    inner: H,
}

impl<H: Handler> Handler for RequireJsonContentType<H> {
    fn serve_http(&self, w: &mut dyn ResponseWriter, r: &Request) {
        let ct = r.header.get("Content-Type").unwrap_or("");
        if !is_json_media_type(ct) {
            error_json(
                w,
                STATUS_UNSUPPORTED_MEDIA,
                "Content-Type must be application/json",
            );
            return;
        }
        self.inner.serve_http(w, r);
    }
}

/// 大小写不敏感校验 media type 是否为 `application/json`（Go `isJSONMediaType`）。
pub fn is_json_media_type(ct: &str) -> bool {
    if ct.is_empty() {
        return false;
    }
    let media_type = match ct.find(';') {
        Some(i) => &ct[..i],
        None => ct,
    };
    media_type.trim().eq_ignore_ascii_case("application/json")
}

/// `cfg.stations` 为空 → 200 + `{"result":-100}`，不调下游、不发 wire。
pub fn require_stations_configured<H: Handler>(
    cfg: Arc<Config>,
    inner: H,
) -> RequireStationsConfigured<H> {
    RequireStationsConfigured { cfg, inner }
}

// 持 Arc<Config>（非 &'a Config）：handler 须 'static 才能存进 Box<dyn Handler> mux，
// 借用会逼出自引用结构（MuxHandler 同时存 handler + cfg）。
pub struct RequireStationsConfigured<H> {
    cfg: Arc<Config>,
    inner: H,
}

impl<H: Handler> Handler for RequireStationsConfigured<H> {
    fn serve_http(&self, w: &mut dyn ResponseWriter, r: &Request) {
        if self.cfg.stations.is_empty() {
            write_json(
                w,
                STATUS_OK,
                Some(&[("result", JsonValue::Number(result::NO_CONFIG as i64))]),
            );
            return;
        }
        self.inner.serve_http(w, r);
    }
}

// ---------------------------------------------------------------------------
// 中间件链组合（G6 mux 注册用）
// ---------------------------------------------------------------------------

/// 业务 endpoint 标准链：`POST` + JSON Content-Type + stations guard → inner。
///
/// 对齐 Go `server.go`：
/// `methodGuardPOST(requireJSONContentType(s.requireStationsConfigured(handler)))`
pub fn chain_business_post<H: Handler>(
    cfg: Arc<Config>,
    inner: H,
) -> MethodGuard<RequireJsonContentType<RequireStationsConfigured<H>>> {
    method_guard_post(require_json_content_type(require_stations_configured(
        cfg, inner,
    )))
}

/// 控制面 POST endpoint 链：`POST` + JSON Content-Type（无 stations guard）。
///
/// 对齐 Go `/auto_unlock` `/auto_hangup` 注册。
pub fn chain_control_post<H: Handler>(inner: H) -> MethodGuard<RequireJsonContentType<H>> {
    method_guard_post(require_json_content_type(inner))
}

// ---------------------------------------------------------------------------
// 运行时 automation flag + 持久化 / push hook（G6）
// ---------------------------------------------------------------------------

/// daemon 自开锁/挂断两个运行时可变 flag（对齐 Go `AutomationState`）。
///
/// 内部两 flag 用 `Arc<AtomicBool>` 持有，故 [`AutomationState::share`] 可派生**共享同一组
/// 原子真值**的第二个 handle——daemon 同一份 state 既注入 HTTP server（handler 拨动）又供
/// Persister value_source 读运行时真值（Phase 4 G3a 接线 §5.4/§6.4：单一 state 不分叉）。
pub struct AutomationState {
    auto_unlock: Arc<AtomicBool>,
    auto_hangup: Arc<AtomicBool>,
}

impl AutomationState {
    pub fn new(auto_unlock: bool, auto_hangup: bool) -> Self {
        Self {
            auto_unlock: Arc::new(AtomicBool::new(auto_unlock)),
            auto_hangup: Arc::new(AtomicBool::new(auto_hangup)),
        }
    }

    /// 派生一个**共享同一组原子真值**的 handle（拨动 / 读取互相可见）。
    ///
    /// daemon 用它把同一份 automation state 既给 HTTP server（handler `store_*` 拨动）又给
    /// Persister value_source（`load_*` 读运行时真值）——两 handle 背后是同一对
    /// `Arc<AtomicBool>`，endpoint 拨动后 persister 立即读到翻转值（不分叉，对齐 Go 单一
    /// `automationState` 注入两处）。
    pub fn share(&self) -> Self {
        Self {
            auto_unlock: Arc::clone(&self.auto_unlock),
            auto_hangup: Arc::clone(&self.auto_hangup),
        }
    }

    pub fn load_auto_unlock(&self) -> bool {
        self.auto_unlock.load(Ordering::SeqCst)
    }

    pub fn load_auto_hangup(&self) -> bool {
        self.auto_hangup.load(Ordering::SeqCst)
    }

    pub fn store_auto_unlock(&self, on: bool) {
        self.auto_unlock.store(on, Ordering::SeqCst);
    }

    pub fn store_auto_hangup(&self, on: bool) {
        self.auto_hangup.store(on, Ordering::SeqCst);
    }
}

impl Default for AutomationState {
    fn default() -> Self {
        Self::new(false, false)
    }
}

/// Phase 2 持久化 hook（`None` = no-op；真原子写盘留 Phase 4）。
pub trait PersistHook: Send + Sync {
    fn persist(&self);
}

/// 闭包持久化 hook；`persist` 吞掉 `Err`（endpoint 仍 `result=0`）。
pub struct FnPersistHook(pub Box<dyn Fn() -> Result<(), String> + Send + Sync>);

impl PersistHook for FnPersistHook {
    fn persist(&self) {
        let _ = (self.0)();
    }
}

/// 反向 push hook（`None` = no-op；对齐 Go `Pusher==nil`）。
pub trait Pusher: Send + Sync {
    fn push(&self, event: &str, fields: &[(&str, &str)]);
}

/// 手动 `/unlock` 经 wire-worker 的派发缝（G3b 决策 5）。
///
/// `handle_unlock` 不再 handler 局部直调 `Sender`，而是经此 trait 把 unlock 参数投到
/// **单 wire-worker 队列**（[`crate::daemon::Job::Unlock`]）并阻塞等 worker 经一次性 reply
/// channel 回灌 [`UnlockOutcome`]——结果含 `terminated_by_bye` / `wire_kind`，足以做 Go
/// `handleUnlock` 的 4 分支 HTTP 映射（OK / 业务错 / bye→silent-FIN→-103 / wire-failure 503）。
///
/// 与 [`Sender`] 的区别（决策 5）：`Sender::execute_unlock` 只返 `Result<i32>`（无 bye/wire_kind
/// 出参，做不出 4 分支）；本 trait 返完整 `UnlockOutcome`。daemon 生产用 worker-backed 实现
/// （main.rs），Phase 2 测试用 [`dispatch_from_sender`] 把既有 `Sender` 退化包装（无 bye/wire_kind）。
pub trait UnlockDispatch: Send + Sync {
    /// 派发一次 unlock（投 worker + 等 reply）；worker 已退 / panic → 退化 wire-failure
    /// `UnlockOutcome`（result=-1，禁永等、禁 panic，决策 8）。
    fn dispatch(
        &self,
        caller_bcd: [u8; 4],
        callee_bcd: [u8; 4],
        target_ip: &str,
        target_port: u16,
    ) -> UnlockOutcome;
}

/// 把既有 [`Sender`] 退化包装成 [`UnlockDispatch`]（Phase 2 测试 / 兼容路径）。
///
/// `Sender::execute_unlock` 只返 `Result<i32>` → 退化 `UnlockOutcome`：`Ok(code)` →
/// `{result:code, terminated_by_bye:false, wire_kind:None}`；`Err` → `{result:-1, ...None}`。
/// 故经此适配的 `/unlock` 永不命中 4 分支里的 bye/wire-failure（503）分支——与 Phase 2
/// 既有行为（wire_err → 200 + result=-1）逐字一致，不引入 503 回归。
pub fn dispatch_from_sender(sender: Arc<dyn Sender>) -> Arc<dyn UnlockDispatch> {
    Arc::new(SenderDispatch(sender))
}

struct SenderDispatch(Arc<dyn Sender>);

impl UnlockDispatch for SenderDispatch {
    fn dispatch(
        &self,
        caller_bcd: [u8; 4],
        callee_bcd: [u8; 4],
        target_ip: &str,
        target_port: u16,
    ) -> UnlockOutcome {
        let cancel = AtomicBool::new(false);
        let result =
            match self
                .0
                .execute_unlock(caller_bcd, callee_bcd, target_ip, target_port, &cancel)
            {
                Ok(code) => code,
                Err(_) => result::ERR,
            };
        UnlockOutcome {
            result,
            retries: 0,
            terminated_by_bye: false,
            wire_kind: None,
        }
    }
}

struct ServerInner {
    cfg: Config,
    version: String,
    automation: Option<AutomationState>,
    persist: Option<Arc<dyn PersistHook>>,
    pusher: Option<Arc<dyn Pusher>>,
    dispatch: Arc<dyn UnlockDispatch>,
}

/// 控制面 HTTP server 依赖（cfg / automation / persist / pusher）。
pub struct Server {
    inner: Arc<ServerInner>,
}

impl Server {
    /// 用既有 [`Sender`] 构造（Phase 2 兼容路径；内部经 [`dispatch_from_sender`] 退化包装）。
    ///
    /// daemon 生产路径用 [`Server::with_dispatch`] 注入 worker-backed [`UnlockDispatch`]
    /// （决策 5：手动 unlock 经 wire-worker）。
    pub fn new(
        cfg: Config,
        version: impl Into<String>,
        automation: Option<AutomationState>,
        persist: Option<Arc<dyn PersistHook>>,
        pusher: Option<Arc<dyn Pusher>>,
        sender: Arc<dyn Sender>,
    ) -> Self {
        Self::with_dispatch(
            cfg,
            version,
            automation,
            persist,
            pusher,
            dispatch_from_sender(sender),
        )
    }

    /// 用 worker-backed [`UnlockDispatch`] 构造（决策 5：`/unlock` 投 wire-worker 队列、
    /// 经 reply channel 等 [`UnlockOutcome`]、做 4 分支 HTTP 映射）。
    pub fn with_dispatch(
        cfg: Config,
        version: impl Into<String>,
        automation: Option<AutomationState>,
        persist: Option<Arc<dyn PersistHook>>,
        pusher: Option<Arc<dyn Pusher>>,
        dispatch: Arc<dyn UnlockDispatch>,
    ) -> Self {
        Self {
            inner: Arc::new(ServerInner {
                cfg,
                version: version.into(),
                automation,
                persist,
                pusher,
                dispatch,
            }),
        }
    }

    /// 同步触发 push；`pusher` 为 `None` 时 no-op（无 WG 跟踪，Phase 4 再补）。
    pub fn maybe_push(&self, event: &str, fields: &[(&str, &str)]) {
        if let Some(p) = &self.inner.pusher {
            p.push(event, fields);
        }
    }

    /// 注册控制面 + 元信息 endpoint 的 `ServeMux` handler（path 对齐 Go `server.go`）。
    pub fn handler(&self) -> MuxHandler {
        let inner = Arc::clone(&self.inner);
        let business_cfg = Arc::new(inner.cfg.clone());
        let mut mux = ServeMux::new();

        {
            let st = Arc::clone(&inner);
            let h = chain_control_post(HandlerFunc(
                move |w: &mut dyn ResponseWriter, r: &Request| {
                    handle_auto_unlock(&st, w, r);
                },
            ));
            mux.handle("/auto_unlock", Some(Box::new(h)));
        }

        {
            let st = Arc::clone(&inner);
            let h = chain_control_post(HandlerFunc(
                move |w: &mut dyn ResponseWriter, r: &Request| {
                    handle_auto_hangup(&st, w, r);
                },
            ));
            mux.handle("/auto_hangup", Some(Box::new(h)));
        }

        {
            let st = Arc::clone(&inner);
            let h = method_guard(
                METHOD_GET,
                HandlerFunc(move |w: &mut dyn ResponseWriter, r: &Request| {
                    handle_automation_get(&st, w, r);
                }),
            );
            mux.handle("/automation", Some(Box::new(h)));
        }

        {
            let st = Arc::clone(&inner);
            let h = method_guard(
                METHOD_GET,
                HandlerFunc(move |w: &mut dyn ResponseWriter, r: &Request| {
                    handle_info(&st, w, r);
                }),
            );
            mux.handle("/info", Some(Box::new(h)));
        }

        {
            let st = Arc::clone(&inner);
            let h = method_guard(
                METHOD_GET,
                HandlerFunc(move |w: &mut dyn ResponseWriter, _r: &Request| {
                    handle_playback(&st, w);
                }),
            );
            mux.handle("/playback", Some(Box::new(h)));
        }

        {
            let st = Arc::clone(&inner);
            let h = chain_business_post(
                Arc::clone(&business_cfg),
                HandlerFunc(move |w: &mut dyn ResponseWriter, r: &Request| {
                    handle_unlock(&st, w, r);
                }),
            );
            mux.handle("/unlock", Some(Box::new(h)));
        }

        MuxHandler { mux }
    }
}

/// `ServeMux` 的 `Handler` 适配器。
///
/// 中间件链通过 `Arc<Config>` 自持配置（非借用），故无需在此另存。
pub struct MuxHandler {
    mux: ServeMux,
}

impl Handler for MuxHandler {
    fn serve_http(&self, w: &mut dyn ResponseWriter, r: &Request) {
        self.mux.serve_http(w, r);
    }
}

fn handle_auto_unlock(st: &ServerInner, w: &mut dyn ResponseWriter, r: &Request) {
    let on = match parse_auto_toggle_request(r) {
        Ok(v) => v,
        Err(msg) => {
            error_json(w, STATUS_BAD_REQUEST, &msg);
            return;
        }
    };
    if let Some(auto) = &st.automation {
        auto.store_auto_unlock(on);
    }
    if let Some(p) = &st.persist {
        p.persist();
    }
    let cur = st
        .automation
        .as_ref()
        .map(|a| a.load_auto_unlock())
        .unwrap_or(on);
    write_json(
        w,
        STATUS_OK,
        Some(&[
            ("result", JsonValue::Number(0)),
            ("auto_unlock", JsonValue::Bool(cur)),
        ]),
    );
}

fn handle_auto_hangup(st: &ServerInner, w: &mut dyn ResponseWriter, r: &Request) {
    let on = match parse_auto_toggle_request(r) {
        Ok(v) => v,
        Err(msg) => {
            error_json(w, STATUS_BAD_REQUEST, &msg);
            return;
        }
    };
    if let Some(auto) = &st.automation {
        auto.store_auto_hangup(on);
    }
    if let Some(p) = &st.persist {
        p.persist();
    }
    let cur = st
        .automation
        .as_ref()
        .map(|a| a.load_auto_hangup())
        .unwrap_or(on);
    write_json(
        w,
        STATUS_OK,
        Some(&[
            ("result", JsonValue::Number(0)),
            ("auto_hangup", JsonValue::Bool(cur)),
        ]),
    );
}

fn handle_automation_get(st: &ServerInner, w: &mut dyn ResponseWriter, _r: &Request) {
    let (au, ah) = match &st.automation {
        Some(auto) => (auto.load_auto_unlock(), auto.load_auto_hangup()),
        None => (false, false),
    };
    write_json(
        w,
        STATUS_OK,
        Some(&[
            ("result", JsonValue::Number(0)),
            ("auto_unlock", JsonValue::Bool(au)),
            ("auto_hangup", JsonValue::Bool(ah)),
        ]),
    );
}

fn handle_info(st: &ServerInner, w: &mut dyn ResponseWriter, _r: &Request) {
    let sd = info::build(Some(&st.cfg), &st.version, false);
    let body = info::render_json(&sd);
    w.header().set("Content-Type", "application/json");
    w.write_header(STATUS_OK);
    let _ = w.write(&body);
}

fn handle_playback(_st: &ServerInner, w: &mut dyn ResponseWriter) {
    w.header().set("Content-Length", "0");
    w.write_header(STATUS_OK);
}

fn handle_unlock(st: &ServerInner, w: &mut dyn ResponseWriter, r: &Request) {
    let raw = match read_request_body(r) {
        Ok(v) => v,
        Err(msg) => {
            error_json(w, STATUS_BAD_REQUEST, &msg);
            return;
        }
    };
    let (from, to) = match parse_unlock_body(&raw) {
        Ok(v) => v,
        Err(msg) => {
            error_json(w, STATUS_BAD_REQUEST, &msg);
            return;
        }
    };
    if to.is_empty() {
        error_json(w, STATUS_BAD_REQUEST, "missing 'to' field");
        return;
    }
    // wire caller=to(外机), callee=from(室内机)
    let (caller_bcd, _, _, err) = parse_uri_bcd(&to);
    if let Some(msg) = err {
        error_json(w, STATUS_BAD_REQUEST, &format!("invalid 'to' URI: {msg}"));
        return;
    }
    let (callee_bcd, _, _, err) = parse_uri_bcd(&from);
    if let Some(msg) = err {
        error_json(w, STATUS_BAD_REQUEST, &format!("invalid 'from' URI: {msg}"));
        return;
    }
    let (_, target_ip, target_port) = match parse_uri_parts(&to) {
        Ok(v) => v,
        Err(msg) => {
            error_json(w, STATUS_BAD_REQUEST, &format!("invalid 'to' URI: {msg}"));
            return;
        }
    };

    // 决策 5：投 wire-worker 队列、阻塞等 worker 经 reply channel 回灌 UnlockOutcome
    // （worker 已退/panic 时 dispatch 返退化 wire-failure outcome，不永等不 panic，决策 8）。
    let outcome = st
        .dispatch
        .dispatch(caller_bcd, callee_bcd, &target_ip, target_port);
    respond_unlock_outcome(st, w, "unlock", &outcome, &from, &to);
}

/// 把 [`UnlockOutcome`] 映射成 4 分支 HTTP 响应（锚 Go `handleUnlock` handlers.go:195-209）：
///   ① `result==OK` → 200（OK 不 push：`respond_business_result` 仅 result≠OK 时 push）。
///   ② `result==ERR && wire_kind==None`（genuine 业务错：外机响应但 body 校验失败，不重试）
///      → 200 + business push。wire 失败折叠成 ERR 但 wire_kind=Some 的落分支④ → 503。
///   ③ `terminated_by_bye`（ring session 已结束）→ silent-FIN 语义 → 200 + result=-103 push。
///   ④ 默认（重试耗尽）→ 据 `wire_kind` 分类（锚 Go `respondWireFailure`）：
///        SilentFin → 200 + result=-103（业务，daemon 健康）；
///        Timeout/其它 → 503 + result（下游故障，daemon 健康）。
fn respond_unlock_outcome(
    st: &ServerInner,
    w: &mut dyn ResponseWriter,
    event: &str,
    outcome: &UnlockOutcome,
    from: &str,
    to: &str,
) {
    if outcome.result == result::OK {
        // ① 成功（OK 不 push：respond_business_result 仅 result≠OK 时 push）。
        respond_business_result(st, w, event, result::OK, from, to);
        return;
    }
    if outcome.result == result::ERR && outcome.wire_kind.is_none() {
        // ② genuine 业务错（外机响应但 body 校验失败，wire_kind==None），不重试 → 200。
        //    wire 失败折叠成 ERR 但带 wire_kind=Some 的（含 worker-dead 退化 outcome）落分支④ → 503。
        respond_business_result(st, w, event, result::ERR, from, to);
        return;
    }
    if outcome.terminated_by_bye {
        // ③ bye → ring session 已结束 → 业务级 no-ring（-103）最合适（传 lastWireErr 可能是
        //    timeout，会误导成 503/-104，锚 Go handlers.go:201-204）。
        respond_wire_failure(st, w, event, Some(WireKind::SilentFin), from, to);
        return;
    }
    // ④ 重试耗尽：保留真实 wire_kind 让 respond_wire_failure 正确分类。
    respond_wire_failure(st, w, event, outcome.wire_kind, from, to);
}

/// wire-failure 翻译成 HTTP status + result（锚 Go `respondWireFailure` handlers.go:691）：
///   - SilentFin → 200 + result=-103（业务：外机 ring 状态机拒绝，daemon 健康，走 push）。
///   - Timeout → 503 + result=-5（下游失败，daemon 健康）。
///   - None（无 wire 错，cancel/cap 在首次尝试前命中）→ 503 + result=0（锚 Go classifyWireErr(nil)=OK）。
///   - 其它 → 503 + result=-1。
fn respond_wire_failure(
    st: &ServerInner,
    w: &mut dyn ResponseWriter,
    event: &str,
    wire_kind: Option<WireKind>,
    from: &str,
    to: &str,
) {
    let result_code = classify_wire_kind(wire_kind);
    if result_code == result::NO_RING {
        // SilentFin 是业务结果（外机协议层拒绝）→ 200 + push（锚 Go handlers.go:693-696）。
        respond_business_result(st, w, event, result_code, from, to);
        return;
    }
    // Timeout / 其它 → 503 + push（HTTP 状态反映下游故障，锚 Go handlers.go:698-708）。
    push_business_fields(st, event, result_code, from, to);
    write_json(
        w,
        STATUS_SERVICE_UNAVAILABLE,
        Some(&[("result", JsonValue::Number(result_code as i64))]),
    );
}

/// wire 分类 → result code（锚 Go `classifyWireErr` handlers.go:714）。
fn classify_wire_kind(wire_kind: Option<WireKind>) -> i32 {
    match wire_kind {
        Some(WireKind::SilentFin) => result::NO_RING,
        Some(WireKind::Timeout) => result::TIMEOUT,
        // 无 wire 错（cancel/cap 在首次尝试前命中）→ OK(0)，锚 Go classifyWireErr(nil)=ResultOK。
        None => result::OK,
        // Retryable（耗尽后）/ Other → -1（下游故障，锚 Go classifyWireErr 末 ResultErr）。
        Some(WireKind::Retryable) | Some(WireKind::Other) => result::ERR,
    }
}

fn respond_business_result(
    st: &ServerInner,
    w: &mut dyn ResponseWriter,
    event: &str,
    result_code: i32,
    from: &str,
    to: &str,
) {
    if result_code != result::OK {
        push_business_fields(st, event, result_code, from, to);
    }
    write_json(
        w,
        STATUS_OK,
        Some(&[("result", JsonValue::Number(result_code as i64))]),
    );
}

/// business-result push（非 OK 时；锚 Go `respondBusinessResult`/`respondWireFailure` 的
/// `asyncPush` from/to 取舍）。经注入的 [`Pusher`]（daemon 生产是 detached `spawn_push`，CR-M1）
/// 发——本函数只组字段，detached 与否由 Pusher 实现决定（决策 3：business push 属骨架 IN、off worker）。
fn push_business_fields(st: &ServerInner, event: &str, result_code: i32, from: &str, to: &str) {
    let code = result_code.to_string();
    match (from.is_empty(), to.is_empty()) {
        (false, false) => {
            maybe_push_inner(st, event, &[("result", &code), ("from", from), ("to", to)]);
        }
        (false, true) => {
            maybe_push_inner(st, event, &[("result", &code), ("from", from)]);
        }
        (true, false) => {
            maybe_push_inner(st, event, &[("result", &code), ("to", to)]);
        }
        (true, true) => {
            maybe_push_inner(st, event, &[("result", &code)]);
        }
    }
}

fn maybe_push_inner(st: &ServerInner, event: &str, fields: &[(&str, &str)]) {
    if let Some(p) = &st.pusher {
        p.push(event, fields);
    }
}

fn parse_unlock_body(raw: &[u8]) -> Result<(String, String), String> {
    if raw.is_empty() {
        // Go parseJSONBody 查 RAW len==0 → 返 nil → body 零值（To/From=""）→ 落到 caller
        // 的 `to.is_empty()` → "missing 'to' field"。复刻之（非 "empty body" 错）。
        return Ok((String::new(), String::new()));
    }
    let trimmed = trim_ascii_whitespace(raw);
    let from = extract_json_string_field(trimmed, "from").unwrap_or_default();
    let to = extract_json_string_field(trimmed, "to").unwrap_or_default();
    Ok((from, to))
}

fn parse_uri_bcd(uri: &str) -> ([u8; 4], String, u16, Option<String>) {
    match parse_uri_parts(uri) {
        Err(msg) => ([0u8; 4], String::new(), 0, Some(msg)),
        Ok((name, ip, port)) => match codec::encode_bcd(&name) {
            Ok(bcd) => (bcd, ip, port, None),
            Err(e) => ([0u8; 4], String::new(), 0, Some(format_bcd_err(&e, uri))),
        },
    }
}

fn parse_uri_parts(uri: &str) -> Result<(String, String, u16), String> {
    codec::parse_uri(uri).map_err(|e| format_uri_err(&e, uri))
}

fn format_uri_err(e: &codec::UriError, uri: &str) -> String {
    match e {
        codec::UriError::Format => format!("uri: malformed, want name@ip:port: {uri:?}"),
        codec::UriError::NameLen { got } => {
            let name = uri.split('@').next().unwrap_or(uri);
            format!("uri: name must be exactly 8 characters: name={name:?} (len={got})")
        }
        codec::UriError::IPv4Only => {
            let ip = uri
                .split('@')
                .nth(1)
                .and_then(|s| s.rsplit_once(':').map(|(ip, _)| ip))
                .unwrap_or(uri);
            format!("uri: only IPv4 addresses are supported: ip={ip:?}")
        }
        codec::UriError::Port => {
            let port = uri.rsplit_once(':').map(|(_, p)| p).unwrap_or("");
            format!("uri: port must be 1-65535: port={port:?}")
        }
    }
}

fn format_bcd_err(e: &codec::BcdError, uri: &str) -> String {
    match e {
        codec::BcdError::Length { got } => {
            format!("bcd: name must be 8 hex digits: {uri:?} (len={got})")
        }
        codec::BcdError::Format { ch, index } => {
            format!("bcd: invalid hex digit {ch:?} at index {index}: {uri:?}")
        }
    }
}

fn extract_json_string_field(input: &[u8], want_key: &str) -> Result<String, &'static str> {
    let bytes = trim_ascii_whitespace(input);
    if bytes.first() != Some(&b'{') {
        return Err("expected object");
    }
    let mut pos = 1usize;
    skip_json_ws(bytes, &mut pos);
    if pos < bytes.len() && bytes[pos] == b'}' {
        return Err("missing field"); // {}
    }
    // 扫到末尾、保留**最后**一次匹配（Go json.Unmarshal 重复 key 取 last-wins）。
    let mut last: Option<String> = None;
    loop {
        skip_json_ws(bytes, &mut pos);
        let key = read_json_string(bytes, &mut pos)?;
        skip_json_ws(bytes, &mut pos);
        if pos >= bytes.len() || bytes[pos] != b':' {
            return Err("expected colon");
        }
        pos += 1;
        skip_json_ws(bytes, &mut pos);
        if key.eq_ignore_ascii_case(want_key) {
            last = Some(read_json_string(bytes, &mut pos)?);
        } else {
            skip_json_value(bytes, &mut pos)?;
        }
        skip_json_ws(bytes, &mut pos);
        match bytes.get(pos) {
            Some(b',') => pos += 1,
            Some(b'}') => return last.ok_or("missing field"),
            _ => return Err("expected comma or closing brace"),
        }
    }
}

/// 从 `&Request` 读取并解析 `{"on":bool}`。
fn parse_auto_toggle_request(r: &Request) -> Result<bool, String> {
    let raw = read_request_body(r)?;
    parse_auto_toggle_body(&raw)
}

/// Handler 只持 `&Request` 时读取 body。body 是已缓冲的 `Vec<u8>`（parser 按 Content-Length
/// 读入内存），经 `&[u8]`（impl Read）读取——**无需 unsafe 别名 cast**：`&mut reader` 只动
/// 局部 slice 游标，不触 `r.body`。
fn read_request_body(r: &Request) -> Result<Vec<u8>, String> {
    let mut reader: &[u8] = &r.body;
    parse_json_body(&mut reader)
}

fn parse_auto_toggle_body(raw: &[u8]) -> Result<bool, String> {
    // Go json.Unmarshal 到 autoToggleBody{On bool}：空 body / `{}` / 缺 on 字段 / `null`
    // / `{"on":null}` 均留零值 On=false（非错误）→ 200。仅非法 JSON → 400。
    // **关键**：Go parseJSONBody 查 **RAW 字节** `len(raw)==0`（非 trim 后），故纯空白
    // body（"   "）非空 → 交 Unmarshal → 报错 → 400（不是零值 200）。复刻该边界。
    if raw.is_empty() {
        return Ok(false);
    }
    let trimmed = trim_ascii_whitespace(raw);
    if trimmed == b"null" {
        return Ok(false);
    }
    // 纯空白（raw 非空、trimmed 空）落到 extract → "expected object" → Err → 400（同 Go）。
    match extract_json_bool_field(trimmed, "on") {
        Ok(v) => Ok(v),
        Err("missing field") => Ok(false),
        Err(e) => Err(format!("parse JSON body: {e}")),
    }
}

fn extract_json_bool_field(input: &[u8], want_key: &str) -> Result<bool, &'static str> {
    let bytes = trim_ascii_whitespace(input);
    if bytes.first() != Some(&b'{') {
        return Err("expected object");
    }
    let mut pos = 1usize;
    skip_json_ws(bytes, &mut pos);
    if pos < bytes.len() && bytes[pos] == b'}' {
        return Err("missing field"); // {}
    }
    // 扫到末尾、保留**最后**一次匹配（Go json.Unmarshal 重复 key 取 last-wins）。
    let mut last: Option<bool> = None;
    loop {
        skip_json_ws(bytes, &mut pos);
        let key = read_json_string(bytes, &mut pos)?;
        skip_json_ws(bytes, &mut pos);
        if pos >= bytes.len() || bytes[pos] != b':' {
            return Err("expected colon");
        }
        pos += 1;
        skip_json_ws(bytes, &mut pos);
        if key.eq_ignore_ascii_case(want_key) {
            last = Some(read_json_bool(bytes, &mut pos)?);
        } else {
            skip_json_value(bytes, &mut pos)?;
        }
        skip_json_ws(bytes, &mut pos);
        match bytes.get(pos) {
            Some(b',') => pos += 1,
            Some(b'}') => return last.ok_or("missing field"),
            _ => return Err("expected comma or closing brace"),
        }
    }
}

fn skip_json_ws(bytes: &[u8], pos: &mut usize) {
    while *pos < bytes.len() && bytes[*pos].is_ascii_whitespace() {
        *pos += 1;
    }
}

fn read_json_string(bytes: &[u8], pos: &mut usize) -> Result<String, &'static str> {
    if bytes.get(*pos) != Some(&b'"') {
        return Err("expected string key");
    }
    *pos += 1;
    let start = *pos;
    while *pos < bytes.len() {
        match bytes[*pos] {
            b'"' => {
                let s = std::str::from_utf8(&bytes[start..*pos]).map_err(|_| "invalid UTF-8")?;
                *pos += 1;
                return Ok(s.to_string());
            }
            b'\\' => return Err("escaped key not supported"),
            b if b < 0x20 => return Err("control char in string"),
            _ => *pos += 1,
        }
    }
    Err("unterminated string")
}

fn read_json_bool(bytes: &[u8], pos: &mut usize) -> Result<bool, &'static str> {
    if bytes[*pos..].starts_with(b"true") {
        *pos += 4;
        return Ok(true);
    }
    if bytes[*pos..].starts_with(b"false") {
        *pos += 5;
        return Ok(false);
    }
    // Go json.Unmarshal：`{"on":null}` 把 bool 字段留零值（false）、不报错。复刻之。
    if bytes[*pos..].starts_with(b"null") {
        *pos += 4;
        return Ok(false);
    }
    Err("expected bool")
}

fn skip_json_value(bytes: &[u8], pos: &mut usize) -> Result<(), &'static str> {
    skip_json_ws(bytes, pos);
    match bytes.get(*pos) {
        Some(b'"') => {
            read_json_string(bytes, pos)?;
            Ok(())
        }
        Some(b'{') => skip_json_object(bytes, pos),
        Some(b'[') => skip_json_array(bytes, pos),
        Some(b't') => {
            expect_json_literal(bytes, pos, "true")?;
            Ok(())
        }
        Some(b'f') => {
            expect_json_literal(bytes, pos, "false")?;
            Ok(())
        }
        Some(b'n') => {
            expect_json_literal(bytes, pos, "null")?;
            Ok(())
        }
        Some(b'0'..=b'9') | Some(b'-') => {
            skip_json_number(bytes, pos);
            Ok(())
        }
        _ => Err("invalid JSON value"),
    }
}

fn skip_json_object(bytes: &[u8], pos: &mut usize) -> Result<(), &'static str> {
    *pos += 1; // {
    skip_json_ws(bytes, pos);
    if bytes.get(*pos) == Some(&b'}') {
        *pos += 1;
        return Ok(());
    }
    loop {
        skip_json_ws(bytes, pos);
        read_json_string(bytes, pos)?;
        skip_json_ws(bytes, pos);
        if bytes.get(*pos) != Some(&b':') {
            return Err("expected colon");
        }
        *pos += 1;
        skip_json_value(bytes, pos)?;
        skip_json_ws(bytes, pos);
        match bytes.get(*pos) {
            Some(b'}') => {
                *pos += 1;
                return Ok(());
            }
            Some(b',') => {
                *pos += 1;
            }
            _ => return Err("expected comma or closing brace"),
        }
    }
}

fn skip_json_array(bytes: &[u8], pos: &mut usize) -> Result<(), &'static str> {
    *pos += 1; // [
    skip_json_ws(bytes, pos);
    if bytes.get(*pos) == Some(&b']') {
        *pos += 1;
        return Ok(());
    }
    loop {
        skip_json_value(bytes, pos)?;
        skip_json_ws(bytes, pos);
        match bytes.get(*pos) {
            Some(b']') => {
                *pos += 1;
                return Ok(());
            }
            Some(b',') => {
                *pos += 1;
            }
            _ => return Err("expected comma or closing bracket"),
        }
    }
}

fn skip_json_number(bytes: &[u8], pos: &mut usize) {
    if bytes.get(*pos) == Some(&b'-') {
        *pos += 1;
    }
    if bytes.get(*pos) == Some(&b'0') {
        *pos += 1;
    } else {
        while matches!(bytes.get(*pos), Some(b'0'..=b'9')) {
            *pos += 1;
        }
    }
    if bytes.get(*pos) == Some(&b'.') {
        *pos += 1;
        while matches!(bytes.get(*pos), Some(b'0'..=b'9')) {
            *pos += 1;
        }
    }
    if matches!(bytes.get(*pos), Some(b'e') | Some(b'E')) {
        *pos += 1;
        if matches!(bytes.get(*pos), Some(b'+') | Some(b'-')) {
            *pos += 1;
        }
        while matches!(bytes.get(*pos), Some(b'0'..=b'9')) {
            *pos += 1;
        }
    }
}

fn expect_json_literal(bytes: &[u8], pos: &mut usize, lit: &str) -> Result<(), &'static str> {
    for b in lit.bytes() {
        if bytes.get(*pos) != Some(&b) {
            return Err("invalid literal");
        }
        *pos += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::httpx::STATUS_BAD_REQUEST;
    use crate::sender::MockSender;
    use std::io::Cursor;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct Recorder {
        headers: Header,
        status: u16,
        body: Vec<u8>,
        wrote_header: bool,
    }

    impl Recorder {
        fn new() -> Self {
            Self {
                headers: Header::new(),
                status: 0,
                body: Vec::new(),
                wrote_header: false,
            }
        }

        fn finish(mut self) -> (u16, Header, Vec<u8>) {
            if !self.wrote_header {
                self.write_header(STATUS_OK);
            }
            (self.status, self.headers, self.body)
        }
    }

    impl ResponseWriter for Recorder {
        fn header(&mut self) -> &mut Header {
            &mut self.headers
        }

        fn write_header(&mut self, status: u16) {
            if self.wrote_header {
                return;
            }
            self.wrote_header = true;
            self.status = status;
        }

        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            if !self.wrote_header {
                self.write_header(STATUS_OK);
            }
            self.body.extend_from_slice(data);
            Ok(data.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn test_request(method: &str, content_type: Option<&str>, body: &[u8]) -> Request {
        let mut header = Header::new();
        if let Some(ct) = content_type {
            header.set("Content-Type", ct);
        }
        Request {
            method: method.to_string(),
            url: crate::httpx::Url::default(),
            header,
            body: body.to_vec(),
            host: String::new(),
            remote_addr: String::new(),
            content_length: body.len() as i64,
        }
    }

    fn conf_with_station() -> Config {
        let mut cfg = Config::default();
        cfg.stations.push(crate::config::Station {
            sip: "06020000@172.16.106.152:18022".to_string(),
            rtsp_url: String::new(),
        });
        cfg
    }

    // --- classify_wire_kind Go-parity ---

    #[test]
    fn classify_wire_kind_none_is_ok_not_err() {
        // 锚 Go classifyWireErr(nil)=ResultOK：cancel/cap 在首次 wire 尝试前命中（wire_kind=None）
        // 须映射 OK(0) 非 ERR(-1)，否则 503 体 result 与 Go 不一致。
        assert_eq!(classify_wire_kind(None), result::OK);
        assert_eq!(
            classify_wire_kind(Some(WireKind::SilentFin)),
            result::NO_RING
        );
        assert_eq!(classify_wire_kind(Some(WireKind::Timeout)), result::TIMEOUT);
        assert_eq!(classify_wire_kind(Some(WireKind::Other)), result::ERR);
        assert_eq!(classify_wire_kind(Some(WireKind::Retryable)), result::ERR);
    }

    // --- 7.1 method_guard 405 字节契约 ---

    #[test]
    fn method_guard_post_rejects_get_with_405_allow_json() {
        let called = AtomicBool::new(false);
        let inner = HandlerFunc(|_w: &mut dyn ResponseWriter, _r: &Request| {
            called.store(true, Ordering::SeqCst);
        });
        let h = method_guard_post(inner);
        let req = test_request("GET", Some("application/json"), b"");
        let (status, headers, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_METHOD_NOT_ALLOWED);
        assert_eq!(headers.get("Allow"), Some("POST"));
        assert_eq!(headers.get("Content-Type"), Some("application/json"));
        assert!(!body.is_empty());
        assert_eq!(
            String::from_utf8(body).unwrap(),
            r#"{"error":"method GET not allowed; use POST"}"#
        );
        assert!(!called.load(Ordering::SeqCst));
    }

    trait Pipe {
        fn pipe(self, f: impl FnOnce(&mut dyn ResponseWriter)) -> (u16, Header, Vec<u8>);
    }

    impl Pipe for Recorder {
        fn pipe(self, f: impl FnOnce(&mut dyn ResponseWriter)) -> (u16, Header, Vec<u8>) {
            let mut rec = self;
            f(&mut rec);
            rec.finish()
        }
    }

    // --- 7.2 require_json_content_type 415 字节契约 ---

    #[test]
    fn require_json_content_type_byte_contract() {
        let cases: &[(&str, u16, bool)] = &[
            ("application/json", STATUS_OK, true),
            ("application/json; charset=utf-8", STATUS_OK, true),
            ("Application/JSON", STATUS_OK, true),
            ("APPLICATION/JSON; CHARSET=UTF-8", STATUS_OK, true),
            ("application/JSON; charset=utf-8", STATUS_OK, true),
            (
                "application/x-www-form-urlencoded",
                STATUS_UNSUPPORTED_MEDIA,
                false,
            ),
            ("", STATUS_UNSUPPORTED_MEDIA, false),
            ("text/plain", STATUS_UNSUPPORTED_MEDIA, false),
            ("application/json garbage", STATUS_UNSUPPORTED_MEDIA, false),
        ];
        for (ct, want_status, want_called) in cases {
            let called = AtomicBool::new(false);
            let inner = HandlerFunc(|w: &mut dyn ResponseWriter, _r: &Request| {
                called.store(true, Ordering::SeqCst);
                w.write_header(STATUS_OK);
            });
            let h = require_json_content_type(inner);
            let req = test_request("POST", if ct.is_empty() { None } else { Some(ct) }, b"");
            let (status, _, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
            assert_eq!(status, *want_status, "ct={ct:?}");
            assert_eq!(called.load(Ordering::SeqCst), *want_called, "ct={ct:?}");
            if !want_called {
                assert_eq!(
                    String::from_utf8(body).unwrap(),
                    r#"{"error":"Content-Type must be application/json"}"#
                );
            }
        }
    }

    // --- 7.3 require_stations_configured ResultNoConfig ---

    #[test]
    fn require_stations_configured_empty_returns_200_result_minus_100() {
        let called = AtomicBool::new(false);
        let inner = HandlerFunc(|_w: &mut dyn ResponseWriter, _r: &Request| {
            called.store(true, Ordering::SeqCst);
        });
        let cfg = Config::default();
        let h = require_stations_configured(std::sync::Arc::new(cfg.clone()), inner);
        let req = test_request("POST", Some("application/json"), b"{}");
        let (status, _, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_OK);
        assert_eq!(String::from_utf8(body).unwrap(), r#"{"result":-100}"#);
        assert!(!called.load(Ordering::SeqCst));
    }

    #[test]
    fn require_stations_configured_with_stations_calls_inner() {
        let called = AtomicBool::new(false);
        let inner = HandlerFunc(|_w: &mut dyn ResponseWriter, _r: &Request| {
            called.store(true, Ordering::SeqCst);
        });
        let cfg = conf_with_station();
        let h = require_stations_configured(std::sync::Arc::new(cfg.clone()), inner);
        let req = test_request("POST", Some("application/json"), b"{}");
        Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert!(called.load(Ordering::SeqCst));
    }

    // --- 7.5 chain_business_post 组合链 ---

    #[test]
    fn chain_business_post_method_mismatch_405() {
        let called = AtomicBool::new(false);
        let inner = HandlerFunc(|_w: &mut dyn ResponseWriter, _r: &Request| {
            called.store(true, Ordering::SeqCst);
        });
        let cfg = conf_with_station();
        let h = chain_business_post(std::sync::Arc::new(cfg.clone()), inner);
        let req = test_request("GET", Some("application/json"), b"");
        let (status, headers, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_METHOD_NOT_ALLOWED);
        assert_eq!(headers.get("Allow"), Some("POST"));
        assert!(String::from_utf8(body).unwrap().contains(r#""error":"#));
        assert!(!called.load(Ordering::SeqCst));
    }

    #[test]
    fn chain_business_post_wrong_content_type_415() {
        let called = AtomicBool::new(false);
        let inner = HandlerFunc(|_w: &mut dyn ResponseWriter, _r: &Request| {
            called.store(true, Ordering::SeqCst);
        });
        let cfg = conf_with_station();
        let h = chain_business_post(std::sync::Arc::new(cfg.clone()), inner);
        let req = test_request("POST", Some("text/plain"), b"{}");
        let (status, _, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_UNSUPPORTED_MEDIA);
        assert_eq!(
            String::from_utf8(body).unwrap(),
            r#"{"error":"Content-Type must be application/json"}"#
        );
        assert!(!called.load(Ordering::SeqCst));
    }

    #[test]
    fn chain_business_post_no_stations_200_minus_100() {
        let called = AtomicBool::new(false);
        let inner = HandlerFunc(|_w: &mut dyn ResponseWriter, _r: &Request| {
            called.store(true, Ordering::SeqCst);
        });
        let cfg = Config::default();
        let h = chain_business_post(std::sync::Arc::new(cfg.clone()), inner);
        let req = test_request("POST", Some("application/json"), b"{}");
        let (status, _, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_OK);
        assert_eq!(String::from_utf8(body).unwrap(), r#"{"result":-100}"#);
        assert!(!called.load(Ordering::SeqCst));
    }

    // --- 7.4 JSON helpers ---

    #[test]
    fn write_json_no_trailing_newline_fixed_length() {
        let (status, headers, body) = Recorder::new().pipe(|w| {
            write_json(
                w,
                STATUS_OK,
                Some(&[
                    ("result", JsonValue::Number(0)),
                    ("auto_unlock", JsonValue::Bool(true)),
                ]),
            );
        });
        assert_eq!(status, STATUS_OK);
        assert_eq!(headers.get("Content-Type"), Some("application/json"));
        assert_eq!(headers.get("Content-Length"), Some("31"));
        assert!(!body.ends_with(b"\n"));
        assert_eq!(
            String::from_utf8(body).unwrap(),
            r#"{"result":0,"auto_unlock":true}"#
        );
    }

    #[test]
    fn error_json_shape() {
        let (_, _, body) = Recorder::new().pipe(|w| {
            error_json(w, STATUS_BAD_REQUEST, "missing 'to' field");
        });
        assert_eq!(
            String::from_utf8(body).unwrap(),
            r#"{"error":"missing 'to' field"}"#
        );
    }

    #[test]
    fn parse_json_body_empty_and_valid() {
        let mut empty = Cursor::new(Vec::<u8>::new());
        assert!(parse_json_body(&mut empty).unwrap().is_empty());

        let mut valid = Cursor::new(br#"{"on":true}"#.to_vec());
        let raw = parse_json_body(&mut valid).unwrap();
        assert_eq!(raw, br#"{"on":true}"#);
    }

    #[test]
    fn parse_json_body_rejects_invalid() {
        let mut bad = Cursor::new(b"not-json{".to_vec());
        assert!(parse_json_body(&mut bad).is_err());
    }

    #[test]
    fn is_json_media_type_unit() {
        assert!(is_json_media_type("application/json"));
        assert!(is_json_media_type("Application/JSON; charset=utf-8"));
        assert!(!is_json_media_type("application/x-www-form-urlencoded"));
        assert!(!is_json_media_type(""));
    }

    // --- G6 控制面 + 元信息 endpoint ---

    fn test_request_path(
        method: &str,
        path: &str,
        content_type: Option<&str>,
        body: &[u8],
    ) -> Request {
        let mut header = Header::new();
        if let Some(ct) = content_type {
            header.set("Content-Type", ct);
        }
        Request {
            method: method.to_string(),
            url: crate::httpx::Url {
                path: path.to_string(),
                raw_query: String::new(),
            },
            header,
            body: body.to_vec(),
            host: String::new(),
            remote_addr: String::new(),
            content_length: body.len() as i64,
        }
    }

    fn sample_server_cfg() -> Config {
        let mut cfg = Config::default();
        cfg.sip = "12345678@10.0.0.20:18022".into();
        cfg.stations.push(crate::config::Station {
            sip: "12340000@10.0.0.10:18022".into(),
            rtsp_url: String::new(),
        });
        cfg
    }

    #[test]
    fn handler_construction_no_duplicate_pattern_panic() {
        let s = Server::new(
            sample_server_cfg(),
            "v0.2.0",
            Some(AutomationState::new(false, false)),
            None,
            None,
            MockSender::success(),
        );
        let _ = s.handler();
    }

    #[test]
    fn auto_toggle_body_matches_go_unmarshal_semantics() {
        // Go json.Unmarshal 到 autoToggleBody{On bool}：空/`{}`/缺字段/null → 零值 false（非错）；
        // key 大小写不敏感；只有非法 JSON → Err。
        assert_eq!(parse_auto_toggle_body(b""), Ok(false)); // 空 body → false（非 400）
        assert_eq!(parse_auto_toggle_body(b"{}"), Ok(false)); // 空对象 → false
        assert_eq!(parse_auto_toggle_body(b"null"), Ok(false)); // null → 零值
        assert_eq!(parse_auto_toggle_body(br#"{"foo":1}"#), Ok(false)); // 缺 on → false
        assert_eq!(parse_auto_toggle_body(br#"{"on":true}"#), Ok(true));
        assert_eq!(parse_auto_toggle_body(br#"{"ON":true}"#), Ok(true)); // 大小写不敏感
        assert!(parse_auto_toggle_body(b"[bad").is_err()); // 非法 JSON → Err
                                                           // review-loop R2 修正的边界：
        assert_eq!(parse_auto_toggle_body(br#"{"on":null}"#), Ok(false)); // null 值 → 零值（非 400）
        assert!(parse_auto_toggle_body(b"   ").is_err()); // 纯空白（raw 非空）→ Go Unmarshal 报错 → 400
        assert_eq!(
            parse_auto_toggle_body(br#"{"on":true,"on":false}"#),
            Ok(false)
        ); // 重复 key → last-wins（Go json.Unmarshal）
        assert!(parse_auto_toggle_body(br#"{"on":1}"#).is_err()); // 非 bool 值 → Err → 400
    }

    #[test]
    fn unlock_empty_body_falls_to_missing_to() {
        // Go handleUnlock 空 body → body 零值 To="" → "missing 'to' field"（非 "empty body"）。
        let (from, to) = parse_unlock_body(b"").expect("empty unlock body → zero value, not Err");
        assert!(from.is_empty() && to.is_empty());
    }

    #[test]
    fn unlock_body_string_extractor_last_wins_and_skips_nested() {
        // extract_json_string_field 与 bool extractor 对称：重复 key last-wins、嵌套同名 key 不误入。
        let (_, to) = parse_unlock_body(br#"{"to":"a","to":"b"}"#).unwrap();
        assert_eq!(to, "b"); // 重复 key → last-wins（Go json.Unmarshal）
        let (_, to) = parse_unlock_body(br#"{"foo":{"to":"x"},"to":"y"}"#).unwrap();
        assert_eq!(to, "y"); // 嵌套对象内的 "to" 不被误当顶层（skip_json_value 跳整个 {}）
    }

    #[test]
    fn post_auto_unlock_toggles_persists_and_returns_field() {
        let persist_calls = Arc::new(AtomicUsize::new(0));
        let pc = Arc::clone(&persist_calls);
        let st = AutomationState::new(false, false);
        let s = Server::new(
            sample_server_cfg(),
            "v0.2.0",
            Some(st),
            Some(Arc::new(FnPersistHook(Box::new(move || {
                pc.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })))),
            None,
            MockSender::success(),
        );
        let h = s.handler();

        let req = test_request_path(
            "POST",
            "/auto_unlock",
            Some("application/json"),
            br#"{"on":true}"#,
        );
        let (status, _, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_OK);
        assert_eq!(
            String::from_utf8(body).unwrap(),
            r#"{"result":0,"auto_unlock":true}"#
        );
        assert_eq!(persist_calls.load(Ordering::SeqCst), 1);

        let req = test_request_path(
            "POST",
            "/auto_unlock",
            Some("application/json"),
            br#"{"on":false}"#,
        );
        let (_, _, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(
            String::from_utf8(body).unwrap(),
            r#"{"result":0,"auto_unlock":false}"#
        );
        assert_eq!(persist_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn post_auto_hangup_returns_auto_hangup_field_not_auto_unlock() {
        let st = AutomationState::new(false, false);
        let s = Server::new(
            sample_server_cfg(),
            "v0.2.0",
            Some(st),
            None,
            None,
            MockSender::success(),
        );
        let h = s.handler();
        let req = test_request_path(
            "POST",
            "/auto_hangup",
            Some("application/json"),
            br#"{"on":true}"#,
        );
        let (status, _, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_OK);
        let raw = String::from_utf8(body).unwrap();
        assert!(raw.contains(r#""auto_hangup":true"#));
        assert!(!raw.contains("auto_unlock"));
        assert_eq!(raw, r#"{"result":0,"auto_hangup":true}"#);
    }

    #[test]
    fn get_automation_returns_both_flags() {
        let st = AutomationState::new(true, false);
        let s = Server::new(
            sample_server_cfg(),
            "v0.2.0",
            Some(st),
            None,
            None,
            MockSender::success(),
        );
        let h = s.handler();
        let req = test_request_path("GET", "/automation", None, b"");
        let (status, _, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_OK);
        assert_eq!(
            String::from_utf8(body).unwrap(),
            r#"{"result":0,"auto_unlock":true,"auto_hangup":false}"#
        );
    }

    #[test]
    fn post_auto_unlock_persist_failure_still_result_zero() {
        let st = AutomationState::new(false, false);
        let s = Server::new(
            sample_server_cfg(),
            "v0.2.0",
            Some(st),
            Some(Arc::new(FnPersistHook(Box::new(|| {
                Err("disk full".to_string())
            })))),
            None,
            MockSender::success(),
        );
        let h = s.handler();
        let req = test_request_path(
            "POST",
            "/auto_unlock",
            Some("application/json"),
            br#"{"on":true}"#,
        );
        let (status, _, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_OK);
        assert_eq!(
            String::from_utf8(body).unwrap(),
            r#"{"result":0,"auto_unlock":true}"#
        );
    }

    #[test]
    fn get_info_uses_render_json_chunked_no_content_length() {
        let s = Server::new(
            sample_server_cfg(),
            "v0.2.0-test",
            None,
            None,
            None,
            MockSender::success(),
        );
        let h = s.handler();
        let req = test_request_path("GET", "/info", None, b"");
        let (status, headers, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_OK);
        assert_eq!(headers.get("Content-Type"), Some("application/json"));
        assert!(headers.get("Content-Length").is_none());
        let rs = String::from_utf8(body).unwrap();
        assert!(rs.contains(r#""daemon":"dooraccess-go""#));
        assert!(rs.contains(r#""version":"v0.2.0-test""#));
        assert!(rs.ends_with('\n'));
    }

    #[test]
    fn get_playback_empty_body_content_length_zero() {
        let s = Server::new(
            sample_server_cfg(),
            "v0.2.0",
            None,
            None,
            None,
            MockSender::success(),
        );
        let h = s.handler();
        let req = test_request_path("GET", "/playback", None, b"");
        let (status, headers, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_OK);
        assert_eq!(headers.get("Content-Length"), Some("0"));
        assert!(body.is_empty());
    }

    #[test]
    fn maybe_push_noop_when_nil() {
        let s = Server::new(
            sample_server_cfg(),
            "v0.2.0",
            None,
            None,
            None,
            MockSender::success(),
        );
        s.maybe_push("unlock", &[("result", "0")]);
    }

    struct FlagPusher(Arc<AtomicBool>);

    impl Pusher for FlagPusher {
        fn push(&self, _event: &str, _fields: &[(&str, &str)]) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn maybe_push_calls_pusher_when_set() {
        let called = Arc::new(AtomicBool::new(false));
        let pusher: Arc<dyn Pusher> = Arc::new(FlagPusher(Arc::clone(&called)));
        let s = Server::new(
            sample_server_cfg(),
            "v0.2.0",
            None,
            None,
            Some(pusher),
            MockSender::success(),
        );
        s.maybe_push("unlock", &[("result", "0")]);
        assert!(called.load(Ordering::SeqCst));
    }

    // --- G5 /unlock 薄切 + mock Sender ---

    fn unlock_body(from: &str, to: &str) -> Vec<u8> {
        format!(r#"{{"from":"{from}","to":"{to}"}}"#).into_bytes()
    }

    #[test]
    fn post_unlock_success_mock_sender_returns_result_zero() {
        let mock = MockSender::success();
        // 方法调用 `.clone()` 得 Arc<MockSender> 再于带注解 let 处 unsize 到 Arc<dyn Sender>；
        // 用 `Arc::clone(&mock)`（UFCS）会让推断反向流、误判 &mock 须为 &Arc<dyn Sender>。
        let sender: Arc<dyn Sender> = mock.clone();
        let s = Server::new(sample_server_cfg(), "v0.2.0", None, None, None, sender);
        let h = s.handler();
        let req = test_request_path(
            "POST",
            "/unlock",
            Some("application/json"),
            &unlock_body("12345678@10.0.0.1:18022", "12340000@10.0.0.10:18022"),
        );
        let (status, _, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_OK);
        assert_eq!(String::from_utf8(body).unwrap(), r#"{"result":0}"#);

        let call = mock.last_call().expect("sender called");
        assert_eq!(call.target_ip, "10.0.0.10");
        assert_eq!(call.target_port, 18022);
        assert_eq!(call.caller_bcd, codec::encode_bcd("12340000").unwrap());
        assert_eq!(call.callee_bcd, codec::encode_bcd("12345678").unwrap());
    }

    #[test]
    fn post_unlock_mock_sender_injectable_failure() {
        let mock: Arc<dyn Sender> = MockSender::wire_err("mock wire failure");
        let s = Server::new(
            sample_server_cfg(),
            "v0.2.0",
            None,
            None,
            None,
            Arc::clone(&mock),
        );
        let h = s.handler();
        let req = test_request_path(
            "POST",
            "/unlock",
            Some("application/json"),
            &unlock_body("12345678@10.0.0.1:18022", "12340000@10.0.0.10:18022"),
        );
        let (status, _, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_OK);
        assert_eq!(
            String::from_utf8(body).unwrap(),
            format!(r#"{{"result":{}}}"#, result::ERR)
        );
    }

    #[test]
    fn post_unlock_missing_to_400() {
        let s = Server::new(
            sample_server_cfg(),
            "v0.2.0",
            None,
            None,
            None,
            MockSender::success(),
        );
        let h = s.handler();
        let req = test_request_path(
            "POST",
            "/unlock",
            Some("application/json"),
            br#"{"from":"12345678@10.0.0.1:18022"}"#,
        );
        let (status, _, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_BAD_REQUEST);
        assert!(String::from_utf8(body)
            .unwrap()
            .contains("missing 'to' field"));
    }

    #[test]
    fn post_unlock_bad_from_uri_400() {
        let s = Server::new(
            sample_server_cfg(),
            "v0.2.0",
            None,
            None,
            None,
            MockSender::success(),
        );
        let h = s.handler();
        let req = test_request_path(
            "POST",
            "/unlock",
            Some("application/json"),
            &unlock_body("garbage", "12340000@10.0.0.10:18022"),
        );
        let (status, _, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_BAD_REQUEST);
        assert!(String::from_utf8(body)
            .unwrap()
            .contains("invalid 'from' URI"));
    }

    #[test]
    fn post_unlock_get_rejected_by_middleware_chain() {
        let s = Server::new(
            sample_server_cfg(),
            "v0.2.0",
            None,
            None,
            None,
            MockSender::success(),
        );
        let h = s.handler();
        let req = test_request_path("GET", "/unlock", Some("application/json"), b"");
        let (status, headers, _) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_METHOD_NOT_ALLOWED);
        assert_eq!(headers.get("Allow"), Some("POST"));
    }

    #[test]
    fn post_unlock_no_stations_returns_result_minus_100() {
        let mut cfg = Config::default();
        cfg.sip = "12345678@10.0.0.20:18022".into();
        let s = Server::new(cfg, "v0.2.0", None, None, None, MockSender::success());
        let h = s.handler();
        let req = test_request_path(
            "POST",
            "/unlock",
            Some("application/json"),
            &unlock_body("12345678@10.0.0.1:18022", "12340000@10.0.0.10:18022"),
        );
        let (status, _, body) = Recorder::new().pipe(|w| h.serve_http(w, &req));
        assert_eq!(status, STATUS_OK);
        assert_eq!(String::from_utf8(body).unwrap(), r#"{"result":-100}"#);
    }
}
