//! Bare HTTP/1.1 原语（复刻 Go `internal/httpx` 子集）。
//!
//! 禁 TLS / HTTP2 / keep-alive / chunked 请求 body；仅 GET + POST。

pub mod client;
pub mod deadline_io;
pub mod json;
pub mod mux;
pub mod parse;
pub mod response;
pub mod server;

pub use client::{new_request, new_request_with_cancel, Client, ClientRequest};
pub use parse::ClientResponse;
pub use server::{Server, ServerClosedError, ShutdownError};

use std::collections::HashMap;
use std::io::{self};

// ---------------------------------------------------------------------------
// 常量（Go: types.go）
// ---------------------------------------------------------------------------

pub const METHOD_GET: &str = "GET";
pub const METHOD_POST: &str = "POST";

pub const STATUS_OK: u16 = 200;
pub const STATUS_BAD_REQUEST: u16 = 400;
pub const STATUS_NOT_FOUND: u16 = 404;
pub const STATUS_METHOD_NOT_ALLOWED: u16 = 405;
pub const STATUS_PAYLOAD_TOO_LARGE: u16 = 413;
pub const STATUS_URI_TOO_LONG: u16 = 414;
pub const STATUS_UPGRADE_REQUIRED: u16 = 426;
pub const STATUS_REQUEST_HEADER_TOO_BIG: u16 = 431;
pub const STATUS_INTERNAL_SERVER_ERROR: u16 = 500;
pub const STATUS_BAD_GATEWAY: u16 = 502;

/// 返回 status reason phrase；未知码返空串（与 Go `StatusText` 一致）。
///
/// 表项对齐 Go `httpx/types.go` statusText 全集（409 在 Go 表中同样缺席 →
/// response.rs 的 "Status" fallback 与 Go 行为一致，golden `409 Status` 为证）。
pub fn status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        413 => "Payload Too Large",
        414 => "URI Too Long",
        415 => "Unsupported Media Type",
        417 => "Expectation Failed",
        426 => "Upgrade Required",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "",
    }
}

// ---------------------------------------------------------------------------
// Header（Go: types.go Header）
// ---------------------------------------------------------------------------

/// HTTP header 集合；key 存 canonical 形式（`Content-Type`）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Header {
    inner: HashMap<String, Vec<String>>,
}

impl Header {
    pub fn new() -> Self {
        Self::default()
    }

    /// 大小写不敏感取首个 value。
    pub fn get(&self, key: &str) -> Option<&str> {
        self.inner
            .get(&canonical_mime_header_key(key))
            .and_then(|v| v.first())
            .map(|s| s.as_str())
    }

    pub fn set(&mut self, key: &str, value: &str) {
        let ck = canonical_mime_header_key(key);
        self.inner.insert(ck, vec![value.to_string()]);
    }

    pub fn add(&mut self, key: &str, value: &str) {
        let ck = canonical_mime_header_key(key);
        self.inner.entry(ck).or_default().push(value.to_string());
    }

    pub fn del(&mut self, key: &str) {
        self.inner.remove(&canonical_mime_header_key(key));
    }

    pub fn values(&self, key: &str) -> Option<&[String]> {
        self.inner
            .get(&canonical_mime_header_key(key))
            .map(|v| v.as_slice())
    }

    /// 迭代 canonical key → values（写响应 header 用）。
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Vec<String>)> {
        self.inner.iter()
    }
}

/// 复刻 Go `textproto.CanonicalMIMEHeaderKey`。
pub fn canonical_mime_header_key(key: &str) -> String {
    key.split('-')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => {
                    let mut s = String::new();
                    s.extend(first.to_uppercase());
                    s.extend(chars.flat_map(|c| c.to_lowercase()));
                    s
                }
            }
        })
        .collect::<Vec<_>>()
        .join("-")
}

// ---------------------------------------------------------------------------
// URL / Request
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct Url {
    pub path: String,
    pub raw_query: String,
}

impl Url {
    pub fn string(&self) -> String {
        if self.raw_query.is_empty() {
            self.path.clone()
        } else {
            format!("{}?{}", self.path, self.raw_query)
        }
    }
}

/// 解析后的 HTTP 请求。
pub struct Request {
    pub method: String,
    pub url: Url,
    pub header: Header,
    /// 完整缓冲的 request body（parser 已按 Content-Length 读入内存）。用 `Vec<u8>` 而非
    /// `Box<dyn Read>`，使 handler 经 `&Request` 读 body 无需 unsafe 别名 cast（body 恒已缓冲）。
    pub body: Vec<u8>,
    pub host: String,
    pub remote_addr: String,
    pub content_length: i64,
}

// 手写 Debug：body 是裸字节，只打长度（避免在 panic/expect 输出里 dump 整个 body）。
// 让 Result::expect_err 等在测试中可格式化（Ok(Request) 路径）。
impl std::fmt::Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Request")
            .field("method", &self.method)
            .field("host", &self.host)
            .field("remote_addr", &self.remote_addr)
            .field("content_length", &self.content_length)
            .field("body_len", &self.body.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// ResponseWriter / Handler
// ---------------------------------------------------------------------------

/// Server 端 handler 写响应接口（复刻 Go `ResponseWriter`）。
pub trait ResponseWriter {
    fn header(&mut self) -> &mut Header;
    fn write_header(&mut self, status: u16);
    fn write(&mut self, data: &[u8]) -> io::Result<usize>;
    fn flush(&mut self) -> io::Result<()>;
}

pub trait Handler: Send + Sync {
    fn serve_http(&self, w: &mut dyn ResponseWriter, r: &Request);
}

/// 函数指针风格 handler。
pub struct HandlerFunc<F>(pub F);

impl<F> Handler for HandlerFunc<F>
where
    F: Fn(&mut dyn ResponseWriter, &Request) + Send + Sync,
{
    fn serve_http(&self, w: &mut dyn ResponseWriter, r: &Request) {
        (self.0)(w, r);
    }
}

// ---------------------------------------------------------------------------
// 协议错误
// ---------------------------------------------------------------------------

/// 协议层错误，携带建议 HTTP 状态码。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpProtocolError {
    pub status: u16,
    pub msg: String,
}

impl HttpProtocolError {
    pub fn new(status: u16, msg: impl Into<String>) -> Self {
        Self {
            status,
            msg: msg.into(),
        }
    }
}

impl std::fmt::Display for HttpProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "httpx: {} (status={})", self.msg, self.status)
    }
}

impl std::error::Error for HttpProtocolError {}

/// `read_request` 返回类型：协议错 / IO EOF / 其它 IO 错。
pub type ReadRequestResult = Result<Request, ReadRequestError>;

#[derive(Debug)]
pub enum ReadRequestError {
    Protocol(HttpProtocolError),
    Eof,
    Io(io::Error),
}

impl From<HttpProtocolError> for ReadRequestError {
    fn from(e: HttpProtocolError) -> Self {
        ReadRequestError::Protocol(e)
    }
}

impl From<io::Error> for ReadRequestError {
    fn from(e: io::Error) -> Self {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            ReadRequestError::Eof
        } else {
            ReadRequestError::Io(e)
        }
    }
}

impl std::fmt::Display for ReadRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadRequestError::Protocol(e) => write!(f, "{e}"),
            ReadRequestError::Eof => f.write_str("unexpected EOF"),
            ReadRequestError::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ReadRequestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ReadRequestError::Protocol(e) => Some(e),
            ReadRequestError::Io(e) => Some(e),
            ReadRequestError::Eof => None,
        }
    }
}
