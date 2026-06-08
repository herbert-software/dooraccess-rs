//! HTTP/1.1 request parser（复刻 Go `parse.go`）。

use std::io::{self, BufRead};

use super::{
    canonical_mime_header_key, Header, HttpProtocolError, ReadRequestError, ReadRequestResult,
    Request, Url, METHOD_GET, METHOD_POST, STATUS_BAD_GATEWAY, STATUS_BAD_REQUEST,
    STATUS_METHOD_NOT_ALLOWED, STATUS_PAYLOAD_TOO_LARGE, STATUS_REQUEST_HEADER_TOO_BIG,
    STATUS_URI_TOO_LONG, STATUS_UPGRADE_REQUIRED,
};

pub const MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
pub const MAX_HEADER_BYTES: usize = 16 * 1024;
pub const MAX_HEADER_COUNT: usize = 64;
pub const MAX_BODY_BYTES: i64 = 1 << 20;

/// 从 buffered reader 解析一个完整 HTTP/1.1 请求。
pub fn read_request<R: BufRead>(
    br: &mut R,
    mut on_headers_done: impl FnMut(),
) -> ReadRequestResult {
    let line = match read_line(br, MAX_REQUEST_LINE_BYTES) {
        Ok(l) => l,
        Err(ReadRequestError::Eof) => return Err(ReadRequestError::Eof),
        Err(e) => return Err(e),
    };

    let (method, target, version) = parse_request_line(&line)?;
    if version != "HTTP/1.1" && version != "HTTP/1.0" {
        return Err(HttpProtocolError::new(
            STATUS_UPGRADE_REQUIRED,
            format!("unsupported HTTP version: {version}"),
        )
        .into());
    }
    if method != METHOD_GET && method != METHOD_POST {
        return Err(HttpProtocolError::new(
            STATUS_METHOD_NOT_ALLOWED,
            format!("unsupported method: {method}"),
        )
        .into());
    }

    let (headers, host) = read_headers(br)?;
    if version == "HTTP/1.1" && host.is_empty() {
        return Err(HttpProtocolError::new(
            STATUS_BAD_REQUEST,
            "HTTP/1.1 requires Host header",
        )
        .into());
    }

    if let Some(te) = headers.get("Transfer-Encoding") {
        for part in te.split(',') {
            if part.trim().eq_ignore_ascii_case("chunked") {
                return Err(HttpProtocolError::new(
                    STATUS_BAD_REQUEST,
                    "chunked request body not supported",
                )
                .into());
            }
        }
    }

    // headers 已读完并校验：切到 body 阶段 deadline（whole-request）再读 body。
    // 复刻 Go：body 在 headers 后走 ReadTimeout 而非更紧的 ReadHeaderTimeout。
    on_headers_done();

    let mut content_length: i64 = 0;
    let body: Vec<u8> = if let Some(cl) = headers.get("Content-Length") {
        let n = parse_content_length(cl)?;
        if n > MAX_BODY_BYTES {
            return Err(HttpProtocolError::new(
                STATUS_PAYLOAD_TOO_LARGE,
                format!("body {n} bytes exceeds {MAX_BODY_BYTES} max"),
            )
            .into());
        }
        content_length = n;
        if n > 0 {
            let mut buf = vec![0u8; n as usize];
            br.read_exact(&mut buf).map_err(ReadRequestError::from)?;
            buf
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    if let Some(up) = headers.get("Upgrade") {
        if up.eq_ignore_ascii_case("h2c") {
            return Err(HttpProtocolError::new(
                STATUS_UPGRADE_REQUIRED,
                "HTTP/2 cleartext upgrade not supported",
            )
            .into());
        }
    }

    let (path, raw_query) = split_path_query(target);
    Ok(Request {
        method: method.to_string(),
        url: Url {
            path: path.to_string(),
            raw_query: raw_query.to_string(),
        },
        header: headers,
        body,
        host,
        remote_addr: String::new(),
        content_length,
    })
}

fn parse_content_length(cl: &str) -> Result<i64, ReadRequestError> {
    let n: i64 = cl
        .parse()
        .map_err(|_| HttpProtocolError::new(STATUS_BAD_REQUEST, format!("invalid Content-Length: {cl}")))?;
    if n < 0 {
        return Err(HttpProtocolError::new(
            STATUS_BAD_REQUEST,
            format!("invalid Content-Length: {cl}"),
        )
        .into());
    }
    Ok(n)
}

/// 读一行（直到 LF），strip CRLF；超 limit 返协议错误。
fn read_line<R: BufRead>(br: &mut R, limit: usize) -> Result<String, ReadRequestError> {
    let mut buf = Vec::new();
    loop {
        let b = br.read_until(b'\n', &mut buf).map_err(|e| {
            if e.kind() == io::ErrorKind::UnexpectedEof && buf.is_empty() {
                ReadRequestError::Eof
            } else {
                ReadRequestError::Io(e)
            }
        })?;
        if b == 0 && buf.is_empty() {
            return Err(ReadRequestError::Eof);
        }
        if buf.len() > limit {
            return Err(HttpProtocolError::new(
                STATUS_URI_TOO_LONG,
                format!("line {} bytes exceeds {limit} max", buf.len()),
            )
            .into());
        }
        if buf.last() == Some(&b'\n') {
            break;
        }
        if buf.len() >= limit {
            return Err(HttpProtocolError::new(
                STATUS_URI_TOO_LONG,
                "line exceeds bufio buffer",
            )
            .into());
        }
    }
    let line = if buf.len() >= 2 && buf[buf.len() - 2] == b'\r' && buf[buf.len() - 1] == b'\n' {
        String::from_utf8_lossy(&buf[..buf.len() - 2]).into_owned()
    } else if buf.last() == Some(&b'\n') {
        String::from_utf8_lossy(&buf[..buf.len() - 1]).into_owned()
    } else {
        String::from_utf8_lossy(&buf).into_owned()
    };
    Ok(line)
}

/// 解析 `"METHOD URI HTTP/1.1"`。
pub fn parse_request_line(line: &str) -> Result<(&str, &str, &str), ReadRequestError> {
    let sp1 = line
        .find(' ')
        .ok_or_else(|| HttpProtocolError::new(STATUS_BAD_REQUEST, format!("malformed request line: {line}")))?;
    let rest = &line[sp1 + 1..];
    let sp2 = rest
        .find(' ')
        .ok_or_else(|| HttpProtocolError::new(STATUS_BAD_REQUEST, format!("malformed request line: {line}")))?;
    let method = &line[..sp1];
    let target = &rest[..sp2];
    let version = &rest[sp2 + 1..];
    if method.is_empty() || target.is_empty() || version.is_empty() {
        return Err(HttpProtocolError::new(
            STATUS_BAD_REQUEST,
            format!("empty field in request line: {line}"),
        )
        .into());
    }
    Ok((method, target, version))
}

fn read_headers<R: BufRead>(br: &mut R) -> Result<(Header, String), ReadRequestError> {
    let mut headers = Header::new();
    let mut total_bytes = 0usize;
    let mut host = String::new();

    for i in 0..=MAX_HEADER_COUNT {
        let line = read_line(br, MAX_REQUEST_LINE_BYTES)?;
        if line.is_empty() {
            return Ok((headers, host));
        }
        total_bytes += line.len() + 2;
        if total_bytes > MAX_HEADER_BYTES {
            return Err(HttpProtocolError::new(
                STATUS_REQUEST_HEADER_TOO_BIG,
                "header section exceeds limit",
            )
            .into());
        }
        if i >= MAX_HEADER_COUNT {
            return Err(HttpProtocolError::new(
                STATUS_REQUEST_HEADER_TOO_BIG,
                "too many headers",
            )
            .into());
        }
        let colon = line
            .find(':')
            .filter(|&p| p > 0)
            .ok_or_else(|| HttpProtocolError::new(STATUS_BAD_REQUEST, format!("malformed header: {line}")))?;
        let key = line[..colon].trim();
        let value = line[colon + 1..].trim();
        let ck = canonical_mime_header_key(key);
        headers.add(&ck, value);
        if ck == "Host" && host.is_empty() {
            host = value.to_string();
        }
    }
    Err(HttpProtocolError::new(
        STATUS_REQUEST_HEADER_TOO_BIG,
        "exceeded maxHeaderCount loop",
    )
    .into())
}

/// 把 `"/path?query"` 拆成 path + query。
pub fn split_path_query(target: &str) -> (&str, &str) {
    match target.find('?') {
        Some(i) => (&target[..i], &target[i + 1..]),
        None => (target, ""),
    }
}

/// 解析 status line `"HTTP/1.1 200 OK"`（client 端用，本组导出供后续组）。
pub fn parse_status_line(line: &str) -> Result<u16, HttpProtocolError> {
    let parts: Vec<&str> = line.splitn(3, ' ').collect();
    if parts.len() < 2 {
        return Err(HttpProtocolError::new(
            STATUS_BAD_GATEWAY,
            format!("malformed status line: {line}"),
        ));
    }
    if !parts[0].starts_with("HTTP/") {
        return Err(HttpProtocolError::new(
            STATUS_BAD_GATEWAY,
            format!("missing HTTP/ prefix: {line}"),
        ));
    }
    let code: u16 = parts[1]
        .parse()
        .map_err(|_| HttpProtocolError::new(STATUS_BAD_GATEWAY, format!("invalid status code: {}", parts[1])))?;
    if !(100..=999).contains(&code) {
        return Err(HttpProtocolError::new(
            STATUS_BAD_GATEWAY,
            format!("invalid status code: {}", parts[1]),
        ));
    }
    Ok(code)
}

/// Client 读到的 HTTP 响应（body 全进内存）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientResponse {
    pub status_code: u16,
    pub header: Header,
    pub body: Vec<u8>,
}

/// Client 端读响应：status line + headers + body（支持 Content-Length / chunked）。
pub fn read_response<R: BufRead>(br: &mut R) -> Result<ClientResponse, io::Error> {
    let line = read_line(br, MAX_REQUEST_LINE_BYTES).map_err(io_err_from_read_request)?;
    let code = parse_status_line(&line).map_err(|e| {
        io::Error::new(io::ErrorKind::InvalidData, e.to_string())
    })?;

    let (headers, _) = read_headers(br).map_err(io_err_from_read_request)?;

    let body = if let Some(te) = headers.get("Transfer-Encoding") {
        if te.eq_ignore_ascii_case("chunked") {
            read_chunked_body(br)?
        } else {
            Vec::new()
        }
    } else if let Some(cl) = headers.get("Content-Length") {
        let n: i64 = cl.parse().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("httpx: invalid response Content-Length: {cl}"),
            )
        })?;
        if n < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("httpx: invalid response Content-Length: {cl}"),
            ));
        }
        if n > MAX_BODY_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("httpx: response body {n} exceeds {MAX_BODY_BYTES} max"),
            ));
        }
        let mut body = vec![0u8; n as usize];
        br.read_exact(&mut body).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("httpx: read response body: {e}"),
            )
        })?;
        body
    } else {
        Vec::new()
    };

    Ok(ClientResponse {
        status_code: code,
        header: headers,
        body,
    })
}

fn io_err_from_read_request(e: ReadRequestError) -> io::Error {
    match e {
        ReadRequestError::Io(e) => e,
        ReadRequestError::Eof => io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected EOF"),
        ReadRequestError::Protocol(e) => {
            io::Error::new(io::ErrorKind::InvalidData, e.to_string())
        }
    }
}

fn read_chunked_body<R: BufRead>(br: &mut R) -> Result<Vec<u8>, io::Error> {
    let mut buf = Vec::new();
    loop {
        let line = read_line(br, 256).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("read chunk size: {e}"))
        })?;
        let size_str = line
            .split([';', ' ', '\t'])
            .next()
            .unwrap_or("");
        let size = i64::from_str_radix(size_str, 16).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid chunk size: {line:?}"),
            )
        })?;
        if size < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid chunk size: {line:?}"),
            ));
        }
        if size == 0 {
            loop {
                let trailer = read_line(br, MAX_REQUEST_LINE_BYTES).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("read chunk trailer: {e}"))
                })?;
                if trailer.is_empty() {
                    return Ok(buf);
                }
            }
        }
        if i64::try_from(buf.len()).unwrap_or(MAX_BODY_BYTES + 1) + size > MAX_BODY_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "chunked body cumulative {} exceeds {MAX_BODY_BYTES} max",
                    buf.len() as i64 + size
                ),
            ));
        }
        let mut chunk = vec![0u8; size as usize];
        br.read_exact(&mut chunk).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("read chunk body: {e}"))
        })?;
        buf.extend_from_slice(&chunk);
        let mut crlf = [0u8; 2];
        br.read_exact(&mut crlf).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("skip chunk CRLF: {e}"))
        })?;
    }
}

