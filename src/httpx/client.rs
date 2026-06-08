//! Bare HTTP/1.1 client（复刻 Go `client.go`）。

use std::io::{self, BufReader, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::deadline_io::AbsoluteDeadlineStream;
use super::parse::{read_response, ClientResponse};
use super::{Header, METHOD_GET, METHOD_POST};

/// 极简 HTTP/1.1 client。
#[derive(Debug, Clone, Default)]
pub struct Client {
    /// dial + 写请求 + 读响应总上限；`None` 或 `ZERO` = 无超时。
    pub timeout: Option<Duration>,
}

/// Client 请求（对齐 Go `ClientRequest`）。
#[derive(Debug, Clone)]
pub struct ClientRequest {
    pub method: String,
    pub url: String,
    pub header: Header,
    pub body: Vec<u8>,
    cancel: Option<Arc<AtomicBool>>,
}

impl ClientRequest {
    pub fn set_header(&mut self, key: &str, value: &str) {
        self.header.set(key, value);
    }

    fn is_cancelled(&self) -> bool {
        self.cancel
            .as_ref()
            .is_some_and(|t| t.load(Ordering::SeqCst))
    }
}

/// 构造 `ClientRequest`（仅 GET/POST；仅 `http://` URL）。
pub fn new_request(method: &str, url: &str, body: Option<&[u8]>) -> Result<ClientRequest, String> {
    if method != METHOD_GET && method != METHOD_POST {
        return Err(format!("httpx: unsupported method {method:?}"));
    }
    if !url.starts_with("http://") {
        return Err(format!("httpx: only http:// URLs supported, got {url:?}"));
    }
    Ok(ClientRequest {
        method: method.to_string(),
        url: url.to_string(),
        header: Header::new(),
        body: body.map(|b| b.to_vec()).unwrap_or_default(),
        cancel: None,
    })
}

/// 同 `new_request`，附带 cancel token（对齐 Go `NewRequestWithContext`）。
pub fn new_request_with_cancel(
    method: &str,
    url: &str,
    body: Option<&[u8]>,
    cancel: Arc<AtomicBool>,
) -> Result<ClientRequest, String> {
    let mut req = new_request(method, url, body)?;
    req.cancel = Some(cancel);
    Ok(req)
}

impl Client {
    pub fn do_request(&self, req: &ClientRequest) -> Result<ClientResponse, String> {
        if req.is_cancelled() {
            return Err("httpx: request cancelled".to_string());
        }

        let (hostport, target) = split_url(&req.url)?;

        let mut header = req.header.clone();
        header.set("Host", &hostport);
        header.set("Connection", "close");
        if req.body.is_empty() {
            header.set("Content-Length", "0");
        } else {
            header.set("Content-Length", &req.body.len().to_string());
        }

        let dial_timeout = self.timeout.filter(|t| *t > Duration::ZERO);
        let stream = tcp_connect(&hostport, dial_timeout, req.cancel.as_ref())?;

        if let (Some(cancel), Ok(clone)) = (req.cancel.as_ref(), stream.try_clone()) {
            let cancel = Arc::clone(cancel);
            thread::spawn(move || {
                while !cancel.load(Ordering::SeqCst) {
                    thread::sleep(Duration::from_millis(10));
                }
                let _ = clone.shutdown(std::net::Shutdown::Both);
            });
        }

        let io_deadline = dial_timeout.map(|t| Instant::now() + t);
        let mut stream = AbsoluteDeadlineStream::new(stream, io_deadline);

        write_request(&mut stream, &req.method, &target, &header, &req.body)?;

        if req.is_cancelled() {
            return Err("httpx: request cancelled".to_string());
        }

        let mut br = BufReader::new(&mut stream);
        match read_response(&mut br) {
            Ok(resp) => Ok(resp),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                Err("httpx: server closed connection before sending response".to_string())
            }
            Err(e) => Err(format!("httpx: read response: {e}")),
        }
    }
}

fn tcp_connect(
    hostport: &str,
    timeout: Option<Duration>,
    cancel: Option<&Arc<AtomicBool>>,
) -> Result<TcpStream, String> {
    if let Some(c) = cancel {
        if c.load(Ordering::SeqCst) {
            return Err("httpx: request cancelled".to_string());
        }
    }

    let addrs: Vec<_> = hostport
        .to_socket_addrs()
        .map_err(|e| format!("httpx: resolve {hostport}: {e}"))?
        .collect();
    if addrs.is_empty() {
        return Err(format!("httpx: no addresses for {hostport}"));
    }

    let start = Instant::now();
    let mut last_err = None;
    loop {
        if let Some(c) = cancel {
            if c.load(Ordering::SeqCst) {
                return Err("httpx: request cancelled".to_string());
            }
        }
        for addr in &addrs {
            match TcpStream::connect(addr) {
                Ok(s) => return Ok(s),
                Err(e) => last_err = Some(e),
            }
        }
        match timeout {
            None => {
                return Err(format!(
                    "httpx: dial {hostport}: {}",
                    last_err.unwrap_or_else(|| {
                        io::Error::other("connect failed")
                    })
                ));
            }
            Some(t) if start.elapsed() >= t => {
                return Err(format!(
                    "httpx: dial {hostport}: {}",
                    last_err.unwrap_or_else(|| {
                        io::Error::new(io::ErrorKind::TimedOut, "connect timed out")
                    })
                ));
            }
            Some(_) => thread::sleep(Duration::from_millis(10)),
        }
    }
}

fn write_request(
    stream: &mut AbsoluteDeadlineStream,
    method: &str,
    target: &str,
    header: &Header,
    body: &[u8],
) -> Result<(), String> {
    write!(stream, "{method} {target} HTTP/1.1\r\n")
        .map_err(|e| format!("httpx: write request line: {e}"))?;
    for (k, vals) in header.iter() {
        for v in vals {
            write!(stream, "{k}: {v}\r\n")
                .map_err(|e| format!("httpx: write header: {e}"))?;
        }
    }
    stream
        .write_all(b"\r\n")
        .map_err(|e| format!("httpx: write CRLF: {e}"))?;
    if !body.is_empty() {
        stream
            .write_all(body)
            .map_err(|e| format!("httpx: write body: {e}"))?;
    }
    stream
        .flush()
        .map_err(|e| format!("httpx: flush: {e}"))?;
    Ok(())
}

/// 把 `"http://host[:port]/path[?query]"` 拆成 `host:port` + `/path?query`。
pub fn split_url(raw_url: &str) -> Result<(String, String), String> {
    if !raw_url.starts_with("http://") {
        return Err(format!("httpx: only http:// URLs supported, got {raw_url:?}"));
    }
    let rest = &raw_url["http://".len()..];
    if rest.is_empty() {
        return Err("httpx: empty host in URL".to_string());
    }
    let (hostport, target) = match rest.find('/') {
        Some(slash) => (&rest[..slash], &rest[slash..]),
        None => (rest, "/"),
    };
    if hostport.is_empty() {
        return Err("httpx: empty host in URL".to_string());
    }
    let hostport = if hostport.contains(':') {
        hostport.to_string()
    } else {
        format!("{hostport}:80")
    };
    Ok((hostport, target.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, Read};
    use std::net::TcpListener;

    fn fake_server<F>(handler: F) -> (String, impl FnOnce())
    where
        F: FnOnce(String) -> String + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr").to_string();
        let done = thread::spawn(move || {
            if let Ok((mut conn, _)) = listener.accept() {
                let _ = conn.set_read_timeout(Some(Duration::from_secs(2)));
                let mut br = BufReader::new(&mut conn);
                let mut lines = Vec::new();
                let mut content_length = 0usize;
                loop {
                    let mut line = String::new();
                    if br.read_line(&mut line).is_err() {
                        return;
                    }
                    let trimmed = line.trim_end_matches(['\r', '\n']);
                    if trimmed.is_empty() {
                        break;
                    }
                    if let Some(rest) = trimmed
                        .to_ascii_lowercase()
                        .strip_prefix("content-length:")
                    {
                        content_length = rest.trim().parse().unwrap_or(0);
                    }
                    lines.push(line);
                }
                let mut body = vec![0u8; content_length];
                if content_length > 0 {
                    let _ = br.read_exact(&mut body);
                }
                let raw = lines.concat() + &String::from_utf8_lossy(&body);
                let resp = handler(raw);
                let _ = conn.write_all(resp.as_bytes());
            }
        });
        let cleanup = move || {
            let _ = done.join();
        };
        (addr, cleanup)
    }

    #[test]
    fn client_basic_get() {
        let (addr, cleanup) = fake_server(|req| {
            assert!(req.starts_with("GET /info HTTP/1.1\r\n"));
            assert!(req.contains("Connection: close"));
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok".to_string()
        });
        let c = Client {
            timeout: Some(Duration::from_secs(1)),
        };
        let req = new_request(METHOD_GET, &format!("http://{addr}/info"), None).unwrap();
        let resp = c.do_request(&req).unwrap();
        assert_eq!(resp.status_code, 200);
        assert_eq!(resp.body, b"ok");
        cleanup();
    }

    #[test]
    fn client_post_with_body() {
        let (addr, cleanup) = fake_server(|req| {
            assert!(req.contains(r#"{"event":"unlock"}"#));
            assert!(req.contains("Content-Type: application/json"));
            assert!(req.contains("Authorization: Bearer xxx"));
            assert!(req.contains("Content-Length: 18"));
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_string()
        });
        let body = br#"{"event":"unlock"}"#;
        let c = Client {
            timeout: Some(Duration::from_secs(1)),
        };
        let mut req =
            new_request(METHOD_POST, &format!("http://{addr}/unlock"), Some(body))
                .unwrap();
        req.set_header("Content-Type", "application/json");
        req.set_header("Authorization", "Bearer xxx");
        let resp = c.do_request(&req).unwrap();
        assert_eq!(resp.status_code, 200);
        cleanup();
    }

    #[test]
    fn client_timeout_fails_fast() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().unwrap().to_string();
        let server = thread::spawn(move || {
            if let Ok((conn, _)) = listener.accept() {
                thread::sleep(Duration::from_secs(2));
                drop(conn);
            }
        });
        let c = Client {
            timeout: Some(Duration::from_millis(200)),
        };
        let req = new_request(METHOD_GET, &format!("http://{addr}/"), None).unwrap();
        let start = Instant::now();
        let err = c.do_request(&req).unwrap_err();
        let elapsed = start.elapsed();
        assert!(!err.is_empty());
        assert!(
            elapsed < Duration::from_millis(500),
            "timeout took {elapsed:?}, expected < 500ms"
        );
        let _ = server.join();
    }

    #[test]
    fn client_context_cancel() {
        let (addr, cleanup) = fake_server(|_req| {
            thread::sleep(Duration::from_secs(2));
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_string()
        });
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_bg = Arc::clone(&cancel);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(150));
            cancel_bg.store(true, Ordering::SeqCst);
        });
        let c = Client {
            timeout: Some(Duration::from_secs(5)),
        };
        let req =
            new_request_with_cancel(METHOD_GET, &format!("http://{addr}/"), None, cancel).unwrap();
        let start = Instant::now();
        let err = c.do_request(&req).unwrap_err();
        let elapsed = start.elapsed();
        assert!(!err.is_empty());
        assert!(
            elapsed < Duration::from_millis(700),
            "ctx cancel didn't kick in fast enough: {elapsed:?}"
        );
        cleanup();
    }

    #[test]
    fn new_request_rejects_bad_method() {
        assert!(new_request("DELETE", "http://x/y", None).is_err());
    }

    #[test]
    fn new_request_rejects_non_http_scheme() {
        assert!(new_request(METHOD_GET, "https://x/y", None).is_err());
        assert!(new_request(METHOD_GET, "ftp://x/y", None).is_err());
        assert!(new_request(METHOD_GET, "x/y", None).is_err());
    }

    #[test]
    fn split_url_cases() {
        let cases = [
            ("http://x", "x:80", "/"),
            ("http://x:8080/api", "x:8080", "/api"),
            (
                "http://1.2.3.4:8123/api/dooraccess/abc",
                "1.2.3.4:8123",
                "/api/dooraccess/abc",
            ),
            ("http://x/api?a=1&b=2", "x:80", "/api?a=1&b=2"),
        ];
        for (url, want_host, want_target) in cases {
            let (host, target) = split_url(url).unwrap();
            assert_eq!(host, want_host, "url={url}");
            assert_eq!(target, want_target, "url={url}");
        }
        for bad in ["https://x/y", "http://", "x"] {
            assert!(split_url(bad).is_err(), "url={bad}");
        }
    }

    #[test]
    fn client_chunked_response_decoded() {
        let (addr, cleanup) = fake_server(|_req| {
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n3\r\n123\r\n0\r\n\r\n"
                .to_string()
        });
        let c = Client {
            timeout: Some(Duration::from_secs(1)),
        };
        let req = new_request(METHOD_GET, &format!("http://{addr}/"), None).unwrap();
        let resp = c.do_request(&req).unwrap();
        assert_eq!(String::from_utf8_lossy(&resp.body), "hello123");
        cleanup();
    }
}
