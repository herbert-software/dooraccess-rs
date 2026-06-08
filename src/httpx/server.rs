//! Bare HTTP/1.1 server（复刻 Go `server.go`）。
//!
//! thread-per-connection + blocking socket；禁 tokio / TLS / HTTP2 / keepalive。

use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use super::deadline_io::apply_write_deadline;
use super::parse::read_request;
use super::response::ResponseWriterImpl;
use super::{status_text, Handler, HttpProtocolError, ReadRequestError, STATUS_BAD_REQUEST};

/// `ListenAndServe` 在 graceful shutdown 后返回此错误（复刻 Go `ErrServerClosed`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerClosedError;

impl std::fmt::Display for ServerClosedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("httpx: server closed")
    }
}

impl std::error::Error for ServerClosedError {}

/// 极简 HTTP/1.1 server（字段语义对齐 Go `httpx.Server`）。
pub struct Server {
    pub addr: String,
    pub handler: Option<Arc<dyn Handler>>,
    /// 整 request 绝对 deadline（含 body）；`None` = 不限。
    pub read_timeout: Option<Duration>,
    /// response 写 deadline；`None` 或 `Some(ZERO)` = 不设（video 长连接预留）。
    pub write_timeout: Option<Duration>,
    /// header 阶段 deadline（连接相对）；`None` = 不限。
    pub read_header_timeout: Option<Duration>,

    listener: Mutex<Option<TcpListener>>,
    closing: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    pub fn new() -> Self {
        Self {
            addr: String::new(),
            handler: None,
            read_timeout: None,
            write_timeout: None,
            read_header_timeout: None,
            listener: Mutex::new(None),
            closing: Arc::new(AtomicBool::new(false)),
            active: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// 在 `addr` 监听并 serve；阻塞到 `shutdown` / `close` 返回 `ServerClosedError`。
    pub fn listen_and_serve(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let handler = self
            .handler
            .clone()
            .ok_or("httpx: Server.Handler required")?;
        let addr = if self.addr.is_empty() {
            ":80".to_string()
        } else {
            self.addr.clone()
        };
        let listener = TcpListener::bind(&addr)?;
        self.serve(listener, handler)
    }

    /// 在已有 listener 上 accept（测试注入用）。
    pub fn serve(
        &self,
        listener: TcpListener,
        handler: Arc<dyn Handler>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        *self.listener.lock().unwrap() = Some(listener);
        loop {
            let accept_result = {
                let guard = self.listener.lock().unwrap();
                match guard.as_ref() {
                    Some(ln) => ln.accept(),
                    None => {
                        if self.closing.load(Ordering::SeqCst) {
                            return Err(Box::new(ServerClosedError));
                        }
                        return Ok(());
                    }
                }
            };

            match accept_result {
                Ok((stream, _)) => {
                    self.active.fetch_add(1, Ordering::SeqCst);
                    let server = ServerConnCtx {
                        handler: Arc::clone(&handler),
                        read_timeout: self.read_timeout,
                        write_timeout: self.write_timeout,
                        read_header_timeout: self.read_header_timeout,
                        closing: Arc::clone(&self.closing),
                        active: Arc::clone(&self.active),
                    };
                    thread::spawn(move || serve_conn(stream, server));
                }
                Err(e) => {
                    if self.closing.load(Ordering::SeqCst) {
                        return Err(Box::new(ServerClosedError));
                    }
                    if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut
                    {
                        continue;
                    }
                    return Err(Box::new(io::Error::new(
                        e.kind(),
                        format!("httpx: accept: {e}"),
                    )));
                }
            }
        }
    }

    /// Graceful shutdown：拒新连接 + 等 in-flight handler 完成。
    pub fn shutdown(&self, timeout: Option<Duration>) -> Result<(), ShutdownError> {
        self.closing.store(true, Ordering::SeqCst);
        if let Some(ln) = self.listener.lock().unwrap().take() {
            let _ = ln.set_nonblocking(true);
            drop(ln);
        }
        let deadline = timeout.map(|t| Instant::now() + t);
        loop {
            if self.active.load(Ordering::SeqCst) == 0 {
                return Ok(());
            }
            if let Some(dl) = deadline {
                if Instant::now() >= dl {
                    return Err(ShutdownError::Timeout);
                }
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    /// 强制关闭 listener 并 join 在飞 handler（不等完成）。
    pub fn close(&self) {
        self.closing.store(true, Ordering::SeqCst);
        if let Some(ln) = self.listener.lock().unwrap().take() {
            let _ = ln.set_nonblocking(true);
            drop(ln);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShutdownError {
    Timeout,
}

impl std::fmt::Display for ShutdownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShutdownError::Timeout => f.write_str("shutdown timeout"),
        }
    }
}

impl std::error::Error for ShutdownError {}

struct ServerConnCtx {
    handler: Arc<dyn Handler>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    read_header_timeout: Option<Duration>,
    closing: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
}

fn serve_conn(mut stream: TcpStream, ctx: ServerConnCtx) {
    struct ActiveGuard {
        active: Arc<AtomicUsize>,
    }
    impl Drop for ActiveGuard {
        fn drop(&mut self) {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let _guard = ActiveGuard {
        active: Arc::clone(&ctx.active),
    };

    let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        serve_conn_inner(&mut stream, &ctx);
    }));
    if panic_result.is_err() {
        let _ = stream.write_all(
            b"HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
        );
    }
    let _ = stream.shutdown(std::net::Shutdown::Both);
}

fn serve_conn_inner(stream: &mut TcpStream, ctx: &ServerConnCtx) {
    let conn_start = Instant::now();

    let write_deadline = ctx
        .write_timeout
        .filter(|wt| *wt > Duration::ZERO)
        .map(|wt| conn_start + wt);

    let req_result = {
        let headers_done = Arc::new(AtomicBool::new(false));
        let mut deadline_reader = DeadlineReader::new(
            stream,
            conn_start,
            ctx.read_header_timeout,
            ctx.read_timeout,
            Arc::clone(&headers_done),
        );
        let mut br = BufReader::new(&mut deadline_reader);
        // read_request 在读 body 前调 on_headers_done → 翻转 flag → body 走 whole-request
        // deadline（与 Go 一致），而非更紧的 header deadline。
        let hd = Arc::clone(&headers_done);
        let result = read_request(&mut br, move || hd.store(true, Ordering::SeqCst));
        // belt-and-suspenders no-op：read_request 内的 on_headers_done 已翻转 flag（载荷在那）；
        // 此调用对早返路径（read_request 在 on_headers_done 前出错）做兜底，幂等 set true。
        deadline_reader.mark_headers_done();
        result
    };

    let mut req = match req_result {
        Ok(r) => r,
        Err(ReadRequestError::Protocol(e)) => {
            write_protocol_error(stream, &e);
            return;
        }
        Err(ReadRequestError::Eof) | Err(ReadRequestError::Io(_)) => return,
    };

    if let Ok(addr) = stream.peer_addr() {
        req.remote_addr = addr.to_string();
    }

    let mut write_wrapper = WriteDeadlineWriter {
        stream,
        deadline: write_deadline,
    };
    let mut bw = BufWriter::new(&mut write_wrapper);
    let mut w = ResponseWriterImpl::new(&mut bw);

    ctx.handler.serve_http(&mut w, &req);

    // body 已由 parser 全量读入 `req.body: Vec<u8>`，socket 上无残留 body 待 drain。
    let _ = w.finish();
    drop(w); // 释放对 bw 的可变借用，才能 bw.flush()
    let _ = bw.flush();

    let _ = ctx.closing.load(Ordering::SeqCst);
}

fn write_protocol_error(stream: &mut TcpStream, err: &HttpProtocolError) {
    let status = if err.status == 0 {
        STATUS_BAD_REQUEST
    } else {
        err.status
    };
    let msg = status_text(status);
    let msg = if msg.is_empty() { "Error" } else { msg };
    let _ = write!(
        stream,
        "HTTP/1.1 {status} {msg}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
    );
}

/// 绝对 deadline + 每次 read 前重设 per-syscall `read_timeout`（复刻 Go `SetReadDeadline` 语义）。
struct DeadlineReader<'a> {
    stream: &'a mut TcpStream,
    conn_start: Instant,
    read_header_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
    // 共享 flag：read_request 在读 body **前**翻转（经 on_headers_done 回调），使 body
    // 读取走 whole-request(read_timeout) 而非更紧的 read_header_timeout。Go body 在
    // headers 后切到整请求 deadline；本 flag 复刻该时序（否则 body 误受 header deadline）。
    headers_done: Arc<AtomicBool>,
}

impl<'a> DeadlineReader<'a> {
    fn new(
        stream: &'a mut TcpStream,
        conn_start: Instant,
        read_header_timeout: Option<Duration>,
        read_timeout: Option<Duration>,
        headers_done: Arc<AtomicBool>,
    ) -> Self {
        Self {
            stream,
            conn_start,
            read_header_timeout,
            read_timeout,
            headers_done,
        }
    }

    fn mark_headers_done(&mut self) {
        self.headers_done.store(true, Ordering::SeqCst);
    }

    fn refresh_read_timeout(&mut self) -> io::Result<()> {
        let now = Instant::now();
        let mut deadline: Option<Instant> = None;

        if let Some(rt) = self.read_timeout {
            deadline = Some(self.conn_start + rt);
        }

        if !self.headers_done.load(Ordering::SeqCst) {
            if let Some(rht) = self.read_header_timeout {
                let hdr = self.conn_start + rht;
                deadline = Some(match deadline {
                    Some(d) => d.min(hdr),
                    None => hdr,
                });
            }
        }

        match deadline {
            None => {
                self.stream.set_read_timeout(None)?;
                Ok(())
            }
            Some(d) if now >= d => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "httpx: read deadline exceeded",
            )),
            Some(d) => {
                let remaining = d - now;
                let remaining = remaining.max(Duration::from_millis(1));
                self.stream.set_read_timeout(Some(remaining))?;
                Ok(())
            }
        }
    }
}

impl Read for DeadlineReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.refresh_read_timeout()?;
        self.stream.read(buf)
    }
}

struct WriteDeadlineWriter<'a> {
    stream: &'a mut TcpStream,
    deadline: Option<Instant>,
}

impl Write for WriteDeadlineWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        apply_write_deadline(self.stream, self.deadline)?;
        self.stream.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        apply_write_deadline(self.stream, self.deadline)?;
        self.stream.flush()
    }
}

/// 单连接 test helper（integration test / golden_http 用）。非 `#[cfg(test)]` 门控，
/// 因 tests/ 集成测试是独立 crate、只见非 test 公共 API。
pub fn test_serve_one_connection(
    stream: TcpStream,
    handler: Arc<dyn Handler>,
    read_timeout: Option<Duration>,
    read_header_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
) {
    let ctx = ServerConnCtx {
        handler,
        read_timeout,
        write_timeout,
        read_header_timeout,
        closing: Arc::new(AtomicBool::new(false)),
        active: Arc::new(AtomicUsize::new(1)),
    };
    serve_conn(stream, ctx);
}

#[cfg(test)]
mod deadline_tests {
    use super::*;
    use std::io::Read;

    /// header timeout(100ms) 紧于 read timeout(2s)：慢 headers 被切断。
    #[test]
    fn read_header_timeout_tighter_than_read_timeout() {
        use super::super::{HandlerFunc, STATUS_OK};

        let handler: Arc<dyn Handler> = Arc::new(HandlerFunc(
            |w: &mut dyn crate::httpx::ResponseWriter, _r: &crate::httpx::Request| {
                w.write_header(STATUS_OK);
            },
        ));
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().unwrap();
        let read_timeout = Duration::from_millis(2000);
        let read_header_timeout = Duration::from_millis(100);
        let h = Arc::clone(&handler);
        let srv = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            test_serve_one_connection(
                stream,
                h,
                Some(read_timeout),
                Some(read_header_timeout),
                None,
            );
        });

        let mut conn = TcpStream::connect(addr).expect("connect");
        conn.write_all(b"GET / HTTP/1.1\r\n")
            .expect("partial headers");
        thread::sleep(Duration::from_millis(200));
        conn.set_read_timeout(Some(Duration::from_millis(500))).ok();
        let mut buf = [0u8; 256];
        let n = conn.read(&mut buf).unwrap_or(0);
        let resp = String::from_utf8_lossy(&buf[..n]);
        assert!(
            !resp.contains("200 OK"),
            "ReadHeaderTimeout should interrupt slow headers, got: {resp:?}"
        );
        let _ = srv.join();
    }

    /// 整 request 绝对 deadline：headers 200ms 内发完，body 阶段累计超 500ms 预算。
    #[test]
    fn read_timeout_is_whole_request_deadline() {
        use super::super::{HandlerFunc, STATUS_OK};

        let handler: Arc<dyn Handler> = Arc::new(HandlerFunc(
            |w: &mut dyn crate::httpx::ResponseWriter, _r: &crate::httpx::Request| {
                w.write_header(STATUS_OK);
            },
        ));
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().unwrap();
        // read_timeout 给足（1500ms）避免并行测试 CPU 争用下 flaky；body 在 350ms 发——
        // 仍 > header deadline(200ms) 保判别力（修复前会被 header deadline 砍），
        // 且 << whole-request(1500ms) 留充裕 slack。
        let read_timeout = Duration::from_millis(1500);
        let read_header_timeout = Duration::from_millis(200);
        let h = Arc::clone(&handler);
        let srv = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            test_serve_one_connection(
                stream,
                h,
                Some(read_timeout),
                Some(read_header_timeout),
                None,
            );
        });

        // 鉴别性场景：headers 立即发（< header deadline 200ms），body 在 350ms 发——
        // 落在 header deadline(200ms) 与 whole-request deadline(1500ms) 之间。
        // 正确（Go / 本修复后）：body 走 whole-request 1500ms → 350 < 1500 → 读成 → 200 OK。
        // 错误（修复前 body 误受 header 200ms deadline）：350 > 200 → 超时 → 无 200。
        // 故断言 200 OK 可区分两种实现，不再是「非 200」的假绿。
        let mut conn = TcpStream::connect(addr).expect("connect");
        conn.write_all(b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\n")
            .expect("headers");
        thread::sleep(Duration::from_millis(350));
        let _ = conn.write_all(b"data");
        conn.set_read_timeout(Some(Duration::from_millis(1000)))
            .ok();
        let mut buf = [0u8; 256];
        let n = conn.read(&mut buf).unwrap_or(0);
        let resp = String::from_utf8_lossy(&buf[..n]);
        assert!(
            resp.contains("200 OK"),
            "body sent within whole-request deadline (350ms < 1500ms) must succeed (whole-request \
             semantics, not header deadline), got: {resp:?}"
        );
        let _ = srv.join();
    }

    /// WriteTimeout=0 / None 不设写 deadline：慢写仍成功。
    #[test]
    fn write_timeout_zero_allows_slow_write() {
        use super::super::{HandlerFunc, STATUS_OK};

        let handler: Arc<dyn Handler> = Arc::new(HandlerFunc(
            |w: &mut dyn crate::httpx::ResponseWriter, _r: &crate::httpx::Request| {
                thread::sleep(Duration::from_millis(150));
                w.header().set("Content-Length", "2");
                w.write_header(STATUS_OK);
                let _ = w.write(b"ok");
            },
        ));
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().unwrap();
        let h = Arc::clone(&handler);
        let srv = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            test_serve_one_connection(
                stream,
                h,
                Some(Duration::from_secs(2)),
                Some(Duration::from_secs(1)),
                None,
            );
        });

        let mut conn = TcpStream::connect(addr).expect("connect");
        conn.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            .expect("req");
        conn.set_read_timeout(Some(Duration::from_secs(2))).ok();
        let mut resp = Vec::new();
        conn.read_to_end(&mut resp).expect("read resp");
        assert!(
            String::from_utf8_lossy(&resp).contains("200 OK"),
            "WriteTimeout=0 must not cut slow response write"
        );
        let _ = srv.join();
    }
}
