//! 绝对 deadline → per-syscall `read`/`write` timeout 换算（Rust socket 无 `SetDeadline`）。

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

pub fn apply_read_deadline(stream: &TcpStream, deadline: Option<Instant>) -> io::Result<()> {
    match deadline {
        None => stream.set_read_timeout(None),
        Some(d) => {
            let now = Instant::now();
            if now >= d {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "httpx: read deadline exceeded",
                ));
            }
            let remaining = (d - now).max(Duration::from_millis(1));
            stream.set_read_timeout(Some(remaining))
        }
    }
}

pub fn apply_write_deadline(stream: &TcpStream, deadline: Option<Instant>) -> io::Result<()> {
    match deadline {
        None => stream.set_write_timeout(None),
        Some(d) => {
            let now = Instant::now();
            if now >= d {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "httpx: write deadline exceeded",
                ));
            }
            let remaining = (d - now).max(Duration::from_millis(1));
            stream.set_write_timeout(Some(remaining))
        }
    }
}

/// 包装 `TcpStream`：每次 read/write 前按绝对 deadline 刷新 per-syscall timeout。
pub struct AbsoluteDeadlineStream {
    inner: TcpStream,
    deadline: Option<Instant>,
}

impl AbsoluteDeadlineStream {
    pub fn new(inner: TcpStream, deadline: Option<Instant>) -> Self {
        Self { inner, deadline }
    }

    pub fn inner_mut(&mut self) -> &mut TcpStream {
        &mut self.inner
    }
}

impl Read for AbsoluteDeadlineStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        apply_read_deadline(&self.inner, self.deadline)?;
        self.inner.read(buf)
    }
}

impl Write for AbsoluteDeadlineStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        apply_write_deadline(&self.inner, self.deadline)?;
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        apply_write_deadline(&self.inner, self.deadline)?;
        self.inner.flush()
    }
}
