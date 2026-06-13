//! HTTP/1.1 response writer。

use std::io::{self, BufWriter, Write};

use super::{status_text, Header, ResponseWriter, STATUS_OK};

/// 具体 `ResponseWriter` 实现（内部 `BufWriter`）。
pub struct ResponseWriterImpl<W: Write> {
    bw: BufWriter<W>,
    headers: Header,
    wrote_header: bool,
    status: u16,
    chunked: bool,
}

impl<W: Write> ResponseWriterImpl<W> {
    pub fn new(inner: W) -> Self {
        Self {
            bw: BufWriter::new(inner),
            headers: Header::new(),
            wrote_header: false,
            status: 0,
            chunked: false,
        }
    }

    /// handler 返回后由 server 调用：写终止 chunk + flush。
    pub fn finish(&mut self) -> io::Result<()> {
        if !self.wrote_header {
            self.write_header(STATUS_OK);
        }
        if self.chunked {
            self.bw.write_all(b"0\r\n\r\n")?;
        }
        self.bw.flush()
    }

    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn is_chunked(&self) -> bool {
        self.chunked
    }
}

impl<W: Write> ResponseWriter for ResponseWriterImpl<W> {
    fn header(&mut self) -> &mut Header {
        &mut self.headers
    }

    fn write_header(&mut self, code: u16) {
        if self.wrote_header {
            return;
        }
        self.wrote_header = true;
        self.status = code;

        self.headers.set("Connection", "close");

        if self.headers.get("Content-Length").is_none() {
            self.headers.set("Transfer-Encoding", "chunked");
            self.chunked = true;
        }

        let reason = status_text(code);
        let reason = if reason.is_empty() { "Status" } else { reason };
        let _ = write!(self.bw, "HTTP/1.1 {code} {reason}\r\n");

        // 按 key 排序后写,保证输出确定性（`Header` 内部是 HashMap,iter 序随机会让
        // HTTP 响应 header 顺序每次不同；HTTP 语义与顺序无关、HACS 按名解析,但 byte-exact
        // golden / 跨语言 parity 需确定序）。
        let mut entries: Vec<(&String, &Vec<String>)> = self.headers.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        for (k, vals) in entries {
            for v in vals {
                let _ = write!(self.bw, "{k}: {v}\r\n");
            }
        }
        let _ = self.bw.write_all(b"\r\n");
    }

    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if !self.wrote_header {
            self.write_header(STATUS_OK);
        }
        if data.is_empty() {
            return Ok(0);
        }
        if self.chunked {
            return write_chunk(&mut self.bw, data);
        }
        self.bw.write(data)
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.wrote_header {
            self.write_header(STATUS_OK);
        }
        self.bw.flush()
    }
}

/// 写一个 chunked frame：`hex_size\r\n` + body + `\r\n`。
pub fn write_chunk<W: Write>(bw: &mut W, data: &[u8]) -> io::Result<usize> {
    write!(bw, "{:x}\r\n", data.len())?;
    bw.write_all(data)?;
    bw.write_all(b"\r\n")?;
    Ok(data.len())
}
