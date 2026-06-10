//! preview：preview 信令客户端（移植 Go `internal/video/preview.go` 的
//! PreviewClient / ValidateStartAck / ValidateStopAck）。
//!
//! req=704 start / req=708 stop 帧 builder 在 [`crate::wire18022`]（design D1：
//! 与 `build_stop_frame` 同居，共用 `assemble_frame` 与 golden 基建——本模块禁止
//! 重复帧拼装）。
//!
//! 行为等价纪律（spec「preview 信令字节与行为等价」）：
//!   - 每帧独立 TCP socket 直拨 outdoor:18022，**不经骨架 wire-worker**
//!     （授权见 `rust-daemon-skeleton` MODIFIED delta）
//!   - 超时结构等价 Go：dial 5s + 连接上独立 deadline 5s（**两段**，最坏总时长
//!     ~10s，禁合并成单一 5s 总闸）——连接段对齐 Go `conn.SetDeadline(now+5s)`
//!     的**绝对** deadline 语义（write+read 共享剩余额度）
//!   - ack 读取是**单次 read（64B buffer，不循环读、不 read-to-EOF）**：首段含
//!     magic+req 即过；read 返回 0 字节（对端 FIN）按 silent-FIN 错误分类
//!   - ack 校验 magic + req 文本匹配、长度宽松（外机不同固件 echo body 可能微差）
//!
//! 超时错误是可判别变体（[`PreviewError::Timeout`]），上层（组 F handler）据此
//! 映射 503。

use std::io::{self, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use super::LogFn;
use crate::wire18022::{build_start_frame, build_stop_frame};

/// dial 与连接 deadline 的默认值（锚 Go `previewDialTimeout` 5s；两段独立取值）。
pub const PREVIEW_DIAL_TIMEOUT: Duration = Duration::from_secs(5);

/// preview 帧 magic 高/低字节（07 b8）。
const PREVIEW_MAGIC_HI: u8 = 0x07;
const PREVIEW_MAGIC_LO: u8 = 0xb8;
/// 帧头长度（magic 2 + length 2 + reserved 2）。
const PREVIEW_HEADER_SIZE: usize = 6;
/// ack 单次 read 的 buffer 上限（锚 Go `buf := make([]byte, 64)`，不循环读）。
const ACK_READ_BUF: usize = 64;

/// preview 信令错误。
///
/// [`PreviewError::Timeout`] 是可判别哨兵——HTTP 层据此映射 503（spec 场景
/// 「preview 超时分类」）；[`PreviewError::SilentFin`] 对应 read 0 字节 + EOF
/// （外机 FIN 不回数据）。
#[derive(Debug)]
pub enum PreviewError {
    /// ack 字节级校验失败（magic / req 文本 / 长度过短）。锚 Go `ErrPreviewBadAck`。
    BadAck(String),
    /// read 返回 0 字节（对端 FIN 不回数据）。锚 Go "empty response (silent FIN)"。
    SilentFin,
    /// dial / write / read 任一阶段 deadline 击中。
    Timeout {
        /// 阶段名（"dial" / "write" / "read"）。
        stage: &'static str,
    },
    /// 其它 IO 错误。
    Io {
        /// 阶段名。
        stage: &'static str,
        /// 底层错误。
        source: io::Error,
    },
}

impl core::fmt::Display for PreviewError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PreviewError::BadAck(msg) => write!(f, "preview: ack frame invalid: {msg}"),
            PreviewError::SilentFin => write!(f, "preview: empty response (silent FIN)"),
            PreviewError::Timeout { stage } => write!(f, "{stage}: preview: socket timeout"),
            PreviewError::Io { stage, source } => write!(f, "{stage}: {source}"),
        }
    }
}

impl std::error::Error for PreviewError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PreviewError::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl PreviewError {
    /// 是否超时类错误（上层映射 503 用）。
    pub fn is_timeout(&self) -> bool {
        matches!(self, PreviewError::Timeout { .. })
    }
}

/// 把 IO 错误的超时归一化为 [`PreviewError::Timeout`]（套路同 `wire_sender::wrap_timeout`：
/// Linux SO_*TIMEO 击中返 `TimedOut`，macOS/BSD 返 `WouldBlock`，均归 Timeout）。
fn wrap_timeout(err: io::Error, stage: &'static str) -> PreviewError {
    match err.kind() {
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => PreviewError::Timeout { stage },
        _ => PreviewError::Io { stage, source: err },
    }
}

/// 校验 req=705 start ack 帧。锚 Go `ValidateStartAck`。
///
/// 仅严校 magic + req 文本，长度宽松（echo body 字段长度外机固件可能微差）。
pub fn validate_start_ack(b: &[u8]) -> Result<(), PreviewError> {
    validate_preview_ack(b, "req=705")
}

/// 校验 req=709 stop ack 帧。锚 Go `ValidateStopAck`。
pub fn validate_stop_ack(b: &[u8]) -> Result<(), PreviewError> {
    validate_preview_ack(b, "req=709")
}

fn validate_preview_ack(b: &[u8], want_text: &str) -> Result<(), PreviewError> {
    if b.len() < PREVIEW_HEADER_SIZE + want_text.len() {
        return Err(PreviewError::BadAck(format!("too short ({})", b.len())));
    }
    if b[0] != PREVIEW_MAGIC_HI || b[1] != PREVIEW_MAGIC_LO {
        return Err(PreviewError::BadAck(format!(
            "magic {:02x} {:02x} != 07b8",
            b[0], b[1]
        )));
    }
    if &b[PREVIEW_HEADER_SIZE..PREVIEW_HEADER_SIZE + want_text.len()] != want_text.as_bytes() {
        return Err(PreviewError::BadAck(format!(
            "missing {want_text:?} at offset 6"
        )));
    }
    Ok(())
}

/// preview 信令客户端：短连接 dial 18022 + 单次读 ack。锚 Go `PreviewClient`。
///
/// 设计：每帧独立 socket（与 wire_sender 一致），避免 connection 复用 race。
/// 不绑定 source IP——video 流量必须从 br-door 走，外机 IP 在 br-door 子网内，
/// OS 路由表自动选对接口；与 wire_sender 不同，不需要 SO_BINDTODEVICE。
///
/// `timeout`：test-tunable 注入口（None → 5s）。dial 与连接 deadline **各自独立**
/// 取该值（两段；组 E teardown 的 best-effort StopPreview 以 3s 构造即得 Go
/// `context.WithTimeout(3s)` 的等价裁短）。
#[derive(Default)]
pub struct PreviewClient {
    /// 可选 log hook。
    pub logf: Option<LogFn>,
    /// dial / 连接 deadline 各自的时长；`None` → [`PREVIEW_DIAL_TIMEOUT`]（5s）。
    pub timeout: Option<Duration>,
}

impl PreviewClient {
    fn logf(&self, msg: &str) {
        if let Some(f) = &self.logf {
            f(msg);
        }
    }

    /// dial outdoor:18022 发 req=704 + 等 req=705 ack。锚 Go `StartPreview`。
    pub fn start_preview(
        &self,
        ip: &str,
        port: u16,
        callee_bcd: [u8; 4],
        caller_bcd: [u8; 4],
    ) -> Result<(), PreviewError> {
        let frame = build_start_frame(callee_bcd, caller_bcd);
        self.logf(&format!("video: preview start dial {ip}:{port}"));
        let resp = self.dial_and_exchange(ip, port, &frame)?;
        validate_start_ack(&resp)?;
        self.logf(&format!(
            "video: preview start ack ok ({} bytes)",
            resp.len()
        ));
        Ok(())
    }

    /// dial outdoor:18022 发 req=708 + 等 req=709 ack。锚 Go `StopPreview`。
    ///
    /// best-effort：失败让 Manager teardown 决定是否 fatal（一般不 fatal）。
    pub fn stop_preview(
        &self,
        ip: &str,
        port: u16,
        outdoor_bcd: [u8; 4],
        monitor_bcd: [u8; 4],
    ) -> Result<(), PreviewError> {
        let frame = build_stop_frame(outdoor_bcd, monitor_bcd);
        self.logf(&format!("video: preview stop dial {ip}:{port}"));
        let resp = self.dial_and_exchange(ip, port, &frame)?;
        validate_stop_ack(&resp)?;
        self.logf(&format!(
            "video: preview stop ack ok ({} bytes)",
            resp.len()
        ));
        Ok(())
    }

    /// dial + write 帧 + **单次** read ack。锚 Go `dialAndExchange`。
    ///
    /// 超时两段：dial 独立 `timeout`；连接建立后取**绝对** deadline = now + `timeout`
    /// （对齐 Go `conn.SetDeadline`，write 与 read 共享剩余额度）。
    fn dial_and_exchange(
        &self,
        ip: &str,
        port: u16,
        frame: &[u8],
    ) -> Result<Vec<u8>, PreviewError> {
        let timeout = self.timeout.unwrap_or(PREVIEW_DIAL_TIMEOUT);
        let addr: IpAddr = ip.parse().map_err(|_| PreviewError::Io {
            stage: "dial",
            source: io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid target IP {ip}"),
            ),
        })?;
        let target = SocketAddr::new(addr, port);

        // 第一段：dial 独立 5s（锚 Go net.Dialer{Timeout: 5s}）。
        let mut stream =
            TcpStream::connect_timeout(&target, timeout).map_err(|e| wrap_timeout(e, "dial"))?;

        // 第二段：连接上独立绝对 deadline（锚 Go conn.SetDeadline(now+5s)）。
        let deadline = Instant::now() + timeout;

        let remaining = deadline.saturating_duration_since(Instant::now());
        stream
            .set_write_timeout(Some(remaining.max(Duration::from_millis(1))))
            .map_err(|e| PreviewError::Io {
                stage: "set_write_timeout",
                source: e,
            })?;
        stream
            .write_all(frame)
            .map_err(|e| wrap_timeout(e, "write"))?;

        // 单次 read（64B buf 不循环、不 read-to-EOF）：首段含 magic+req 即足。
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(PreviewError::Timeout { stage: "read" });
        }
        stream
            .set_read_timeout(Some(remaining))
            .map_err(|e| PreviewError::Io {
                stage: "set_read_timeout",
                source: e,
            })?;
        let mut buf = [0u8; ACK_READ_BUF];
        let n = stream.read(&mut buf).map_err(|e| wrap_timeout(e, "read"))?;
        if n == 0 {
            // std read Ok(0) = EOF（对端 FIN 不回数据）→ silent FIN 分类。
            // 锚 Go：`if n == 0 { if err == nil || err == io.EOF { silent FIN } }`。
            return Err(PreviewError::SilentFin);
        }
        Ok(buf[..n].to_vec())
    }
}

// ── 单测（mock 18022 server；移植 Go preview_test.go + 任务点名用例）─────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    fn hex(s: &str) -> Vec<u8> {
        assert!(s.len().is_multiple_of(2), "odd hex: {s:?}");
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// req=705 start ack 典型样本（golden video_wire.txt `ack_ok|start|typical`）。
    fn start_ack_typical() -> Vec<u8> {
        hex("07b8180000007265713d3730352671756572792a00008000000002000001")
    }

    /// req=709 stop ack 典型样本。
    fn stop_ack_typical() -> Vec<u8> {
        hex("07b8180000007265713d3730392671756572792a00008000000002000001")
    }

    // --- validate（移植 Go TestValidateStartAck_* / TestValidateStopAck_*）---

    #[test]
    fn validate_start_ack_accepts() {
        validate_start_ack(&start_ack_typical()).expect("valid ack rejected");
        // 长度宽松：短 ack（仅 header + req 文本）同样接受。
        validate_start_ack(&hex("07b8070000007265713d373035")).expect("short lenient ack");
    }

    #[test]
    fn validate_start_ack_rejects_wrong_magic() {
        let ack = hex("0000180000007265713d3730352671756572792a0000");
        assert!(matches!(
            validate_start_ack(&ack),
            Err(PreviewError::BadAck(_))
        ));
    }

    #[test]
    fn validate_start_ack_rejects_wrong_req() {
        // req=704 not 705。
        let ack = hex("07b8180000007265713d3730342671756572792a0000");
        assert!(validate_start_ack(&ack).is_err(), "wrong req accepted");
    }

    #[test]
    fn validate_start_ack_rejects_too_short() {
        assert!(validate_start_ack(&hex("07b818000000726571")).is_err());
    }

    #[test]
    fn validate_stop_ack_accepts() {
        validate_stop_ack(&stop_ack_typical()).expect("valid stop ack rejected");
    }

    #[test]
    fn validate_stop_ack_rejects_start_ack() {
        assert!(validate_stop_ack(&start_ack_typical()).is_err());
    }

    // --- PreviewClient mock 18022 server 行为用例 ---

    /// 起单连接 mock 外机 server，handler 收 conn 自由处置；返回 (ip, port)。
    fn mock_server<F>(handler: F) -> (String, u16)
    where
        F: FnOnce(TcpStream) + Send + 'static,
    {
        let ln = TcpListener::bind("127.0.0.1:0").expect("listen");
        let addr = ln.local_addr().unwrap();
        thread::spawn(move || {
            if let Ok((conn, _)) = ln.accept() {
                handler(conn);
            }
        });
        (addr.ip().to_string(), addr.port())
    }

    /// 成功路径（移植 Go TestPreviewClient_RoundTrip）：mock 收 req=704 帧回 705 ack。
    #[test]
    fn start_preview_round_trip() {
        let (frame_tx, frame_rx) = mpsc::channel::<Vec<u8>>();
        let (ip, port) = mock_server(move |mut conn| {
            let mut buf = [0u8; 64];
            let n = conn.read(&mut buf).unwrap_or(0);
            let _ = frame_tx.send(buf[..n].to_vec());
            let _ = conn.write_all(&start_ack_typical());
        });

        let c = PreviewClient {
            timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        };
        c.start_preview(
            &ip,
            port,
            [0x06, 0x02, 0x00, 0x00],
            [0x06, 0x02, 0x11, 0x03],
        )
        .expect("start_preview");

        let f = frame_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("mock server did not receive frame");
        assert_eq!(f.len(), 36, "recv frame len");
        assert_eq!(&f[0..2], &[0x07, 0xb8], "magic");
        assert!(
            f[6..20].windows(7).any(|w| w == b"req=704"),
            "wrong req in frame"
        );
    }

    /// stop 成功路径：mock 回 709 ack。
    #[test]
    fn stop_preview_round_trip() {
        let (ip, port) = mock_server(move |mut conn| {
            let mut buf = [0u8; 64];
            let _ = conn.read(&mut buf);
            let _ = conn.write_all(&stop_ack_typical());
        });
        let c = PreviewClient {
            timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        };
        c.stop_preview(
            &ip,
            port,
            [0x06, 0x02, 0x00, 0x00],
            [0x06, 0x02, 0x11, 0x03],
        )
        .expect("stop_preview");
    }

    /// 超时路径：mock accept 后不读不回 → read deadline 击中 → Timeout（可判别，
    /// 上层映射 503）。tunable 压缩到 150ms，禁真实 5s。
    #[test]
    fn start_preview_read_timeout() {
        let (done_tx, done_rx) = mpsc::channel::<()>();
        let (ip, port) = mock_server(move |conn| {
            // 持连接到测试结束（不回数据、不 FIN）。
            let _ = done_rx.recv_timeout(Duration::from_secs(5));
            drop(conn);
        });
        let c = PreviewClient {
            timeout: Some(Duration::from_millis(150)),
            ..Default::default()
        };
        let t0 = Instant::now();
        let err = c
            .start_preview(
                &ip,
                port,
                [0x06, 0x02, 0x00, 0x00],
                [0x06, 0x02, 0x11, 0x03],
            )
            .expect_err("expected timeout");
        assert!(err.is_timeout(), "err = {err:?}, want Timeout");
        assert!(
            t0.elapsed() < Duration::from_secs(2),
            "timeout should fire promptly, took {:?}",
            t0.elapsed()
        );
        let _ = done_tx.send(());
    }

    /// ack 不匹配：mock 回 wrong-req ack → BadAck（且 Timeout 判别为 false）。
    #[test]
    fn start_preview_ack_mismatch() {
        let (ip, port) = mock_server(move |mut conn| {
            let mut buf = [0u8; 64];
            let _ = conn.read(&mut buf);
            // 回 stop ack（req=709）冒充 start ack → 校验必须拒绝。
            let _ = conn.write_all(&stop_ack_typical());
        });
        let c = PreviewClient {
            timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        };
        let err = c
            .start_preview(
                &ip,
                port,
                [0x06, 0x02, 0x00, 0x00],
                [0x06, 0x02, 0x11, 0x03],
            )
            .expect_err("expected BadAck");
        assert!(
            matches!(err, PreviewError::BadAck(_)),
            "err = {err:?}, want BadAck"
        );
        assert!(!err.is_timeout());
    }

    /// FIN 不回数据：mock 读完帧直接 close → 单次 read Ok(0) → SilentFin。
    #[test]
    fn start_preview_silent_fin() {
        let (ip, port) = mock_server(move |mut conn| {
            let mut buf = [0u8; 64];
            let _ = conn.read(&mut buf);
            // handler 退出 → conn drop → FIN，无响应 body。
        });
        let c = PreviewClient {
            timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        };
        let err = c
            .start_preview(
                &ip,
                port,
                [0x06, 0x02, 0x00, 0x00],
                [0x06, 0x02, 0x11, 0x03],
            )
            .expect_err("expected SilentFin");
        assert!(
            matches!(err, PreviewError::SilentFin),
            "err = {err:?}, want SilentFin"
        );
    }

    /// dial 失败（移植 Go TestPreviewClient_DialFailReturnsError）：关死端口 → 错误
    /// （refused 是 Io，非 BadAck/SilentFin）。
    #[test]
    fn start_preview_dial_fail() {
        let ln = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = ln.local_addr().unwrap().port();
        drop(ln);
        let c = PreviewClient {
            timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        };
        let err = c
            .start_preview(
                "127.0.0.1",
                port,
                [0x06, 0x02, 0x00, 0x00],
                [0x06, 0x02, 0x11, 0x03],
            )
            .expect_err("expected dial error to closed port");
        assert!(
            !matches!(err, PreviewError::BadAck(_) | PreviewError::SilentFin),
            "dial fail must not be BadAck/SilentFin, got {err:?}"
        );
    }

    /// 慢速分段送达：单次 read 读到多少算多少——首段含 magic+req 即过（spec 场景
    /// 「ack 单次 read 语义」）。mock 先送含 req=705 的首段，停顿后再送剩余字节；
    /// 客户端不等第二段。
    #[test]
    fn start_preview_single_read_first_segment_suffices() {
        let full = start_ack_typical();
        let (ip, port) = mock_server(move |mut conn| {
            let mut buf = [0u8; 64];
            let _ = conn.read(&mut buf);
            // 首段：header + "req=705"（13B，已过校验门槛）。
            let _ = conn.write_all(&full[..13]);
            // 故意停顿后补第二段——客户端单次 read 早已返回。
            thread::sleep(Duration::from_millis(300));
            let _ = conn.write_all(&full[13..]);
        });
        let c = PreviewClient {
            timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        };
        let t0 = Instant::now();
        c.start_preview(
            &ip,
            port,
            [0x06, 0x02, 0x00, 0x00],
            [0x06, 0x02, 0x11, 0x03],
        )
        .expect("first segment with magic+req must pass");
        assert!(
            t0.elapsed() < Duration::from_millis(250),
            "single read must not wait for second segment, took {:?}",
            t0.elapsed()
        );
    }
}
