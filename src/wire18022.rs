//! wire18022：构造与解析安居宝 TCP 18022 协议的 5 个 wire 模板。
//!
//!   - req=705 preview（30B 全固定）
//!   - req=710 unlock-A（36B + caller/callee BCD 替换）
//!   - req=518 标准 unlock-B（30B + caller/callee BCD 替换 + 校验和）
//!   - req=708 bye（36B + caller/callee BCD 替换）
//!   - req=518 变体 appoint（29B + from BCD 后 2 字节替换）
//!
//! Go 参考实现：`dooraccess-go/internal/wire18022/frames.go`，逐字节对齐。
//! 任何模板字节漂移都会导致外机静默 / FIN —— golden 向量逐字节回归。
//!
//! 帧头 length 字段是**小端**（与 Go `binary.LittleEndian` 一致），即使部署目标
//! MIPS24Kc 是大端 —— wire 格式固定，不随 host 字节序变化。

/// 通用帧头 magic 高字节。
pub const MAGIC_HI: u8 = 0x07;
/// 通用帧头 magic 低字节。
pub const MAGIC_LO: u8 = 0xb8;
/// 帧头长度：magic(2) + length(2) + reserved(2)。
pub const HEADER_SIZE: usize = 6;

/// req=518 标准帧的校验和种子：`'m'` = 0x6d。
///
///   checksum = ('m' + sum(frame[0..29])) & 0xff
const CHECKSUM_SEED: u32 = 0x6d; // 'm'

/// 响应帧解析失败的错误类型。
///
/// 对应 Go sentinel `ErrFrameMalformed`。按 design D1，错误**分类**（sentinel 级）
/// 须与 Go 对应，但 message 字面只要求语义等价 —— `reason` 仅供调试/诊断，
/// 不构成 parity 断言面。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameError {
    /// 人类可读的失败原因（语义等价于 Go 的 `fmt.Errorf` 上下文，非字面 parity）。
    pub reason: &'static str,
}

impl FrameError {
    fn new(reason: &'static str) -> Self {
        FrameError { reason }
    }
}

impl core::fmt::Display for FrameError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "wire18022: malformed frame: {}", self.reason)
    }
}

impl std::error::Error for FrameError {}

// ── 响应帧固定 body 模板（ValidateResponse 用）──────────────────────────────
//
// 验证响应除了对 req 编号还要查 body 内容，避免外机伪造或半坏的响应骗过状态机。

/// req=711 OK ack body（10 字节）。
///
/// v0.3.3 fix-doorbell-pipeline 实测：byte 0 = `0x00`（**不是**反编译残留注释的 `0x2a`）。
/// SoT = Go `frames.go` 变量 `resp711Body` + `samples/anjubao-doorbell/observations.md §4`。
/// 见 `testdata/golden/EXCEPTIONS.md §wire②`。
const RESP_711_BODY: [u8; 10] = [0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00];

/// req=519 OK ack body（10 字节全 0，unlock-B / appoint 共用响应）。
const RESP_519_BODY: [u8; 10] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

// ── 帧构造 ──────────────────────────────────────────────────────────────────

/// 拼装通用帧头 + body。length 字段 = `body.len()`（小端）。
fn assemble_frame(body: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(HEADER_SIZE + body.len());
    frame.push(MAGIC_HI);
    frame.push(MAGIC_LO);
    frame.extend_from_slice(&(body.len() as u16).to_le_bytes());
    frame.push(0x00); // reserved
    frame.push(0x00); // reserved
    frame.extend_from_slice(body);
    frame
}

/// 构造 req=705 preview 帧（30B 全固定模板）。
///
/// 无 BCD 替换、无校验和、无 caller/callee 字段。
pub fn build_preview_frame() -> Vec<u8> {
    let body: [u8; 24] = [
        b'r', b'e', b'q', b'=', b'7', b'0', b'5', b'&', b'q', b'u', b'e', b'r', b'y', b'=', 0x00,
        0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x01,
    ];
    assemble_frame(&body)
}

/// 构造 req=710 unlock-A 帧（36B）。
///
/// 字节布局（body 内 offset）：
///   - 14-17: caller BCD = `from_bcd`
///   - 18-21: callee BCD = `to_bcd`
///   - 22-29: 8B 固定尾 `01 00 00 00 03 00 00 00`
///
/// 调用方负责决定 from/to 方向（HA POST 路径 from=室内机 to=外机；
/// ring→unlock automation 路径 from=外机 to=室内机）。
pub fn build_unlock_a_frame(from_bcd: [u8; 4], to_bcd: [u8; 4]) -> Vec<u8> {
    let mut body = Vec::with_capacity(30);
    body.extend_from_slice(b"req=710&query=");
    body.extend_from_slice(&from_bcd);
    body.extend_from_slice(&to_bcd);
    body.extend_from_slice(&[0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00]);
    assemble_frame(&body)
}

/// 构造 req=518 标准帧（30B + 校验和）。
///
/// 字节布局（整帧 offset）：
///   - 0-5:   `07 b8 18 00 00 00`（length = 24）
///   - 6-19:  `"req=518&query="`（14 ascii）
///   - 20:    op = `0x22`
///   - 21-24: caller BCD = `from_bcd`
///   - 25-28: callee BCD = `to_bcd`
///   - 29:    checksum = `('m' + sum(frame[0..29])) & 0xff`
///
/// length 字段须**先填入**再纳入 sum（对照 golden checksum 行）。
pub fn build_unlock_b_frame(from_bcd: [u8; 4], to_bcd: [u8; 4]) -> Vec<u8> {
    let mut frame = vec![0u8; 30];
    frame[0] = MAGIC_HI;
    frame[1] = MAGIC_LO;
    // length = 24（小端），须先填好才能纳入 checksum sum。
    let len = 24u16.to_le_bytes();
    frame[2] = len[0];
    frame[3] = len[1];
    // bytes 4-5 已经是 0x00 0x00 (reserved)
    frame[6..20].copy_from_slice(b"req=518&query=");
    frame[20] = 0x22;
    frame[21..25].copy_from_slice(&from_bcd);
    frame[25..29].copy_from_slice(&to_bcd);
    frame[29] = unlock_b_checksum_of(&frame);
    frame
}

/// 计算 req=518 标准帧的校验和字节（offset 29）。
///
/// `('m' + sum(frame[0..29])) & 0xff`，length 字段须已填入 `frame[2..4]`。
fn unlock_b_checksum_of(frame: &[u8]) -> u8 {
    let mut sum: u32 = 0;
    for &b in &frame[..29] {
        sum += b as u32;
    }
    ((sum + CHECKSUM_SEED) & 0xff) as u8
}

/// 计算给定 caller/callee BCD 的 req=518 标准帧校验和字节（offset 29）。
///
/// 与 [`build_unlock_b_frame`] 内部一致；供 golden checksum 逐 case 断言用。
pub fn unlock_b_checksum(from_bcd: [u8; 4], to_bcd: [u8; 4]) -> u8 {
    let frame = build_unlock_b_frame(from_bcd, to_bcd);
    frame[29]
}

/// 构造 req=708 bye 帧（36B，与 req=710 同形）。
///
/// 注意：bye 的 caller=callee=室内机号"自指"语义，调用方应传
/// `from_bcd == to_bcd` = 室内机 BCD。
pub fn build_bye_frame(from_bcd: [u8; 4], to_bcd: [u8; 4]) -> Vec<u8> {
    let mut body = Vec::with_capacity(30);
    body.extend_from_slice(b"req=708&query=");
    body.extend_from_slice(&from_bcd);
    body.extend_from_slice(&to_bcd);
    body.extend_from_slice(&[0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00]);
    assemble_frame(&body)
}

/// 构造 req=518 变体 appoint 帧（29B）。
///
/// 字节布局（整帧 offset）：
///   - 0-5:   `07 b8 17 00 00 00`（length = 23）
///   - 6-19:  `"req=518&query="`（14 ascii）
///   - 20:    op = `0xed`（区别于 standard 0x22）
///   - 21-24: `00 01 00 00`（4B 固定）
///   - 25:    `from_bcd[2]`
///   - 26:    `from_bcd[3]`
///   - 27-28: `00 07`（2B 固定尾）
///
/// **无校验和**（与 standard unlock-B 区分）。fromBCD 是完整 4 字节伪 BCD，
/// 帧只用末 2 字节（`from_bcd[2..4]`）。
pub fn build_appoint_frame(from_bcd: [u8; 4]) -> Vec<u8> {
    let mut body = Vec::with_capacity(23);
    body.extend_from_slice(b"req=518&query=");
    body.push(0xed);
    body.extend_from_slice(&[0x00, 0x01, 0x00, 0x00]);
    body.push(from_bcd[2]);
    body.push(from_bcd[3]);
    body.extend_from_slice(&[0x00, 0x07]);
    assemble_frame(&body)
}

// ── 响应帧解析 ──────────────────────────────────────────────────────────────

/// 解析 18022 响应帧的通用 0x07b8 结构。
///
/// 返回 `(req, body)`：
///   - `req`: ASCII 解析 `"req=NNN"` 的 NNN 整数（如 711 / 519）
///   - `body`: `"&query="` 之后的二进制 payload（不含 magic/length/reserved/req= 前缀）
///
/// 帧不完整 / magic 错 / reserved 错 / length 不匹配 / 缺 `"req="` 或 `"&query[=*]"`
/// 时返 [`FrameError`]。
///
/// **双分隔符**（v0.3.3 实测例外，见 `EXCEPTIONS.md §wire①`）：同时接受 `&query=`
/// 与 `&query*`（外机 req=705/709/711 ack 实测用 `*` = 0x2a）。仅认 `&query=`
/// 会重踩"unlock-A OK ack 解析失败 → daemon 报 unexpected response"。
pub fn parse_response_frame(b: &[u8]) -> Result<(i64, Vec<u8>), FrameError> {
    if b.len() < HEADER_SIZE {
        return Err(FrameError::new("frame too short"));
    }
    if b[0] != MAGIC_HI || b[1] != MAGIC_LO {
        return Err(FrameError::new("bad magic, want 07 b8"));
    }
    if b[4] != 0 || b[5] != 0 {
        return Err(FrameError::new("reserved not 00 00"));
    }
    let length = u16::from_le_bytes([b[2], b[3]]) as usize;
    if b.len() - HEADER_SIZE < length {
        return Err(FrameError::new("declared length > available body"));
    }

    let frame_body = &b[HEADER_SIZE..HEADER_SIZE + length];

    const REQ_PREFIX: &[u8] = b"req=";
    // 两 sep 等长：len("&query=") == len("&query*") == 7。
    const QUERY_SEP_LEN: usize = 7;

    if frame_body.len() < REQ_PREFIX.len() + 1 + QUERY_SEP_LEN {
        return Err(FrameError::new("body too short for req=N&query?"));
    }
    if &frame_body[..REQ_PREFIX.len()] != REQ_PREFIX {
        return Err(FrameError::new("body lacks 'req=' prefix"));
    }

    let mut sep_idx: Option<usize> = None;
    let mut i = REQ_PREFIX.len();
    while i + QUERY_SEP_LEN <= frame_body.len() {
        let s = &frame_body[i..i + QUERY_SEP_LEN];
        if s == b"&query=" || s == b"&query*" {
            sep_idx = Some(i);
            break;
        }
        i += 1;
    }
    let sep_idx = match sep_idx {
        Some(idx) => idx,
        None => return Err(FrameError::new("body lacks '&query[=*]' separator")),
    };

    let req_bytes = &frame_body[REQ_PREFIX.len()..sep_idx];
    let req = parse_req_num(req_bytes)?;

    let body = frame_body[sep_idx + QUERY_SEP_LEN..].to_vec();
    Ok((req, body))
}

/// 严格解析 req 编号 ASCII：纯数字、长度 1..=10、值须落在平台 `int` 范围内。
///
/// **int 宽度对齐（部署目标 GOARCH=mips → int32）**：Go `parseReqNum` 末尾用
/// `strconv.Atoi`，其返回平台 `int`。hAP QCA9533 是 32-bit，故 Go-MIPS 上 Atoi 对
/// `> i32::MAX`（如 10 位 `3000000000`）**返错 → 帧被拒**。Rust 累加用 `i64` 不会自然
/// 拒，故显式加 `> i32::MAX` 检查对齐 Go-MIPS（10 位 ≤ `i64` 不溢出，安全累加后再判范围）。
fn parse_req_num(s: &[u8]) -> Result<i64, FrameError> {
    if s.is_empty() {
        return Err(FrameError::new("empty req number"));
    }
    if s.len() > 10 {
        return Err(FrameError::new("req number too long"));
    }
    let mut n: i64 = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            return Err(FrameError::new("non-digit in req number"));
        }
        n = n * 10 + (c - b'0') as i64;
    }
    // 对齐 Go-MIPS strconv.Atoi(int=int32)：超 int32 范围拒（10 位可达 9_999_999_999 > i32::MAX）。
    if n > i32::MAX as i64 {
        return Err(FrameError::new("req number out of platform int range"));
    }
    Ok(n)
}

/// 严格校验响应：`req` 必须等于 `want_req`，且 body 必须等于该 req 的标准模板。
///
/// 比 [`parse_response_frame`] 仅看 req 更严：
///   - 711 必须 byte 0 = `0x00` + 标准 10 字节模板（[`RESP_711_BODY`]，v0.3.3 实测）
///   - 519 必须全 10 字节 0 body（[`RESP_519_BODY`]）
///   - 其它 req 编号未在 spec 规定 body 模板，仅 req 校验通过即 true
///
/// 任一不匹配返 `false`。
pub fn validate_response(resp: &[u8], want_req: i64) -> bool {
    let (req, body) = match parse_response_frame(resp) {
        Ok(v) => v,
        Err(_) => return false,
    };
    if req != want_req {
        return false;
    }
    match want_req {
        711 => body.len() == RESP_711_BODY.len() && body == RESP_711_BODY,
        519 => body.len() == RESP_519_BODY.len() && body == RESP_519_BODY,
        // 其它 req 编号未在 spec 中规定 body 模板，仅 req 校验。
        _ => true,
    }
}
