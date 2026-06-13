//! codec 模块：安居宝协议字段级编解码。
//!
//! 三块（均对 committed golden 字节逐字节回归）：
//!   - 伪 BCD 编解码
//!   - SIP URI 拆分（含 IPv4 收紧）
//!   - result 码常量集
//!
//! wire/输出逐字节精确，错误以 sentinel 级分类（见各错误枚举），
//! 错误 message 仅作诊断、不构成 parity 断言面。

use std::net::Ipv4Addr;
use std::str::FromStr;

// ---------------------------------------------------------------------------
// 伪 BCD 编解码
// ---------------------------------------------------------------------------

/// 伪 BCD 编码错误（两个 sentinel：长度错 / 格式错）。
///
/// 携带的动态上下文（got / ch / index）仅供诊断。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BcdError {
    /// 号码串长度不是 8。
    Length { got: usize },
    /// 号码串含非 hex 字符。
    Format { ch: char, index: usize },
}

/// 把 8 位号码字符串编码为 4 字节伪 BCD。
///
/// 编码规则：拆 4 个 2 字符 hex pair，每对当一字节看（伪 BCD：`"06"` → `0x06` 非 `0x60`）。
/// 接受 `'0'-'9' / 'a'-'f' / 'A'-'F'`（按 base-16 解析每个 nibble）。
pub fn encode_bcd(num_str: &str) -> Result<[u8; 4], BcdError> {
    let bytes = num_str.as_bytes();
    if bytes.len() != 8 {
        return Err(BcdError::Length { got: bytes.len() });
    }
    let mut out = [0u8; 4];
    for i in 0..4 {
        let hi = hex_nibble(bytes[i * 2]).ok_or(BcdError::Format {
            ch: bytes[i * 2] as char,
            index: i * 2,
        })?;
        let lo = hex_nibble(bytes[i * 2 + 1]).ok_or(BcdError::Format {
            ch: bytes[i * 2 + 1] as char,
            index: i * 2 + 1,
        })?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

/// 反向：4 字节 → 8 位小写 hex 字符串（每字节 `%02x` 拼接）。
pub fn decode_bcd(b: [u8; 4]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0u8; 8];
    for (i, &v) in b.iter().enumerate() {
        out[i * 2] = HEX[(v >> 4) as usize];
        out[i * 2 + 1] = HEX[(v & 0x0f) as usize];
    }
    // 全部为 ASCII hex，构造合法 UTF-8。
    String::from_utf8(out.to_vec()).expect("bcd hex output is ASCII")
}

/// 把单个 ASCII hex 字符转 0-15；非 hex 返回 `None`。
fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// SIP URI 拆分（含 IPv4 收紧）
// ---------------------------------------------------------------------------

/// SIP URI 解析错误（四个 sentinel：格式 / name 长度 / IPv4 / port）。
///
/// "缺 @" 与 "缺 :port" 两种格式错统一归到 `Format` 一个 sentinel，不再细分子类。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UriError {
    /// 整体格式错（缺 `@` / 缺 `:port` 等）。
    Format,
    /// 名不是恰好 8 字符。
    NameLen { got: usize },
    /// IP 不是规范点分十进制 IPv4。
    IPv4Only,
    /// 端口不是 1-65535 整数。
    Port,
}

/// 拆分 `name@ip:port` 形式的 SIP URI。
///
/// anjubao 模式严格 8 位号码 + IPv4 + 整数端口（1-65535）。
/// IP 用 `std::net::Ipv4Addr`：只接受规范点分十进制，拒前导零（`01.2.3.4`）、
/// 拒 IPv4-mapped IPv6（`::ffff:1.2.3.4`）。
///
/// 返回的 `ip` 是原始子串（未做规范化）。检查顺序：
/// 格式 → name 长度 → IPv4 → port。
pub fn parse_uri(uri: &str) -> Result<(String, String, u16), UriError> {
    let at = match uri.find('@') {
        Some(i) if i > 0 && i != uri.len() - 1 => i,
        _ => return Err(UriError::Format),
    };
    let name = &uri[..at];
    let ip_port = &uri[at + 1..];

    let colon = match ip_port.rfind(':') {
        Some(i) if i > 0 && i != ip_port.len() - 1 => i,
        _ => return Err(UriError::Format),
    };
    let ip = &ip_port[..colon];
    let port_str = &ip_port[colon + 1..];

    if name.len() != 8 {
        return Err(UriError::NameLen { got: name.len() });
    }

    // 规范点分十进制 IPv4 永不含 ':'，显式拒含冒号输入（v6-mapped）+ Ipv4Addr 拒前导零。
    if ip.contains(':') || Ipv4Addr::from_str(ip).is_err() {
        return Err(UriError::IPv4Only);
    }

    // 以 i64 解析后做 1<=p<=65535 范围检查（"-1"/"65536"/"abc" 均归 Port）。
    let port = match port_str.parse::<i64>() {
        Ok(p) if (1..=65535).contains(&p) => p as u16,
        _ => return Err(UriError::Port),
    };

    Ok((name.to_string(), ip.to_string(), port))
}

// ---------------------------------------------------------------------------
// result 码常量集
// ---------------------------------------------------------------------------

/// doorlink 内部 result code（int32 范围）。
///
/// 无 result→中文消息表：daemon 响应 body 是 result int 十进制字符串，
/// i18n 是 HA 端责任。本模块只暴露命名常量，不引入消息表。
pub mod result {
    pub const OK: i32 = 0;
    pub const NO_CHANGE: i32 = 1;
    pub const PARTIAL: i32 = 7;
    pub const NOT_RUN: i32 = 8;
    pub const ERR: i32 = -1;
    pub const INVALID: i32 = -2;
    pub const TIMEOUT: i32 = -5;
    pub const NOT_FOUND: i32 = -6;
    pub const UNSUPPORT: i32 = -8;
    pub const LEN_LIMIT: i32 = -9;
    pub const NO_CONFIG: i32 = -100;
    pub const LOGIN_FAIL: i32 = -101;
    pub const PCAP_FAIL: i32 = -102;
    pub const NO_RING: i32 = -103;
    pub const FILE_READ: i32 = -302;
    pub const FILE_WRITE: i32 = -303;
    pub const IP_UNREACH: i32 = 1100;
    pub const IP_OK: i32 = 1101;
    pub const IP_BUSY: i32 = 1102;
    pub const IP_CONFLICT: i32 = 1103;
    pub const NET_ERR: i32 = 1001;
    pub const CONN_FAIL: i32 = 1002;
    pub const CONN_TIME: i32 = 1003;
    pub const DNS_FAIL: i32 = 1004;
    pub const TLS_ERR: i32 = 1005;
    pub const NET_OK: i32 = 2000;
    pub const NO_RETURN: i32 = 2001;
    pub const AUTH_ERR: i32 = 4000;
    pub const AUTH_FAIL: i32 = 4001;
    pub const AUTH_DENY: i32 = 4002;
}
