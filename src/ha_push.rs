//! HA 反向 push client（移植 Go `internal/hapush`）。

use std::collections::BTreeMap;
use std::fmt;
use std::net::Ipv4Addr;
use std::time::Duration;

use crate::config::Config;
use crate::httpx::json::{encode_map, JsonOptions, JsonValue};
use crate::httpx::{new_request, Client, METHOD_POST, STATUS_OK};

/// 日志回调（line-based）。抽 type alias 避免 clippy::type_complexity。
pub type LogFn = Box<dyn Fn(&str) + Send + Sync>;

/// `hass.api` 为空时静默跳过（非硬错误）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotConfiguredError;

impl fmt::Display for NotConfiguredError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("hapush: HA api not configured (handshake pending)")
    }
}

impl std::error::Error for NotConfiguredError {}

/// 反向 push HTTP 客户端（对齐 Go `hapush.Client`）。
pub struct HaPushClient {
    cfg: Config,
    http: Client,
    logf: Option<LogFn>,
}

impl HaPushClient {
    /// 用默认 5s timeout 构造 Client。
    pub fn new(cfg: Config, logf: Option<LogFn>) -> Self {
        Self {
            cfg,
            http: Client {
                timeout: Some(Duration::from_secs(5)),
            },
            logf,
        }
    }

    fn logf(&self, msg: &str) {
        if let Some(f) = &self.logf {
            f(msg);
        }
    }

    /// 同步发一次反向 push（JSON body + Bearer auth）。
    pub fn push(&self, event: &str, fields: &BTreeMap<String, String>) -> Result<(), PushError> {
        if self.cfg.hass.api.is_empty() {
            self.logf("HA Push skipped: api not configured");
            return Err(PushError::NotConfigured(NotConfiguredError));
        }

        let mut body = BTreeMap::new();
        body.insert("event".into(), JsonValue::String(event.to_string()));
        for (k, v) in fields {
            body.insert(k.clone(), JsonValue::String(v.clone()));
        }

        let body_bytes = encode_map(&body, JsonOptions::MARSHAL);
        self.logf(&format!(
            "HA Push: {}",
            String::from_utf8_lossy(&body_bytes)
        ));

        let target = build_url(
            &self.cfg.hass.ipaddr,
            self.cfg.hass.port,
            &self.cfg.hass.api,
        );
        let mut req =
            new_request(METHOD_POST, &target, Some(&body_bytes)).map_err(PushError::Request)?;
        req.set_header("Content-Type", "application/json");
        req.set_header("Authorization", &format!("Bearer {}", self.cfg.hass.token));

        let resp = self.http.do_request(&req).map_err(|e| {
            self.logf(&format!("HA Push failed: {e}"));
            PushError::Do(e)
        })?;

        if resp.status_code != STATUS_OK {
            self.logf(&format!(
                "HA Push got status {} (event={event})",
                resp.status_code
            ));
            return Err(PushError::Status(resp.status_code));
        }
        Ok(())
    }

    /// 启动诊断 push（body 字母序基线：event, video_format, video_forward, wwan）。
    pub fn push_diagnosis(&self) -> Result<(), PushError> {
        let video_forward = if self.cfg.video.forward { "1" } else { "0" };
        let mut wwan = discover_ha_facing_ip(&self.cfg.hass.ipaddr, self.cfg.hass.port);
        if wwan.is_empty() {
            wwan = lookup_wwan(&self.cfg.iface);
        }
        let mut fields = BTreeMap::new();
        fields.insert("video_forward".into(), video_forward.to_string());
        fields.insert("video_format".into(), self.cfg.video.format.clone());
        fields.insert("wwan".into(), wwan);
        self.push("diagnosis", &fields)
    }

    /// 通过「假装连 HA」找出 OS 选的源 IPv4（家庭网卡 IP）。
    pub fn discover_ha_facing_ip(&self) -> String {
        discover_ha_facing_ip(&self.cfg.hass.ipaddr, self.cfg.hass.port)
    }
}

/// push 错误（`NotConfigured` 为静默跳过语义）。
#[derive(Debug)]
pub enum PushError {
    NotConfigured(NotConfiguredError),
    Request(String),
    Do(String),
    Status(u16),
}

impl fmt::Display for PushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PushError::NotConfigured(e) => write!(f, "{e}"),
            PushError::Request(e) => write!(f, "hapush: build request: {e}"),
            PushError::Do(e) => write!(f, "hapush: do: {e}"),
            PushError::Status(code) => write!(f, "hapush: status {code}"),
        }
    }
}

impl std::error::Error for PushError {}

/// UDP Dial 选源 IP（不真发包）；失败返空串。
pub fn discover_ha_facing_ip(ha_ip: &str, ha_port: i64) -> String {
    if ha_ip.is_empty() {
        return String::new();
    }
    let target = format!("{ha_ip}:{ha_port}");
    let sock = match std::net::UdpSocket::bind("0.0.0.0:0") {
        Ok(s) => s,
        Err(_) => return String::new(),
    };
    if sock.connect(&target).is_err() {
        return String::new();
    }
    match sock.local_addr() {
        Ok(addr) => match addr.ip() {
            std::net::IpAddr::V4(v4) => v4.to_string(),
            _ => String::new(),
        },
        Err(_) => String::new(),
    }
}

/// 查询 `iface` 的第一个非 loopback IPv4；失败返空串。
pub fn lookup_wwan(iface: &str) -> String {
    if iface.is_empty() {
        return String::new();
    }
    let addrs = iface_ipv4_addrs(iface);
    pick_non_loopback_ipv4(&addrs)
}

/// 从地址列表挑第一个非 loopback IPv4 字符串。
pub fn pick_non_loopback_ipv4(addrs: &[Ipv4Addr]) -> String {
    for ip in addrs {
        if !ip.is_loopback() {
            return ip.to_string();
        }
    }
    String::new()
}

/// 拼接 HA push 目标 URL（`http://<host>:<port><api>`）。
///
/// 复刻 Go `net.JoinHostPort`：host 含 `:`（IPv6 字面量）时加 `[]` 包裹。doorlink 部署
/// 恒 IPv4 故通常不触发，但保持与 Go buildURL 字节等价（避免未来 IPv6 HA 地址分叉）。
pub fn build_url(host: &str, port: i64, api: &str) -> String {
    if host.contains(':') {
        format!("http://[{host}]:{port}{api}")
    } else {
        format!("http://{host}:{port}{api}")
    }
}

fn iface_ipv4_addrs(iface: &str) -> Vec<Ipv4Addr> {
    #[cfg(target_os = "linux")]
    {
        return linux_iface_ipv4(iface);
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = iface;
        Vec::new()
    }
}

#[cfg(target_os = "linux")]
fn linux_iface_ipv4(iface: &str) -> Vec<Ipv4Addr> {
    use std::ffi::CString;
    use std::mem;
    use std::os::raw::{c_char, c_int, c_ulong, c_void};

    const AF_INET: c_int = 2;
    const SOCK_DGRAM: c_int = 2;
    const SIOCGIFADDR: c_ulong = 0x8915;

    #[repr(C)]
    struct InAddr {
        s_addr: u32,
    }

    #[repr(C)]
    struct SockAddrIn {
        sin_family: u16,
        sin_port: u16,
        sin_addr: InAddr,
        sin_zero: [u8; 8],
    }

    #[repr(C)]
    struct IfReq {
        ifr_name: [c_char; 16],
        ifr_addr: SockAddrIn,
    }

    extern "C" {
        fn socket(domain: c_int, ty: c_int, protocol: c_int) -> c_int;
        fn ioctl(fd: c_int, request: c_ulong, arg: *mut c_void) -> c_int;
        fn close(fd: c_int) -> c_int;
    }

    let c_iface = match CString::new(iface) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    if c_iface.as_bytes_with_nul().len() > 16 {
        return Vec::new();
    }

    unsafe {
        let fd = socket(AF_INET, SOCK_DGRAM, 0);
        if fd < 0 {
            return Vec::new();
        }

        let mut ifr: IfReq = mem::zeroed();
        for (i, &b) in c_iface.as_bytes_with_nul().iter().enumerate() {
            if i >= 16 {
                break;
            }
            ifr.ifr_name[i] = b as c_char;
        }

        let ok = ioctl(fd, SIOCGIFADDR, &mut ifr as *mut _ as *mut c_void);
        close(fd);
        if ok < 0 {
            return Vec::new();
        }

        let b = ifr.ifr_addr.sin_addr.s_addr.to_ne_bytes();
        vec![Ipv4Addr::new(b[0], b[1], b[2], b[3])]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_non_loopback_ipv4_cases() {
        assert_eq!(
            pick_non_loopback_ipv4(&[Ipv4Addr::new(127, 0, 0, 1), Ipv4Addr::new(192, 168, 1, 10),]),
            "192.168.1.10"
        );
        assert_eq!(pick_non_loopback_ipv4(&[Ipv4Addr::new(127, 0, 0, 1)]), "");
        assert_eq!(pick_non_loopback_ipv4(&[]), "");
    }

    #[test]
    fn lookup_wwan_empty_iface() {
        assert_eq!(lookup_wwan(""), "");
    }

    #[test]
    fn lookup_wwan_missing_iface() {
        assert_eq!(lookup_wwan("nonexistent_iface_zzz_99"), "");
    }

    #[test]
    fn build_url_format() {
        assert_eq!(
            build_url("10.0.0.66", 8123, "/api/dooraccess/x"),
            "http://10.0.0.66:8123/api/dooraccess/x"
        );
    }

    #[test]
    fn discover_ha_facing_ip_empty_host() {
        assert_eq!(discover_ha_facing_ip("", 8123), "");
    }
}
