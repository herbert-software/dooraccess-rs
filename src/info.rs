//! `/info` 元信息（`SelfDesc` + `RenderJSON`）。
//!
//! no-probe 子集：`build(probe_ha=false)` 只构造 `/info` 的 6 字段
//! （daemon/version/brand/monitor/outdoor_stations/video），不解析 iface IP /
//! 不探测 HA-facing IP（这些结果不进 `/info` body）。

use crate::config::{Config, SUPPORTED_BRAND};
use crate::httpx::json::{encode_struct, JsonOptions, JsonValue};

/// daemon self-description（banner-only 字段省略）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelfDesc {
    pub version: String,
    pub pid: u32,
    pub brand: String,
    pub daemon: String,
    pub monitor: String,
    pub stations: Vec<StationDesc>,
    pub video: VideoDesc,
    /// `probe_ha=true` 时填充；`GET /info` 路径（`probe_ha=false`）恒为 `None`。
    pub hass_reachable: Option<bool>,
}

/// `/info` 暴露给 HACS 的外机简化描述。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StationDesc {
    pub sip: String,
}

/// 反映 `cfg.video.*`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoDesc {
    pub forward: bool,
    pub forward_supported: bool,
    pub protocol: String,
    pub format: String,
    pub cache_path: String,
}

/// 构造 `SelfDesc`。`probe_ha=false` 保证幂等、无 HA dial 副作用。
///
/// `probe_ha=true` 时可选 TCP dial 探测 HA 可达性（仅 banner 路径；不进 `/info` JSON）。
pub fn build(cfg: Option<&Config>, version: &str, probe_ha: bool) -> SelfDesc {
    let mut sd = SelfDesc {
        version: safe_or_default(version, "dev"),
        pid: std::process::id(),
        brand: SUPPORTED_BRAND.to_string(),
        daemon: "dooraccess-go".to_string(),
        monitor: String::new(),
        stations: Vec::new(),
        // 默认 Video 零值（protocol=""）；仅在 `cfg` 存在时（下方 Some 分支）设
        // protocol="anjubao-h264"，无 cfg 时 VideoDesc 零值 protocol=""。
        video: VideoDesc {
            forward: false,
            forward_supported: false,
            protocol: String::new(),
            format: String::new(),
            cache_path: String::new(),
        },
        hass_reachable: None,
    };

    if let Some(cfg) = cfg {
        sd.monitor = cfg.sip.clone();
        for s in &cfg.stations {
            sd.stations.push(StationDesc { sip: s.sip.clone() });
        }
        sd.video = VideoDesc {
            forward: cfg.video.forward,
            forward_supported: cfg.video.forward,
            protocol: "anjubao-h264".to_string(),
            format: cfg.video.format.clone(),
            cache_path: cfg.video.cache_path.clone(),
        };

        if probe_ha && !cfg.hass.ipaddr.is_empty() {
            sd.hass_reachable = Some(probe_dial(
                &cfg.hass.ipaddr,
                cfg.hass.port,
                std::time::Duration::from_millis(1500),
            ));
        }
    }

    sd
}

/// 把 `SelfDesc` 序列化成 `GET /info` 响应 JSON bytes。
///
/// struct 声明序：daemon, version, brand, monitor, outdoor_stations, video；
/// `SetEscapeHTML(false)` + 尾随 `\n`（`JsonOptions::ENCODE`）。
pub fn render_json(sd: &SelfDesc) -> Vec<u8> {
    let stations: Vec<JsonValue> = if sd.stations.is_empty() {
        vec![]
    } else {
        sd.stations
            .iter()
            .map(|s| JsonValue::Struct(vec![("sip".into(), JsonValue::String(s.sip.clone()))]))
            .collect()
    };

    let video = JsonValue::Struct(vec![
        ("forward".into(), JsonValue::Bool(sd.video.forward)),
        (
            "forward_supported".into(),
            JsonValue::Bool(sd.video.forward_supported),
        ),
        (
            "protocol".into(),
            JsonValue::String(sd.video.protocol.clone()),
        ),
        ("format".into(), JsonValue::String(sd.video.format.clone())),
        (
            "cache_path".into(),
            JsonValue::String(sd.video.cache_path.clone()),
        ),
    ]);

    encode_struct(
        &[
            ("daemon", JsonValue::String(sd.daemon.clone())),
            ("version", JsonValue::String(sd.version.clone())),
            ("brand", JsonValue::String(sd.brand.clone())),
            ("monitor", JsonValue::String(sd.monitor.clone())),
            (
                "outdoor_stations",
                JsonValue::Array(stations), // 空 → `[]` 非 `null`
            ),
            ("video", video),
        ],
        JsonOptions::ENCODE,
    )
}

fn safe_or_default(v: &str, def: &str) -> String {
    if v.is_empty() {
        def.to_string()
    } else {
        v.to_string()
    }
}

fn probe_dial(host: &str, port: i64, timeout: std::time::Duration) -> bool {
    use std::net::{TcpStream, ToSocketAddrs};
    let target = format!("{host}:{port}");
    let addrs: Vec<_> = match target.to_socket_addrs() {
        Ok(a) => a.collect(),
        Err(_) => return false,
    };
    for addr in addrs {
        if TcpStream::connect_timeout(&addr, timeout).is_ok() {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Hass, Listen, Station, Video};

    fn sample_cfg() -> Config {
        // Config 含跨模块私有字段（missing_fields 等），不能用 `..Default::default()`
        // 结构体更新语法；改 default + 逐字段赋值（与 config 私有不变量兼容）。
        let mut cfg = Config::default();
        cfg.sip = "12345678@10.0.0.20:18022".into();
        cfg.iface = "lo0".into();
        cfg.listen = Listen {
            addr: "0.0.0.0".into(),
            port: 8080,
        };
        cfg.stations = vec![Station {
            sip: "12340000@10.0.0.10:18022".into(),
            rtsp_url: String::new(),
        }];
        cfg.hass = Hass {
            ipaddr: "10.0.0.66".into(),
            port: 8123,
            api: "/api/x".into(),
            token: "should-not-leak".into(),
        };
        cfg.video = Video {
            forward: false,
            format: String::new(),
            cache_path: String::new(),
        };
        cfg
    }

    #[test]
    fn build_basic_fields() {
        let sd = build(Some(&sample_cfg()), "v0.2.0", false);
        assert_eq!(sd.daemon, "dooraccess-go");
        assert_eq!(sd.brand, "anjubao");
        assert_eq!(sd.version, "v0.2.0");
        assert_eq!(sd.monitor, "12345678@10.0.0.20:18022");
        assert_eq!(sd.stations.len(), 1);
        assert_eq!(sd.stations[0].sip, "12340000@10.0.0.10:18022");
        assert!(sd.hass_reachable.is_none());
        assert!(sd.pid > 0);
    }

    #[test]
    fn render_json_fields_subset() {
        let sd = build(Some(&sample_cfg()), "v0.2.0-test", false);
        let raw = render_json(&sd);
        let rs = String::from_utf8(raw).unwrap();

        for want in [
            r#""daemon":"dooraccess-go""#,
            r#""brand":"anjubao""#,
            r#""version":"v0.2.0-test""#,
            r#""monitor":"12345678@10.0.0.20:18022""#,
            "\"outdoor_stations\":",
        ] {
            assert!(rs.contains(want), "missing {want}\nfull: {rs}");
        }

        for banned in [
            "should-not-leak",
            "hass_token",
            "token_source",
            "iface_ip",
            "hass_endpoint",
            "hass_facing",
        ] {
            assert!(!rs.contains(banned), "leaked {banned}\nfull: {rs}");
        }
        assert!(rs.ends_with('\n'));
    }

    #[test]
    fn render_json_empty_stations() {
        let sd = build(None, "test", false);
        let raw = render_json(&sd);
        let rs = String::from_utf8(raw).unwrap();
        assert!(rs.contains(r#""outdoor_stations":[]"#));
    }
}
