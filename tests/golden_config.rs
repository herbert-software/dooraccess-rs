//! config 模块 golden parity 回归（组 E，tasks 5.5）。
//!
//! 读 `testdata/golden/config.txt` 逐 CASE 断言：
//!   - 解析结果逐字段（INI 怪癖：inline 注释 / 未闭合 quote / `[]string` trim /
//!     `[section "subname"]` slice append / 同 subname 合并）
//!   - unknown key / section → warning（精确字面）+ warnings 计数
//!   - deprecated 检测、validate_uri accept/reject
//!   - `ValidationError` / 缺字段错误**分类**与 Go 对应（sentinel 级，不断言 message 字面）
//!
//! golden 文件只钉**期望输出**，不含输入 INI；输入在本测试内显式重列（与 Go
//! `iniparser_test.go` / `config_test.go` 同款 case，即 Go-behavior-verified 语料）。
//! 因 Rust 去反射（D4）：INI 怪癖 case 用一个 `TestCfg` sink（镜像 Go 局部 `testCfg`）
//! 走**同一个**解析引擎，故怪癖行为与 Config 路径共享、与 Go reflect 路径外部等价。

use std::collections::BTreeMap;

use dooraccess_rs::config::{
    self, parse_ini, Config, ConfigError, FieldOutcome, IniSink, ValidationError,
};

// ---------------------------------------------------------------------------
// golden 文件解析
// ---------------------------------------------------------------------------

const GOLDEN: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/testdata/golden/config.txt"
));

/// name -> 该 CASE 下的断言行列表。
fn parse_golden_cases(text: &str) -> Vec<(String, Vec<String>)> {
    let mut cases = Vec::new();
    let mut cur: Option<(String, Vec<String>)> = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("### CASE|") {
            cur = Some((rest.trim().to_string(), Vec::new()));
        } else if line.starts_with("### END") {
            if let Some(c) = cur.take() {
                cases.push(c);
            }
        } else if let Some((_, lines)) = cur.as_mut() {
            let l = line.trim();
            if !l.is_empty() {
                lines.push(l.to_string());
            }
        }
    }
    cases
}

// ---------------------------------------------------------------------------
// 输入语料（与 Go iniparser_test.go / config_test.go 同款 case）
// ---------------------------------------------------------------------------

const PROD_SHAPE: &str = "
sip = 06021103@172.16.106.91:18022
iface = br-door

[listen]
addr = 0.0.0.0
port = 8080

[hass]
ipaddr = 192.168.2.66
port = 8123
api = /api/dooraccess/REDACTED
token = REDACTED.LLT.token

[video]
forward = true
format = flv
cache_path =

[station \"1\"]
sip = 06020000@172.16.106.152:18022
rtsp_url =
";

const INLINE_COMMENTS: &str = "
; full-line semicolon comment
# full-line hash comment
name = hi   ; inline semicolon
count = 5   # inline hash
enabled = true ; trailing
";

const QUOTED_SEMICOLON: &str = "name = \"hello;world\"";
const ODD_QUOTE: &str = "name = \"open ; not a comment";
const ARRAY_TRIM: &str = "tags = a , b ,c,d   ,  e";

const EMPTY_VALUE: &str = "
name =
count = 0
enabled =
";

const UNKNOWN_KEY: &str = "
name = ok
unknown_key = value
";

const UNKNOWN_SECTION: &str = "
name = ok

[automation]
foo = bar
";

const MULTI_STATION: &str = "
[station \"1\"]
sip = a@1.1.1.1:1
[station \"2\"]
sip = b@2.2.2.2:2
[station \"third\"]
sip = c@3.3.3.3:3
";

const SAME_SUBNAME: &str = "
[station \"1\"]
sip = a@1.1.1.1:1

[station \"1\"]
rtsp_url = rtsp://a
";

/// 完整 happy-path（含 v0.1 旧 deprecated 字段 + [automation] 旧整数 key），对应 Go `validBody`。
const VALID_BODY: &str = "
brand = anjubao
sip = 12345678@10.0.0.20:18022
family = 1
elev = 0
iface = eth0.1

[listen]
addr = 0.0.0.0
port = 8080

[station \"1\"]
sip = 12340000@10.0.0.10:18022
rtsp_url =

[automation]
unlock = -1
hangup = -1
call_elev = 0

[hass]
ipaddr = 10.1.0.66
port = 8123
token = \"eyJhbG...\"

[notification]
diagnosis = true
conversation = true
others = true

[video]
forward = false
format = flv
cache_path =
";

// ---------------------------------------------------------------------------
// 测试用 sink：镜像 Go 局部 testCfg（Name/Count/Enabled/Tags）
// ---------------------------------------------------------------------------

#[derive(Default)]
struct TestCfg {
    name: String,
    count: i64,
    enabled: bool,
    tags: Vec<String>,
}

impl IniSink for TestCfg {
    fn set_top(&mut self, key: &str, value: &str) -> FieldOutcome {
        match key {
            "name" => {
                self.name = value.to_string();
                FieldOutcome::Set
            }
            "count" => match config::conv_int(value, key) {
                Ok(n) => {
                    self.count = n;
                    FieldOutcome::Set
                }
                Err(m) => FieldOutcome::TypeErr(m),
            },
            "enabled" => match config::conv_bool(value, key) {
                Ok(b) => {
                    self.enabled = b;
                    FieldOutcome::Set
                }
                Err(m) => FieldOutcome::TypeErr(m),
            },
            "tags" => {
                self.tags = config::conv_string_array(value);
                FieldOutcome::Set
            }
            _ => FieldOutcome::Unknown,
        }
    }
    // testCfg 在 golden 中只用顶级标量字段——无 section/subsection 行被解析到这里。
    fn section_known(&self, _section: &str) -> bool {
        false
    }
    fn section_is_struct(&self, _section: &str) -> bool {
        false
    }
    fn section_is_slice(&self, _section: &str) -> bool {
        false
    }
    fn set_section(&mut self, _s: &str, _k: &str, _v: &str) -> FieldOutcome {
        unreachable!("TestCfg has no struct sections in golden cases")
    }
    fn subsection_len(&self, _section: &str) -> usize {
        0
    }
    fn append_subsection(&mut self, _section: &str) {
        unreachable!("TestCfg has no slice sections in golden cases")
    }
    fn set_subsection(&mut self, _s: &str, _k: &str, _v: &str) -> FieldOutcome {
        unreachable!("TestCfg has no slice sections in golden cases")
    }
}

impl TestCfg {
    fn get(&self, path: &str) -> String {
        match path {
            "name" => self.name.clone(),
            "count" => self.count.to_string(),
            "enabled" => bool_str(self.enabled),
            "tags" => self.tags.join(","),
            other => panic!("TestCfg unknown field path {other:?}"),
        }
    }
}

fn bool_str(b: bool) -> String {
    if b {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

// ---------------------------------------------------------------------------
// Config 字段访问
// ---------------------------------------------------------------------------

fn conf_get(cfg: &Config, path: &str) -> String {
    match path {
        "sip" => cfg.sip.clone(),
        "iface" => cfg.iface.clone(),
        "iface_list" => cfg.iface_list.join(","),
        "listen.addr" => cfg.listen.addr.clone(),
        "listen.port" => cfg.listen.port.to_string(),
        "hass.ipaddr" => cfg.hass.ipaddr.clone(),
        "hass.port" => cfg.hass.port.to_string(),
        "hass.api" => cfg.hass.api.clone(),
        "hass.token" => cfg.hass.token.clone(),
        "video.forward" => bool_str(cfg.video.forward),
        "video.format" => cfg.video.format.clone(),
        "video.cache_path" => cfg.video.cache_path.clone(),
        "automation.auto_unlock" => bool_str(cfg.automation.auto_unlock),
        "automation.auto_hangup" => bool_str(cfg.automation.auto_hangup),
        "deprecated" => {
            // deprecated 是集合语义；golden 按字母序列，排序后比较（Go 导出端亦排序）。
            let mut d: Vec<String> = cfg.deprecated_fields().to_vec();
            d.sort();
            d.join(",")
        }
        "missing" => cfg.missing_fields().join(","),
        p if p.starts_with("stations[") => {
            let rest = &p["stations[".len()..];
            let close = rest.find(']').expect("stations[N] missing ]");
            let idx: usize = rest[..close].parse().expect("stations index");
            let field = &rest[close + 2..]; // skip "]."
            let st = &cfg.stations[idx];
            match field {
                "sip" => st.sip.clone(),
                "rtsp_url" => st.rtsp_url.clone(),
                other => panic!("station unknown field {other:?}"),
            }
        }
        other => panic!("Config unknown field path {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 断言驱动
// ---------------------------------------------------------------------------

/// 对一组 field/warning/count 断言行执行检查。
fn check_lines<F: Fn(&str) -> String>(
    case: &str,
    lines: &[String],
    get: F,
    warnings: &[String],
    stations_len: Option<usize>,
) {
    for line in lines {
        let f: Vec<&str> = line.splitn(3, '|').collect();
        match f[0] {
            "field" => {
                let path = f[1];
                let want = if f.len() == 3 { f[2] } else { "" };
                let got = get(path);
                assert_eq!(got, want, "case {case}: field {path}");
            }
            "warning" => {
                let want = line["warning|".len()..].trim();
                assert!(
                    warnings.iter().any(|w| w.as_str() == want),
                    "case {case}: missing warning {want:?}; got {warnings:?}"
                );
            }
            "count" => {
                let what = f[1];
                let n: usize = f[2].trim().parse().expect("count n");
                match what {
                    "warnings" => assert_eq!(warnings.len(), n, "case {case}: warnings count"),
                    "stations" => assert_eq!(
                        stations_len.expect("stations_len for count|stations"),
                        n,
                        "case {case}: stations count"
                    ),
                    other => panic!("case {case}: unknown count target {other:?}"),
                }
            }
            "validateuri" => panic!("case {case}: validateuri handled separately"),
            other => panic!("case {case}: unknown verb {other:?}"),
        }
    }
}

fn parse_test(input: &str) -> (TestCfg, Vec<String>) {
    let mut cfg = TestCfg::default();
    let w = parse_ini(input, &mut cfg).unwrap_or_else(|e| panic!("parse_ini TestCfg: {e:?}"));
    (cfg, w)
}

fn parse_conf(input: &str) -> (Config, Vec<String>) {
    let mut cfg = Config::default();
    let w = parse_ini(input, &mut cfg).unwrap_or_else(|e| panic!("parse_ini Config: {e:?}"));
    (cfg, w)
}

#[test]
fn golden_config() {
    let cases = parse_golden_cases(GOLDEN);
    assert!(!cases.is_empty(), "no golden cases parsed");

    let mut seen: BTreeMap<&str, bool> = BTreeMap::new();

    for (name, lines) in &cases {
        seen.insert(name.as_str(), true);
        match name.as_str() {
            "prod-shape" => {
                let (cfg, w) = parse_conf(PROD_SHAPE);
                let len = cfg.stations.len();
                check_lines(name, lines, |p| conf_get(&cfg, p), &w, Some(len));
            }
            "inline-comments" => {
                let (cfg, w) = parse_test(INLINE_COMMENTS);
                check_lines(name, lines, |p| cfg.get(p), &w, None);
            }
            "quoted-semicolon" => {
                let (cfg, w) = parse_test(QUOTED_SEMICOLON);
                check_lines(name, lines, |p| cfg.get(p), &w, None);
            }
            "odd-quote" => {
                let (cfg, w) = parse_test(ODD_QUOTE);
                check_lines(name, lines, |p| cfg.get(p), &w, None);
            }
            "array-trim" => {
                let (cfg, w) = parse_test(ARRAY_TRIM);
                check_lines(name, lines, |p| cfg.get(p), &w, None);
            }
            "empty-value" => {
                let (cfg, w) = parse_test(EMPTY_VALUE);
                check_lines(name, lines, |p| cfg.get(p), &w, None);
            }
            "unknown-key" => {
                let (cfg, w) = parse_test(UNKNOWN_KEY);
                check_lines(name, lines, |p| cfg.get(p), &w, None);
            }
            "unknown-section" => {
                let (cfg, w) = parse_test(UNKNOWN_SECTION);
                check_lines(name, lines, |p| cfg.get(p), &w, None);
            }
            "bool-variants" => check_bool_variants(name, lines),
            "multi-station-append" => {
                let (cfg, w) = parse_conf(MULTI_STATION);
                let len = cfg.stations.len();
                check_lines(name, lines, |p| conf_get(&cfg, p), &w, Some(len));
            }
            "same-subname-merge" => {
                let (cfg, w) = parse_conf(SAME_SUBNAME);
                let len = cfg.stations.len();
                check_lines(name, lines, |p| conf_get(&cfg, p), &w, Some(len));
            }
            "loadconfig-validbody" => check_loadconfig_validbody(name, lines),
            "validateuri" => check_validateuri(name, lines),
            other => panic!("unhandled golden case {other:?} — input corpus missing"),
        }
    }

    // 确保关键怪癖 case 都被覆盖（防 golden 退化）。
    for must in [
        "prod-shape",
        "inline-comments",
        "odd-quote",
        "array-trim",
        "unknown-key",
        "unknown-section",
        "bool-variants",
        "multi-station-append",
        "same-subname-merge",
        "validateuri",
    ] {
        assert!(seen.contains_key(must), "golden missing case {must:?}");
    }
}

/// bool-variants：每行 `field|enabled["X"]|Y` → 输入 `enabled = X` 应解析为 enabled==Y。
fn check_bool_variants(case: &str, lines: &[String]) {
    let mut n = 0;
    for line in lines {
        let f: Vec<&str> = line.splitn(3, '|').collect();
        assert_eq!(f[0], "field", "case {case}: expected field line");
        let path = f[1];
        let want = f[2] == "true";
        // path 形如 enabled["true"] / enabled[""]：取首尾 `"` 之间的输入。
        let first = path.find('"').expect("variant open quote");
        let last = path.rfind('"').expect("variant close quote");
        let input_val = &path[first + 1..last];
        let body = format!("enabled = {input_val}");
        let (cfg, _w) = parse_test(&body);
        assert_eq!(
            cfg.enabled, want,
            "case {case}: variant {input_val:?} → enabled"
        );
        n += 1;
    }
    assert!(n > 0, "case {case}: no bool variants");
}

/// loadconfig-validbody：组装 parse + apply_defaults + detect_deprecated（不读盘），
/// 断言 deprecated 集 / automation flag / missing。
fn check_loadconfig_validbody(case: &str, lines: &[String]) {
    let mut cfg = Config::default();
    let w = parse_ini(VALID_BODY, &mut cfg).unwrap_or_else(|e| panic!("parse_ini: {e:?}"));
    cfg.set_parser_warnings(w);
    cfg.apply_defaults();
    cfg.detect_deprecated(VALID_BODY);
    // validBody 是合法配置——validate 应通过（额外保险）。
    cfg.validate()
        .unwrap_or_else(|e| panic!("case {case}: validBody should validate, got {e:?}"));

    check_lines(case, lines, |p| conf_get(&cfg, p), &[], None);
}

/// validateuri：每行 `validateuri|<uri>|accept|reject`。
fn check_validateuri(case: &str, lines: &[String]) {
    let mut n = 0;
    for line in lines {
        let f: Vec<&str> = line.rsplitn(2, '|').collect();
        // rsplitn → [verdict, "validateuri|<uri>"]
        let verdict = f[0].trim();
        let head = f[1];
        let uri = &head["validateuri|".len()..];
        let res = config::validate_uri(uri);
        match verdict {
            "accept" => assert!(
                res.is_ok(),
                "case {case}: {uri:?} should accept, got {res:?}"
            ),
            "reject" => assert!(res.is_err(), "case {case}: {uri:?} should reject, got Ok"),
            other => panic!("case {case}: unknown verdict {other:?}"),
        }
        n += 1;
    }
    assert!(n > 0, "case {case}: no validateuri lines");
}

// ---------------------------------------------------------------------------
// ValidationError / 缺字段错误**分类**与 Go 对应（D1 / tasks 5.5；不断言 message 字面）
// ---------------------------------------------------------------------------

/// 构造一个带单个合法 station 的 Config（Config 有私有字段，外部不能用结构字面量构造，
/// 故经 default() + 公开字段赋值组装）。
fn conf_with_station(sip: &str, station_sip: &str) -> Config {
    let mut cfg = Config::default();
    cfg.sip = sip.to_string();
    cfg.stations.push(config::Station {
        sip: station_sip.to_string(),
        rtsp_url: String::new(),
    });
    cfg
}

#[test]
fn validation_error_classification() {
    // 空 sip → ValidationError{field:"sip"}（缺字段）。
    let mut cfg = conf_with_station("", "12340000@1.2.3.4:18022");
    cfg.sip = String::new();
    cfg.apply_defaults();
    match cfg.validate() {
        Err(ConfigError::Validation(ValidationError { field, .. })) => {
            assert_eq!(field, "sip", "empty sip → field sip")
        }
        other => panic!("empty sip → {other:?}, want Validation(sip)"),
    }

    // 无 stations → ValidationError{field:"stations"}。
    let mut cfg = Config::default();
    cfg.sip = "12345678@10.0.0.1:18022".to_string();
    cfg.apply_defaults();
    match cfg.validate() {
        Err(ConfigError::Validation(ValidationError { field, .. })) => {
            assert_eq!(field, "stations")
        }
        other => panic!("no stations → {other:?}, want Validation(stations)"),
    }

    // 坏 sip URI → ValidationError{field:"sip"}。
    let mut cfg = conf_with_station("garbage", "12340000@1.2.3.4:18022");
    cfg.apply_defaults();
    match cfg.validate() {
        Err(ConfigError::Validation(ValidationError { field, .. })) => assert_eq!(field, "sip"),
        other => panic!("bad sip → {other:?}"),
    }

    // 坏 station sip → ValidationError{field:"stations[0].sip"}。
    let mut cfg = conf_with_station("12345678@10.0.0.1:18022", "garbage");
    cfg.apply_defaults();
    match cfg.validate() {
        Err(ConfigError::Validation(ValidationError { field, .. })) => {
            assert_eq!(field, "stations[0].sip")
        }
        other => panic!("bad station sip → {other:?}"),
    }

    // 非法 video.format → ValidationError{field:"video.format"}。
    let mut cfg = conf_with_station("12345678@10.0.0.1:18022", "12340000@1.2.3.4:18022");
    cfg.video.format = "h264".to_string();
    cfg.apply_defaults(); // format 非空，不被改
    match cfg.validate() {
        Err(ConfigError::Validation(ValidationError { field, .. })) => {
            assert_eq!(field, "video.format")
        }
        other => panic!("bad format → {other:?}"),
    }
}

/// load_config 缺文件 → ConfigError::NotFound 分类（Go ErrConfigNotFound 对应）。
#[test]
fn load_config_missing_file_classified() {
    match config::load_config("/nonexistent/dooraccess-go/config.ini") {
        Err(ConfigError::NotFound { .. }) => {}
        other => panic!("missing file → {other:?}, want NotFound"),
    }
}

// ── int 宽度 / 溢出回归（代码 review round 1：#14 octet panic / #9 conv_int int32）──
// 差分测试发现：Go 平台 int=int32（部署目标 GOARCH=mips），Rust i64/u32 在此分叉。

#[test]
fn validate_uri_overlong_octet_rejected_no_panic() {
    // ≥10 位 octet：修前 Rust debug 构建 panic（u32 累加溢出）。修后数字循环内即拒。
    assert!(config::validate_uri("12345678@4294967041.2.3.4:18022").is_err());
    assert!(config::validate_uri("12345678@99999999999.1.1.1:1").is_err());
    assert!(config::validate_uri("12345678@256.1.1.1:1").is_err());
    assert!(config::validate_uri("06021103@10.0.0.10:18022").is_ok());
}

#[test]
fn conv_int_int32_range_aligns_go_mips() {
    // Go-MIPS int=int32 + OverflowInt：超 i32 范围拒。
    assert!(config::conv_int("3000000000", "port").is_err());
    assert!(config::conv_int("2147483648", "port").is_err());
    assert_eq!(config::conv_int("2147483647", "port"), Ok(2147483647));
    // 负边界（Go int32 min=-2147483648；OverflowInt 拒 min-1）。
    assert_eq!(config::conv_int("-2147483648", "port"), Ok(-2147483648));
    assert!(config::conv_int("-2147483649", "port").is_err());
    assert_eq!(config::conv_int("18022", "port"), Ok(18022));
    assert_eq!(config::conv_int("", "port"), Ok(0));
}
