//! Phase1 config 模块：dooraccess 主配置 INI 的 Rust 等价实现（去反射）。
//!
//! 与 Go `local/dooraccess-go internal/config` golden parity：
//!   - minimal INI parser（`iniparser.go`）
//!   - schema / 默认值 / deprecated 检测 / 校验（`config.go`）
//!
//! 去反射化（design D4）：Go `iniparser.go` 靠 `reflect` + `ini:"tag"` 自动分发字段。
//! Rust 无反射，改为把解析**引擎**（注释剥离 / section header / key=value / 标量转换 /
//! `lastSubname` 状态）与**字段分发**分离：引擎泛型于 [`IniSink`] trait，sink 用显式
//! `match (section, key)` 把值写到已知结构字段。`Config` 实现 `IniSink`；引擎对任意 sink
//! 复用，故 INI 怪癖（inline 注释 / 未闭合 quote / `[section "subname"]` slice append /
//! `[]string` trim）的行为对所有目标一致——与 Go reflect 路径外部可观测等价。
//!
//! **不**移植 `ResolveIfaceList`（调 `net.Interfaces()` 系统调用，超 Phase1，见 design D3）。
//!
//! D1：结构字段值逐字段精确相等；错误**分类**（sentinel 级 `ConfigError` / `ValidationError`）
//! 与 Go 对应，错误 message 仅语义等价、不复刻 Go `fmt.Errorf` 字面。

use std::fs;
use std::io;

// ===========================================================================
// schema（与 Go Config 字面级对齐）
// ===========================================================================

/// 替代品支持的唯一品牌（Go `SupportedBrand` 常量）。
pub const SUPPORTED_BRAND: &str = "anjubao";

pub const DEFAULT_IFACE: &str = "eth0.1";
pub const DEFAULT_HASS_PORT: i64 = 8123;
pub const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0";
pub const DEFAULT_LISTEN_PORT: i64 = 8080;

/// 主配置文件 schema（Go `Config`）。
///
/// 顶级 key=value → `sip` / `iface` / `iface_list`；嵌套 struct → `[listen]` / `[hass]` /
/// `[video]` / `[automation]` section；`stations` 数组 → `[station "1"]` / `[station "2"]`
/// subsection。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Config {
    pub sip: String,
    pub iface: String,
    /// 显式监听接口列表（v0.4.1 escape hatch）；为空时由 `ResolveIfaceList` 检测（不在 Phase1）。
    pub iface_list: Vec<String>,
    pub listen: Listen,
    pub stations: Vec<Station>,
    pub hass: Hass,
    pub video: Video,
    pub automation: Automation,

    // 后处理派生（不参与解析；对应 Go 未导出字段 + accessor）。
    missing_fields: Vec<String>,
    deprecated_fields: Vec<String>,
    parser_warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Listen {
    pub addr: String,
    pub port: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Station {
    pub sip: String,
    pub rtsp_url: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Hass {
    pub ipaddr: String,
    pub port: i64,
    /// HACS 端 view path（如 `/api/dooraccess/<id>`），daemon 反向 push 用。
    pub api: String,
    pub token: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Video {
    pub forward: bool,
    pub format: String,
    pub cache_path: String,
}

/// `[automation]` section 的出厂默认 flag（formalize-daemon-auto-unlock §3，v0.10.0 复活为正式 schema）。
///
/// 旧 v0.1 残留 key（`unlock` / `hangup` / `call_elev` 等整数 key）被引擎当「未知 key」收集到
/// `parser_warnings`、**不 fatal**——只按名取 `auto_unlock` / `auto_hangup` 两 bool。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Automation {
    pub auto_unlock: bool,
    pub auto_hangup: bool,
}

// ===========================================================================
// 错误分类（sentinel 级，D1）
// ===========================================================================

/// config 加载 / 解析 / 校验错误。分类与 Go 对应（`ErrConfigNotFound` / parse error /
/// `ValidationError`），message 仅语义等价。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// 配置文件不存在（Go `ErrConfigNotFound`）。
    NotFound { path: String },
    /// 读盘失败（非 NotExist，如把目录当文件 / 非 UTF-8）。
    Read { path: String, msg: String },
    /// INI 解析失败（坏 section header / 标量类型错），带行号。
    Parse { line: usize, msg: String },
    /// 启动校验失败（Go `ValidationError`）。
    Validation(ValidationError),
}

/// 校验失败原因（Go `ValidationError` 结构等价）。`field` 是 sentinel 级分类锚点。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    pub field: String,
    pub value: String,
    pub want: String,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::NotFound { path } => write!(f, "config file not found: {path}"),
            ConfigError::Read { path, msg } => write!(f, "read config {path}: {msg}"),
            ConfigError::Parse { line, msg } => write!(f, "config: line {line}: {msg}"),
            ConfigError::Validation(e) => {
                write!(f, "config field {} = {}, want {}", e.field, e.value, e.want)
            }
        }
    }
}

impl std::error::Error for ConfigError {}

// ===========================================================================
// INI 解析引擎（泛型于 IniSink；去反射的核心）
// ===========================================================================

/// 单条字段写入的结果。引擎据此发 warning / 报 parse 错。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldOutcome {
    /// 已知字段、值已写入。
    Set,
    /// 未知 key → 收集为 warning（不报错，与 Go 一致）。
    Unknown,
    /// 类型错（如 struct key 当标量、坏 int / bool）→ 引擎包成 `ConfigError::Parse`。
    TypeErr(String),
}

/// INI 写入目标。引擎驱动解析，sink 用显式 `match` 把值分发到已知字段（替代 Go reflect）。
///
/// 约定与 Go `iniParser` 三个 setter + `lookupFieldByINITag` 等价：
///   - `set_top`：顶级 `key = value`（section == ""）。
///   - `set_section`：`[section]`（无 subname）`key = value`；引擎保证已知且是 struct section。
///   - subsection（`[section "subname"]`）：引擎用 `subsection_len` + 内部 `lastSubname` 状态
///     决定何时 `append_subsection`，再 `set_subsection` 写当前（末）元素。
pub trait IniSink {
    /// 顶级 `key = value`。
    fn set_top(&mut self, key: &str, value: &str) -> FieldOutcome;

    /// 是否存在 ini-tag 等于 `section` 的字段（任意 kind）——决定是否 unknown section warning。
    fn section_known(&self, section: &str) -> bool;
    /// 该 section 字段是否是 struct（`[section]` 无 subname 路径要求）。
    fn section_is_struct(&self, section: &str) -> bool;
    /// 该 section 字段是否是 slice-of-struct（`[section "subname"]` 路径要求）。
    fn section_is_slice(&self, section: &str) -> bool;

    /// `[section]` 内 `key = value`（引擎保证 `section_is_struct`）。
    fn set_section(&mut self, section: &str, key: &str, value: &str) -> FieldOutcome;

    /// slice-of-struct section 当前元素个数（用于 `lastSubname` 的 `len==0` 规则）。
    fn subsection_len(&self, section: &str) -> usize;
    /// 追加一个新空元素到 slice-of-struct section。
    fn append_subsection(&mut self, section: &str);
    /// 写当前（末）元素的 key（引擎保证 `section_is_slice` 且已 append）。
    fn set_subsection(&mut self, section: &str, key: &str, value: &str) -> FieldOutcome;
}

/// 解析 INI 文本，按 [`IniSink`] 分发字段。返回 unknown section / key 的 warning 列表
/// （与 Go `ParseINI` 返回的 warnings 字面等价）。
///
/// 错误约定：坏 section header / 标量类型错返 `ConfigError::Parse`；未知 key / section
/// 只收集 warning 不报错（与 Go 一致）。
pub fn parse_ini<S: IniSink>(input: &str, sink: &mut S) -> Result<Vec<String>, ConfigError> {
    let mut warnings: Vec<String> = Vec::new();
    let mut section = String::new();
    let mut subname = String::new();
    // 小规模 (section -> 上次 subname) 状态；section 数极少，线性查足够。
    let mut last_subname: Vec<(String, String)> = Vec::new();

    for (idx, raw) in input.split('\n').enumerate() {
        let line_no = idx + 1;
        // 对齐 Go `bufio.Scanner` 1<<20 token 上限（iniparser.go：超长行 → ErrTooLong → 扫描错）。
        // Go bufio scan.go 在 `len >= maxTokenSize` 即报错，故用 `>=`（恰好 1 MiB 也拒）。
        if raw.len() >= (1 << 20) {
            return Err(ConfigError::Parse {
                line: line_no,
                msg: "line too long (> 1 MiB)".to_string(),
            });
        }
        let line = match strip_comment_trim(raw) {
            Some(l) => l,
            None => continue,
        };

        // section header
        if line.starts_with('[') {
            let (s, sub) = parse_section_header(&line)
                .map_err(|msg| ConfigError::Parse { line: line_no, msg })?;
            if !s.is_empty() && sub.is_empty() {
                if !sink.section_known(&s) {
                    warnings.push(format!("config: unknown section [{s}]"));
                }
            } else if !s.is_empty() && !sub.is_empty() && !sink.section_known(&s) {
                warnings.push(format!("config: unknown subsection [{s} {sub:?}]"));
            }
            section = s;
            subname = sub;
            continue;
        }

        // key = value
        let (key, value) =
            parse_key_value(&line).map_err(|msg| ConfigError::Parse { line: line_no, msg })?;

        if section.is_empty() {
            match sink.set_top(&key, &value) {
                FieldOutcome::Set => {}
                FieldOutcome::Unknown => warnings.push(format!("config: unknown key {key:?}")),
                FieldOutcome::TypeErr(msg) => {
                    return Err(ConfigError::Parse { line: line_no, msg })
                }
            }
        } else if subname.is_empty() {
            // unknown section 在 header 时已 warn；此处静默跳过（与 Go setSectionField !ok → nil 一致）。
            if !sink.section_known(&section) {
                continue;
            }
            if !sink.section_is_struct(&section) {
                return Err(ConfigError::Parse {
                    line: line_no,
                    msg: format!("section [{section}] expected struct field"),
                });
            }
            match sink.set_section(&section, &key, &value) {
                FieldOutcome::Set => {}
                FieldOutcome::Unknown => {
                    warnings.push(format!("config: unknown key {key:?} in [{section}]"))
                }
                FieldOutcome::TypeErr(msg) => {
                    return Err(ConfigError::Parse { line: line_no, msg })
                }
            }
        } else {
            if !sink.section_known(&section) {
                continue;
            }
            if !sink.section_is_slice(&section) {
                return Err(ConfigError::Parse {
                    line: line_no,
                    msg: format!("subsection [{section} {subname:?}] expected slice field"),
                });
            }
            // lazy append（Go setSubsectionField）：(section,subname) 与上次不同、或当前 slice 为空时
            // append 新元素；同 subname 内多 key 共用同元素。
            let prev = last_subname
                .iter()
                .find(|(s, _)| *s == section)
                .map(|(_, v)| v.as_str());
            let need_new = match prev {
                None => true,
                Some(p) => p != subname,
            } || sink.subsection_len(&section) == 0;
            if need_new {
                sink.append_subsection(&section);
                set_last_subname(&mut last_subname, &section, &subname);
            }
            match sink.set_subsection(&section, &key, &value) {
                FieldOutcome::Set => {}
                FieldOutcome::Unknown => warnings.push(format!(
                    "config: unknown key {key:?} in [{section} {subname:?}]"
                )),
                FieldOutcome::TypeErr(msg) => {
                    return Err(ConfigError::Parse { line: line_no, msg })
                }
            }
        }
    }

    Ok(warnings)
}

fn set_last_subname(map: &mut Vec<(String, String)>, section: &str, subname: &str) {
    if let Some(e) = map.iter_mut().find(|(s, _)| s == section) {
        e.1 = subname.to_string();
    } else {
        map.push((section.to_string(), subname.to_string()));
    }
}

/// 去行尾注释 + 前后空白。返回 `None` 表示空行 / 整行注释。
///
/// 注释规则（Go `stripCommentTrim`）：`;` / `#` 行首整行注释；inline 时若其前 quote 未闭合
/// （奇数个 `"`）则不算注释——简化实现的已知行为，复刻以保 parity。
fn strip_comment_trim(s: &str) -> Option<String> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let bytes = s.as_bytes();
    if bytes[0] == b';' || bytes[0] == b'#' {
        return None;
    }
    let mut in_quote = false;
    let mut cut = s.len();
    for (i, &c) in bytes.iter().enumerate() {
        if c == b'"' {
            in_quote = !in_quote;
            continue;
        }
        if !in_quote && (c == b';' || c == b'#') {
            cut = i;
            break;
        }
    }
    // 注释字符是 ASCII，cut 落在 char 边界，按字节切安全。
    let out = s[..cut].trim_end_matches([' ', '\t']);
    if out.is_empty() {
        None
    } else {
        Some(out.to_string())
    }
}

/// 解析 `[name]` 或 `[name "subname"]`，返回 (name, subname)。Go `parseSectionHeader`。
fn parse_section_header(s: &str) -> Result<(String, String), String> {
    let bytes = s.as_bytes();
    if s.len() < 3 || bytes[s.len() - 1] != b']' {
        return Err(format!("invalid section header {s:?}"));
    }
    let inner = s[1..s.len() - 1].trim();
    if inner.is_empty() {
        return Err("empty section header".to_string());
    }
    if let Some(sp) = inner.find(' ') {
        // Go IndexByte(inner,' '); sp>0（inner 已 trim，不会前导空格）。
        if sp > 0 {
            let name = inner[..sp].trim();
            let rest = inner[sp + 1..].trim();
            let rb = rest.as_bytes();
            if rest.len() < 2 || rb[0] != b'"' || rb[rest.len() - 1] != b'"' {
                return Err(format!("subsection name must be quoted: {s:?}"));
            }
            let subname = &rest[1..rest.len() - 1];
            return Ok((name.to_string(), subname.to_string()));
        }
    }
    Ok((inner.to_string(), String::new()))
}

/// 解析 `key = value`；value 允许空。quoted value 自动 strip 首尾 `"`。Go `parseKeyValue`。
fn parse_key_value(s: &str) -> Result<(String, String), String> {
    let eq = s.find('=').ok_or_else(|| format!("missing '=' in {s:?}"))?;
    let key = s[..eq].trim();
    if key.is_empty() {
        return Err(format!("empty key in {s:?}"));
    }
    let mut value = s[eq + 1..].trim();
    let vb = value.as_bytes();
    if value.len() >= 2 && vb[0] == b'"' && vb[value.len() - 1] == b'"' {
        value = &value[1..value.len() - 1];
    }
    Ok((key.to_string(), value.to_string()))
}

// ---------------------------------------------------------------------------
// 标量转换助手（sink 共用；对应 Go setScalarField 各 case）
// ---------------------------------------------------------------------------

/// int 转换（Go `strconv.ParseInt(.,10,64)` + `OverflowInt`；空 → 0）。返回 `TypeErr` message 供 sink 透传。
///
/// **int 宽度对齐（部署目标 GOARCH=mips → int32）**：Go 把配置 int 字段（`Listen.Port`/
/// `Hass.Port` 等均 `int`）经 `ParseInt(.,10,64)` 解析后用 `field.OverflowInt(n)` 按字段类型
/// 校验，hAP 上 `int`=int32，故 `> i32` 的值 Go-MIPS **拒（startup 报错）**。Rust 字段存 `i64`，
/// 故显式加 i32 范围检查对齐 Go-MIPS。
pub fn conv_int(value: &str, key: &str) -> Result<i64, String> {
    if value.is_empty() {
        return Ok(0);
    }
    let n = value
        .parse::<i64>()
        .map_err(|_| format!("{key:?} expects int, got {value:?}"))?;
    // 对齐 Go-MIPS OverflowInt(int=int32)：超 int32 范围拒。
    if n < i32::MIN as i64 || n > i32::MAX as i64 {
        return Err(format!("{key:?} expects int, got {value:?}"));
    }
    Ok(n)
}

/// bool 转换：`true/1` → true，`false/0/空` → false，其它 → `TypeErr`（大小写不敏感）。
pub fn conv_bool(value: &str, key: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" | "" => Ok(false),
        _ => Err(format!(
            "{key:?} expects bool (true/false/1/0), got {value:?}"
        )),
    }
}

/// `a, b, c` → `["a","b","c"]`（逗号分隔 + trim 周围空白 + 跳过空段）。空 value → 空 vec。
pub fn conv_string_array(value: &str) -> Vec<String> {
    if value.is_empty() {
        return Vec::new();
    }
    value
        .split(',')
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .map(|p| p.to_string())
        .collect()
}

// ===========================================================================
// Config 作为 IniSink（显式 match 分发，去反射）
// ===========================================================================

impl IniSink for Config {
    fn set_top(&mut self, key: &str, value: &str) -> FieldOutcome {
        match key {
            "sip" => {
                self.sip = value.to_string();
                FieldOutcome::Set
            }
            "iface" => {
                self.iface = value.to_string();
                FieldOutcome::Set
            }
            "iface_list" => {
                self.iface_list = conv_string_array(value);
                FieldOutcome::Set
            }
            // struct field 当顶级标量 → 报错（Go setTopLevelField struct case）。
            "listen" | "hass" | "video" | "automation" => FieldOutcome::TypeErr(format!(
                "key {key:?} is a struct field, use [{key}] section"
            )),
            // slice-of-struct 当顶级标量 → 报错。
            "station" => FieldOutcome::TypeErr(format!(
                "key {key:?} is a slice-of-struct, use [{key} \"name\"] subsections"
            )),
            _ => FieldOutcome::Unknown,
        }
    }

    fn section_known(&self, section: &str) -> bool {
        matches!(
            section,
            "sip" | "iface" | "iface_list" | "listen" | "station" | "hass" | "video" | "automation"
        )
    }

    fn section_is_struct(&self, section: &str) -> bool {
        matches!(section, "listen" | "hass" | "video" | "automation")
    }

    fn section_is_slice(&self, section: &str) -> bool {
        section == "station"
    }

    fn set_section(&mut self, section: &str, key: &str, value: &str) -> FieldOutcome {
        match section {
            "listen" => match key {
                "addr" => {
                    self.listen.addr = value.to_string();
                    FieldOutcome::Set
                }
                "port" => match conv_int(value, key) {
                    Ok(n) => {
                        self.listen.port = n;
                        FieldOutcome::Set
                    }
                    Err(m) => FieldOutcome::TypeErr(m),
                },
                _ => FieldOutcome::Unknown,
            },
            "hass" => match key {
                "ipaddr" => {
                    self.hass.ipaddr = value.to_string();
                    FieldOutcome::Set
                }
                "port" => match conv_int(value, key) {
                    Ok(n) => {
                        self.hass.port = n;
                        FieldOutcome::Set
                    }
                    Err(m) => FieldOutcome::TypeErr(m),
                },
                "api" => {
                    self.hass.api = value.to_string();
                    FieldOutcome::Set
                }
                "token" => {
                    self.hass.token = value.to_string();
                    FieldOutcome::Set
                }
                _ => FieldOutcome::Unknown,
            },
            "video" => match key {
                "forward" => match conv_bool(value, key) {
                    Ok(b) => {
                        self.video.forward = b;
                        FieldOutcome::Set
                    }
                    Err(m) => FieldOutcome::TypeErr(m),
                },
                "format" => {
                    self.video.format = value.to_string();
                    FieldOutcome::Set
                }
                "cache_path" => {
                    self.video.cache_path = value.to_string();
                    FieldOutcome::Set
                }
                _ => FieldOutcome::Unknown,
            },
            "automation" => match key {
                "auto_unlock" => match conv_bool(value, key) {
                    Ok(b) => {
                        self.automation.auto_unlock = b;
                        FieldOutcome::Set
                    }
                    Err(m) => FieldOutcome::TypeErr(m),
                },
                "auto_hangup" => match conv_bool(value, key) {
                    Ok(b) => {
                        self.automation.auto_hangup = b;
                        FieldOutcome::Set
                    }
                    Err(m) => FieldOutcome::TypeErr(m),
                },
                // 旧 v0.1 残留整数 key（unlock/hangup/call_elev 等）→ 未知 key warning、不 fatal。
                _ => FieldOutcome::Unknown,
            },
            _ => FieldOutcome::Unknown,
        }
    }

    fn subsection_len(&self, section: &str) -> usize {
        match section {
            "station" => self.stations.len(),
            _ => 0,
        }
    }

    fn append_subsection(&mut self, section: &str) {
        if section == "station" {
            self.stations.push(Station::default());
        }
    }

    fn set_subsection(&mut self, section: &str, key: &str, value: &str) -> FieldOutcome {
        if section != "station" {
            return FieldOutcome::Unknown;
        }
        let last = match self.stations.last_mut() {
            Some(s) => s,
            None => return FieldOutcome::Unknown,
        };
        match key {
            "sip" => {
                last.sip = value.to_string();
                FieldOutcome::Set
            }
            "rtsp_url" => {
                last.rtsp_url = value.to_string();
                FieldOutcome::Set
            }
            _ => FieldOutcome::Unknown,
        }
    }
}

// ===========================================================================
// 默认值 / deprecated 检测 / 校验 / 读盘（Go config.go）
// ===========================================================================

impl Config {
    /// `MissingFields`：被 `apply_defaults` 替换为默认值的字段名（caller log warning）。
    pub fn missing_fields(&self) -> &[String] {
        &self.missing_fields
    }
    /// `DeprecatedFields`：存在但已被本版本忽略的字段名。
    pub fn deprecated_fields(&self) -> &[String] {
        &self.deprecated_fields
    }
    /// `ParserWarnings`：INI parser 报告的 unknown section / key。
    pub fn parser_warnings(&self) -> &[String] {
        &self.parser_warnings
    }

    /// 设置 parser warnings（`load_config` 内部用；测试组装时亦可调）。
    pub fn set_parser_warnings(&mut self, w: Vec<String>) {
        self.parser_warnings = w;
    }

    /// 填充缺省值并记录 `missing_fields`（Go `applyDefaults`）。
    pub fn apply_defaults(&mut self) {
        self.missing_fields.clear();

        if self.iface.is_empty() {
            self.iface = DEFAULT_IFACE.to_string();
            self.missing_fields.push("iface".to_string());
        }
        if self.listen.addr.is_empty() {
            self.listen.addr = DEFAULT_LISTEN_ADDR.to_string();
        }
        if self.listen.port == 0 {
            self.listen.port = DEFAULT_LISTEN_PORT;
        }
        if self.hass.port == 0 {
            self.hass.port = DEFAULT_HASS_PORT;
            self.missing_fields.push("hass.port".to_string());
        }
        // v0.3.0：video.format 缺省 flv。
        if self.video.format.is_empty() {
            self.video.format = "flv".to_string();
            self.missing_fields.push("video.format".to_string());
        }
    }

    /// 扫描原始 INI 检测已废弃顶级 section / key（Go `detectDeprecated`）。
    ///
    /// v0.1.6 移除：`brand` / `family` / `elev` / `notification`；`automation` 自
    /// formalize-daemon-auto-unlock §3.4 复活为正式 schema，**不**在此列表。
    ///
    /// 顶级 key 检测须跟踪 `current_section`：仅 `current_section == ""` 时识别顶级 key form；
    /// section 内的同名 key 是该 section 的 unknown key（由 parser warnings 处理）。
    pub fn detect_deprecated(&mut self, raw: &str) {
        const DEPRECATED: [&str; 4] = ["brand", "family", "elev", "notification"];
        let mut seen: Vec<&str> = Vec::new();
        let mut current_section = String::new();

        for raw_line in raw.split('\n') {
            // 去注释（; 或 # 都识别，不区分 quote——deprecated 检测精度不需那么高，与 Go 一致）。
            let mut s = raw_line;
            if let Some(i) = s.find([';', '#']) {
                s = &s[..i];
            }
            let s = s.trim();
            if s.is_empty() {
                continue;
            }
            let bytes = s.as_bytes();
            // 形式 1：[name] section header
            if bytes.len() >= 3 && bytes[0] == b'[' && bytes[s.len() - 1] == b']' {
                let mut inner = s[1..s.len() - 1].trim();
                // 去 subsection 部分仅留主 name
                if let Some(sp) = inner.find(' ') {
                    if sp > 0 {
                        inner = inner[..sp].trim();
                    }
                }
                current_section = inner.to_string();
                for k in DEPRECATED {
                    if inner == k && !seen.contains(&k) {
                        seen.push(k);
                        self.deprecated_fields.push(k.to_string());
                    }
                }
                continue;
            }
            // 形式 2：顶级 key = value（仅 current_section == "" 时识别）
            if current_section.is_empty() {
                if let Some(eq) = s.find('=') {
                    if eq > 0 {
                        let key = s[..eq].trim();
                        for k in DEPRECATED {
                            if key == k && !seen.contains(&k) {
                                seen.push(k);
                                self.deprecated_fields.push(k.to_string());
                            }
                        }
                    }
                }
            }
        }
    }

    /// 校验已加载配置（Go `Validate`）：sip / stations[i].sip URI 结构 + video.format 白名单。
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.sip.is_empty() {
            return Err(ConfigError::Validation(ValidationError {
                field: "sip".to_string(),
                value: String::new(),
                want: "non-empty SIP URI like 12345678@10.0.0.20:18022".to_string(),
            }));
        }
        if let Err(e) = validate_uri(&self.sip) {
            return Err(ConfigError::Validation(ValidationError {
                field: "sip".to_string(),
                value: self.sip.clone(),
                want: format!("valid SIP URI: {e}"),
            }));
        }
        if self.stations.is_empty() {
            return Err(ConfigError::Validation(ValidationError {
                field: "stations".to_string(),
                value: "0".to_string(),
                want: "at least one station entry".to_string(),
            }));
        }
        for (i, s) in self.stations.iter().enumerate() {
            if s.sip.is_empty() {
                return Err(ConfigError::Validation(ValidationError {
                    field: format!("stations[{i}].sip"),
                    value: String::new(),
                    want: "non-empty SIP URI".to_string(),
                }));
            }
            if let Err(e) = validate_uri(&s.sip) {
                return Err(ConfigError::Validation(ValidationError {
                    field: format!("stations[{i}].sip"),
                    value: s.sip.clone(),
                    want: format!("valid SIP URI: {e}"),
                }));
            }
        }
        match self.video.format.as_str() {
            "flv" | "mjpeg" => {}
            _ => {
                return Err(ConfigError::Validation(ValidationError {
                    field: "video.format".to_string(),
                    value: self.video.format.clone(),
                    want: "\"flv\" or \"mjpeg\"".to_string(),
                }));
            }
        }
        Ok(())
    }
}

/// 从 path 读盘 + 解析 + 默认值 + deprecated 检测 + 校验（Go `LoadConfig`）。
///
/// `std::fs` 读盘属确定性范围 IN（design D3）。
pub fn load_config(path: &str) -> Result<Config, ConfigError> {
    let raw = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(ConfigError::NotFound {
                path: path.to_string(),
            });
        }
        Err(e) => {
            return Err(ConfigError::Read {
                path: path.to_string(),
                msg: e.to_string(),
            });
        }
    };
    // Go bufio.Scanner 对非 UTF-8 字节按原样保留；config 实际是 ASCII/UTF-8。非法 UTF-8 视为读错。
    let text = match String::from_utf8(raw) {
        Ok(t) => t,
        Err(e) => {
            return Err(ConfigError::Read {
                path: path.to_string(),
                msg: format!("config not valid UTF-8: {e}"),
            });
        }
    };

    let mut cfg = Config::default();
    let warnings = parse_ini(&text, &mut cfg)?;
    cfg.parser_warnings = warnings;

    cfg.apply_defaults();
    cfg.detect_deprecated(&text);
    cfg.validate()?;

    Ok(cfg)
}

// ===========================================================================
// validate_uri（手写字符扫描，Go config.go validateURI——与 codec.parse_uri 两套）
// ===========================================================================

/// 校验 SIP URI 结构：`<8 位 hex 号码>@<IPv4>:<port>`。
///
/// **手写实现**（不依赖 codec），与 Go `config.validateURI` 字符扫描逐行对齐：
///   - 名恰好 8 字符且全 hex（`0-9a-fA-F`）。
///   - IPv4：4 段 0-255，**拒前导零**（CVE-2021-29923 / Go 1.17+ 对齐）；只收 `[0-9.]`
///     字符 → 天然**拒 IPv4-mapped IPv6**（`::ffff:1.2.3.4` 含 `:` 直接判非 IPv4）。
///   - port 1-65535 整数。
///
/// 返回 `Err(message)`——分类只需 accept/reject（Go `validateURI` 本身不用 sentinel）。
pub fn validate_uri(uri: &str) -> Result<(), String> {
    // '@'：第一个；位置须 0 < at < len-1。
    let at = match uri.find('@') {
        Some(i) => i,
        None => return Err("missing '@'".to_string()),
    };
    if at == 0 || at == uri.len() - 1 {
        return Err("missing '@'".to_string());
    }
    let name = &uri[..at];
    let ip_port = &uri[at + 1..];

    if name.len() != 8 {
        return Err(format!("name {name:?} must be 8 chars"));
    }
    for c in name.chars() {
        if !c.is_ascii_hexdigit() {
            return Err(format!("name {name:?} contains non-hex char {c:?}"));
        }
    }

    // ':'：最后一个；位置须 0 < colon < len-1。
    let ipb = ip_port.as_bytes();
    let mut colon: isize = -1;
    for i in (0..ip_port.len()).rev() {
        if ipb[i] == b':' {
            colon = i as isize;
            break;
        }
    }
    if colon <= 0 || colon as usize == ip_port.len() - 1 {
        return Err("missing ':port'".to_string());
    }
    let colon = colon as usize;
    let ip = &ip_port[..colon];
    let port_str = &ip_port[colon + 1..];

    // IPv4 字面：4 段 0-255，禁 leading-zero；只收 [0-9.]（含 ':' 的 v6-mapped 在此判非 IPv4）。
    let mut parts = 0;
    let mut cur: u32 = 0;
    let mut octet_len = 0;
    let mut leading_zero = false;
    for &c in ip.as_bytes() {
        if c == b'.' {
            if octet_len == 0 || cur > 255 || leading_zero {
                return Err(format!("ip {ip:?} is not IPv4"));
            }
            parts += 1;
            cur = 0;
            octet_len = 0;
            leading_zero = false;
            continue;
        }
        if !c.is_ascii_digit() {
            return Err(format!("ip {ip:?} is not IPv4"));
        }
        // 第一位是 '0' 且后面还有数字 → leading zero。
        if octet_len == 1 && cur == 0 {
            leading_zero = true;
        }
        cur = cur * 10 + (c - b'0') as u32;
        octet_len += 1;
        // 数字循环内即拒 > 255，防累加溢出（Rust debug 构建会 panic；release `u32` 回绕到
        // 巨值仍 > 255 拒）。等价 Go-HOST(int64) 行为（亦拒）。注：Go-MIPS(int32) 对超长 octet
        // 会回绕到负值误「接受」——那是 int32 wrap bug、IP 之后 dial 仍失败；Rust 在此提前拒
        // 是更安全的有意分叉（避免 panic + 匹配 Go-host 语义）。
        if cur > 255 {
            return Err(format!("ip {ip:?} is not IPv4"));
        }
    }
    if octet_len == 0 || cur > 255 || leading_zero {
        return Err(format!("ip {ip:?} is not IPv4"));
    }
    if parts != 3 {
        return Err(format!("ip {ip:?} is not IPv4 (need 4 octets)"));
    }

    // port 1-65535。
    let mut port: u32 = 0;
    for &c in port_str.as_bytes() {
        if !c.is_ascii_digit() {
            return Err(format!("port {port_str:?} not numeric"));
        }
        port = port * 10 + (c - b'0') as u32;
        if port > 65535 {
            return Err(format!("port {port_str:?} out of range"));
        }
    }
    if port < 1 {
        return Err(format!("port {port_str:?} out of range"));
    }
    Ok(())
}
