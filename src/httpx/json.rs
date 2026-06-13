//! 手写 JSON encoder，固定五项隐式字节行为（0-crate）。
//!
//! - map 路径（hapush）：key 字母序
//! - struct 路径（writeJSON / RenderJSON）：key 声明序
//! - 空 slice → `[]` 非 `null`
//! - Marshal 无尾随 `\n`；Encoder（/info）有尾随 `\n`
//! - HTML 转义可切换

use std::collections::BTreeMap;

/// JSON 值。
#[derive(Debug, Clone, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(i64),
    String(String),
    /// 数组；空 vec 编码为 `[]` 非 `null`。
    Array(Vec<JsonValue>),
    /// struct 路径嵌套对象：key 按声明序。
    Struct(Vec<(String, JsonValue)>),
    /// map 路径嵌套对象：key 按字母序（`BTreeMap` 有序）。
    Map(BTreeMap<String, JsonValue>),
}

/// 编码选项（marshal vs encode 两种模式的字节差异）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonOptions {
    /// `true` = HTML 转义（writeJSON / hapush）；`false` = 不转义 HTML（/info）。
    pub escape_html: bool,
    /// `true` = 尾随 `\n`（/info）；`false` = 无尾随换行。
    pub trailing_newline: bool,
}

impl JsonOptions {
    /// writeJSON / hapush：HTML 转义 + 无尾随换行。
    pub const MARSHAL: Self = Self {
        escape_html: true,
        trailing_newline: false,
    };

    /// RenderJSON /info：不转义 HTML + 尾随 `\n`。
    pub const ENCODE: Self = Self {
        escape_html: false,
        trailing_newline: true,
    };
}

/// struct 路径：按字段声明序输出 key（非字母序）。
pub fn encode_struct(fields: &[(&str, JsonValue)], opts: JsonOptions) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'{');
    for (i, (k, v)) in fields.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        write_string(&mut out, k, opts.escape_html);
        out.push(b':');
        write_value(&mut out, v, opts);
    }
    out.push(b'}');
    if opts.trailing_newline {
        out.push(b'\n');
    }
    out
}

/// map 路径：key 按字母序输出（hapush body）。
pub fn encode_map(fields: &BTreeMap<String, JsonValue>, opts: JsonOptions) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'{');
    for (i, (k, v)) in fields.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        write_string(&mut out, k, opts.escape_html);
        out.push(b':');
        write_value(&mut out, v, opts);
    }
    out.push(b'}');
    if opts.trailing_newline {
        out.push(b'\n');
    }
    out
}

/// 通用入口：按 `JsonValue` 编码（无对象 key 序语义，仅供标量/数组）。
pub fn encode_value(value: &JsonValue, opts: JsonOptions) -> Vec<u8> {
    let mut out = Vec::new();
    write_value(&mut out, value, opts);
    if opts.trailing_newline {
        out.push(b'\n');
    }
    out
}

fn write_value(out: &mut Vec<u8>, value: &JsonValue, opts: JsonOptions) {
    match value {
        JsonValue::Null => out.extend_from_slice(b"null"),
        JsonValue::Bool(true) => out.extend_from_slice(b"true"),
        JsonValue::Bool(false) => out.extend_from_slice(b"false"),
        JsonValue::Number(n) => {
            let s = n.to_string();
            out.extend_from_slice(s.as_bytes());
        }
        JsonValue::String(s) => write_string(out, s, opts.escape_html),
        JsonValue::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_value(out, item, opts);
            }
            out.push(b']');
        }
        JsonValue::Struct(fields) => {
            out.push(b'{');
            for (i, (k, v)) in fields.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_string(out, k, opts.escape_html);
                out.push(b':');
                write_value(out, v, opts);
            }
            out.push(b'}');
        }
        JsonValue::Map(fields) => {
            out.push(b'{');
            for (i, (k, v)) in fields.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_string(out, k, opts.escape_html);
                out.push(b':');
                write_value(out, v, opts);
            }
            out.push(b'}');
        }
    }
}

fn write_string(out: &mut Vec<u8>, s: &str, escape_html: bool) {
    out.push(b'"');
    for ch in s.chars() {
        match ch {
            '"' => out.extend_from_slice(br#"\""#),
            '\\' => out.extend_from_slice(br#"\\"#),
            '\n' => out.extend_from_slice(br#"\n"#),
            '\r' => out.extend_from_slice(br#"\r"#),
            '\t' => out.extend_from_slice(br#"\t"#),
            '\x08' => out.extend_from_slice(br#"\b"#),
            '\x0c' => out.extend_from_slice(br#"\f"#),
            // 只转义 C0 控制符（U+0000–U+001F）；不转义 DEL(0x7F) 与 C1(0x80–0x9F)。
            // Rust `char::is_control()` 含 0x7F–0x9F（Unicode Cc）范围过宽，
            // 故用 `< 0x20` 精确界定。
            c if (c as u32) < 0x20 => write_unicode_escape(out, c as u32),
            '<' if escape_html => out.extend_from_slice(br#"\u003c"#),
            '>' if escape_html => out.extend_from_slice(br#"\u003e"#),
            '&' if escape_html => out.extend_from_slice(br#"\u0026"#),
            // 无条件（两种 escape_html 模式都）把 U+2028/U+2029 转义
            // （JSONP / JS 字符串安全），不受 escape_html 开关影响。
            '\u{2028}' => write_unicode_escape(out, 0x2028),
            '\u{2029}' => write_unicode_escape(out, 0x2029),
            c => {
                let mut buf = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
    out.push(b'"');
}

fn write_unicode_escape(out: &mut Vec<u8>, code: u32) {
    use std::fmt::Write as _;
    let mut buf = String::with_capacity(6);
    write!(&mut buf, "\\u{:04x}", code).unwrap();
    out.extend_from_slice(buf.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_keys_alphabetical() {
        let mut m = BTreeMap::new();
        m.insert("event".into(), JsonValue::String("x".into()));
        m.insert("video_forward".into(), JsonValue::String("1".into()));
        m.insert("video_format".into(), JsonValue::String("2".into()));
        m.insert("wwan".into(), JsonValue::String("3".into()));
        let out = encode_map(&m, JsonOptions::MARSHAL);
        let s = String::from_utf8(out).unwrap();
        assert!(s.starts_with(r#"{"event":"x","video_format":"2","video_forward":"1","wwan":"3"}"#));
    }

    #[test]
    fn struct_keys_declaration_order() {
        let out = encode_struct(
            &[
                ("result", JsonValue::Number(0)),
                ("auto_unlock", JsonValue::Bool(true)),
            ],
            JsonOptions::MARSHAL,
        );
        assert_eq!(
            String::from_utf8(out).unwrap(),
            r#"{"result":0,"auto_unlock":true}"#
        );
    }

    #[test]
    fn empty_array_not_null() {
        let out = encode_struct(
            &[("outdoor_stations", JsonValue::Array(vec![]))],
            JsonOptions::ENCODE,
        );
        assert!(out.starts_with(br#"{"outdoor_stations":[]}"#));
        assert!(out.ends_with(b"\n"));
    }

    #[test]
    fn trailing_newline_modes() {
        let fields = &[("result", JsonValue::Number(0))];
        let marshal = encode_struct(fields, JsonOptions::MARSHAL);
        assert!(!marshal.ends_with(b"\n"));
        let encode = encode_struct(fields, JsonOptions::ENCODE);
        assert!(encode.ends_with(b"\n"));
    }

    #[test]
    fn escape_edge_cases_match_go() {
        // 实测基线：
        // U+2028/U+2029 两模式都转  / ；0x7F(DEL) 不转；< 0x20 转 \u00xx。
        // U+2028 在 MARSHAL 与 ENCODE 模式都转义（不受 escape_html 开关影响）。
        let v = JsonValue::String("\u{2028}\u{2029}".into());
        assert_eq!(
            String::from_utf8(encode_value(&v, JsonOptions::MARSHAL)).unwrap(),
            "\"\\u2028\\u2029\""
        );
        assert_eq!(
            String::from_utf8(encode_value(&v, JsonOptions::ENCODE)).unwrap(),
            "\"\\u2028\\u2029\"\n"
        );
        // DEL 0x7F 不转义（原样保留）；C0 控制符 0x1F 转 。
        let del = JsonValue::String("a\u{7f}b".into());
        assert_eq!(
            String::from_utf8(encode_value(&del, JsonOptions::MARSHAL)).unwrap(),
            "\"a\u{7f}b\""
        );
        let ctrl = JsonValue::String("\u{1f}".into());
        assert_eq!(
            String::from_utf8(encode_value(&ctrl, JsonOptions::MARSHAL)).unwrap(),
            "\"\\u001f\""
        );
    }

    #[test]
    fn html_escape_toggle() {
        let v = JsonValue::String("<>&".into());
        let escaped = encode_value(&v, JsonOptions::MARSHAL);
        assert_eq!(
            String::from_utf8(escaped).unwrap(),
            r#""\u003c\u003e\u0026""#
        );
        let plain = encode_value(&v, JsonOptions::ENCODE);
        assert_eq!(String::from_utf8(plain).unwrap(), "\"<>&\"\n");
    }
}
