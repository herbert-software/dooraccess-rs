// golden parity 回归：automation_state parse + render，对照 committed 向量
// testdata/golden/automation_state.txt（SoT = Go local/dooraccess-go internal/automationstate）。
//
// 断言粒度（per spec rust-protocol-core / D1）：
//   - render 逐字节精确相等。
//   - parse_ok：State 两 bool 逐字段相等。
//   - parse_err：parse 返 ParseError（整文件丢弃分类与 Go ErrParse 对应，sentinel 级，
//     不断言 message 字面）；覆盖缺 key / 未知 key / 非法 bool / 缺 = / 空 / 半写各路径。

use dooraccess_rs::automation_state::{parse, render, ParseError, State};

const GOLDEN: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/testdata/golden/automation_state.txt"
));

#[derive(Default, Debug)]
struct Case {
    name: String,
    kind: String,
    input_hex: Option<String>,
    expect: Option<String>,
    output_hex: Option<String>,
    input: Option<String>,
}

fn parse_golden(text: &str) -> Vec<Case> {
    let mut cases = Vec::new();
    let mut cur: Option<Case> = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("### CASE|") {
            cur = Some(Case {
                name: rest.trim().to_string(),
                ..Default::default()
            });
        } else if line.starts_with("### END") {
            if let Some(c) = cur.take() {
                cases.push(c);
            }
        } else if let Some(c) = cur.as_mut() {
            if let Some(v) = line.strip_prefix("KIND|") {
                c.kind = v.trim().to_string();
            } else if let Some(v) = line.strip_prefix("INPUT_HEX|") {
                c.input_hex = Some(v.trim().to_string());
            } else if let Some(v) = line.strip_prefix("EXPECT|") {
                c.expect = Some(v.trim().to_string());
            } else if let Some(v) = line.strip_prefix("OUTPUT_HEX|") {
                c.output_hex = Some(v.trim().to_string());
            } else if let Some(v) = line.strip_prefix("INPUT|") {
                c.input = Some(v.trim().to_string());
            }
        }
        // file-level header lines (single '#') outside a case are ignored.
    }
    cases
}

fn hex_decode(s: &str) -> Vec<u8> {
    let s = s.trim();
    assert!(s.len() % 2 == 0, "odd-length hex: {s:?}");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("valid hex"))
        .collect()
}

// 解析 "auto_unlock=true,auto_hangup=false" 形式（EXPECT / INPUT 共用）。
fn parse_state_spec(s: &str) -> State {
    let mut auto_unlock = None;
    let mut auto_hangup = None;
    for kv in s.split(',') {
        let (k, v) = kv.split_once('=').expect("k=v");
        let b = match v.trim() {
            "true" => true,
            "false" => false,
            other => panic!("bad bool in spec: {other:?}"),
        };
        match k.trim() {
            "auto_unlock" => auto_unlock = Some(b),
            "auto_hangup" => auto_hangup = Some(b),
            other => panic!("bad key in spec: {other:?}"),
        }
    }
    State {
        auto_unlock: auto_unlock.expect("auto_unlock"),
        auto_hangup: auto_hangup.expect("auto_hangup"),
    }
}

#[test]
fn golden_automation_state() {
    let cases = parse_golden(GOLDEN);
    assert!(!cases.is_empty(), "no golden cases parsed");

    let mut n_ok = 0;
    let mut n_err = 0;
    let mut n_render = 0;

    for c in &cases {
        match c.kind.as_str() {
            "parse_ok" => {
                let raw = hex_decode(c.input_hex.as_ref().expect("INPUT_HEX"));
                let got = parse(&raw)
                    .unwrap_or_else(|_| panic!("case {}: expected Ok, got ParseError", c.name));
                let want = parse_state_spec(c.expect.as_ref().expect("EXPECT"));
                assert_eq!(got, want, "case {}: state mismatch", c.name);
                n_ok += 1;
            }
            "parse_err" => {
                assert_eq!(
                    c.expect.as_deref(),
                    Some("ErrParse"),
                    "case {}: unexpected EXPECT for parse_err",
                    c.name
                );
                let raw = hex_decode(c.input_hex.as_deref().unwrap_or(""));
                let got = parse(&raw);
                // sentinel 级分类：整文件丢弃 → Err(ParseError)，与 Go ErrParse 对应。
                assert_eq!(
                    got,
                    Err(ParseError),
                    "case {}: expected ParseError (whole-file discard), got {:?}",
                    c.name,
                    got
                );
                n_err += 1;
            }
            "render" => {
                let st = parse_state_spec(c.input.as_ref().expect("INPUT"));
                let want = hex_decode(c.output_hex.as_ref().expect("OUTPUT_HEX"));
                let got = render(st);
                assert_eq!(got, want, "case {}: render byte mismatch", c.name);
                n_render += 1;
            }
            other => panic!("case {}: unknown KIND {other:?}", c.name),
        }
    }

    // 确保各分支都被向量覆盖（防 golden 退化成只测一条路径）。
    assert!(n_ok > 0, "no parse_ok cases");
    assert!(n_err > 0, "no parse_err cases");
    assert!(n_render > 0, "no render cases");
}

// 显式针对各丢弃路径的分类断言（不依赖 golden 文件内容的额外保险）：
// 缺 key / 未知 key / 非法 bool / 缺 = / 空 都归 ParseError sentinel。
#[test]
fn parse_discard_paths_are_errparse() {
    // 缺 key（只一个）
    assert_eq!(parse(b"auto_unlock=true\n"), Err(ParseError));
    // 未知 key
    assert_eq!(
        parse(b"auto_unlock=true\nauto_hangup=false\nring_at=1\n"),
        Err(ParseError)
    );
    // 非法 bool
    assert_eq!(
        parse(b"auto_unlock=maybe\nauto_hangup=false\n"),
        Err(ParseError)
    );
    // 缺 =
    assert_eq!(
        parse(b"auto_unlock true\nauto_hangup=false\n"),
        Err(ParseError)
    );
    // 空文件
    assert_eq!(parse(b""), Err(ParseError));
    // 半写
    assert_eq!(parse(b"auto_unlock=tru"), Err(ParseError));
}

// parse_ok：注释/空行跳过 + 大小写不敏感 + 数字 bool + 行序无关。
#[test]
fn parse_ok_comments_and_bool_forms() {
    // key 精确匹配（与 Go 一致：switch key{case "auto_hangup"} 大小写敏感）；
    // 仅 VALUE 大小写不敏感（Go parseBool 对 value 做 ToLower）。故 key 小写、value 用大写 TRUE 验值大小写不敏感。
    let raw = b"; daemon-managed\n# header\n\nauto_hangup=TRUE\nauto_unlock=0\n";
    assert_eq!(
        parse(raw),
        Ok(State {
            auto_unlock: false,
            auto_hangup: true
        })
    );
}

// render 确定性两行顺序。
#[test]
fn render_fixed_two_lines() {
    assert_eq!(
        render(State {
            auto_unlock: true,
            auto_hangup: false
        }),
        b"auto_unlock=true\nauto_hangup=false\n"
    );
}
