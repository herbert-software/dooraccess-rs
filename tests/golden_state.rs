// golden parity 回归：automation_state parse + render，对照 committed golden 向量
// testdata/golden/automation_state.txt。
//
// 断言粒度：
//   - render 逐字节精确相等。
//   - parse_ok：State 两 bool 逐字段相等。
//   - parse_err：parse 返 ParseError（整文件丢弃分类，sentinel 级，
//     不断言 message 字面）；覆盖缺 key / 未知 key / 非法 bool / 缺 = / 空 / 半写各路径。

use dooraccess_rs::automation_state::{parse, render, ParseError, State};
use dooraccess_rs::automation_state::{write_atomic, Persister};

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
    assert!(s.len().is_multiple_of(2), "odd-length hex: {s:?}");
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
                // sentinel 级分类：整文件丢弃 → Err(ParseError)。
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
    // key 精确匹配（大小写敏感）；仅 VALUE 大小写不敏感（bool 解析对 value 做小写化）。
    // 故 key 小写、value 用大写 TRUE 验值大小写不敏感。
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

// ===========================================================================
// Persister / write_atomic
// ===========================================================================

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// 进程内唯一临时目录（避免引第三方 tempdir crate；守 crate gate std+libc only）。
fn unique_tmp_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    let mut d = std::env::temp_dir();
    d.push(format!("dooraccess_rs_persister_{tag}_{pid}_{n}"));
    std::fs::create_dir_all(&d).expect("create temp dir");
    d
}

// write_atomic：temp 与目标同目录、产出 = render 字节、无残留 .tmp。
#[test]
fn write_atomic_produces_render_bytes_no_tmp_residue() {
    let dir = unique_tmp_dir("wa");
    let path = dir.join("automation.state");
    write_atomic(
        &path,
        State {
            auto_unlock: true,
            auto_hangup: false,
        },
    )
    .expect("write_atomic ok");

    let got = std::fs::read(&path).expect("read back");
    assert_eq!(
        got,
        render(State {
            auto_unlock: true,
            auto_hangup: false
        })
    );
    // rename 成功后无残留 .tmp。
    let tmp = dir.join("automation.state.tmp");
    assert!(!tmp.exists(), "no residual .tmp after successful rename");

    let _ = std::fs::remove_dir_all(&dir);
}

// 拨动后原子落盘 + 重启回读一致。
#[test]
fn persist_writes_then_reload_reads_back() {
    let dir = unique_tmp_dir("rt");
    let path = dir.join("automation.state");

    let au = Arc::new(AtomicBool::new(true));
    let ah = Arc::new(AtomicBool::new(false));
    let (au_c, ah_c) = (au.clone(), ah.clone());
    let p = Persister::new(
        path.clone(),
        Box::new(move || (au_c.load(Ordering::SeqCst), ah_c.load(Ordering::SeqCst))),
        None,
    );

    p.persist();
    // 重启回读：从盘上 parse 回与拨动值一致。
    let raw = std::fs::read(&path).expect("file written");
    assert_eq!(
        parse(&raw),
        Ok(State {
            auto_unlock: true,
            auto_hangup: false
        })
    );

    // 翻转 auto_hangup → 再 persist → 盘上反映新值（无半写：parse 成功且为新值）。
    ah.store(true, Ordering::SeqCst);
    p.persist();
    let raw = std::fs::read(&path).expect("file written");
    assert_eq!(
        parse(&raw),
        Ok(State {
            auto_unlock: true,
            auto_hangup: true
        })
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// 纯值去重：同值重复 persist 是 no-op（不重写文件）。
//
// 侧证手法：首次 persist 写文件 → 删文件 → 同值再 persist。若去重生效（no-op），文件
// **不会**被重建；若去重失效则文件被重写出现。再翻转值 persist → 文件应重新出现（验
// 翻转仍写）。
#[test]
fn persist_same_value_is_noop() {
    let dir = unique_tmp_dir("dedup");
    let path = dir.join("automation.state");

    let au = Arc::new(AtomicBool::new(true));
    let ah = Arc::new(AtomicBool::new(false));
    let (au_c, ah_c) = (au.clone(), ah.clone());
    let p = Persister::new(
        path.clone(),
        Box::new(move || (au_c.load(Ordering::SeqCst), ah_c.load(Ordering::SeqCst))),
        None,
    );

    // 首次：写盘。
    p.persist();
    assert!(path.exists(), "first persist writes file");

    // 删文件，同值再 persist → no-op，不重建。
    std::fs::remove_file(&path).expect("remove");
    p.persist();
    assert!(
        !path.exists(),
        "same-value persist must be no-op (file not recreated)"
    );

    // 翻转值 → persist 应重新落盘。
    au.store(false, Ordering::SeqCst);
    p.persist();
    assert!(path.exists(), "flipped value persist writes file");
    assert_eq!(
        parse(&std::fs::read(&path).unwrap()),
        Ok(State {
            auto_unlock: false,
            auto_hangup: false
        })
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// 落盘失败 best-effort：目录不可写（path 指向不存在的子目录）→ persist 不 panic、不阻塞，
// log hook 收到一行错误。
#[test]
fn persist_write_failure_is_best_effort() {
    let dir = unique_tmp_dir("fail");
    // 指向不存在的子目录下的文件 → write 必失败（父目录不存在）。
    let path = dir.join("nonexistent_subdir").join("automation.state");

    let logged = Arc::new(AtomicUsize::new(0));
    let l_c = logged.clone();
    let p = Persister::new(
        path,
        Box::new(|| (true, true)),
        Some(Box::new(move |msg: &str| {
            assert!(msg.contains("persist failed"), "log msg: {msg}");
            l_c.fetch_add(1, Ordering::SeqCst);
        })),
    );

    // 不应 panic。
    p.persist();
    assert_eq!(
        logged.load(Ordering::SeqCst),
        1,
        "write failure must log one warning"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
