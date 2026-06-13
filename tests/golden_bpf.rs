//! BPF filter golden parity 回归。
//!
//! 读 committed golden 向量 `testdata/golden/bpf.txt`（每行一条 BPF 指令），对 Rust 烤好的
//! `bpf::BPF_6672` / `bpf::BPF_18022` const 逐字段（op / jt / jf / k）断言相等。
//!
//! 若 Rust const 与 golden 向量不一致 → const 烤错（真 bug），本测试 fail。
//!
//! 向量已 committed，本测试只读静态文件。

use std::fs;
use std::path::PathBuf;

use dooraccess_rs::bpf::{BPF_18022, BPF_6672};
use dooraccess_rs::ffi::SockFilter;

fn load_golden() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/golden/bpf.txt");
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {:?}: {}", path, e))
}

/// 一条 golden 指令行：`<name>|<idx>|<op_hex>|<jt>|<jf>|<k_hex>`。
struct GoldenInst {
    name: String,
    idx: usize,
    op: u16,
    jt: u8,
    jf: u8,
    k: u32,
}

fn parse_line(line: &str) -> GoldenInst {
    let f: Vec<&str> = line.split('|').collect();
    assert_eq!(f.len(), 6, "bpf line must have 6 fields: {:?}", line);
    GoldenInst {
        name: f[0].to_string(),
        idx: f[1]
            .parse()
            .unwrap_or_else(|_| panic!("bad idx: {:?}", line)),
        op: u16::from_str_radix(f[2], 16).unwrap_or_else(|_| panic!("bad op: {:?}", line)),
        jt: f[3]
            .parse()
            .unwrap_or_else(|_| panic!("bad jt: {:?}", line)),
        jf: f[4]
            .parse()
            .unwrap_or_else(|_| panic!("bad jf: {:?}", line)),
        k: u32::from_str_radix(f[5], 16).unwrap_or_else(|_| panic!("bad k: {:?}", line)),
    }
}

fn assert_inst_eq(actual: &SockFilter, g: &GoldenInst) {
    assert_eq!(
        actual.code, g.op,
        "{} [{}] op: rust 0x{:04x} != go 0x{:04x}",
        g.name, g.idx, actual.code, g.op
    );
    assert_eq!(
        actual.jt, g.jt,
        "{} [{}] jt: rust {} != go {}",
        g.name, g.idx, actual.jt, g.jt
    );
    assert_eq!(
        actual.jf, g.jf,
        "{} [{}] jf: rust {} != go {}",
        g.name, g.idx, actual.jf, g.jf
    );
    assert_eq!(
        actual.k, g.k,
        "{} [{}] k: rust 0x{:08x} != go 0x{:08x}",
        g.name, g.idx, actual.k, g.k
    );
}

/// Rust const 对 `bpf.txt` 逐字段断言相等。
#[test]
fn golden_bpf_parity() {
    let text = load_golden();

    let mut n6672 = 0usize;
    let mut n18022 = 0usize;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let g = parse_line(line);
        let prog: &[SockFilter] = match g.name.as_str() {
            "bpf6672" => &BPF_6672,
            "bpf18022" => &BPF_18022,
            other => panic!("unknown bpf golden name {:?} in {:?}", other, line),
        };
        assert!(
            g.idx < prog.len(),
            "{} golden idx {} out of bounds (rust len {})",
            g.name,
            g.idx,
            prog.len()
        );
        assert_inst_eq(&prog[g.idx], &g);
        match g.name.as_str() {
            "bpf6672" => n6672 += 1,
            "bpf18022" => n18022 += 1,
            _ => unreachable!(),
        }
    }

    // 长度一致 + 防 golden 被悄悄清空导致空跑假绿。
    assert_eq!(
        n6672,
        BPF_6672.len(),
        "bpf6672 golden line count != const len"
    );
    assert_eq!(
        n18022,
        BPF_18022.len(),
        "bpf18022 golden line count != const len"
    );
    assert_eq!(BPF_6672.len(), 11, "BPF_6672 must be 11 insts");
    assert_eq!(BPF_18022.len(), 13, "BPF_18022 must be 13 insts");
}

/// sanity：每条 jump 的 jt/jf 偏移 ≤ len-1-idx（不越界）。
/// 与 bpf.rs 内单测重复一份，独立于 const 内联路径，防 golden 解析失效时仍有结构校验。
#[test]
fn golden_bpf_jumps_in_bounds() {
    for (name, prog) in [("bpf6672", &BPF_6672[..]), ("bpf18022", &BPF_18022[..])] {
        let len = prog.len();
        for (idx, f) in prog.iter().enumerate() {
            // BPF jump class = 0x05（低 3 位）；RET(0x06) 等非 jump 不用 jt/jf 作偏移。
            if (f.code & 0x07) == 0x05 {
                let max_off = (len - 1 - idx) as u8;
                assert!(
                    f.jt <= max_off,
                    "{name} idx {idx}: jt={} exceeds bound {max_off}",
                    f.jt
                );
                assert!(
                    f.jf <= max_off,
                    "{name} idx {idx}: jf={} exceeds bound {max_off}",
                    f.jf
                );
            }
        }
    }
}
