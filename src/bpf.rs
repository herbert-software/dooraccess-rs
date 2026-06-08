//! 烤好的 BPF filter 常量（组 A，task 6.1）。
//!
//! 两个 filter 静态（"udp dst port 6672" / "tcp port 18022"），Go 运行时
//! `bpf.Assemble`（listener.go buildBPFFilter）是纯浪费——这里烤成 `const [SockFilter; N]`，
//! **不移植汇编器**（design D-D）。
//!
//! **真相来源（SoT）= Go `bpf.Assemble()` 导出 golden**。下面的字节值由
//! `golang.org/x/net/bpf.Assemble` 对 `listen6672/listener.go buildBPFFilter`(126 起) 与
//! `listen18022/listener.go buildBPFFilter`(224 起) 的 `[]bpf.Instruction` 逐指令汇编得到
//! （2026-06-08 经 scratch dump 验证）。后续组 E 会用 `testdata/golden/bpf.txt` golden 逐字段
//! 验证字节一致；改 filter 须先改 Go（有 Assemble 越界兜底）再重导。
//!
//! 字段：`code:u16, jt:u8, jf:u8, k:u32`（= `libc::sock_filter`）。

use crate::ffi::{SockFilter, SockFprog};

/// 构造一条 `SockFilter`（const 友好）。
const fn ins(code: u16, jt: u8, jf: u8, k: u32) -> SockFilter {
    SockFilter { code, jt, jf, k }
}

/// "udp dst port 6672" filter（11 条）。
///
/// 对照 Go `listen6672 buildBPFFilter` → `bpf.Assemble` 输出：
/// ```text
/// [ 0] op=0x0028 jt=0 jf=0 k=0x0000000c   LoadAbsolute off=12 size=2 (ethertype)
/// [ 1] op=0x0015 jt=0 jf=8 k=0x00000800   JEQ 0x0800 ? next : drop(10)
/// [ 2] op=0x0030 jt=0 jf=0 k=0x00000017   LoadAbsolute off=23 size=1 (ip proto)
/// [ 3] op=0x0015 jt=0 jf=6 k=0x00000011   JEQ 17 ? next : drop(10)
/// [ 4] op=0x0028 jt=0 jf=0 k=0x00000014   LoadAbsolute off=20 size=2 (flag+frag)
/// [ 5] op=0x0045 jt=4 jf=0 k=0x00003fff   JSET 0x3fff ? drop(10) : next
/// [ 6] op=0x00b1 jt=0 jf=0 k=0x0000000e   LoadMemShift off=14 (X=IHL*4)
/// [ 7] op=0x0048 jt=0 jf=0 k=0x00000010   LoadIndirect off=16 size=2 (udp dst port)
/// [ 8] op=0x0015 jt=0 jf=1 k=0x00001a10   JEQ 6672 ? accept(9) : drop(10)
/// [ 9] op=0x0006 jt=0 jf=0 k=0x0000ffff   RET 0xffff (accept)
/// [10] op=0x0006 jt=0 jf=0 k=0x00000000   RET 0 (drop)
/// ```
pub const BPF_6672: [SockFilter; 11] = [
    ins(0x0028, 0, 0, 0x0000000c),
    ins(0x0015, 0, 8, 0x00000800),
    ins(0x0030, 0, 0, 0x00000017),
    ins(0x0015, 0, 6, 0x00000011),
    ins(0x0028, 0, 0, 0x00000014),
    ins(0x0045, 4, 0, 0x00003fff),
    ins(0x00b1, 0, 0, 0x0000000e),
    ins(0x0048, 0, 0, 0x00000010),
    ins(0x0015, 0, 1, 0x00001a10),
    ins(0x0006, 0, 0, 0x0000ffff),
    ins(0x0006, 0, 0, 0x00000000),
];

/// "tcp port 18022" filter（13 条；src 或 dst = 18022 都接受）。
///
/// 对照 Go `listen18022 buildBPFFilter` → `bpf.Assemble` 输出：
/// ```text
/// [ 0] op=0x0028 jt=0  jf=0  k=0x0000000c  LoadAbsolute off=12 size=2 (ethertype)
/// [ 1] op=0x0015 jt=0  jf=10 k=0x00000800  JEQ 0x0800 ? next : drop(12)
/// [ 2] op=0x0030 jt=0  jf=0  k=0x00000017  LoadAbsolute off=23 size=1 (ip proto)
/// [ 3] op=0x0015 jt=0  jf=8  k=0x00000006  JEQ 6 ? next : drop(12)
/// [ 4] op=0x0028 jt=0  jf=0  k=0x00000014  LoadAbsolute off=20 size=2 (flag+frag)
/// [ 5] op=0x0045 jt=6  jf=0  k=0x00003fff  JSET 0x3fff ? drop(12) : next
/// [ 6] op=0x00b1 jt=0  jf=0  k=0x0000000e  LoadMemShift off=14 (X=IHL*4)
/// [ 7] op=0x0048 jt=0  jf=0  k=0x0000000e  LoadIndirect off=14 size=2 (tcp src port)
/// [ 8] op=0x0015 jt=2  jf=0  k=0x00004666  JEQ 18022 ? accept(11) : next
/// [ 9] op=0x0048 jt=0  jf=0  k=0x00000010  LoadIndirect off=16 size=2 (tcp dst port)
/// [10] op=0x0015 jt=0  jf=1  k=0x00004666  JEQ 18022 ? accept(11) : drop(12)
/// [11] op=0x0006 jt=0  jf=0  k=0x0000ffff  RET 0xffff (accept)
/// [12] op=0x0006 jt=0  jf=0  k=0x00000000  RET 0 (drop)
/// ```
pub const BPF_18022: [SockFilter; 13] = [
    ins(0x0028, 0, 0, 0x0000000c),
    ins(0x0015, 0, 10, 0x00000800),
    ins(0x0030, 0, 0, 0x00000017),
    ins(0x0015, 0, 8, 0x00000006),
    ins(0x0028, 0, 0, 0x00000014),
    ins(0x0045, 6, 0, 0x00003fff),
    ins(0x00b1, 0, 0, 0x0000000e),
    ins(0x0048, 0, 0, 0x0000000e),
    ins(0x0015, 2, 0, 0x00004666),
    ins(0x0048, 0, 0, 0x00000010),
    ins(0x0015, 0, 1, 0x00004666),
    ins(0x0006, 0, 0, 0x0000ffff),
    ins(0x0006, 0, 0, 0x00000000),
];

/// 把一个 const filter 切片包成 `SockFprog`（attach 前调用）。
///
/// **生命周期**：返回的 `SockFprog` 借用 `filter` 的指针——调用方必须保证 `filter` 在
/// `attach_filter` 期间存活（const 数组是 `'static`，传 `&BPF_6672` 即满足）。
pub fn as_fprog(filter: &'static [SockFilter]) -> SockFprog {
    SockFprog {
        len: filter.len() as u16,
        filter: filter.as_ptr() as *mut SockFilter,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// task 6.3 sanity：每条 jump 指令的 jt/jf 偏移不越界（≤ len-1-idx）。
    /// 抓手抄 const 笔误（组 E 后续补对 Go golden 的逐字段断言）。
    fn assert_jumps_in_bounds(prog: &[SockFilter]) {
        let len = prog.len();
        for (idx, f) in prog.iter().enumerate() {
            // BPF jump class = 0x05；只有 jump 指令才用 jt/jf 作偏移。
            // class 在 code 低 3 位（BPF_JMP=0x05）。
            let is_jump = (f.code & 0x07) == 0x05;
            // RET（0x06）也落在该掩码外；JSET/JEQ 等都是 0x05 class。
            if is_jump {
                let max_off = (len - 1 - idx) as u8;
                assert!(
                    f.jt <= max_off,
                    "idx {idx}: jt={} exceeds bound {max_off}",
                    f.jt
                );
                assert!(
                    f.jf <= max_off,
                    "idx {idx}: jf={} exceeds bound {max_off}",
                    f.jf
                );
            }
        }
    }

    #[test]
    fn bpf_6672_jumps_in_bounds() {
        assert_eq!(BPF_6672.len(), 11);
        assert_jumps_in_bounds(&BPF_6672);
    }

    #[test]
    fn bpf_18022_jumps_in_bounds() {
        assert_eq!(BPF_18022.len(), 13);
        assert_jumps_in_bounds(&BPF_18022);
    }

    #[test]
    fn fprog_len_matches() {
        let p = as_fprog(&BPF_6672);
        assert_eq!(p.len, 11);
        let p = as_fprog(&BPF_18022);
        assert_eq!(p.len, 13);
    }

    /// 钉死 6672 端口 k 值 = 6672 (0x1a10)、18022 端口 = 18022 (0x4666)。
    #[test]
    fn port_constants_baked_correctly() {
        assert_eq!(BPF_6672[8].k, 6672);
        assert_eq!(BPF_18022[8].k, 18022);
        assert_eq!(BPF_18022[10].k, 18022);
    }
}
