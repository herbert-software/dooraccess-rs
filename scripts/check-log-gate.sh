#!/usr/bin/env bash
# add-rust-log-timestamps 机械门禁（task 2.4 + 2.4b）。
#
# 替代人工复查，断言「无残留绕过中央带戳入口、直写 stderr 的生产日志路径」。
# 三道闸：
#   ① 多行感知扫描所有直接输出宏（print 系单行 + 跨行 writeln/write!(stderr)），
#      命中的每一处 MUST 带 // EARLY-STAGE / // TEST-ONLY / // CENTRAL-SINK 标记之一
#      （命中集 ⊆ 标记集）。任何无标记残留 → 失败。
#   ② // CENTRAL-SINK 计数 ≤ 2（仅 logf body + log_line）。
#   ③ src/log.rs 整文件代码无真实 .unwrap()/.expect() 调用（F2 高爆破面零 panic；
#      panic=abort 下一次 unwrap 即 abort daemon）。
#
# exit 0 = 通过；非 0 = 失败（打印未通过项）。
set -u

# 脚本位于 dooraccess-rs/scripts/，src 在其上一级。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/src"

if ! command -v rg >/dev/null 2>&1; then
    echo "FAIL: ripgrep (rg) required for multi-line aware gate" >&2
    exit 1
fi

fail=0

# ── 闸 ①：多行感知扫描直接输出宏，断言每命中带标记之一 ──────────────────────────
# print 系（头部即定，单行匹配即可）：eprintln! / println! / eprint! / print!
# 跨行 stderr 写（rustfmt 可能把 sink 拆到下一行 writeln!(\n  stderr,）：
#   (writeln|write)!\(\s*stderr —— 用 -U 多行匹配。
# 命中以「文件:行号」定位，再逐行查该行是否带标记之一。

MARKERS='// EARLY-STAGE|// TEST-ONLY|// CENTRAL-SINK'

# 收集命中行（文件:行号:行内容）。-U 多行 + --multiline-dotall 让 \s* 跨行。
hits="$(
    rg -nU --no-heading \
       -e 'eprintln!|println!|eprint!|print!' \
       -e '(writeln|write)!\(\s*stderr' \
       "$SRC_DIR" 2>/dev/null
)"

# 对跨行 writeln!(\n stderr,) 命中，rg -U 返回的是起始行号（writeln! 那行），
# 但标记可能落在 sink 参数那一行（stderr, // EARLY-STAGE）。故对每个命中块，
# 取「起始行 + 下一行」范围内是否含标记之一。
while IFS= read -r line; do
    [ -z "$line" ] && continue
    file="${line%%:*}"
    rest="${line#*:}"
    lineno="${rest%%:*}"
    content="${rest#*:}"
    # 注释行里提及的宏名（doc 注释 //! /// // 里写 `eprintln!` 等）不是真实调用，跳过。
    if printf '%s' "$content" | grep -qE '^[[:space:]]*//'; then
        continue
    fi
    # 标记窗口（F-1：按命中类型收窄，防「下一行恰有标记注释」误放行单行 print 系）：
    #   - print 系（eprintln!/println!/eprint!/print!，头部即定、单行）→ 标记 MUST 在**命中行本身**。
    #   - stderr 写（(writeln|write)!(…stderr，rustfmt 可能把 sink 拆到下一行）→ 起始行 + 下一行。
    if printf '%s' "$content" | grep -qE 'eprintln!|println!|eprint!|print!'; then
        window="$content"
    else
        window="$(sed -n "${lineno},$((lineno + 1))p" "$file" 2>/dev/null)"
    fi
    if ! printf '%s\n' "$window" | rg -q "$MARKERS"; then
        echo "FAIL: 直写 stderr 残留无标记：$file:$lineno" >&2
        printf '       %s\n' "$rest" >&2
        fail=1
    fi
done <<< "$hits"

# ── 闸 ②：// CENTRAL-SINK 计数 ≤ 2 ───────────────────────────────────────────
central_count="$(rg -n --no-heading '// CENTRAL-SINK' "$SRC_DIR" 2>/dev/null | wc -l | tr -d ' ')"
if [ "$central_count" -gt 2 ]; then
    echo "FAIL: // CENTRAL-SINK 计数 $central_count > 2（仅 logf body + log_line）" >&2
    rg -n --no-heading '// CENTRAL-SINK' "$SRC_DIR" >&2
    fail=1
fi

# ── 闸 ⑤：标记位置绑定（spec「三个标记 MUST 各自有界可枚举」F-A）──────────────────
# 防「生产 eprintln 借用错位标记绕过」：仅靠计数（闸②）不够，三个标记还须各自绑定位置——
#   - // EARLY-STAGE 仅限 src/main.rs（flag/usage 极早期块）。
#   - // CENTRAL-SINK 仅限 src/main.rs（logf body）与 src/log.rs（log_line）。
#   - // TEST-ONLY  仅限**含 `#[cfg(test)]` 文件、且命中行在首个 `#[cfg(test)]` 之后**
#     （行式 grep 无法精确判块归属，用「文件含 cfg(test) + 行号在其后」作可机械的有界近似）。
es_bad="$(rg -n --no-heading '// EARLY-STAGE' "$SRC_DIR" 2>/dev/null | grep -vE '(^|/)main\.rs:' || true)"
if [ -n "$es_bad" ]; then
    echo "FAIL: // EARLY-STAGE 出现在 src/main.rs 之外（仅限 flag/usage 极早期块）：" >&2
    printf '%s\n' "$es_bad" >&2
    fail=1
fi
cs_bad="$(rg -n --no-heading '// CENTRAL-SINK' "$SRC_DIR" 2>/dev/null | grep -vE '(^|/)(main|log)\.rs:' || true)"
if [ -n "$cs_bad" ]; then
    echo "FAIL: // CENTRAL-SINK 出现在 src/main.rs / src/log.rs 之外（仅 logf body + log_line）：" >&2
    printf '%s\n' "$cs_bad" >&2
    fail=1
fi
while IFS= read -r to_hit; do
    [ -z "$to_hit" ] && continue
    to_file="${to_hit%%:*}"
    to_rest="${to_hit#*:}"
    to_ln="${to_rest%%:*}"
    cfg_ln="$(grep -nE '#\[cfg\(test\)\]' "$to_file" 2>/dev/null | head -1 | cut -d: -f1)"
    if [ -z "$cfg_ln" ] || [ "$to_ln" -le "$cfg_ln" ]; then
        echo "FAIL: // TEST-ONLY 不在 #[cfg(test)] 区内（生产代码不得借用 TEST-ONLY 绕过）：$to_file:$to_ln" >&2
        fail=1
    fi
done <<< "$(rg -n --no-heading '// TEST-ONLY' "$SRC_DIR" 2>/dev/null)"

# ── 闸 ③：src/log.rs 整文件代码无真实 .unwrap()/.expect() 调用（2.4b）─────────────
# 只匹配真实调用形 .unwrap( / .expect(，**排除注释行**（doc 注释 //! /// // 里引用该词
# 不算真实调用）——剥掉首非空字符为 // 的行后再 grep。
unwraps="$(grep -nE '\.unwrap\(|\.expect\(' "$SRC_DIR/log.rs" 2>/dev/null \
    | grep -vE '^[0-9]+:[[:space:]]*//')"
if [ -n "$unwraps" ]; then
    echo "FAIL: src/log.rs 含 .unwrap()/.expect()（panic=abort 下零容忍）：" >&2
    printf '%s\n' "$unwraps" >&2
    fail=1
fi

# ── 闸 ④：log_line 并发原子性结构断言（task 3.2b / F1）──────────────────────────
# `log_line` 整行原子写：render 出完整行后**单次 write_all 持 stderr().lock()**，不分段
# write! 跨行交错。以代码结构断言（非易 flaky 的多线程交错运行时测试）：
#   - log.rs 中 `write_all` 恰 1 处（log_line 内单次写）。
#   - log.rs 中 `.lock()` 存在（持锁写）。
# 注：`grep -c` 无匹配时已打印 `0` 并 exit 1；**不可** `|| echo 0`（会得两行 "0\n0"，
# 在无 set -e 下令 `[ -ne ]` 报 "integer expression expected" 被吞、误判通过——F1 闸④ 假绿）。
write_all_count="$(grep -cE '\.write_all\(' "$SRC_DIR/log.rs" 2>/dev/null)"
write_all_count="${write_all_count:-0}" # grep 读不到文件时的空值兜底
if [ "$write_all_count" -ne 1 ]; then
    echo "FAIL: src/log.rs write_all 计数 $write_all_count != 1（log_line 须单次 write_all 整行）" >&2
    grep -nE '\.write_all\(' "$SRC_DIR/log.rs" >&2
    fail=1
fi
# 须**剥注释行**再匹配（与 闸③ 对称）：否则 doc 注释里提到的 `stderr().lock()`（log.rs:20/78）
# 会令本检查恒真——删掉真实 `.lock()` 也误判通过（harness 假绿）。
if ! grep -nE '\.lock\(\)' "$SRC_DIR/log.rs" 2>/dev/null | grep -qvE '^[0-9]+:[[:space:]]*//'; then
    echo "FAIL: src/log.rs 未见真实 stderr().lock()（log_line 须持锁写整行保原子性）" >&2
    fail=1
fi

if [ "$fail" -eq 0 ]; then
    echo "OK: log gate passed (no unmarked stderr writes; CENTRAL-SINK=$central_count<=2; log.rs no unwrap/expect; log_line single locked write_all)"
fi
exit "$fail"
