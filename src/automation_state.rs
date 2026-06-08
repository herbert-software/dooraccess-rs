// Phase1: automation_state 模块（parse + render，不接写盘）。
//
// 移植 Go `local/dooraccess-go internal/automationstate` 的两个纯函数 `parse` / `render`。
// **不**移植 `writeAtomic` / `Persister`（原子写 + 串行化锁 + 节流）——有状态写盘超出
// Phase1 范围，推迟到后续 daemon Phase。
//
// 文件格式（INI 2 行）：
//
//     auto_unlock=true
//     auto_hangup=false
//
// 解析约束（与 Go 等价）：
//   - 严格 2-key：必须 `auto_unlock` 与 `auto_hangup` 两个 key 都存在且都是合法 bool
//     （`true/false/1/0`，大小写不敏感），否则**整文件丢弃**返 `ParseError`。
//   - 缺任一 key / 空文件 / 未知 key / 非法 bool / 缺 `=` 一律整文件丢弃（**禁止**部分恢复），
//     与 Go `ErrParse` 路径等价（单一 sentinel）。
//   - `;` 与 `#` 注释行、空行被跳过。

/// State 是持久文件解析出的两个 bool。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct State {
    pub auto_unlock: bool,
    pub auto_hangup: bool,
}

/// ParseError 表示 state 文件存在但解析失败（空 / 半写 / 非法 bool / 缺 key / 未知 key /
/// 缺 `=`）。整文件丢弃，禁部分恢复——与 Go `ErrParse` 同性质的单一 sentinel。
/// caller 须降级用 config 默认。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseError;

/// parse 解析 state 文件字节。两个 key 都须存在且合法 bool，否则整文件丢弃返 `ParseError`。
///
/// 对照 Go `automationstate.parse`：非 UTF-8 输入视为损坏 → 整文件丢弃。
pub fn parse(raw: &[u8]) -> Result<State, ParseError> {
    let text = core::str::from_utf8(raw).map_err(|_| ParseError)?;

    let mut auto_unlock = false;
    let mut auto_hangup = false;
    let mut saw_unlock = false;
    let mut saw_hangup = false;

    for line in text.split('\n') {
        let s = line.trim();
        if s.is_empty() {
            continue;
        }
        if s.starts_with(';') || s.starts_with('#') {
            continue;
        }
        // Go: eq := strings.IndexByte(s, '='); if eq <= 0 → ErrParse
        // （未找到 = -1、或位于 0 意味空 key）。
        let eq = match s.find('=') {
            Some(i) if i > 0 => i,
            _ => return Err(ParseError),
        };
        let key = s[..eq].trim();
        let val = s[eq + 1..].trim();
        let b = parse_bool(val).ok_or(ParseError)?;
        match key {
            "auto_unlock" => {
                auto_unlock = b;
                saw_unlock = true;
            }
            "auto_hangup" => {
                auto_hangup = b;
                saw_hangup = true;
            }
            // 未知 key：整文件丢弃（schema 固定 2 bool，未知 key 视为损坏/半写）。
            _ => return Err(ParseError),
        }
    }

    if !saw_unlock || !saw_hangup {
        // 缺一个 key（含空文件/0 字节）→ 整文件丢弃降级。
        return Err(ParseError);
    }
    Ok(State {
        auto_unlock,
        auto_hangup,
    })
}

/// parse_bool 接受 `true/false/1/0`（大小写不敏感，与 config INI parser 同款）。
/// 返回 None 表示非法 bool（caller 整文件丢弃降级）。
fn parse_bool(v: &str) -> Option<bool> {
    match v.to_ascii_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

/// render 把 State 序列化成固定 2 行 INI 字节（确定性顺序）。
///
/// 输出恒为 `auto_unlock=<bool>\nauto_hangup=<bool>\n`，`<bool>` 为 `true`/`false`
/// 字面，逐字节与 Go `render` 相等。
pub fn render(st: State) -> Vec<u8> {
    let mut out = String::new();
    out.push_str("auto_unlock=");
    out.push_str(if st.auto_unlock { "true" } else { "false" });
    out.push('\n');
    out.push_str("auto_hangup=");
    out.push_str(if st.auto_hangup { "true" } else { "false" });
    out.push('\n');
    out.into_bytes()
}
