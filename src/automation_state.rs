// automation_state 模块（parse + render + 原子写 Persister）。
//
// 移植 Go `local/dooraccess-go internal/automationstate`：
//   - Phase1：两个纯函数 `parse` / `render`。
//   - Phase4（本组 G1）：`write_atomic`（temp + rename 原子写）+ `Persister`（自带内部锁
//     串行化 + 纯值去重，逐行对齐 Go `Persist`）。
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

// ===========================================================================
// 原子写 + Persister（Phase4 G1，锚 Go `writeAtomic` / `Persister`）
// ===========================================================================

use std::path::Path;
use std::sync::Mutex;

/// temp 文件后缀（固定名覆盖写：rename 前崩留的残留下次被覆盖，禁唯一/pid 名累积
/// 16MB flash，逐字对齐 Go `tempSuffix`）。
const TEMP_SUFFIX: &str = ".tmp";

/// 原子写：写固定名 temp（**同目录**）→ `rename` 到目标（锚 Go `writeAtomic`）。
///
/// `path + ".tmp"` 与 `path` 必然同目录（POSIX `rename` 同文件系统内原子替换；跨 fs
/// rename 非原子——同目录前缀拼接结构性保证同目录，无需运行时断言跨 fs）。
///
/// 串行化由 caller（`Persister` 的内部锁）保证。失败返 `io::Error`（caller best-effort
/// log，不 crash、不让 endpoint 失败）。rename 失败时清掉残留 temp（best-effort）。
pub fn write_atomic(path: &Path, st: State) -> std::io::Result<()> {
    // tmp 名 = 目标路径 + ".tmp"，与目标同目录（结构性同 fs）。
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(TEMP_SUFFIX);
    let tmp = std::path::PathBuf::from(tmp);

    std::fs::write(&tmp, render(st))?;
    if let Err(e) = std::fs::rename(&tmp, path) {
        // rename 失败：清残留 temp（best-effort），返错让 caller log。
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(())
}

/// 返回当前运行时两 flag 真值的回调类型（锚 Go `valueSource func() (autoUnlock, autoHangup bool)`）。
///
/// `Persist` 每次调用时读它取**当前真值**（非入队/构造时快照）——保证翻转后 pending
/// 同值写不回退翻转值。
pub type ValueSource = Box<dyn Fn() -> (bool, bool) + Send + Sync>;

/// 写失败 warning 日志钩子类型（`None` 安全）。
pub type LogFn = Box<dyn Fn(&str) + Send + Sync>;

/// 串行化 + 纯值去重地把当前 flag 值落盘（锚 Go `Persister`）。
///
/// 用法：endpoint 改运行时 atomic flag 后调 `persist()`；`persist` 读 `value_source`
/// 当前真值（非快照）+ 只在与已落盘值不同时写（**纯值去重**：重复同值 no-op、翻转立即
/// 落盘，**无时间窗节流**——逐行对齐 Go `Persist`）。
///
/// 串行化由 `Persister` **自身的内部锁** `Mutex<PersistedState>` 保证（`persist` 由 HTTP
/// 线程调，`/auto_unlock` 与 `/auto_hangup` 可在不同 HTTP 线程并发拨动；已删除的 wireMu
/// 不覆盖此路径——故 Persister MUST 自带锁，不依赖 worker/wire 锁）。
///
/// 写失败 best-effort：log warning 但不返错给 endpoint、不 crash。
pub struct Persister {
    path: std::path::PathBuf,
    value_source: ValueSource,
    logf: Option<LogFn>,
    /// 内部串行化锁 + 已落盘值（节流：当前==last 则 no-op）。
    /// 锁同时串行化「读真值→比对→写盘→更新 last」整个临界区，防两次 persist 交错半写。
    state: Mutex<PersistedState>,
}

/// 已落盘状态（受 `Persister.state` 锁保护）。
struct PersistedState {
    /// `last` 是否有效（首次 persist 前无意义，对齐 Go `hasWrote`）。
    has_wrote: bool,
    /// 已成功落盘的值（去重：当前==last 则 no-op）。
    last: State,
}

impl Persister {
    /// 构造一个 `Persister`（锚 Go `NewPersister`）。
    ///
    ///   - `path`：state 文件绝对路径（生产 `/etc/dooraccess-go/automation.state`，落 /etc overlay）。
    ///   - `value_source`：返回当前运行时两 flag 真值 `(auto_unlock, auto_hangup)` 的回调。
    ///     `persist` 每次读它取当前真值。
    ///   - `logf`：写失败 warning 日志钩子（`None` 安全）。
    pub fn new(path: std::path::PathBuf, value_source: ValueSource, logf: Option<LogFn>) -> Self {
        Self {
            path,
            value_source,
            logf,
            state: Mutex::new(PersistedState {
                has_wrote: false,
                last: State {
                    auto_unlock: false,
                    auto_hangup: false,
                },
            }),
        }
    }

    fn logf(&self, msg: &str) {
        if let Some(f) = &self.logf {
            f(msg);
        }
    }

    /// 把当前 flag 真值落盘（串行化 + 纯值去重）。best-effort：写失败 log warning 不返错。
    ///
    /// 去重语义（逐行对齐 Go `Persist`）：读当前真值，仅在与已落盘值不同时写——重复同值
    /// no-op（防 flash 磨损），值翻转立即落盘（**无时间窗去抖**）。
    pub fn persist(&self) {
        // 锁住整个临界区（读真值→去重比对→写盘→更新 last），串行化并发 persist。
        // lock poison 时仍取内层数据继续（best-effort，落盘不该因别处 panic 而停摆）。
        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };

        let (au, ah) = (self.value_source)();
        let cur = State {
            auto_unlock: au,
            auto_hangup: ah,
        };

        // 去重：当前真值 == 已落盘值 → 合并（no-op），不磨损 flash。
        if guard.has_wrote && cur == guard.last {
            return;
        }

        if let Err(e) = write_atomic(&self.path, cur) {
            // best-effort：内存 flag 已更新，持久失败仅 log warning，不 crash、不让 endpoint 失败。
            self.logf(&format!(
                "automation.state persist failed (best-effort, flag still applied in-memory): {e}"
            ));
            return;
        }
        guard.last = cur;
        guard.has_wrote = true;
    }
}
