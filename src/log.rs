//! 中央带时间戳日志层（`add-rust-log-timestamps` 组 A）。
//!
//! `dooraccess-rs` 此前无中央日志层：`main.rs:logf`、视频 `SharedLogFn`、散落 `eprintln!`
//! 都只输出 `dooraccess-rs: <msg>`，**不带任何墙钟时间戳**（进程内只有 `Instant::now()`
//! 单调钟，无法格式化为日期）。本模块补上进程内自带的墙钟时间戳，对齐 Go stderr 回退路径的
//! `dooraccess-rs: YYYY/MM/DD HH:MM:SS.mmm <msg>` 格式。
//!
//! ## 设计要点（design D1/D2/D3）
//!
//! - 时间源仅 `std + libc`（移植硬约束，禁 chrono/time 外部 crate）：`std::time::SystemTime`
//!   取 epoch 秒 + 亚秒纳秒，`libc::localtime_r` 取本地分解 `tm`，再用**纯函数** [`render`]
//!   手工 `format!` 拼接。**不用 `strftime`**（消除其缓冲/返回值失败面）、**不用 `tzset`**
//!   （libc 0.2 unix 不导出；依赖 `localtime_r` 内部按需初始化 tz，glibc/musl MT-safe）。
//! - **零 panic（`panic=abort` 下一次 unwrap 即 abort daemon）**：[`format_log_line`] 函数体内
//!   禁止任何 `.unwrap()`/`.expect()`——`duration_since` 返 `Err`（时钟早于 epoch，冷启动 RTC
//!   无电真实可发生）或 `localtime_r` 返 null 时，降级为占位戳后**仍输出日志行**。
//! - [`render`] 是**纯函数**（只读 `tm` 字段做 `format!`，不调 libc、不读环境）——组 C 确定性
//!   单测的接缝，可被外部测试用手工构造的 `tm` 调用。
//! - [`log_line`] 是中央入口：先组装完整行（含换行）再**单次加锁写 stderr**
//!   （`io::stderr().lock()`），保留 `eprintln!` 的行级原子性、防多线程交错。

use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

/// 纯函数：把已分解的本地时间 `tm` + 毫秒 + 正文渲染为完整日志行（含进程 tag，无换行）。
///
/// **纯**：只读 `tm` 字段做 `format!`，不调 libc、不读环境、不依赖进程 TZ 全局态——故可被
/// 确定性单测用手工构造的 `tm` 调用（design D3：把「时间格式正确性」从 TZ 全局态解耦）。
///
/// 格式对齐 Go stderr 回退路径（毫秒精度，Go 用微秒是有意权衡差异）：
/// `dooraccess-rs: YYYY/MM/DD HH:MM:SS.mmm <msg>`。`tm_year` 是「1900 起的偏移」、`tm_mon`
/// 是 0-11，故 `+1900` / `+1`；`{:03}` 零填充覆盖 7ms→`.007`。
pub fn render(tm: &libc::tm, millis: u32, msg: &str) -> String {
    format!(
        "dooraccess-rs: {:04}/{:02}/{:02} {:02}:{:02}:{:02}.{:03} {}",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec,
        millis,
        msg,
    )
}

/// 由墙钟 `now` + 正文组装完整日志行（含 tag、无换行）。
///
/// **零 panic（MUST）**：本函数体内**禁止任何 `.unwrap()`/`.expect()`**。三条失败路径都显式降级
/// （仍输出行、不 panic、不输出错误日期）：
///
/// - `now.duration_since(UNIX_EPOCH)` 返 `Err`（墙钟早于 epoch，冷启动 RTC 无电）→ `secs=0,nanos=0` 占位戳。
/// - `secs` 超出目标 `time_t` 范围（32-bit MIPS post-2038，[`secs_in_time_t_range`] 返 false）→ `None`。
/// - `localtime_r` 返 null（本地时间分解失败）→ `None`。
///
/// `None` 经纯函数 [`compose`] 降级为全 0 占位 `tm`（`render` 加 `+1900`/`+1` 渲染
/// `1900/01/00 00:00:00.mmm`）；该决策抽到 `compose` 便于注入 `None` 单测（见 tests）。
// `libc::time_t` 是 deprecated 别名（libc 预告 musl 1.2.0 转 64-bit）；本处 MIPS32 用 32-bit
// time_t 正确（由 ffi.rs const-assert `size_of::<time_t>()==4` 钉死），允许 deprecated。
// revisit on musl 1.2.5 升级（const-assert 届时会失败、捕获转变）。byte-identical 行为。
#[allow(deprecated)]
pub fn format_log_line(now: SystemTime, msg: &str) -> String {
    // ① epoch 秒 + 亚秒纳秒；Err（pre-epoch）降级为 0。
    let (secs, nanos): (i64, u32) = match now.duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64, d.subsec_nanos()),
        Err(_) => (0, 0),
    };
    let millis = nanos / 1_000_000;

    // ② epoch 秒 → libc::time_t。32-bit time_t（MIPS post-2038）放不下 → 降级 None（同 null 路径，
    //    render 出 1900/01/00 占位），**不输出错误日期**，与 pre-epoch / localtime_r-null 降级对称、
    //    零静默撒谎。范围检测走纯泛型 [`secs_in_time_t_range`]（单测注入 i32 验 MIPS 截断、无漂移）。
    let local_tm = if secs_in_time_t_range::<libc::time_t>(secs) {
        // secs 已确认在 time_t 范围内（上面 if 守卫），下面 cast 无损。
        let t = secs as libc::time_t;
        // SAFETY: tm 是已零初始化的 libc::tm；传 &t（有效 time_t）与 &mut tm（有效可写）。
        // localtime_r 内部按需初始化 tz（不显式 tzset，design RISK），MT-safe。
        let mut tm: libc::tm = unsafe { core::mem::zeroed() };
        let rc = unsafe { libc::localtime_r(&t, &mut tm) };
        if rc.is_null() {
            None
        } else {
            Some(tm)
        }
    } else {
        None
    };

    compose(local_tm, millis, msg)
}

/// `secs`（epoch 秒）是否落在目标 `time_t` 范围内（放得下 = 无截断）。
///
/// **纯泛型函数、无漂移**：生产以 `T = libc::time_t` 调用，单测以 `T = i32` 调用——后者复现
/// 32-bit MIPS 的截断判定**在 64-bit host 上可确定性单测**（host 的 `libc::time_t = i64` 永不
/// 截断，分支不可达，故用 i32 实例化测同一函数，非另写 mock，杜绝生产/测试逻辑漂移）。
fn secs_in_time_t_range<T: TryFrom<i64>>(secs: i64) -> bool {
    T::try_from(secs).is_ok()
}

/// 由「本地分解时间（`None` = `localtime_r` 失败）」+ 毫秒 + 正文组装行——`localtime_r` 的
/// null 降级决策接缝。**纯函数**（除占位 `zeroed` 外不调 libc、不读环境）：组 C 注入 `None`
/// 即可确定性单测 `localtime_r`-null 降级路径（无须真 libc 返 null，design/task 3.1b 的注入 seam）。
///
/// `None` → 全 0 占位 `tm`，`render` 加 `+1900`/`+1` 后渲染 `1900/01/00 00:00:00.mmm`；不 panic。
fn compose(local_tm: Option<libc::tm>, millis: u32, msg: &str) -> String {
    let tm = match local_tm {
        Some(t) => t,
        // SAFETY: libc::tm 全 0 字段是合法占位；render 只读整数字段、不解引用 tm_zone 指针。
        None => unsafe { core::mem::zeroed() },
    };
    render(&tm, millis, msg)
}

/// 中央日志入口：用 `SystemTime::now()` 取墙钟，组装完整带戳行，**单次加锁写 stderr**。
///
/// 先 `format_log_line` 组装完整行（含换行）再单次 `write_all` 持 `stderr().lock()`——保证多线程
/// （视频 / 18022/6672 监听 / worker）并发写不跨行交错（保留 `eprintln!` 的行级原子性）。
pub fn log_line(msg: &str) {
    let mut line = format_log_line(SystemTime::now(), msg);
    line.push('\n');
    // 单次加锁写整行（含换行）——多线程不交错。
    let stderr = std::io::stderr();
    let mut lock = stderr.lock();
    let _ = lock.write_all(line.as_bytes()); // CENTRAL-SINK
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// 手工构造 `libc::tm`（绕过 TZ/libc 运行时；render 是纯函数）。
    fn make_tm(year: i32, mon: i32, mday: i32, hour: i32, min: i32, sec: i32) -> libc::tm {
        // SAFETY: libc::tm 全字段为整数，zeroed 是合法初值；随后逐字段填充。
        let mut tm: libc::tm = unsafe { core::mem::zeroed() };
        tm.tm_year = year - 1900; // tm_year 是 1900 起的偏移
        tm.tm_mon = mon - 1; // tm_mon 是 0-11
        tm.tm_mday = mday;
        tm.tm_hour = hour;
        tm.tm_min = min;
        tm.tm_sec = sec;
        tm
    }

    /// 3.1：纯函数 `render` 确定性单测——精确值断言，无任何全局 TZ 改动。
    /// 覆盖毫秒补零（7ms → `.007`）。render 不读环境/不调 libc，故完全确定。
    #[test]
    fn render_exact_value_with_millis_zero_pad() {
        let tm = make_tm(1970, 1, 1, 0, 0, 1);
        assert_eq!(
            render(&tm, 7, "hello"),
            "dooraccess-rs: 1970/01/01 00:00:01.007 hello"
        );
    }

    /// 3.1：毫秒非补零边界（123ms → `.123`）+ 多位日期字段补零。
    #[test]
    fn render_millis_no_pad_and_field_widths() {
        let tm = make_tm(2026, 6, 10, 9, 5, 3);
        assert_eq!(
            render(&tm, 123, "ring detected"),
            "dooraccess-rs: 2026/06/10 09:05:03.123 ring detected"
        );
    }

    /// 3.1b：`duration_since` 返 `Err`（墙钟早于 epoch，冷启动 RTC 无电）路径。
    /// 注入 `UNIX_EPOCH - Duration` 构造的 pre-epoch `now`：MUST 仍输出行 + 不 panic
    /// （能跑到断言即证未 abort）+ 降级占位戳（epoch 0 → `1970/01/01 00:00:00.000`）。
    #[test]
    fn format_log_line_pre_epoch_degrades_no_panic() {
        let pre = UNIX_EPOCH - Duration::from_secs(86_400); // epoch 前 1 天
        let line = format_log_line(pre, "x");
        assert!(!line.is_empty(), "pre-epoch 仍须输出行");
        assert!(line.contains('x'), "正文须保留: {line}");
        assert!(line.starts_with("dooraccess-rs: "), "须带 tag 前缀: {line}");
        // Err 分支降级为 secs=0,nanos=0；UTC 渲染即 epoch 0（TZ 未设退化 UTC）。
        // 不硬断完整日期（避免依赖运行环境 TZ），仅断毫秒占位为 .000。
        assert!(line.contains(".000 x"), "降级占位戳毫秒应为 .000: {line}");
    }

    /// 3.1b：`localtime_r` 返 null（本地时间分解失败）降级路径——经 `compose` 注入 seam
    /// 确定性单测（无须真 libc 返 null，design/task 3.1b）。`None` MUST → 全 0 占位 tm 渲染
    /// `1900/01/00 00:00:00.mmm`、仍输出行、不 panic、正文保留。
    #[test]
    fn compose_localtime_null_degrades_to_placeholder() {
        assert_eq!(
            compose(None, 0, "x"),
            "dooraccess-rs: 1900/01/00 00:00:00.000 x"
        );
        // 毫秒仍透传（占位 tm 不影响 millis）。
        assert_eq!(
            compose(None, 42, "ring"),
            "dooraccess-rs: 1900/01/00 00:00:00.042 ring"
        );
    }

    /// 3.1b：`compose` 的 `Some`（`localtime_r` 成功）路径如实渲染传入的 `tm`，
    /// 与 null 占位路径对称——证明降级仅在 `None` 时发生、成功时不被污染。
    #[test]
    fn compose_localtime_some_renders_given_tm() {
        let tm = make_tm(1970, 1, 1, 0, 0, 1);
        assert_eq!(
            compose(Some(tm), 7, "y"),
            "dooraccess-rs: 1970/01/01 00:00:01.007 y"
        );
    }

    /// 3.1b：32-bit `time_t`（MIPS post-2038）截断判定——用 `i32` 实例化纯泛型
    /// `secs_in_time_t_range`（生产用 `libc::time_t`，同一函数无漂移）在 64-bit host 上
    /// 确定性单测：超出 `i32::MAX` 的 secs 越界（→ 生产降级占位、不输出错误日期），范围内则放下。
    #[test]
    fn secs_in_time_t_range_detects_i32_truncation() {
        // i32::MAX = 2_147_483_647（约 2038-01-19）。
        assert!(
            secs_in_time_t_range::<i32>(i64::from(i32::MAX)),
            "i32::MAX 应放得下"
        );
        assert!(
            !secs_in_time_t_range::<i32>(i64::from(i32::MAX) + 1),
            "i32::MAX+1（post-2038）应越界 → 降级"
        );
        assert!(
            !secs_in_time_t_range::<i32>(4_000_000_000),
            "4e9（远超 2038）应越界"
        );
        assert!(
            secs_in_time_t_range::<i32>(1_700_000_000),
            "当前 epoch 秒应放得下 i32"
        );
        // i64 time_t（64-bit host）永不截断。
        assert!(secs_in_time_t_range::<i64>(i64::MAX), "i64 恒放得下");
    }

    /// 3.1：`format_log_line` 正常路径产出合法行（带亚秒纳秒 → 毫秒）。
    /// 不对 HH 做精确断言（依赖进程 TZ），仅验形状与毫秒换算。
    #[test]
    fn format_log_line_normal_shape() {
        let now = UNIX_EPOCH + Duration::new(1_700_000_000, 456_000_000); // .456s
        let line = format_log_line(now, "msg");
        assert!(line.starts_with("dooraccess-rs: "), "{line}");
        assert!(line.ends_with(" msg"), "{line}");
        assert!(line.contains(".456 "), "纳秒→毫秒应为 .456: {line}");
    }
}
