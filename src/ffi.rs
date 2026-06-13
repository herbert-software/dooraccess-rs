//! PF_PACKET raw-socket FFI 缝。
//!
//! 这是整个 Rust port **第一次破 0-crate gate**——`AF_PACKET` / `bind(sockaddr_ll)` /
//! `setsockopt(SO_ATTACH_FILTER / PACKET_ADD_MEMBERSHIP)` / `recvfrom` 无 std 等价物，
//! 必须直接调 `libc`。破口被严格框死（见 `lib.rs` 顶部边界注释）：
//!   ① `listen6672` PF_PACKET ② `listen18022` PF_PACKET ③ `wire_sender` 的 `SO_BINDTODEVICE`
//! 本模块只承载 ①②（PF_PACKET 通用缝）+ htons + 平台无关 BPF 类型；SO_BINDTODEVICE
//! 由 `wire_sender` 自持。
//!
//! **大端字节序**：目标硬件 hAP ac lite = QCA9533 MIPS24Kc **big-endian**，开发机
//! （macOS-arm64）与常规 CI（Linux-x86）都小端。memory `dooraccess_pf_packet_byte_order.md`
//! 记录过 `v<<8|v>>8` 的 htons bug 会致 daemon 在 hAP 上**收 0 帧**、开发机全过
//! （/proc/net/packet Proto=0300 vs 期望 0003）。本模块的 `htons` 用平台无关写法（见下），
//! **禁止** `v<<8|v>>8`。
//!
//! **Linux-only**：PF_PACKET recv 缝用 `#[cfg(target_os = "linux")]` 守护，非 Linux 提供
//! stub，让 macOS 也能 `cargo build` / `cargo test`。

// ---------------------------------------------------------------------------
// htons — 平台无关 host→network byte order
// ---------------------------------------------------------------------------

/// 把主机字节序 `u16` 转为网络字节序（big-endian）。
///
/// 写出 BE buffer 再按主机序读回：
///   - `to_be_bytes()`：永远写出 BE 顺序的 2 字节
///   - `from_ne_bytes()`：按主机序读回
///
/// 语义验证：
///   - LE 主机（x86 / arm64 LE）：`0x0003u16.to_be_bytes()` = `[0x00, 0x03]`，
///     `from_ne_bytes([0x00,0x03])` = `0x0300` ← swap，符合 LE 的 htons。
///   - BE 主机（MIPS24Kc / QCA9533）：`0x0003u16.to_be_bytes()` = `[0x00, 0x03]`，
///     `from_ne_bytes([0x00,0x03])` = `0x0003` ← no-op，符合 BE 的 htons（host=network）。
///
/// **禁止** `v << 8 | v >> 8`：它始终 swap，在 BE 上等于 ntohs，让 PF_PACKET socket
/// 注册的 protocol 字段错位（hAP 收 0 帧）。详见 memory `dooraccess_pf_packet_byte_order.md`。
#[inline]
pub const fn htons(v: u16) -> u16 {
    u16::from_ne_bytes(v.to_be_bytes())
}

// BE 第二层防御（编译期断言分支）：本机 macOS（LE）不跑 qemu，但**任何**针对
// 大端目标（hAP MIPS24Kc）的编译都会触发这两条 const 断言——`htons` 在 BE 上必须是 no-op
// （host=network），否则就是 `v<<8|v>>8` 那类 swap bug 回归（hAP 收 0 帧）。const 求值是
// 编译期，无需运行 BE 二进制即可在 `cargo build --target mips-unknown-linux-musl` 时认证。
// 与 §tests::htons_matches_platform_semantics 的运行期断言互补（后者在 CI qemu-mips 下跑真
// BE 寄存器）。LE host 上这两条 cfg 分支不编译，靠 golden + qemu 兜底。
#[cfg(target_endian = "big")]
const _: () = assert!(
    htons(0x0003) == 0x0003,
    "htons must be a no-op on big-endian (host byte order == network byte order)"
);
#[cfg(target_endian = "big")]
const _: () = assert!(
    htons(0x0800) == 0x0800,
    "htons(ETH_P_IP) must be a no-op on big-endian"
);
#[cfg(target_endian = "little")]
const _: () = assert!(
    htons(0x0003) == 0x0300,
    "htons must swap on little-endian (sanity: const-eval path mirrors runtime)"
);

// ---------------------------------------------------------------------------
// BPF 类型重导出 + size 守门
// ---------------------------------------------------------------------------

/// BPF 单条指令。字段 `code:u16, jt:u8, jf:u8, k:u32`。
///
/// **Linux**：直接 alias 到 `libc::sock_filter`（实测 libc 0.2.186
/// mips 目标完整导出，无需手搓）。
/// **非 Linux（macOS host）**：libc 不导出 `sock_filter`（它是 `linux/filter.h` 的 kernel
/// uapi，仅 linux_like target 定义）。为让 `bpf.rs` 的 const 表与 golden 测试在 macOS 上
/// 也能编译/运行，提供一个 `repr(C)` 字段等价 mirror（布局 `code:u16 jt:u8 jf:u8 k:u32`）。
/// 此 mirror **不**用于真 socket（macOS 无 AF_PACKET，PF_PACKET 路径全 stub），仅承载 const
/// 字节值供 golden 对照。
#[cfg(target_os = "linux")]
pub type SockFilter = libc::sock_filter;

/// 非 Linux host 的 `sock_filter` 等价 mirror（布局 `code:u16 jt:u8 jf:u8 k:u32`）。
#[cfg(not(target_os = "linux"))]
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SockFilter {
    pub code: u16,
    pub jt: u8,
    pub jf: u8,
    pub k: u32,
}

/// BPF 程序头。**Linux** alias `libc::sock_fprog`；**非 Linux** repr(C) mirror。
#[cfg(target_os = "linux")]
pub type SockFprog = libc::sock_fprog;

/// 非 Linux host 的 `sock_fprog` 等价 mirror。
#[cfg(not(target_os = "linux"))]
#[repr(C)]
pub struct SockFprog {
    pub len: u16,
    pub filter: *mut SockFilter,
}

// size 守门：防 libc 版本漂移把字段宽度改了（实测 mips：sock_fprog=8 / sock_filter=8）。
// 仅当未来 libc 版本回归致断言失败时，才钉版本或退回 repr(C) 手搓，**不引第二个 crate**。
const _: () = assert!(
    core::mem::size_of::<SockFilter>() == 8,
    "sock_filter must be 8 bytes (code:u16 jt:u8 jf:u8 k:u32)"
);
// 注：sock_fprog 含一个裸指针，64-bit host 上 = 2+6(pad)+8 = 16；mips32 上 = 2+2(pad)+4 = 8。
// 因此 size 断言按指针宽度分叉（与实测「sock_fprog=8(ptr 4)」一致）。
#[cfg(target_pointer_width = "32")]
const _: () = assert!(
    core::mem::size_of::<SockFprog>() == 8,
    "sock_fprog must be 8 bytes on 32-bit (mips)"
);
#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    core::mem::size_of::<SockFprog>() == 16,
    "sock_fprog must be 16 bytes on 64-bit host"
);

// ---------------------------------------------------------------------------
// int 宽度回归钉死
// ---------------------------------------------------------------------------
//
// 三处 struct 字段宽度需钉死：
// sockaddr_ll.sll_ifindex=c_int、timeval.tv_sec(mips32=32-bit)、BPF 字段宽度。
// 这里写回归断言钉死实测 mips struct 宽度——
// host 上跑 host 宽度断言，mips target 上 cfg 分叉断言 mips 宽度。

// sll_ifindex 是 c_int（32-bit on all linux targets）；
// 必须用 c_int 不能用 i64。
const _: () = assert!(
    core::mem::size_of::<libc::c_int>() == 4,
    "c_int must be 32-bit (sll_ifindex)"
);

// timeval.tv_sec：mips32 是 32-bit（**非 time64**，实测 timeval=8 / time_t=4）；
// host（macOS-arm64 / Linux-x86_64）是 64-bit。两处都断言以钉死不分叉到意外宽度。
// `libc::time_t` 是 deprecated 别名（libc 预告 musl 1.2.0 转 64-bit）；这两个 const-assert
// 正是钉死 time_t 宽度的守卫，允许 deprecated。revisit on musl 1.2.5 升级。
#[cfg(target_arch = "mips")]
#[allow(deprecated)]
const _: () = assert!(
    core::mem::size_of::<libc::time_t>() == 4,
    "time_t must be 32-bit on mips (timeval=8)"
);
#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
#[allow(deprecated)]
const _: () = assert!(
    core::mem::size_of::<libc::time_t>() == 8,
    "time_t must be 64-bit on 64-bit linux"
);

// BPF 字段宽度：SockFilter.code=u16 / jt=u8 / jf=u8 / k=u32。
// size 已由上面断言守，这里再钉字段宽度防 libc 把某字段改成 native int。
//
// Linux：用 `libc::sock_filter` 字面量构造——若 libc 改了字段类型，赋值会编译失败。
// 非 Linux：用本模块 repr(C) mirror，同样的字面量校验字段类型。
const _: () = {
    let f = SockFilter {
        code: 0u16,
        jt: 0u8,
        jf: 0u8,
        k: 0u32,
    };
    let _ = f.code;
    let _ = f.jt;
    let _ = f.jf;
    let _ = f.k;
};

// ---------------------------------------------------------------------------
// PF_PACKET 缝 —— Linux only
// ---------------------------------------------------------------------------

/// FFI / socket 层错误。携带 errno + 阶段标签，便于上层归类 / log。
#[derive(Debug)]
pub enum FfiError {
    /// PF_PACKET 仅 Linux 支持（macOS 等 stub 路径）。
    Unsupported,
    /// 某个 libc 调用失败，携带阶段名 + errno。
    Syscall { stage: &'static str, errno: i32 },
    /// 接口名含 NUL 或查不到 ifindex。
    Interface(String),
}

impl core::fmt::Display for FfiError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            FfiError::Unsupported => write!(f, "PF_PACKET only supported on Linux"),
            FfiError::Syscall { stage, errno } => write!(f, "{stage}: errno {errno}"),
            FfiError::Interface(msg) => write!(f, "interface: {msg}"),
        }
    }
}

impl std::error::Error for FfiError {}

#[cfg(target_os = "linux")]
mod linux {
    use super::{htons, FfiError, SockFprog};
    use std::os::unix::io::RawFd;

    /// 把 `errno` 读出包成 `FfiError::Syscall`。
    fn last_errno(stage: &'static str) -> FfiError {
        FfiError::Syscall {
            stage,
            errno: std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
        }
    }

    /// `if_nametoindex(iface)` → ifindex（>0）。错误时返 `FfiError::Interface`。
    pub fn iface_index(iface: &str) -> Result<u32, FfiError> {
        let cname = std::ffi::CString::new(iface)
            .map_err(|_| FfiError::Interface(format!("name {iface} contains NUL")))?;
        // SAFETY: cname 是有效 NUL 结尾 C 字符串。
        let idx = unsafe { libc::if_nametoindex(cname.as_ptr()) };
        if idx == 0 {
            return Err(FfiError::Interface(format!(
                "if_nametoindex({iface}) failed"
            )));
        }
        Ok(idx)
    }

    /// `socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL))`。
    ///
    /// 注意 protocol 参数用平台无关 `htons`（**这是 BE 错位 bug 的核心点**）。
    pub fn open_packet_socket() -> Result<RawFd, FfiError> {
        // SAFETY: 标准 socket(2) 调用。
        let fd = unsafe {
            libc::socket(
                libc::AF_PACKET,
                libc::SOCK_RAW,
                htons(libc::ETH_P_ALL as u16) as libc::c_int,
            )
        };
        if fd < 0 {
            return Err(last_errno("socket AF_PACKET"));
        }
        Ok(fd)
    }

    /// `bind(fd, sockaddr_ll{protocol=htons(ETH_P_ALL), ifindex})`。
    pub fn bind_to_ifindex(fd: RawFd, ifindex: u32) -> Result<(), FfiError> {
        let mut sll: libc::sockaddr_ll = unsafe { core::mem::zeroed() };
        sll.sll_family = libc::AF_PACKET as libc::c_ushort;
        sll.sll_protocol = htons(libc::ETH_P_ALL as u16);
        // sll_ifindex 是 c_int（32-bit）——见 int 宽度断言。
        sll.sll_ifindex = ifindex as libc::c_int;
        // SAFETY: sll 是已初始化的 sockaddr_ll；长度精确。
        let rc = unsafe {
            libc::bind(
                fd,
                &sll as *const libc::sockaddr_ll as *const libc::sockaddr,
                core::mem::size_of::<libc::sockaddr_ll>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(last_errno("bind sockaddr_ll"));
        }
        Ok(())
    }

    /// `setsockopt(SOL_PACKET, PACKET_ADD_MEMBERSHIP, packet_mreq{ifindex, PACKET_MR_PROMISC})`。
    pub fn set_promisc(fd: RawFd, ifindex: u32) -> Result<(), FfiError> {
        let mut mreq: libc::packet_mreq = unsafe { core::mem::zeroed() };
        // mr_ifindex 是 c_int（32-bit）。
        mreq.mr_ifindex = ifindex as libc::c_int;
        mreq.mr_type = libc::PACKET_MR_PROMISC as libc::c_ushort;
        // SAFETY: mreq 已初始化；长度精确。
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_PACKET,
                libc::PACKET_ADD_MEMBERSHIP,
                &mreq as *const libc::packet_mreq as *const libc::c_void,
                core::mem::size_of::<libc::packet_mreq>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(last_errno("set PROMISC"));
        }
        Ok(())
    }

    /// `setsockopt(SOL_SOCKET, SO_ATTACH_FILTER, sock_fprog)`。
    ///
    /// `prog` 由调用方用 `&const [SockFilter; N]` 构造（见 bpf.rs）。
    pub fn attach_filter(fd: RawFd, prog: &SockFprog) -> Result<(), FfiError> {
        // SAFETY: prog 指向调用方持有的合法 sock_fprog；其 .filter 指向合法 SockFilter 数组。
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_ATTACH_FILTER,
                prog as *const SockFprog as *const libc::c_void,
                core::mem::size_of::<SockFprog>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(last_errno("attach BPF"));
        }
        Ok(())
    }

    /// `setsockopt(SOL_SOCKET, SO_RCVTIMEO, timeval{0, 500_000})`（500ms 周期 wakeup）。
    ///
    /// 失败**不致命**（仅 log 后继续）：返回 errno 让调用方决定是否仅 log。
    // `libc::time_t`/`suseconds_t` 是 deprecated 别名（libc 预告 musl 1.2.0 转 64-bit）；
    // 宽度由上方 const-assert 钉死，允许 deprecated。revisit on musl 1.2.5 升级。
    #[allow(deprecated)]
    pub fn set_rcvtimeo(fd: RawFd, sec: i64, usec: i64) -> Result<(), FfiError> {
        // tv_sec/tv_usec 是 time_t/suseconds_t；mips32=32-bit、host=64-bit（见 int 宽度断言）。
        let tv = libc::timeval {
            tv_sec: sec as libc::time_t,
            tv_usec: usec as libc::suseconds_t,
        };
        // SAFETY: tv 已初始化；长度精确。
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVTIMEO,
                &tv as *const libc::timeval as *const libc::c_void,
                core::mem::size_of::<libc::timeval>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(last_errno("set SO_RCVTIMEO"));
        }
        Ok(())
    }

    /// `recvfrom(fd, buf, 0, NULL, NULL)`。返回读到的字节数；
    /// `Ok(None)` = `EAGAIN/EWOULDBLOCK/EINTR`（周期 wakeup / 信号，调用方应 continue）。
    pub fn recv(fd: RawFd, buf: &mut [u8]) -> Result<usize, FfiError> {
        // SAFETY: buf 是合法可写切片；不传 src_addr（NULL）。
        let n = unsafe {
            libc::recvfrom(
                fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
                0,
                core::ptr::null_mut(),
                core::ptr::null_mut(),
            )
        };
        if n < 0 {
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK || errno == libc::EINTR {
                // 周期 wakeup（SO_RCVTIMEO）或信号——调用方查 shutdown flag 后 continue。
                return Err(FfiError::Syscall {
                    stage: "recvfrom-retry",
                    errno,
                });
            }
            return Err(last_errno("recvfrom"));
        }
        Ok(n as usize)
    }

    /// `close(fd)`，忽略错误（cleanup 路径）。
    pub fn close(fd: RawFd) {
        // SAFETY: fd 由本模块 open_packet_socket 产生，close 幂等由调用方保证。
        unsafe {
            libc::close(fd);
        }
    }

    /// 判断 errno 是否是「周期 wakeup / 信号」类（调用方 continue 而非报错）。
    pub fn is_retry_errno(errno: i32) -> bool {
        errno == libc::EAGAIN || errno == libc::EWOULDBLOCK || errno == libc::EINTR
    }
}

#[cfg(target_os = "linux")]
pub use linux::*;

// ---------------------------------------------------------------------------
// 非 Linux stub —— 让 macOS 能编译 + 测纯软逻辑
// ---------------------------------------------------------------------------

#[cfg(not(target_os = "linux"))]
mod stub {
    use super::FfiError;
    use std::os::unix::io::RawFd;

    pub fn iface_index(_iface: &str) -> Result<u32, FfiError> {
        Err(FfiError::Unsupported)
    }
    pub fn open_packet_socket() -> Result<RawFd, FfiError> {
        Err(FfiError::Unsupported)
    }
    pub fn bind_to_ifindex(_fd: RawFd, _ifindex: u32) -> Result<(), FfiError> {
        Err(FfiError::Unsupported)
    }
    pub fn set_promisc(_fd: RawFd, _ifindex: u32) -> Result<(), FfiError> {
        Err(FfiError::Unsupported)
    }
    pub fn attach_filter(_fd: RawFd, _prog: &super::SockFprog) -> Result<(), FfiError> {
        Err(FfiError::Unsupported)
    }
    pub fn set_rcvtimeo(_fd: RawFd, _sec: i64, _usec: i64) -> Result<(), FfiError> {
        Err(FfiError::Unsupported)
    }
    pub fn recv(_fd: RawFd, _buf: &mut [u8]) -> Result<usize, FfiError> {
        Err(FfiError::Unsupported)
    }
    pub fn close(_fd: RawFd) {}
    pub fn is_retry_errno(_errno: i32) -> bool {
        false
    }
}

#[cfg(not(target_os = "linux"))]
pub use stub::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn htons_matches_platform_semantics() {
        // htons = to_be_bytes + from_ne_bytes。
        let got = htons(0x0003);
        if cfg!(target_endian = "little") {
            assert_eq!(got, 0x0300, "LE host must swap (htons of 0x0003 = 0x0300)");
        } else {
            assert_eq!(
                got, 0x0003,
                "BE host must be no-op (htons of 0x0003 = 0x0003)"
            );
        }
        // 与朴素 to_be 等价（双重确认，禁止 v<<8|v>>8 写法）。
        assert_eq!(htons(0x0800), 0x0800u16.to_be());
        assert_eq!(htons(0xabcd), 0xabcdu16.to_be());
    }

    #[test]
    fn sock_filter_field_widths() {
        // 回归：BPF 字段宽度不分叉。
        assert_eq!(core::mem::size_of::<SockFilter>(), 8);
        // 字段类型钉死：能用这些字面量构造即证字段类型正确。
        let f = SockFilter {
            code: 0x0028u16,
            jt: 0u8,
            jf: 0u8,
            k: 0x0000000cu32,
        };
        assert_eq!(f.code, 0x0028);
        assert_eq!(f.k, 0x0000000c);
    }

    #[test]
    fn c_int_is_32bit() {
        // sll_ifindex / mr_ifindex 是 c_int，全 linux target 32-bit。
        assert_eq!(core::mem::size_of::<libc::c_int>(), 4);
    }
}
