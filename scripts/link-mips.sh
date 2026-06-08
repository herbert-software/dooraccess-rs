#!/usr/bin/env bash
# Linker wrapper: drive rust-lld with an OpenWrt ath79 mips_24kc musl sysroot so a
# Rust mips-unknown-linux-musl binary links fully static (no libc.so) on any host
# (macOS arm64 included, where the SDK's own Linux gcc can't run).
#
# Override the toolchain location with OPENWRT_TOOLCHAIN; otherwise auto-detect the
# extracted SDK under ../.openwrt-sdk (see `make fetch-sdk`).
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
: "${OPENWRT_TOOLCHAIN:=$(ls -d "$here/../.openwrt-sdk"/openwrt-sdk-*/staging_dir/toolchain-mips_24kc_gcc-*_musl 2>/dev/null | head -1)}"
if [ -z "${OPENWRT_TOOLCHAIN:-}" ] || [ ! -d "${OPENWRT_TOOLCHAIN}/lib" ]; then
  echo "link-mips.sh: OpenWrt mips_24kc toolchain not found." >&2
  echo "  Set OPENWRT_TOOLCHAIN=<.../toolchain-mips_24kc_gcc-*_musl>, or run 'make fetch-sdk'." >&2
  exit 1
fi
SDK_LIB="${OPENWRT_TOOLCHAIN}/lib"
SDK_GCC="$(ls -d "$SDK_LIB"/gcc/mips-openwrt-linux-musl/*/ 2>/dev/null | head -1)"

# Locate rust-lld: rustc usually prepends the rustlib bin dir to PATH, but be robust.
RUST_LLD="$(command -v rust-lld || true)"
if [ -z "$RUST_LLD" ]; then
  _sys="$(rustc +nightly --print sysroot 2>/dev/null)"
  _host="$(rustc +nightly -vV 2>/dev/null | sed -n 's/^host: //p')"
  RUST_LLD="${_sys}/lib/rustlib/${_host}/bin/rust-lld"
fi
[ -x "$RUST_LLD" ] || { echo "link-mips.sh: rust-lld not found ($RUST_LLD)" >&2; exit 1; }

# Drop rustc's dynamic-libc args; we link static .a archives by absolute path.
newargs=()
for a in "$@"; do
  case "$a" in
    -lgcc_s|-lc|-Bdynamic|-pie) ;;
    *) newargs+=("$a") ;;
  esac
done

exec "$RUST_LLD" -flavor gnu \
  -static -no-pie --no-dynamic-linker -Bstatic \
  "$SDK_LIB/crt1.o" "$SDK_LIB/crti.o" \
  ${newargs[@]+"${newargs[@]}"} \
  -L"$SDK_LIB" -L"$SDK_GCC" \
  --start-group "$SDK_LIB/libc.a" "$SDK_GCC/libgcc.a" --end-group \
  "$SDK_LIB/crtn.o"
