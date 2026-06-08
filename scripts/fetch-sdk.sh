#!/usr/bin/env bash
# Fetch + extract the OpenWrt ath79 mips_24kc musl toolchain sysroot used to link
# the MIPS probe. Only the toolchain lib/ subtree is extracted (not the Linux gcc
# binaries). Idempotent: skips if the toolchain is already present.
set -euo pipefail

SDK_VER="${OPENWRT_SDK_VER:-24.10.1}"
SDK_NAME="openwrt-sdk-${SDK_VER}-ath79-generic_gcc-13.3.0_musl.Linux-x86_64"
SDK_URL="https://downloads.openwrt.org/releases/${SDK_VER}/targets/ath79/generic/${SDK_NAME}.tar.zst"

here="$(cd "$(dirname "$0")" && pwd)"
dest="$here/../.openwrt-sdk"
tc="$dest/${SDK_NAME}/staging_dir/toolchain-mips_24kc_gcc-13.3.0_musl"

if [ -d "$tc/lib" ] && [ -f "$tc/lib/libc.a" ]; then
  echo "fetch-sdk: toolchain already present at $tc"
  exit 0
fi

mkdir -p "$dest"
echo "fetch-sdk: downloading $SDK_URL"
curl -fSL --retry 2 -o "$dest/sdk.tar.zst" "$SDK_URL"
echo "fetch-sdk: extracting toolchain lib/ subtree"
# GNU tar (Linux) needs --wildcards for glob patterns; bsdtar (macOS) globs by
# default and rejects --wildcards, so add it only for GNU tar.
TAR_WILDCARDS=""
tar --version 2>/dev/null | grep -qi 'GNU tar' && TAR_WILDCARDS="--wildcards"
zstd -dc "$dest/sdk.tar.zst" | tar -x $TAR_WILDCARDS -f - -C "$dest" "*/toolchain-mips_24kc_gcc-13.3.0_musl/lib/*"
rm -f "$dest/sdk.tar.zst"

for f in lib/libc.a lib/crt1.o lib/crti.o lib/crtn.o; do
  [ -f "$tc/$f" ] || { echo "fetch-sdk: missing $f after extract" >&2; exit 1; }
done
echo "fetch-sdk: ready at $tc"
