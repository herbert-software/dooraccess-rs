# dooraccess-rs

Rust port exploration of [`dooraccess-go`](https://github.com/HerbertGao/dooraccess-go) — the
anjubao-only door-access daemon that runs on a **MikroTik hAP ac lite (QCA9533, MIPS 24Kc
big-endian, no FPU, OpenWrt musl, 64 MB RAM)**.

> **Status: Phase 0 (toolchain feasibility) — ✅ PASSED (2026-06-08).**
> Rust can produce a runnable MIPS big-endian soft-float static binary for the hAP. This repo
> currently contains only the Phase 0 probe; no protocol / HTTP / video / automation logic yet.
> Full evidence: [`PHASE0.md`](PHASE0.md). Roadmap: umbrella `rust_port_roadmap.md`.

## Phase 0 result

| | |
|---|---|
| Target | `mips-unknown-linux-musl` (MIPS BE, 32-bit, musl, **soft-float**) |
| Toolchain | `nightly` + `-Z build-std=std,panic_abort` + `-C panic=immediate-abort` |
| musl sysroot | OpenWrt ath79 `mips_24kc_gcc-13.3.0_musl` SDK (static `libc.a`/crt/`libgcc.a`) |
| Linker | `rust-lld` driven by [`scripts/link-mips.sh`](scripts/link-mips.sh) (fully static, non-PIE) |
| hello binary | **66,876 bytes**, statically linked, stripped |
| softfloat proof | `.MIPS.abiflags` FP ABI = **Soft float** (ELF-positive, not just RUSTFLAGS) |
| hAP run | native on QCA9533: **exit 0**, VmHWM 64 kB |

Why the unusual toolchain: `mips-unknown-linux-musl` is a Rust **Tier 3** target — `std` is
available but rustup ships no precompiled std/sysroot, so std must be compiled from `rust-src`
(`build-std`) and linked against an external musl sysroot. macOS can't run the SDK's Linux gcc,
so the wrapper uses the host `rust-lld` with the SDK's static archives.

## Build

```bash
# host (stable) — sanity build/test
make build
make test

# MIPS cross-build (one-time setup)
rustup toolchain install nightly --profile minimal
rustup component add rust-src llvm-tools-preview --toolchain nightly
make fetch-sdk        # downloads + extracts the OpenWrt mips_24kc musl sysroot to .openwrt-sdk/
make build-mips       # → target/mips-unknown-linux-musl/release/dooraccess-rs-probe
make verify-mips      # asserts MIPS BE + soft-float + statically linked
```

`make build-mips` does not change the global default toolchain (uses per-invocation `+nightly`).

## Layout

```
dooraccess-rs/
├── Cargo.toml          # binary crate, empty [dependencies], size-tuned release profile
├── src/main.rs         # minimal hello + /proc/self/status RSS self-report
├── Makefile            # build / test / fmt / clippy / fetch-sdk / build-mips / verify-mips
├── scripts/
│   ├── link-mips.sh    # rust-lld + OpenWrt musl sysroot linker wrapper (committed)
│   └── fetch-sdk.sh    # download/extract the OpenWrt mips_24kc musl toolchain
├── PHASE0.md           # full Phase 0 evidence log
└── .openwrt-sdk/       # SDK download + sysroot (gitignored; via make fetch-sdk)
```
