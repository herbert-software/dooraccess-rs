# dooraccess-rs

Rust port exploration of [`dooraccess-go`](https://github.com/HerbertGao/dooraccess-go) — the
anjubao-only door-access daemon that runs on a **MikroTik hAP ac lite (QCA9533, MIPS 24Kc
big-endian, no FPU, OpenWrt musl, 64 MB RAM)**.

> **Status: Phase 5 (video forward) — ✅ PASSED (2026-06-10).**
> A full-feature Rust daemon now exists: protocol core (Phase 1), bare HTTP + HA push (Phase 2),
> PF_PACKET listeners + wire sender **hAP-authenticated** (Phase 3), daemon orchestration +
> ring self-unlock + auto-hangup (Phase 4), and the **video forward stack** (Phase 5: preview
> signalling / RTP receiver + proprietary H.264 reassembler / RTCP / FLV transmux / session
> TTL + fan-out / 3 HTTP routes). MIPS BE soft-float static daemon bin = **563,228 bytes**
> (≈ 1/7.1 of Go v0.10.0's 3,997,853). crate gate held at `libc` only.
> Per-phase evidence: [`PHASE0.md`](PHASE0.md) … [`PHASE5.md`](PHASE5.md);
> hAP self-unlock verification: [`HAP_VERIFY.md`](HAP_VERIFY.md). Roadmap: umbrella `rust_port_roadmap.md`.
> Gate so far = dev mock-e2e + local MIPS size measurement; hAP grey-rollout is Phase 7.

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
make build-mips       # → target/mips-unknown-linux-musl/release/dooraccess-rs (the daemon bin)
make verify-mips      # asserts MIPS BE + soft-float + statically linked
```

`make build-mips` does not change the global default toolchain (uses per-invocation `+nightly`).

## Layout

```
dooraccess-rs/
├── Cargo.toml          # binary crate, [dependencies] = libc only, size-tuned release profile
├── src/main.rs         # daemon orchestration (config / threads / shutdown); src/ = full daemon
│                       #   (codec / wire18022 / listen{6672,18022} / control / video/ / daemon …)
├── examples/probe.rs   # Phase 0-3 passive-shadow probe (hAP BE-auth reproduction tool)
├── Makefile            # build / test / fmt / clippy / fetch-sdk / build-mips / verify-mips
├── scripts/
│   ├── link-mips.sh    # rust-lld + OpenWrt musl sysroot linker wrapper (committed)
│   └── fetch-sdk.sh    # download/extract the OpenWrt mips_24kc musl toolchain
├── PHASE0.md           # full Phase 0 evidence log
└── .openwrt-sdk/       # SDK download + sysroot (gitignored; via make fetch-sdk)
```
