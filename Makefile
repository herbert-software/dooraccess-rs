# dooraccess-rs — Phase0 toolchain probe build interface.
#
# host build/test run on stable; the MIPS cross-build needs nightly + rust-src
# (build-std) + the OpenWrt mips_24kc musl sysroot (`make fetch-sdk`).

NIGHTLY      ?= nightly
MIPS_TARGET  := mips-unknown-linux-musl
WRAPPER      := $(CURDIR)/scripts/link-mips.sh
MIPS_RUSTFLAGS := -C linker-flavor=ld -C linker=$(WRAPPER) -C relocation-model=static \
                  -Z unstable-options -C panic=immediate-abort
MIPS_BIN := target/$(MIPS_TARGET)/release/dooraccess-rs

.PHONY: build test fmt fmt-check clippy fetch-sdk build-mips verify-mips dist clean

## host build (stable)
build:
	cargo build --release

test:
	cargo test

fmt:
	cargo fmt

fmt-check:
	cargo fmt --check

clippy:
	cargo clippy --release -- -D warnings

## download + extract the OpenWrt ath79 mips_24kc musl sysroot
fetch-sdk:
	bash scripts/fetch-sdk.sh

## cross-build the static MIPS BE softfloat probe (needs: rustup toolchain install
## nightly + rustup component add rust-src --toolchain nightly + make fetch-sdk)
build-mips: fetch-sdk
	RUSTFLAGS="$(MIPS_RUSTFLAGS)" \
	  cargo +$(NIGHTLY) build -Z build-std=std,panic_abort --target $(MIPS_TARGET) --release

## assert the artifact is MIPS big-endian, softfloat, statically linked
verify-mips:
	@file $(MIPS_BIN)
	@file $(MIPS_BIN) | grep -q 'ELF 32-bit MSB executable, MIPS' \
	  || { echo "verify-mips: not MIPS big-endian ELF"; exit 1; }
	@file $(MIPS_BIN) | grep -q 'statically linked' \
	  || { echo "verify-mips: not statically linked"; exit 1; }
	@RO=$$(ls $$(rustc +$(NIGHTLY) --print sysroot)/lib/rustlib/*/bin/llvm-readobj 2>/dev/null | head -1); \
	  "$$RO" -A $(MIPS_BIN) | grep -q 'FP ABI: Soft float' \
	  || { echo "verify-mips: FP ABI is not Soft float"; exit 1; }
	@echo "verify-mips: OK (MIPS BE, soft-float, static) — $$(ls -l $(MIPS_BIN) | awk '{print $$5}') bytes"

## copy the verified MIPS binary to a stable dist/ path for scp
## (used by the hAP verification SOP — see openspec verify-rust-self-unlock-on-hap)
dist: verify-mips
	@mkdir -p dist
	@cp $(MIPS_BIN) dist/dooraccess-rs-mips
	@echo "dist: dist/dooraccess-rs-mips — $$(ls -l dist/dooraccess-rs-mips | awk '{print $$5}') bytes"

clean:
	cargo clean
	rm -rf dist
