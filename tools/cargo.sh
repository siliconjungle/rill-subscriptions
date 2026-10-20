#!/bin/sh
# Keep dependency-local historical rust-toolchain files from selecting rustc.
# Build flags remain in Cargo.toml; this wrapper is not a timed build benchmark.
set -eu
export RUSTUP_TOOLCHAIN="${RILL_RUST_TOOLCHAIN:-stable}"
export RUSTC="$(rustup which rustc)"
export RUSTDOC="$(rustup which rustdoc)"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
exec cargo "$@"
