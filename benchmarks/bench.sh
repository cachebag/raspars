#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RASPARS="$ROOT/target/release/raspars-cli"
FIXTURES_DIR="$ROOT/raspars-formats/tests/fixtures"
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

if [ ! -f "$RASPARS" ]; then
    echo "Building raspars-cli (release)..."
    cargo build --release -p raspars-cli --manifest-path "$ROOT/Cargo.toml"
fi

printf "%-14s %8s %8s %8s %7s %7s\n" "project" "original" "zstd" "raspars" "zstd%" "rsp%"
printf "%-14s %8s %8s %8s %7s %7s\n" "-------" "--------" "----" "-------" "-----" "----"

for dir in "$FIXTURES_DIR"/*/; do
    lockfile="$dir/Cargo.lock"
    [ -f "$lockfile" ] || continue

    project=$(basename "$dir")
    orig_size=$(stat -c%s "$lockfile")

    zstd -f -q "$lockfile" -o "$TMPDIR/zstd.out"
    zstd_size=$(stat -c%s "$TMPDIR/zstd.out")

    "$RASPARS" compress "$lockfile" "$TMPDIR/rsp.out"
    rsp_size=$(stat -c%s "$TMPDIR/rsp.out")

    # verify roundtrip
    "$RASPARS" decompress "$TMPDIR/rsp.out" "$TMPDIR/roundtrip.lock"
    if ! diff -q "$lockfile" "$TMPDIR/roundtrip.lock" > /dev/null 2>&1; then
        echo "  ⚠ $project: roundtrip MISMATCH"
    fi

    zstd_pct=$(awk "BEGIN {printf \"%.1f\", ($zstd_size/$orig_size)*100}")
    rsp_pct=$(awk "BEGIN {printf \"%.1f\", ($rsp_size/$orig_size)*100}")

    printf "%-14s %7sK %7sK %7sK %6s%% %6s%%\n" \
        "$project" \
        "$((orig_size / 1024))" \
        "$((zstd_size / 1024))" \
        "$((rsp_size / 1024))" \
        "$zstd_pct" \
        "$rsp_pct"
done
