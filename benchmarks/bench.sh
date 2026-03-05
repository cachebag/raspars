#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RASPARS="$ROOT/target/release/raspars-cli"
FIXTURES_DIR="$ROOT/raspars-formats/tests/fixtures"
README="$ROOT/benchmarks/README.md"
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

if [ ! -f "$RASPARS" ]; then
    echo "Building raspars-cli (release)..."
    cargo build --release -p raspars-cli --manifest-path "$ROOT/Cargo.toml"
fi

printf "%-20s %-20s %8s %8s %8s %7s %7s\n" "project" "file" "original" "zstd" "raspars" "zstd%" "rsp%"
printf "%-20s %-20s %8s %8s %8s %7s %7s\n" "-------" "----" "--------" "----" "-------" "-----" "----"

MD_ROWS=""

for dir in "$FIXTURES_DIR"/*/; do
    project=$(basename "$dir")

    for lockfile in "$dir"Cargo.lock "$dir"package-lock.json; do
        [ -f "$lockfile" ] || continue

        orig_size=$(stat -c%s "$lockfile")
        orig_size_k=$((orig_size / 1024))

        zstd -f -q "$lockfile" -o "$TMPDIR/zstd.out"
        zstd_size=$(stat -c%s "$TMPDIR/zstd.out")
        zstd_size_k=$((zstd_size / 1024))

        fname=$(basename "$lockfile")
        "$RASPARS" compress "$lockfile" "$TMPDIR/rsp.out"
        rsp_size=$(stat -c%s "$TMPDIR/rsp.out")
        rsp_size_k=$((rsp_size / 1024))

        "$RASPARS" decompress "$TMPDIR/rsp.out" "$TMPDIR/$fname"
        if ! diff -q "$lockfile" "$TMPDIR/$fname" > /dev/null 2>&1; then
            echo "  ⚠ $project ($fname): roundtrip MISMATCH"
        fi

        zstd_pct=$(awk "BEGIN {printf \"%.1f\", ($zstd_size/$orig_size)*100}")
        rsp_pct=$(awk "BEGIN {printf \"%.1f\", ($rsp_size/$orig_size)*100}")

        printf "%-20s %-20s %7sK %7sK %7sK %6s%% %6s%%\n" \
            "$project" \
            "$fname" \
            "$orig_size_k" \
            "$zstd_size_k" \
            "$rsp_size_k" \
            "$zstd_pct" \
            "$rsp_pct"

        MD_ROWS="${MD_ROWS}| ${project} | ${fname} | ${orig_size_k}K | ${zstd_size_k}K | ${rsp_size_k}K | ${zstd_pct}% | ${rsp_pct}% |\n"
    done
done

TODAY=$(date +%Y-%m-%d)

cat > "$README" <<EOF
# Benchmarks

Compares raspars (columnar + zstd) against plain zstd on lockfiles from various open-source projects.

## Usage

\`\`\`bash
cargo build --release -p raspars-cli
bash benchmarks/bench.sh
\`\`\`

## Results

Last updated: ${TODAY}

| project | file | original | zstd | raspars | zstd % | raspars % |
|---------|------|----------|------|---------|--------|-----------|
$(echo -e "$MD_ROWS")

Compression level: zstd default (level 3) for both.

## Fixtures

Lockfiles sourced from open-source projects:

**Cargo.lock** — Rust ecosystem
- **ripgrep** — small CLI tool
- **nmrs** — Rust bindings for NetworkManager
- **alacritty** — terminal emulator
- **rustup** — Rust toolchain manager
- **rustc** — the Rust compiler
- **servo** — web browser engine

**package-lock.json** — npm ecosystem
- **typescript** — TypeScript compiler (lockfileVersion 2)
- **vscode** — Visual Studio Code (lockfileVersion 3)
- **babylon.js** — 3D engine (lockfileVersion 3)
EOF

echo ""
echo "README updated: $README"
