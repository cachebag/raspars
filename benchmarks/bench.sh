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

bench_file() {
    local lockfile="$1"
    [ -f "$lockfile" ] || return 0

    local dir
    dir=$(dirname "$lockfile")
    local project
    project=$(basename "$dir")

    local orig_size orig_size_k zstd_size zstd_size_k rsp_size rsp_size_k fname zstd_pct rsp_pct
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
        "$project" "$fname" "$orig_size_k" "$zstd_size_k" "$rsp_size_k" "$zstd_pct" "$rsp_pct"

    MD_ROWS="${MD_ROWS}| ${project} | ${fname} | ${orig_size_k}K | ${zstd_size_k}K | ${rsp_size_k}K | ${zstd_pct}% | ${rsp_pct}% |\n"
}

for dir in "$FIXTURES_DIR"/*/; do bench_file "$dir"Cargo.lock; done
for dir in "$FIXTURES_DIR"/*/; do bench_file "$dir"package-lock.json; done
for dir in "$FIXTURES_DIR"/*/; do bench_file "$dir"pnpm-lockfile.yml; done

TODAY=$(date +%Y-%m-%d)

TABLE_HEADER="| project | file | original | zstd | raspars | zstd % | raspars % |
|---------|------|----------|------|---------|--------|-----------|"
TABLE_BODY=$(echo -e "$MD_ROWS" | sed '/^$/d')

NEW_TABLE="${TABLE_HEADER}
${TABLE_BODY}"

awk -v date="$TODAY" -v table="$NEW_TABLE" '
    /^Last updated:/ { print "Last updated: " date; next }
    /^\| project/ { in_table=1; print table; next }
    /^\|[-|]/ && in_table { next }
    /^\| / && in_table { next }
    { in_table=0; print }
' "$README" > "$TMPDIR/readme_new.md"

mv "$TMPDIR/readme_new.md" "$README"

echo ""
echo "README updated: $README"
