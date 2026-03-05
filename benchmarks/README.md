# Benchmarks

Compares raspars (columnar + zstd) against plain zstd on real-world `Cargo.lock` files.

## Usage

```bash
cargo build --release -p raspars-cli
bash benchmarks/bench.sh
```

## Results

All fixtures roundtrip byte-perfectly.

| project    | original | zstd  | raspars | zstd % | raspars % |
|------------|----------|-------|---------|--------|-----------|
| ripgrep    | 12K      | 3K    | 3K      | 26.6%  | 24.8%     |
| nmrs       | 45K      | 10K   | 9K      | 23.7%  | 21.0%     |
| alacritty  | 70K      | 16K   | 14K     | 23.5%  | 20.7%     |
| rustup     | 93K      | 22K   | 19K     | 23.5%  | 20.3%     |
| rustc      | 150K     | 35K   | 28K     | 23.4%  | 19.0%     |
| servo      | 277K     | 61K   | 52K     | 22.1%  | 18.9%     |

Compression level: zstd default (level 3) for both.

## Fixtures

Lockfiles sourced from open-source Rust projects:

- **ripgrep** — small CLI tool
- **nmrs** — medium project
- **alacritty** — terminal emulator
- **rustup** — Rust toolchain manager
- **rustc** — the Rust compiler
- **servo** — web browser engine
