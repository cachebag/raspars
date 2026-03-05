# Benchmarks

Compares raspars (columnar + zstd) against plain zstd on lockfiles from various open-source projects.

## Usage

```bash
cargo build --release -p raspars-cli
bash benchmarks/bench.sh
```

## Results

Last updated: 2026-03-05

| project | file | original | zstd | raspars | zstd % | raspars % |
|---------|------|----------|------|---------|--------|-----------|
| alacritty | Cargo.lock | 70K | 16K | 14K | 23.5% | 21.1% |
| babylon.js | package-lock.json | 1100K | 191K | 175K | 17.4% | 15.9% |
| nmrs | Cargo.lock | 45K | 10K | 9K | 23.7% | 21.3% |
| ripgrep | Cargo.lock | 12K | 3K | 3K | 26.6% | 24.3% |
| rustc | Cargo.lock | 150K | 35K | 29K | 23.4% | 19.5% |
| rustup | Cargo.lock | 93K | 22K | 19K | 23.5% | 20.9% |
| servo | Cargo.lock | 277K | 61K | 53K | 22.1% | 19.4% |
| typescript | package-lock.json | 360K | 50K | 52K | 14.1% | 14.7% |
| vscode | package-lock.json | 758K | 160K | 151K | 21.2% | 19.9% |

Compression level: zstd default (level 3) for both.

**Note on typescript:** This fixture uses lockfileVersion 2, a transitional format from npm v7–v8 that includes a legacy `dependencies` block alongside `packages`. The `dependencies` block accounts for ~98% of the file and is passed through as a raw blob (not columnized). npm v9+ (Node 18+) defaults to lockfileVersion 3 which drops `dependencies` entirely — see babylon.js and vscode for v3 results. Columnar provides no benefit for this fixture.

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
