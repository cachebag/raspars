use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use std::fs;
use std::path::{Path, PathBuf};

use raspars_core::bundle;
use raspars_core::compress::compress_columns;
use raspars_core::decompress::decompress_archive;
use raspars_core::models::ColumnSet;
use raspars_formats::lockfiles::{
    cargo_lock::CargoLock, format::LockfileFormat, package_lock::PackageLock, pnpm_lock::PnpmLock,
};

#[derive(Parser)]
#[command(name = "raspars")]
#[command(about = "Structure-aware compression for developer artifacts")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Compress { input: PathBuf, output: PathBuf },
    Decompress { input: PathBuf, output: PathBuf },
}

/// Supported lockfile formats, detected from filename.
enum LockfileKind {
    Cargo,
    Npm,
    Pnpm,
}

fn detect_format(path: &Path) -> Result<LockfileKind> {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    match name {
        "Cargo.lock" => Ok(LockfileKind::Cargo),
        "package-lock.json" => Ok(LockfileKind::Npm),
        "pnpm-lock.yaml" | "pnpm-lockfile.yml" => Ok(LockfileKind::Pnpm),
        _ => bail!("unsupported file format: {name}"),
    }
}

fn parse_to_columns(kind: &LockfileKind, data: &[u8]) -> Result<ColumnSet> {
    let columns = match kind {
        LockfileKind::Cargo => CargoLock::parse_to_columns(data)?,
        LockfileKind::Npm => PackageLock::parse_to_columns(data)?,
        LockfileKind::Pnpm => PnpmLock::parse_to_columns(data)?,
    };
    Ok(columns)
}

fn reconstruct(kind: &LockfileKind, columns: ColumnSet) -> Result<Vec<u8>> {
    let bytes = match kind {
        LockfileKind::Cargo => CargoLock::reconstruct(columns)?,
        LockfileKind::Npm => PackageLock::reconstruct(columns)?,
        LockfileKind::Pnpm => PnpmLock::reconstruct(columns)?,
    };
    Ok(bytes)
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Compress { input, output } => {
            let format = detect_format(&input)?;
            let data = fs::read(&input)?;
            let columns = parse_to_columns(&format, &data)?;
            let compressed = compress_columns(&columns)?;
            let bundled = bundle::bundle(&compressed)?;
            fs::write(&output, bundled)?;
        }
        Commands::Decompress { input, output } => {
            let format = detect_format(&output)?;
            let data = fs::read(&input)?;
            let columns = decompress_archive(&data)?;
            let reconstructed = reconstruct(&format, columns)?;
            fs::write(&output, reconstructed)?;
        }
    }

    Ok(())
}
