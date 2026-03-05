use anyhow::Result;
use clap::{Parser, Subcommand};
use std::fs;
use std::path::PathBuf;

use raspars_core::bundle;
use raspars_core::compress::compress_columns;
use raspars_core::decompress::decompress_archive;
use raspars_formats::lockfiles::cargo_lock::CargoLock;
use raspars_formats::lockfiles::format::LockfileFormat;

#[derive(Parser)]
#[command(name = "raspars")]
#[command(about = "Structure-aware compression for developer artifacts")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Compress { input, output } => {
            let data = fs::read(&input)?;
            let columns = CargoLock::parse_to_columns(&data)?;
            let compressed = compress_columns(&columns)?;
            let bundled = bundle::bundle(&compressed)?;
            fs::write(&output, bundled)?;
        }
        Commands::Decompress { input, output } => {
            let data = fs::read(&input)?;
            let columns = decompress_archive(&data)?;
            let reconstructed = CargoLock::reconstruct(columns)?;
            fs::write(&output, reconstructed)?;
        }
    }

    Ok(())
}

#[derive(Subcommand)]
enum Commands {
    Compress { input: PathBuf, output: PathBuf },
    Decompress { input: PathBuf, output: PathBuf },
}
