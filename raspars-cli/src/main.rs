use anyhow::Result;
use clap::{Parser, Subcommand};
use std::fs;
use std::io::BufReader;
use std::path::PathBuf;

use raspars_core::bundle;
use raspars_core::compress::compress_columns;
use raspars_core::decompress::decompress_archive;
use raspars_formats::cargo_lock;

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
            let file = fs::File::open(&input)?;
            let lock = cargo_lock::parse(BufReader::new(file))?;
            let compressed = compress_columns(
                &lock.header,
                &lock.names,
                &lock.versions,
                &lock.sources,
                &lock.checksums,
                &lock.dependencies,
            )?;
            let bundled = bundle::bundle(&compressed)?;
            fs::write(&output, bundled)?;
        }
        Commands::Decompress { input, output } => {
            let data = fs::read(&input)?;
            let reconstructed = decompress_archive(&data)?;
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
