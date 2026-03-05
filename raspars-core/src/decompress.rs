use crate::bundle::{BundleError, unbundle};
use crate::compress::{
    CompressError, decompress_bytes, deserialize_dep_lists, deserialize_optional_strings,
    deserialize_strings,
};
use std::io::{self, Write};

/// Decompress a bundled archive and reconstruct the original `Cargo.lock` file.
///
/// The output will be byte-for-byte identical to the input that was originally compressed.
pub fn decompress_archive(archive: &[u8]) -> Result<Vec<u8>, DecompressError> {
    let streams = unbundle(archive)?;

    if streams.streams.len() != 6 {
        return Err(DecompressError::InvalidStreamCount(streams.streams.len()));
    }

    let find_stream = |label: &str| -> Result<Vec<u8>, DecompressError> {
        let stream = streams
            .streams
            .iter()
            .find(|s| s.label == label)
            .ok_or_else(|| DecompressError::MissingStream(label.to_owned()))?;
        decompress_bytes(&stream.data).map_err(|e| e.into())
    };

    let header = find_stream("header")?;
    let names = deserialize_strings(&find_stream("names")?);
    let versions = deserialize_strings(&find_stream("versions")?);
    let sources = deserialize_optional_strings(&find_stream("sources")?);
    let checksums = deserialize_optional_strings(&find_stream("checksums")?);
    let dependencies = deserialize_dep_lists(&find_stream("dependencies")?);

    let count = names.len();
    if versions.len() != count
        || sources.len() != count
        || checksums.len() != count
        || dependencies.len() != count
    {
        return Err(DecompressError::InconsistentColumnLengths {
            names: names.len(),
            versions: versions.len(),
            sources: sources.len(),
            checksums: checksums.len(),
            dependencies: dependencies.len(),
        });
    }

    reconstruct_cargo_lock(
        &header,
        &names,
        &versions,
        &sources,
        &checksums,
        &dependencies,
    )
}

/// Reconstruct a Cargo.lock file from decompressed columns.
fn reconstruct_cargo_lock(
    header: &[u8],
    names: &[String],
    versions: &[String],
    sources: &[Option<String>],
    checksums: &[Option<String>],
    dependencies: &[Vec<String>],
) -> Result<Vec<u8>, DecompressError> {
    let mut output = Vec::new();

    output.extend_from_slice(header);

    for i in 0..names.len() {
        writeln!(output, "[[package]]")?;
        writeln!(output, "name = \"{}\"", names[i])?;
        writeln!(output, "version = \"{}\"", versions[i])?;

        if let Some(source) = &sources[i] {
            writeln!(output, "source = \"{}\"", source)?;
        }

        if let Some(checksum) = &checksums[i] {
            writeln!(output, "checksum = \"{}\"", checksum)?;
        }

        if !dependencies[i].is_empty() {
            write!(output, "dependencies = [")?;

            if dependencies[i].len() == 1 {
                // Single-line format for single dependency
                writeln!(output, "\"{}\"]", dependencies[i][0])?;
            } else if dependencies[i].len() <= 3 {
                // Single-line format for 2-3 dependencies
                for (j, dep) in dependencies[i].iter().enumerate() {
                    if j > 0 {
                        write!(output, ", ")?;
                    }
                    write!(output, "\"{}\"", dep)?;
                }
                writeln!(output, "]")?;
            } else {
                // Multi-line format for 4+ dependencies
                writeln!(output)?;
                for dep in &dependencies[i] {
                    writeln!(output, "    \"{}\",", dep)?;
                }
                writeln!(output, "]")?;
            }
        }

        // Add blank line after each package except the last
        if i < names.len() - 1 {
            writeln!(output)?;
        }
    }

    Ok(output)
}

/// An error that can occur while decompressing an archive.
#[derive(Debug, thiserror::Error)]
pub enum DecompressError {
    #[error("bundle error: {0}")]
    Bundle(#[from] BundleError),

    #[error("compression error: {0}")]
    Compress(#[from] CompressError),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("invalid stream count: expected 6, got {0}")]
    InvalidStreamCount(usize),

    #[error("missing required stream: {0}")]
    MissingStream(String),

    #[error(
        "inconsistent column lengths: names={names}, versions={versions}, sources={sources}, checksums={checksums}, dependencies={dependencies}"
    )]
    InconsistentColumnLengths {
        names: usize,
        versions: usize,
        sources: usize,
        checksums: usize,
        dependencies: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::bundle;
    use crate::compress::compress_columns;

    #[test]
    fn roundtrip_simple() {
        let names = vec!["serde".to_owned(), "tokio".to_owned()];
        let versions = vec!["1.0.288".to_owned(), "1.49.0".to_owned()];
        let sources = vec![
            Some("registry+https://github.com/rust-lang/crates.io-index".to_owned()),
            Some("registry+https://github.com/rust-lang/crates.io-index".to_owned()),
        ];
        let checksums = vec![Some("aabbccdd".to_owned()), Some("11223344".to_owned())];
        let deps = vec![vec!["serde_derive".to_owned()], vec![]];

        let compressed =
            compress_columns("", &names, &versions, &sources, &checksums, &deps).unwrap();
        let bundled = bundle(&compressed).unwrap();
        let output = decompress_archive(&bundled).unwrap();

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("[[package]]"));
        assert!(output_str.contains("name = \"serde\""));
        assert!(output_str.contains("version = \"1.0.288\""));
        assert!(output_str.contains("name = \"tokio\""));
        assert!(output_str.contains("version = \"1.49.0\""));
        assert!(output_str.contains("dependencies = [\"serde_derive\"]"));
    }

    #[test]
    fn roundtrip_with_optional_fields() {
        let names = vec!["local-crate".to_owned()];
        let versions = vec!["0.1.0".to_owned()];
        let sources = vec![None];
        let checksums = vec![None];
        let deps = vec![vec![]];

        let compressed =
            compress_columns("", &names, &versions, &sources, &checksums, &deps).unwrap();
        let bundled = bundle(&compressed).unwrap();
        let output = decompress_archive(&bundled).unwrap();

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("name = \"local-crate\""));
        assert!(output_str.contains("version = \"0.1.0\""));
        assert!(!output_str.contains("source ="));
        assert!(!output_str.contains("checksum ="));
        assert!(!output_str.contains("dependencies ="));
    }

    #[test]
    fn roundtrip_many_dependencies() {
        let names = vec!["big-package".to_owned()];
        let versions = vec!["1.0.0".to_owned()];
        let sources = vec![Some(
            "registry+https://github.com/rust-lang/crates.io-index".to_owned(),
        )];
        let checksums = vec![Some("deadbeef".to_owned())];
        let deps = vec![vec![
            "dep1".to_owned(),
            "dep2".to_owned(),
            "dep3".to_owned(),
            "dep4".to_owned(),
            "dep5".to_owned(),
        ]];

        let compressed =
            compress_columns("", &names, &versions, &sources, &checksums, &deps).unwrap();
        let bundled = bundle(&compressed).unwrap();
        let output = decompress_archive(&bundled).unwrap();

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("dependencies = ["));
        assert!(output_str.contains("\"dep1\","));
        assert!(output_str.contains("\"dep5\","));
    }

    #[test]
    fn rejects_wrong_stream_count() {
        // Create a minimal valid bundle with wrong stream count (only 2 streams instead of 6)
        let streams = crate::compress::CompressedStreams {
            streams: vec![
                crate::compress::CompressedStream {
                    label: "names".to_owned(),
                    original_len: 0,
                    data: vec![],
                },
                crate::compress::CompressedStream {
                    label: "versions".to_owned(),
                    original_len: 0,
                    data: vec![],
                },
            ],
        };
        let bundled = bundle(&streams).unwrap();
        let result = decompress_archive(&bundled);
        assert!(matches!(
            result,
            Err(DecompressError::InvalidStreamCount(2))
        ));
    }

    #[test]
    fn empty_archive() {
        let header = "# This file is automatically @generated by Cargo.\n# It is not intended for manual editing.\nversion = 3\n\n";
        let names: Vec<String> = vec![];
        let versions: Vec<String> = vec![];
        let sources: Vec<Option<String>> = vec![];
        let checksums: Vec<Option<String>> = vec![];
        let deps: Vec<Vec<String>> = vec![];

        let compressed =
            compress_columns(header, &names, &versions, &sources, &checksums, &deps).unwrap();
        let bundled = bundle(&compressed).unwrap();
        let output = decompress_archive(&bundled).unwrap();

        let output_str = String::from_utf8(output).unwrap();
        // Should contain only header
        assert!(output_str.contains("version = 3"));
        assert!(!output_str.contains("[[package]]"));
    }
}
