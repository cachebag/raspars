use std::io::{self, Read, Write};

use crate::{
    bundle::ColumnKind,
    models::{ColumnData, ColumnSet},
};

/// Compress a collection of columns of data into a collection of compressed streams.
pub fn compress_columns(columns: &ColumnSet) -> Result<CompressedStreams, CompressError> {
    compress_columns_with_level(columns, DEFAULT_LEVEL)
}

/// Compress a collection of columns of data into a collection of compressed streams with a custom compression level.
pub fn compress_columns_with_level(
    columns: &ColumnSet,
    level: i32,
) -> Result<CompressedStreams, CompressError> {
    let mut streams = Vec::new();
    for (label, data) in &columns.columns {
        let (bytes, kind) = match data {
            ColumnData::Strings(v) => (serialize_strings(v), ColumnKind::Strings),
            ColumnData::OptionalStrings(v) => {
                (serialize_optional_strings(v), ColumnKind::OptionalStrings)
            }
            ColumnData::StringLists(v) => (serialize_dep_lists(v), ColumnKind::StringLists),
        };
        streams.push(compress_stream(label, kind, &bytes, level)?);
    }
    Ok(CompressedStreams { streams })
}

/// Decompress a compressed stream of data into a raw stream of data.
pub fn decompress_bytes(compressed: &[u8]) -> Result<Vec<u8>, CompressError> {
    let mut decoder = zstd::Decoder::new(compressed)?;
    let mut buf = Vec::new();
    decoder.read_to_end(&mut buf)?;
    Ok(buf)
}

/// Deserialize a raw stream of strings into a collection of strings.
pub fn deserialize_strings(raw: &[u8]) -> Vec<String> {
    if raw.is_empty() {
        return Vec::new();
    }
    let text = String::from_utf8_lossy(raw);
    text.strip_suffix('\n')
        .unwrap_or(&text)
        .split('\n')
        .map(|s| s.to_owned())
        .collect()
}

/// Deserialize a raw stream of optional strings into a collection of optional strings.
pub fn deserialize_optional_strings(raw: &[u8]) -> Vec<Option<String>> {
    if raw.is_empty() {
        return Vec::new();
    }
    let text = String::from_utf8_lossy(raw);
    text.strip_suffix('\n')
        .unwrap_or(&text)
        .split('\n')
        .map(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s.to_owned())
            }
        })
        .collect()
}

/// Deserialize a raw stream of dependency lists into a collection of dependency lists.
pub fn deserialize_dep_lists(raw: &[u8]) -> Vec<Vec<String>> {
    if raw.is_empty() {
        return Vec::new();
    }
    let text = String::from_utf8_lossy(raw);
    text.strip_suffix('\n')
        .unwrap_or(&text)
        .split('\n')
        .map(|line| {
            if line.is_empty() {
                Vec::new()
            } else {
                line.split('\t').map(|s| s.to_owned()).collect()
            }
        })
        .collect()
}

/// Serialize a collection of strings into a raw stream of strings.
fn serialize_strings(values: &[String]) -> Vec<u8> {
    let mut buf = Vec::new();
    for v in values {
        buf.extend_from_slice(v.as_bytes());
        buf.push(b'\n');
    }
    buf
}

/// Serialize a collection of optional strings into a raw stream of optional strings.
fn serialize_optional_strings(values: &[Option<String>]) -> Vec<u8> {
    let mut buf = Vec::new();
    for v in values {
        if let Some(s) = v {
            buf.extend_from_slice(s.as_bytes());
        }
        buf.push(b'\n');
    }
    buf
}

/// Serialize a collection of dependency lists into a raw stream of dependency lists.
fn serialize_dep_lists(deps: &[Vec<String>]) -> Vec<u8> {
    let mut buf = Vec::new();
    for group in deps {
        for (i, dep) in group.iter().enumerate() {
            if i > 0 {
                buf.push(b'\t');
            }
            buf.extend_from_slice(dep.as_bytes());
        }
        buf.push(b'\n');
    }
    buf
}

/// Compress a raw stream of data into a compressed stream of data.
fn compress_bytes(raw: &[u8], level: i32) -> Result<Vec<u8>, CompressError> {
    let mut encoder = zstd::Encoder::new(Vec::new(), level)?;
    encoder.write_all(raw)?;
    Ok(encoder.finish()?)
}

/// Compress a raw stream of data into a compressed stream of data with a custom label.
fn compress_stream(
    label: impl Into<String>,
    kind: ColumnKind,
    raw: &[u8],
    level: i32,
) -> Result<CompressedStream, CompressError> {
    let data = compress_bytes(raw, level)?;
    Ok(CompressedStream {
        label: label.into(),
        kind,
        original_len: raw.len(),
        data,
    })
}

/// Something went wrong while compressing or decompressing data.
#[derive(Debug, thiserror::Error)]
pub enum CompressError {
    #[error("zstd compression failed: {0}")]
    Compress(#[from] io::Error),
}

/// A compressed stream of data.
pub struct CompressedStream {
    pub label: String,
    pub kind: ColumnKind,
    pub original_len: usize,
    pub data: Vec<u8>,
}

impl CompressedStream {
    pub fn ratio(&self) -> f64 {
        if self.original_len == 0 {
            return 0.0;
        }
        self.data.len() as f64 / self.original_len as f64
    }
}

/// A collection of compressed streams of data.
pub struct CompressedStreams {
    pub streams: Vec<CompressedStream>,
}

impl CompressedStreams {
    pub fn total_compressed(&self) -> usize {
        self.streams.iter().map(|s| s.data.len()).sum()
    }

    pub fn total_original(&self) -> usize {
        self.streams.iter().map(|s| s.original_len).sum()
    }
}

/// The default compression level for zstd.
/// https://github.com/facebook/zstd/blob/dev/lib/compress/clevels.h
const DEFAULT_LEVEL: i32 = 3;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::helpers::{
        construct_column_set, sample_checksums, sample_deps, sample_names, sample_sources,
        sample_versions,
    };

    #[test]
    fn roundtrip_strings() {
        let original = sample_names();
        let serialized = serialize_strings(&original);
        let compressed = compress_bytes(&serialized, DEFAULT_LEVEL).unwrap();
        let decompressed = decompress_bytes(&compressed).unwrap();
        let recovered = deserialize_strings(&decompressed);
        assert_eq!(original, recovered);
    }

    #[test]
    fn roundtrip_optional_strings() {
        let original = sample_sources();
        let serialized = serialize_optional_strings(&original);
        let compressed = compress_bytes(&serialized, DEFAULT_LEVEL).unwrap();
        let decompressed = decompress_bytes(&compressed).unwrap();
        let recovered = deserialize_optional_strings(&decompressed);
        assert_eq!(original, recovered);
    }

    #[test]
    fn roundtrip_dep_lists() {
        let original = sample_deps();
        let serialized = serialize_dep_lists(&original);
        let compressed = compress_bytes(&serialized, DEFAULT_LEVEL).unwrap();
        let decompressed = decompress_bytes(&compressed).unwrap();
        let recovered = deserialize_dep_lists(&decompressed);
        assert_eq!(original, recovered);
    }

    #[test]
    fn compress_columns_roundtrip() {
        let columns = construct_column_set();
        let compressed = compress_columns(&columns).unwrap();

        assert_eq!(compressed.streams.len(), 6);
        assert_eq!(compressed.streams[0].label, "header");
        assert_eq!(compressed.streams[1].label, "names");
        assert_eq!(compressed.streams[2].label, "versions");
        assert_eq!(compressed.streams[3].label, "sources");
        assert_eq!(compressed.streams[4].label, "checksums");
        assert_eq!(compressed.streams[5].label, "dependencies");

        let rec_names =
            deserialize_strings(&decompress_bytes(&compressed.streams[1].data).unwrap());
        let rec_versions =
            deserialize_strings(&decompress_bytes(&compressed.streams[2].data).unwrap());
        let rec_sources =
            deserialize_optional_strings(&decompress_bytes(&compressed.streams[3].data).unwrap());
        let rec_checksums =
            deserialize_optional_strings(&decompress_bytes(&compressed.streams[4].data).unwrap());
        let rec_deps =
            deserialize_dep_lists(&decompress_bytes(&compressed.streams[5].data).unwrap());

        assert_eq!(sample_names(), rec_names);
        assert_eq!(sample_versions(), rec_versions);
        assert_eq!(sample_sources(), rec_sources);
        assert_eq!(sample_checksums(), rec_checksums);
        assert_eq!(sample_deps(), rec_deps);
    }

    #[test]
    fn compressed_is_smaller_than_original() {
        let compressed = compress_columns(&construct_column_set()).unwrap();
        assert!(compressed.total_compressed() < compressed.total_original());
    }

    #[test]
    fn ratio_is_between_zero_and_one() {
        let compressed = compress_columns(&construct_column_set()).unwrap();
        for stream in &compressed.streams {
            let r = stream.ratio();
            assert!(r > 0.0, "ratio out of range: {r}");
        }
    }

    #[test]
    fn empty_streams() {
        let columns = ColumnSet {
            columns: vec![
                ("names".into(), ColumnData::Strings(vec![])),
                ("versions".into(), ColumnData::Strings(vec![])),
            ],
        };
        let compressed = compress_columns(&columns).unwrap();
        for stream in &compressed.streams {
            let decompressed = decompress_bytes(&stream.data).unwrap();
            assert!(decompressed.is_empty());
        }
    }
}
