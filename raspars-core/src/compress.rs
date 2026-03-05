use std::io::{self, Read, Write};

use crate::{
    bundle::ColumnKind,
    models::{ColumnData, ColumnSet},
};

/// Compress all columns into a single zstd stream.
///
/// Serializes each column into a contiguous byte buffer, then compresses the
/// entire buffer as one zstd frame. This gives zstd full cross-column context,
/// eliminating per-column frame overhead and enabling better pattern matching.
pub fn compress_columns(columns: &ColumnSet) -> Result<CompressedBundle, CompressError> {
    compress_columns_with_level(columns, DEFAULT_LEVEL)
}

/// Compress all columns into a single zstd stream at a given compression level.
pub fn compress_columns_with_level(
    columns: &ColumnSet,
    level: i32,
) -> Result<CompressedBundle, CompressError> {
    let mut combined = Vec::new();
    let mut segments = Vec::new();

    for (label, data) in &columns.columns {
        let (bytes, kind) = match data {
            ColumnData::Strings(v) => (serialize_strings(v), ColumnKind::Strings),
            ColumnData::OptionalStrings(v) => {
                (serialize_optional_strings(v), ColumnKind::OptionalStrings)
            }
            ColumnData::StringLists(v) => (serialize_dep_lists(v), ColumnKind::StringLists),
        };
        let offset = combined.len();
        combined.extend_from_slice(&bytes);
        segments.push(ColumnSegment {
            label: label.clone(),
            kind,
            offset,
            len: bytes.len(),
        });
    }

    let original_len = combined.len();
    let compressed = compress_bytes(&combined, level)?;

    Ok(CompressedBundle {
        segments,
        original_len,
        data: compressed,
    })
}

/// Decompress a single zstd frame back into raw bytes.
pub fn decompress_bytes(compressed: &[u8]) -> Result<Vec<u8>, CompressError> {
    let mut decoder = zstd::Decoder::new(compressed)?;
    let mut buf = Vec::new();
    decoder.read_to_end(&mut buf)?;
    Ok(buf)
}

/// Deserialize a raw stream of newline-delimited strings.
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

/// Deserialize a raw stream of newline-delimited optional strings.
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

/// Deserialize a raw stream of tab-separated, newline-delimited dependency lists.
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

/// Serialize a collection of strings as newline-delimited bytes.
pub fn serialize_strings(values: &[String]) -> Vec<u8> {
    let mut buf = Vec::new();
    for v in values {
        buf.extend_from_slice(v.as_bytes());
        buf.push(b'\n');
    }
    buf
}

/// Serialize a collection of optional strings as newline-delimited bytes.
pub fn serialize_optional_strings(values: &[Option<String>]) -> Vec<u8> {
    let mut buf = Vec::new();
    for v in values {
        if let Some(s) = v {
            buf.extend_from_slice(s.as_bytes());
        }
        buf.push(b'\n');
    }
    buf
}

/// Serialize a collection of dependency lists as tab+newline delimited bytes.
pub fn serialize_dep_lists(deps: &[Vec<String>]) -> Vec<u8> {
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

/// Compress raw bytes with zstd at the given level.
fn compress_bytes(raw: &[u8], level: i32) -> Result<Vec<u8>, CompressError> {
    let mut encoder = zstd::Encoder::new(Vec::new(), level)?;
    encoder.write_all(raw)?;
    Ok(encoder.finish()?)
}

/// Something went wrong while compressing or decompressing data.
#[derive(Debug, thiserror::Error)]
pub enum CompressError {
    #[error("zstd compression failed: {0}")]
    Compress(#[from] io::Error),
}

/// Metadata for a single column's position within the combined buffer.
pub struct ColumnSegment {
    pub label: String,
    pub kind: ColumnKind,
    pub offset: usize,
    pub len: usize,
}

/// All columns compressed into a single zstd frame, with segment metadata.
pub struct CompressedBundle {
    pub segments: Vec<ColumnSegment>,
    pub original_len: usize,
    pub data: Vec<u8>,
}

impl CompressedBundle {
    pub fn compressed_size(&self) -> usize {
        self.data.len()
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
        let bundle = compress_columns(&columns).unwrap();

        assert_eq!(bundle.segments.len(), 6);
        assert_eq!(bundle.segments[0].label, "header");
        assert_eq!(bundle.segments[1].label, "names");

        let decompressed = decompress_bytes(&bundle.data).unwrap();

        let names_seg = &bundle.segments[1];
        let raw_names = &decompressed[names_seg.offset..names_seg.offset + names_seg.len];
        let rec_names = deserialize_strings(raw_names);

        let versions_seg = &bundle.segments[2];
        let raw_versions =
            &decompressed[versions_seg.offset..versions_seg.offset + versions_seg.len];
        let rec_versions = deserialize_strings(raw_versions);

        let sources_seg = &bundle.segments[3];
        let raw_sources = &decompressed[sources_seg.offset..sources_seg.offset + sources_seg.len];
        let rec_sources = deserialize_optional_strings(raw_sources);

        let checksums_seg = &bundle.segments[4];
        let raw_checksums =
            &decompressed[checksums_seg.offset..checksums_seg.offset + checksums_seg.len];
        let rec_checksums = deserialize_optional_strings(raw_checksums);

        let deps_seg = &bundle.segments[5];
        let raw_deps = &decompressed[deps_seg.offset..deps_seg.offset + deps_seg.len];
        let rec_deps = deserialize_dep_lists(raw_deps);

        assert_eq!(sample_names(), rec_names);
        assert_eq!(sample_versions(), rec_versions);
        assert_eq!(sample_sources(), rec_sources);
        assert_eq!(sample_checksums(), rec_checksums);
        assert_eq!(sample_deps(), rec_deps);
    }

    #[test]
    fn compressed_is_smaller_than_original() {
        let bundle = compress_columns(&construct_column_set()).unwrap();
        assert!(bundle.compressed_size() < bundle.original_len);
    }

    #[test]
    fn empty_streams() {
        let columns = ColumnSet {
            columns: vec![
                ("names".into(), ColumnData::Strings(vec![])),
                ("versions".into(), ColumnData::Strings(vec![])),
            ],
        };
        let bundle = compress_columns(&columns).unwrap();
        let decompressed = decompress_bytes(&bundle.data).unwrap();
        for seg in &bundle.segments {
            let raw = &decompressed[seg.offset..seg.offset + seg.len];
            assert!(raw.is_empty());
        }
    }
}
