use crate::compress::CompressedBundle;

/// Bundle a compressed column set into a single byte vector.
///
/// Format (v3 — single-stream):
///   [MAGIC 4B] [VERSION 1B] [segment_count u32 LE]
///   [compressed_len u64 LE] [original_len u64 LE]
///   for each segment:
///     [kind u8] [label_len u8] [label ...] [offset u64 LE] [len u64 LE]
///   [compressed data ...]
pub fn bundle(compressed: &CompressedBundle) -> Result<Vec<u8>, BundleError> {
    let count = compressed.segments.len();

    let header_size = HEADER_BASE_LEN
        + compressed
            .segments
            .iter()
            .map(|s| 1 + 1 + s.label.len() + 8 + 8)
            .sum::<usize>();

    let mut buf = Vec::with_capacity(header_size + compressed.data.len());

    buf.extend_from_slice(MAGIC);
    buf.push(FORMAT_VERSION);
    buf.extend_from_slice(&(count as u32).to_le_bytes());
    buf.extend_from_slice(&(compressed.data.len() as u64).to_le_bytes());
    buf.extend_from_slice(&(compressed.original_len as u64).to_le_bytes());

    for seg in &compressed.segments {
        buf.push(seg.kind as u8);
        buf.push(seg.label.len() as u8);
        buf.extend_from_slice(seg.label.as_bytes());
        buf.extend_from_slice(&(seg.offset as u64).to_le_bytes());
        buf.extend_from_slice(&(seg.len as u64).to_le_bytes());
    }

    buf.extend_from_slice(&compressed.data);

    Ok(buf)
}

/// Metadata for a column segment within the decompressed buffer.
pub struct SegmentMeta {
    pub label: String,
    pub kind: ColumnKind,
    pub offset: usize,
    pub len: usize,
}

/// Result of unbundling: segment metadata + the single compressed blob.
pub struct UnbundledArchive {
    pub segments: Vec<SegmentMeta>,
    pub compressed_data: Vec<u8>,
}

/// Unbundle a byte vector into segment metadata and the compressed data blob.
pub fn unbundle(data: &[u8]) -> Result<UnbundledArchive, BundleError> {
    if data.len() < HEADER_BASE_LEN {
        return Err(BundleError::TooShort);
    }
    if &data[..MAGIC.len()] != MAGIC {
        return Err(BundleError::BadMagic);
    }
    if data[4] != FORMAT_VERSION {
        return Err(BundleError::UnsupportedVersion(data[4]));
    }

    let count = u32::from_le_bytes(data[5..9].try_into().unwrap()) as usize;
    let compressed_len = u64::from_le_bytes(data[9..17].try_into().unwrap()) as usize;
    let _original_len = u64::from_le_bytes(data[17..25].try_into().unwrap()) as usize;

    let mut pos = HEADER_BASE_LEN;
    let mut segments = Vec::with_capacity(count);

    for _ in 0..count {
        if pos >= data.len() {
            return Err(BundleError::TooShort);
        }

        let kind = ColumnKind::from_tag(data[pos])?;
        pos += 1;

        let label_len = data[pos] as usize;
        pos += 1;

        if pos + label_len > data.len() {
            return Err(BundleError::TooShort);
        }
        let label = String::from_utf8(data[pos..pos + label_len].to_vec())
            .map_err(|_| BundleError::InvalidLabel)?;
        pos += label_len;

        if pos + 16 > data.len() {
            return Err(BundleError::TooShort);
        }
        let offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;
        let len = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;

        segments.push(SegmentMeta {
            label,
            kind,
            offset,
            len,
        });
    }

    if data.len() < pos + compressed_len {
        return Err(BundleError::TooShort);
    }

    let compressed_data = data[pos..pos + compressed_len].to_vec();

    Ok(UnbundledArchive {
        segments,
        compressed_data,
    })
}

/// An error that can occur while bundling or unbundling streams.
#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error("input too short to contain a valid bundle")]
    TooShort,
    #[error("bad magic bytes")]
    BadMagic,
    #[error("unsupported bundle version: {0}")]
    UnsupportedVersion(u8),
    #[error("unknown stream kind tag: {0}")]
    UnknownKind(u8),
    #[error("unknown stream label: {0}")]
    UnknownLabel(String),
    #[error("invalid utf-8 label")]
    InvalidLabel,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    Strings = 0,
    OptionalStrings = 1,
    StringLists = 2,
}

impl ColumnKind {
    pub fn from_tag(tag: u8) -> Result<Self, BundleError> {
        match tag {
            0 => Ok(Self::Strings),
            1 => Ok(Self::OptionalStrings),
            2 => Ok(Self::StringLists),
            other => Err(BundleError::UnknownKind(other)),
        }
    }
}

/// The magic bytes for a bundle.
const MAGIC: &[u8; 4] = b"RSPR";
/// The format version for a bundle (v3 = single-stream).
const FORMAT_VERSION: u8 = 3;
/// Base header length: magic(4) + version(1) + count(4) + compressed_len(8) + original_len(8).
const HEADER_BASE_LEN: usize = 4 + 1 + 4 + 8 + 8;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::{compress_columns, decompress_bytes, deserialize_strings};
    use crate::test_helpers::helpers::construct_column_set;

    #[test]
    fn roundtrip() {
        let columns = construct_column_set();
        let compressed = compress_columns(&columns).unwrap();
        let bundled = bundle(&compressed).unwrap();
        let archive = unbundle(&bundled).unwrap();

        assert_eq!(archive.segments.len(), compressed.segments.len());

        let decompressed = decompress_bytes(&archive.compressed_data).unwrap();

        let names_seg = archive
            .segments
            .iter()
            .find(|s| s.label == "names")
            .unwrap();
        let raw_names = &decompressed[names_seg.offset..names_seg.offset + names_seg.len];
        let rec_names = deserialize_strings(raw_names);
        assert_eq!(rec_names, vec!["serde", "tokio", "anyhow", "clap"]);
    }

    #[test]
    fn rejects_bad_magic() {
        assert!(matches!(
            unbundle(b"NOPE\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
            Err(BundleError::BadMagic)
        ));
    }

    #[test]
    fn rejects_truncated_input() {
        assert!(matches!(unbundle(b"RS"), Err(BundleError::TooShort)));
    }

    #[test]
    fn rejects_unsupported_version() {
        assert!(matches!(
            unbundle(b"RSPR\xFF\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
            Err(BundleError::UnsupportedVersion(0xFF))
        ));
    }

    #[test]
    fn empty_bundle() {
        let columns = crate::models::ColumnSet { columns: vec![] };
        let compressed = compress_columns(&columns).unwrap();
        let bundled = bundle(&compressed).unwrap();
        let archive = unbundle(&bundled).unwrap();
        assert!(archive.segments.is_empty());
    }
}
