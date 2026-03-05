use crate::compress::{CompressedStream, CompressedStreams};

/// Bundle a collection of compressed streams into a single byte vector.
pub fn bundle(streams: &CompressedStreams) -> Result<Vec<u8>, BundleError> {
    let count = streams.streams.len();

    // Calculate header size dynamically since labels are variable length
    let header_size = HEADER_BASE_LEN
        + streams
            .streams
            .iter()
            .map(|s| {
                // kind + label_len + label + original_len + offset + compressed_len
                1 + 1 + s.label.len() + 8 + 8 + 8
            })
            .sum::<usize>();

    let total_data: usize = streams.streams.iter().map(|s| s.data.len()).sum();
    let mut buf = Vec::with_capacity(header_size + total_data);

    buf.extend_from_slice(MAGIC);
    buf.push(FORMAT_VERSION);
    buf.extend_from_slice(&(count as u32).to_le_bytes());

    let mut data_offset = header_size as u64;
    for s in &streams.streams {
        buf.push(s.kind as u8);
        buf.push(s.label.len() as u8);
        buf.extend_from_slice(s.label.as_bytes());
        buf.extend_from_slice(&(s.original_len as u64).to_le_bytes());
        buf.extend_from_slice(&data_offset.to_le_bytes());
        buf.extend_from_slice(&(s.data.len() as u64).to_le_bytes());
        data_offset += s.data.len() as u64;
    }

    for s in &streams.streams {
        buf.extend_from_slice(&s.data);
    }

    Ok(buf)
}

/// Unbundle a byte vector into a collection of compressed streams.
pub fn unbundle(data: &[u8]) -> Result<CompressedStreams, BundleError> {
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

    let mut streams = Vec::with_capacity(count);
    let mut pos = HEADER_BASE_LEN;

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

        let original_len = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;

        let offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;

        let compressed_len = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;

        if data.len() < offset + compressed_len {
            return Err(BundleError::TooShort);
        }

        streams.push(CompressedStream {
            label,
            kind,
            original_len,
            data: data[offset..offset + compressed_len].to_vec(),
        });
    }

    Ok(CompressedStreams { streams })
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
/// The format version for a bundle.
const FORMAT_VERSION: u8 = 2;
/// The base length of a header in a bundle.
const HEADER_BASE_LEN: usize = 4 + 1 + 4;

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
        let recovered = unbundle(&bundled).unwrap();

        assert_eq!(recovered.streams.len(), compressed.streams.len());
        for (orig, rec) in compressed.streams.iter().zip(recovered.streams.iter()) {
            assert_eq!(orig.label, rec.label);
            assert_eq!(orig.original_len, rec.original_len);
            assert_eq!(orig.data, rec.data);
        }

        let names_stream = recovered
            .streams
            .iter()
            .find(|s| s.label == "names")
            .unwrap();
        let rec_names = deserialize_strings(&decompress_bytes(&names_stream.data).unwrap());
        assert_eq!(rec_names, vec!["serde", "tokio", "anyhow", "clap"]);
    }

    #[test]
    fn rejects_bad_magic() {
        assert!(matches!(
            unbundle(b"NOPE\x01\x00\x00\x00\x00"),
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
            unbundle(b"RSPR\xFF\x00\x00\x00\x00"),
            Err(BundleError::UnsupportedVersion(0xFF))
        ));
    }

    #[test]
    fn empty_bundle() {
        let empty = CompressedStreams { streams: vec![] };
        let bundled = bundle(&empty).unwrap();
        let recovered = unbundle(&bundled).unwrap();
        assert!(recovered.streams.is_empty());
    }
}
