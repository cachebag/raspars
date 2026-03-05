use crate::compress::{CompressedStream, CompressedStreams};

/// Bundle a collection of compressed streams into a single byte vector.
pub fn bundle(streams: &CompressedStreams) -> Result<Vec<u8>, BundleError> {
    let count = streams.streams.len();
    let header_len = HEADER_BASE_LEN + count * ENTRY_LEN;
    let total_data: usize = streams.streams.iter().map(|s| s.data.len()).sum();
    let mut buf = Vec::with_capacity(header_len + total_data);

    buf.extend_from_slice(MAGIC);
    buf.push(FORMAT_VERSION);
    buf.extend_from_slice(&(count as u32).to_le_bytes());

    let mut data_offset = header_len as u64;
    for s in &streams.streams {
        let kind = StreamKind::from_label(&s.label)?;
        buf.push(kind as u8);
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
    let expected_header = HEADER_BASE_LEN + count * ENTRY_LEN;
    if data.len() < expected_header {
        return Err(BundleError::TooShort);
    }

    let mut streams = Vec::with_capacity(count);
    let mut pos = HEADER_BASE_LEN;

    for _ in 0..count {
        let kind = StreamKind::from_tag(data[pos])?;
        pos += 1;

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
            label: kind.to_label().to_owned(),
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
}

/// A kind of stream in a bundle.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamKind {
    Names = 0,
    Versions = 1,
    Sources = 2,
    Checksums = 3,
    Dependencies = 4,
    Header = 5,
}

impl StreamKind {
    fn from_label(label: &str) -> Result<Self, BundleError> {
        match label {
            "names" => Ok(Self::Names),
            "versions" => Ok(Self::Versions),
            "sources" => Ok(Self::Sources),
            "checksums" => Ok(Self::Checksums),
            "dependencies" => Ok(Self::Dependencies),
            "header" => Ok(Self::Header),
            other => Err(BundleError::UnknownLabel(other.to_owned())),
        }
    }

    fn from_tag(tag: u8) -> Result<Self, BundleError> {
        match tag {
            0 => Ok(Self::Names),
            1 => Ok(Self::Versions),
            2 => Ok(Self::Sources),
            3 => Ok(Self::Checksums),
            4 => Ok(Self::Dependencies),
            5 => Ok(Self::Header),
            other => Err(BundleError::UnknownKind(other)),
        }
    }

    fn to_label(self) -> &'static str {
        match self {
            Self::Names => "names",
            Self::Versions => "versions",
            Self::Sources => "sources",
            Self::Checksums => "checksums",
            Self::Dependencies => "dependencies",
            Self::Header => "header",
        }
    }
}

/// The magic bytes for a bundle.
const MAGIC: &[u8; 4] = b"RSPR";
/// The format version for a bundle.
const FORMAT_VERSION: u8 = 1;
/// The base length of a header in a bundle.
const HEADER_BASE_LEN: usize = 4 + 1 + 4;
/// The length of an entry in a bundle.
const ENTRY_LEN: usize = 1 + 8 + 8 + 8;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::{compress_columns, decompress_bytes, deserialize_strings};

    #[test]
    fn roundtrip() {
        let names: Vec<String> = ["serde", "tokio", "anyhow"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let versions: Vec<String> = ["1.0", "1.49", "1.0"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let sources = vec![Some("registry".into()), None, Some("git".into())];
        let checksums = vec![Some("aabb".into()), Some("ccdd".into()), None];
        let deps = vec![
            vec!["serde_derive".into()],
            vec![],
            vec!["thiserror".into()],
        ];

        let compressed =
            compress_columns("", &names, &versions, &sources, &checksums, &deps).unwrap();
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
        assert_eq!(names, rec_names);
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
