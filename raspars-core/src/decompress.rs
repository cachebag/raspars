use crate::bundle::{BundleError, ColumnKind, unbundle};
use crate::compress::{
    CompressError, decompress_bytes, deserialize_dep_lists, deserialize_optional_strings,
    deserialize_strings,
};
use crate::models::{ColumnData, ColumnSet};
use std::io;

/// Decompress a bundled archive back into a ColumnSet.
///
/// Unbundles the header to get segment metadata, decompresses the single zstd
/// frame, then slices out each column's bytes and deserializes them.
pub fn decompress_archive(archive: &[u8]) -> Result<ColumnSet, DecompressError> {
    let unbundled = unbundle(archive)?;
    let decompressed = decompress_bytes(&unbundled.compressed_data)?;
    let mut columns = Vec::new();

    for seg in &unbundled.segments {
        if seg.offset + seg.len > decompressed.len() {
            return Err(DecompressError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "segment '{}' out of bounds: offset={} len={} total={}",
                    seg.label,
                    seg.offset,
                    seg.len,
                    decompressed.len()
                ),
            )));
        }
        let raw = &decompressed[seg.offset..seg.offset + seg.len];
        let data = match seg.kind {
            ColumnKind::Strings => ColumnData::Strings(deserialize_strings(raw)),
            ColumnKind::OptionalStrings => {
                ColumnData::OptionalStrings(deserialize_optional_strings(raw))
            }
            ColumnKind::StringLists => ColumnData::StringLists(deserialize_dep_lists(raw)),
        };
        columns.push((seg.label.clone(), data));
    }

    Ok(ColumnSet { columns })
}

#[derive(Debug, thiserror::Error)]
pub enum DecompressError {
    #[error("bundle error: {0}")]
    Bundle(#[from] BundleError),

    #[error("compression error: {0}")]
    Compress(#[from] CompressError),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::bundle;
    use crate::compress::compress_columns;
    use crate::test_helpers::helpers::construct_column_set;

    #[test]
    fn roundtrip_columns() {
        let original = construct_column_set();
        let compressed = compress_columns(&original).unwrap();
        let bundled = bundle(&compressed).unwrap();
        let recovered = decompress_archive(&bundled).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn empty_columns() {
        let columns = ColumnSet {
            columns: vec![
                ("names".into(), ColumnData::Strings(vec![])),
                ("versions".into(), ColumnData::Strings(vec![])),
            ],
        };
        let compressed = compress_columns(&columns).unwrap();
        let bundled = bundle(&compressed).unwrap();
        let recovered = decompress_archive(&bundled).unwrap();

        assert_eq!(columns, recovered);
    }
}
