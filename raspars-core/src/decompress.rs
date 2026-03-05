use crate::bundle::{BundleError, ColumnKind, unbundle};
use crate::compress::{
    CompressError, decompress_bytes, deserialize_dep_lists, deserialize_optional_strings,
    deserialize_strings,
};
use crate::models::{ColumnData, ColumnSet};
use std::io;

/// Decompress a bundled archive and reconstruct the original `Cargo.lock` file.
///
/// The output will be byte-for-byte identical to the input that was originally compressed.
pub fn decompress_archive(archive: &[u8]) -> Result<ColumnSet, DecompressError> {
    let streams = unbundle(archive)?;
    let mut columns = Vec::new();

    for stream in &streams.streams {
        let bytes = decompress_bytes(&stream.data)?;
        let data = match stream.kind {
            ColumnKind::Strings => ColumnData::Strings(deserialize_strings(&bytes)),
            ColumnKind::OptionalStrings => {
                ColumnData::OptionalStrings(deserialize_optional_strings(&bytes))
            }
            ColumnKind::StringLists => ColumnData::StringLists(deserialize_dep_lists(&bytes)),
        };
        columns.push((stream.label.clone(), data));
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
