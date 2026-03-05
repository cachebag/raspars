use std::io;

use raspars_core::models::ColumnSet;

pub trait LockfileFormat {
    fn parse_to_columns(input: &[u8]) -> Result<ColumnSet, io::Error>
    where
        Self: Sized;
    fn reconstruct(columns: ColumnSet) -> Result<Vec<u8>, io::Error>
    where
        Self: Sized;
}
