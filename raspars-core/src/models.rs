#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnData {
    Strings(Vec<String>),
    OptionalStrings(Vec<Option<String>>),
    StringLists(Vec<Vec<String>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSet {
    pub columns: Vec<(String, ColumnData)>,
}
