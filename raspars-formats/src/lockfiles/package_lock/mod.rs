use std::collections::HashMap;
use std::io;

use raspars_core::models::{ColumnData, ColumnSet};
use serde_json::{Map, Value};

use super::format::LockfileFormat;

const EXTRACTED_FIELDS: &[&str] = &["version", "resolved", "integrity", "dev"];

pub struct PackageLock;

impl LockfileFormat for PackageLock {
    fn parse_to_columns(input: &[u8]) -> Result<ColumnSet, io::Error> {
        let streams = parse(input)?;
        Ok(streams.into_column_set())
    }

    fn reconstruct(columns: ColumnSet) -> Result<Vec<u8>, io::Error> {
        let streams = PackageLockStreams::from_column_set(&columns);
        let mut output = Vec::new();

        output.extend_from_slice(streams.header.as_bytes());

        let entry_indent = &streams.entry_indent;
        let step = &streams.indent_step;
        let field_indent = format!("{}{}", entry_indent, step);

        let schemas: Vec<Vec<String>> = streams
            .schemas
            .iter()
            .map(|s| {
                serde_json::from_str(s).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("bad schema JSON: {e}"))
                })
            })
            .collect::<Result<_, _>>()?;

        for i in 0..streams.paths.len() {
            if i > 0 {
                output.push(b',');
            }
            output.push(b'\n');

            output.extend_from_slice(
                format!("{}{}: ", entry_indent, json_encode(&streams.paths[i])).as_bytes(),
            );

            let residual: Map<String, Value> = serde_json::from_str(&streams.residuals[i])
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("bad residual JSON: {e}"),
                    )
                })?;

            let schema_idx: usize = streams.schema_indices[i].parse().map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("bad schema index: {e}"))
            })?;

            let key_order = schemas.get(schema_idx).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "schema index out of bounds")
            })?;

            let merged = rebuild_entry(
                key_order,
                &residual,
                &streams.versions[i],
                streams.resolved[i].as_deref(),
                streams.integrity[i].as_deref(),
                &streams.dev_flags[i],
            );

            format_object(&merged, &field_indent, step, &mut output);
        }

        output.extend_from_slice(streams.footer.as_bytes());

        Ok(output)
    }
}

/// Rebuild a full entry object from key_order, residual, and extracted column values.
fn rebuild_entry(
    key_order: &[String],
    residual: &Map<String, Value>,
    version: &str,
    resolved: Option<&str>,
    integrity: Option<&str>,
    dev_flag: &str,
) -> Map<String, Value> {
    let mut entry = Map::new();
    for key in key_order {
        match key.as_str() {
            "version" => {
                entry.insert(key.clone(), Value::String(version.to_owned()));
            }
            "resolved" => {
                if let Some(r) = resolved {
                    entry.insert(key.clone(), Value::String(r.to_owned()));
                }
            }
            "integrity" => {
                if let Some(ig) = integrity {
                    entry.insert(key.clone(), Value::String(ig.to_owned()));
                }
            }
            "dev" => match dev_flag {
                "true" => {
                    entry.insert(key.clone(), Value::Bool(true));
                }
                "false" => {
                    entry.insert(key.clone(), Value::Bool(false));
                }
                _ => {}
            },
            _ => {
                if let Some(val) = residual.get(key) {
                    entry.insert(key.clone(), val.clone());
                }
            }
        }
    }
    entry
}

/// Parse a package-lock.json file into columnar streams.
fn parse(input: &[u8]) -> Result<PackageLockStreams, io::Error> {
    let text =
        std::str::from_utf8(input).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let root: Value =
        serde_json::from_str(text).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let packages = root
        .get("packages")
        .and_then(|v| v.as_object())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing \"packages\" object"))?;

    let (entry_indent, indent_step) = detect_indents(text);
    let (header, footer) = split_header_footer(text)?;

    let mut streams = PackageLockStreams {
        header,
        footer,
        entry_indent,
        indent_step,
        ..Default::default()
    };

    let mut schema_map: HashMap<String, usize> = HashMap::new();

    for (path, entry) in packages {
        let obj = entry.as_object().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("package entry for {path:?} is not an object"),
            )
        })?;

        streams.paths.push(path.clone());

        let key_order: Vec<String> = obj.keys().cloned().collect();
        let schema_json = serde_json::to_string(&key_order)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let next_idx = schema_map.len();
        let idx = *schema_map.entry(schema_json.clone()).or_insert_with(|| {
            streams.schemas.push(schema_json.clone());
            next_idx
        });
        streams.schema_indices.push(idx.to_string());

        streams.versions.push(
            obj.get("version")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned(),
        );

        streams.resolved.push(
            obj.get("resolved")
                .and_then(|v| v.as_str())
                .map(|s| s.to_owned()),
        );

        streams.integrity.push(
            obj.get("integrity")
                .and_then(|v| v.as_str())
                .map(|s| s.to_owned()),
        );

        let dev_flag = match obj.get("dev") {
            Some(Value::Bool(true)) => "true",
            Some(Value::Bool(false)) => "false",
            _ => "",
        };
        streams.dev_flags.push(dev_flag.to_owned());

        let mut residual = Map::new();
        for (key, val) in obj {
            if !EXTRACTED_FIELDS.contains(&key.as_str()) {
                residual.insert(key.clone(), val.clone());
            }
        }
        streams.residuals.push(
            serde_json::to_string(&Value::Object(residual))
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        );
    }

    Ok(streams)
}

/// Split raw input into header (up through the opening `{` of packages)
/// and footer (from the whitespace before the closing `}` of packages to EOF).
fn split_header_footer(text: &str) -> Result<(String, String), io::Error> {
    let packages_key_pos = text.find("\"packages\"").ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "cannot find \"packages\" key")
    })?;

    let after_key = &text[packages_key_pos..];
    let brace_offset = after_key.find('{').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "cannot find packages opening brace",
        )
    })?;

    let header_end = packages_key_pos + brace_offset + 1;
    let header = text[..header_end].to_owned();

    let packages_body = &text[header_end..];
    let mut depth = 1;
    let mut close_pos = None;
    let mut in_string = false;
    let mut escape_next = false;

    for (idx, ch) in packages_body.char_indices() {
        if escape_next {
            escape_next = false;
            continue;
        }
        if ch == '\\' && in_string {
            escape_next = true;
            continue;
        }
        if ch == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        if ch == '{' {
            depth += 1;
        } else if ch == '}' {
            depth -= 1;
            if depth == 0 {
                close_pos = Some(idx);
                break;
            }
        }
    }

    let close = close_pos
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unmatched packages brace"))?;

    let before_close = &packages_body[..close];
    let ws_start = before_close
        .rfind(|c: char| !c.is_whitespace())
        .map(|p| p + 1)
        .unwrap_or(0);

    let footer_start = header_end + ws_start;
    let footer = text[footer_start..].to_owned();

    Ok((header, footer))
}

/// Detect indentation by looking at the first entry inside `"packages": {}`.
///
/// Returns `(entry_indent, indent_step)` where entry_indent is the whitespace
/// before entry keys and indent_step is the per-level indent within entries.
fn detect_indents(text: &str) -> (String, String) {
    let fallback = ("        ".to_owned(), "    ".to_owned());

    let packages_pos = match text.find("\"packages\"") {
        Some(p) => p,
        None => return fallback,
    };
    let after = &text[packages_pos..];
    let brace = match after.find('{') {
        Some(p) => p,
        None => return fallback,
    };
    let inside = &after[brace + 1..];

    let mut entry_ws = 0usize;
    let mut field_ws = 0usize;
    let mut found_entry = false;

    for line in inside.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let leading = line.len() - line.trim_start().len();
        if !found_entry {
            if trimmed.starts_with('"') {
                entry_ws = leading;
                found_entry = true;
            }
        } else if trimmed.starts_with('"') || trimmed == "}" {
            field_ws = leading;
            break;
        }
    }

    if found_entry && field_ws > entry_ws {
        let step = field_ws - entry_ws;
        (" ".repeat(entry_ws), " ".repeat(step))
    } else if found_entry {
        (" ".repeat(entry_ws), "    ".to_owned())
    } else {
        fallback
    }
}

/// Encode a string as a JSON string literal (with quotes and escaping).
fn json_encode(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| format!("\"{}\"", s))
}

/// Format a JSON object with proper indentation, writing directly to output.
fn format_object(map: &Map<String, Value>, current_indent: &str, step: &str, out: &mut Vec<u8>) {
    if map.is_empty() {
        out.extend_from_slice(b"{}");
        return;
    }
    out.push(b'{');
    out.push(b'\n');
    let entries: Vec<_> = map.iter().collect();
    for (i, (k, v)) in entries.iter().enumerate() {
        out.extend_from_slice(current_indent.as_bytes());
        out.extend_from_slice(json_encode(k).as_bytes());
        out.extend_from_slice(b": ");
        format_value_into(v, current_indent, step, out);
        if i < entries.len() - 1 {
            out.push(b',');
        }
        out.push(b'\n');
    }
    if current_indent.len() >= step.len() {
        out.extend_from_slice(&current_indent.as_bytes()[..current_indent.len() - step.len()]);
    }
    out.push(b'}');
}

/// Format any JSON value with proper indentation.
fn format_value_into(val: &Value, current_indent: &str, step: &str, out: &mut Vec<u8>) {
    match val {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        Value::Number(n) => out.extend_from_slice(n.to_string().as_bytes()),
        Value::String(s) => out.extend_from_slice(json_encode(s).as_bytes()),
        Value::Array(arr) => {
            if arr.is_empty() {
                out.extend_from_slice(b"[]");
                return;
            }
            let child_indent = format!("{}{}", current_indent, step);
            out.extend_from_slice(b"[\n");
            for (i, item) in arr.iter().enumerate() {
                out.extend_from_slice(child_indent.as_bytes());
                format_value_into(item, &child_indent, step, out);
                if i < arr.len() - 1 {
                    out.push(b',');
                }
                out.push(b'\n');
            }
            out.extend_from_slice(current_indent.as_bytes());
            out.push(b']');
        }
        Value::Object(map) => {
            if map.is_empty() {
                out.extend_from_slice(b"{}");
                return;
            }
            let child_indent = format!("{}{}", current_indent, step);
            out.push(b'{');
            out.push(b'\n');
            let entries: Vec<_> = map.iter().collect();
            for (i, (k, v)) in entries.iter().enumerate() {
                out.extend_from_slice(child_indent.as_bytes());
                out.extend_from_slice(json_encode(k).as_bytes());
                out.extend_from_slice(b": ");
                format_value_into(v, &child_indent, step, out);
                if i < entries.len() - 1 {
                    out.push(b',');
                }
                out.push(b'\n');
            }
            out.extend_from_slice(current_indent.as_bytes());
            out.push(b'}');
        }
    }
}

/// Columnar representation of a package-lock.json file.
#[derive(Debug, Clone, Default)]
pub struct PackageLockStreams {
    pub header: String,
    pub footer: String,
    pub entry_indent: String,
    pub indent_step: String,
    pub paths: Vec<String>,
    pub versions: Vec<String>,
    pub resolved: Vec<Option<String>>,
    pub integrity: Vec<Option<String>>,
    pub dev_flags: Vec<String>,
    pub schemas: Vec<String>,
    pub schema_indices: Vec<String>,
    pub residuals: Vec<String>,
}

impl PackageLockStreams {
    /// Convert into a format-agnostic ColumnSet for compression.
    pub fn into_column_set(self) -> ColumnSet {
        ColumnSet {
            columns: vec![
                (
                    "header".into(),
                    ColumnData::Strings(self.header.lines().map(String::from).collect()),
                ),
                (
                    "footer".into(),
                    ColumnData::Strings(self.footer.lines().map(String::from).collect()),
                ),
                (
                    "entry_indent".into(),
                    ColumnData::Strings(vec![self.entry_indent]),
                ),
                (
                    "indent_step".into(),
                    ColumnData::Strings(vec![self.indent_step]),
                ),
                ("paths".into(), ColumnData::Strings(self.paths)),
                ("versions".into(), ColumnData::Strings(self.versions)),
                (
                    "resolved".into(),
                    ColumnData::OptionalStrings(self.resolved),
                ),
                (
                    "integrity".into(),
                    ColumnData::OptionalStrings(self.integrity),
                ),
                ("dev_flags".into(), ColumnData::Strings(self.dev_flags)),
                ("schemas".into(), ColumnData::Strings(self.schemas)),
                (
                    "schema_indices".into(),
                    ColumnData::Strings(self.schema_indices),
                ),
                ("residuals".into(), ColumnData::Strings(self.residuals)),
            ],
        }
    }

    /// Reconstruct from a ColumnSet.
    pub fn from_column_set(columns: &ColumnSet) -> Self {
        let mut streams = PackageLockStreams::default();
        for (label, data) in &columns.columns {
            match (label.as_str(), data) {
                ("header", ColumnData::Strings(v)) => streams.header = v.join("\n"),
                ("footer", ColumnData::Strings(v)) => streams.footer = v.join("\n"),
                ("entry_indent", ColumnData::Strings(v)) => {
                    streams.entry_indent = v.first().cloned().unwrap_or_default();
                }
                ("indent_step", ColumnData::Strings(v)) => {
                    streams.indent_step = v.first().cloned().unwrap_or_default();
                }
                ("paths", ColumnData::Strings(v)) => streams.paths = v.clone(),
                ("versions", ColumnData::Strings(v)) => streams.versions = v.clone(),
                ("resolved", ColumnData::OptionalStrings(v)) => streams.resolved = v.clone(),
                ("integrity", ColumnData::OptionalStrings(v)) => streams.integrity = v.clone(),
                ("dev_flags", ColumnData::Strings(v)) => streams.dev_flags = v.clone(),
                ("schemas", ColumnData::Strings(v)) => streams.schemas = v.clone(),
                ("schema_indices", ColumnData::Strings(v)) => {
                    streams.schema_indices = v.clone();
                }
                ("residuals", ColumnData::Strings(v)) => streams.residuals = v.clone(),
                _ => {}
            }
        }
        streams
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TYPESCRIPT: &[u8] =
        include_bytes!("../../../tests/fixtures/typescript/package-lock.json");
    const VSCODE: &[u8] = include_bytes!("../../../tests/fixtures/vscode/package-lock.json");
    const BABYLON: &[u8] = include_bytes!("../../../tests/fixtures/babylon.js/package-lock.json");

    fn roundtrip_check(name: &str, input: &[u8]) {
        let columns = PackageLock::parse_to_columns(input).unwrap();
        let output = PackageLock::reconstruct(columns).unwrap();
        if output != input {
            let pos = output
                .iter()
                .zip(input.iter())
                .position(|(a, b)| a != b)
                .unwrap_or(output.len().min(input.len()));
            let context_start = pos.saturating_sub(60);
            let context_end = (pos + 60).min(output.len()).min(input.len());
            panic!(
                "{name} roundtrip mismatch at byte {pos}\n  expected: {:?}\n    actual: {:?}",
                String::from_utf8_lossy(&input[context_start..context_end]),
                String::from_utf8_lossy(&output[context_start..context_end]),
            );
        }
    }

    #[test]
    fn roundtrip_typescript() {
        roundtrip_check("typescript", TYPESCRIPT);
    }

    #[test]
    fn roundtrip_vscode() {
        roundtrip_check("vscode", VSCODE);
    }

    #[test]
    fn roundtrip_babylon() {
        roundtrip_check("babylon", BABYLON);
    }

    #[test]
    fn parses_package_count() {
        let streams = parse(TYPESCRIPT).unwrap();
        assert!(
            streams.paths.len() > 100,
            "expected >100 packages, got {}",
            streams.paths.len()
        );
    }

    #[test]
    fn column_lengths_consistent() {
        let streams = parse(BABYLON).unwrap();
        let len = streams.paths.len();
        assert_eq!(streams.versions.len(), len);
        assert_eq!(streams.resolved.len(), len);
        assert_eq!(streams.integrity.len(), len);
        assert_eq!(streams.dev_flags.len(), len);
        assert_eq!(streams.schema_indices.len(), len);
        assert_eq!(streams.residuals.len(), len);
        assert!(
            streams.schemas.len() < len,
            "expected fewer unique schemas ({}) than entries ({len})",
            streams.schemas.len()
        );
    }
}
