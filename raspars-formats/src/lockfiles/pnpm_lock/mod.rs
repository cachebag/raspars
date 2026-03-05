use std::io;

use raspars_core::models::{ColumnData, ColumnSet};

use super::format::LockfileFormat;

pub struct PnpmLock;

impl LockfileFormat for PnpmLock {
    fn parse_to_columns(input: &[u8]) -> Result<ColumnSet, io::Error> {
        let streams = parse(input)?;
        Ok(streams.into_column_set())
    }

    fn reconstruct(columns: ColumnSet) -> Result<Vec<u8>, io::Error> {
        let streams = PnpmLockStreams::from_column_set(&columns);
        let mut out = Vec::new();

        out.extend_from_slice(streams.header.as_bytes());
        out.extend_from_slice(b"packages:\n");

        for i in 0..streams.pkg_keys.len() {
            out.push(b'\n');
            out.extend_from_slice(b"  ");
            out.extend_from_slice(streams.pkg_keys[i].as_bytes());
            out.extend_from_slice(b":\n");
            out.extend_from_slice(b"    resolution: ");
            out.extend_from_slice(streams.pkg_resolutions[i].as_bytes());
            out.push(b'\n');

            if !streams.pkg_residuals[i].is_empty() {
                let decoded = streams.pkg_residuals[i].replace(RESIDUAL_LINE_SEP, "\n");
                out.extend_from_slice(decoded.as_bytes());
            }
        }

        out.push(b'\n');
        out.extend_from_slice(streams.footer.as_bytes());

        Ok(out)
    }
}

/// Parse a pnpm-lock.yaml file into columnar streams via line-based splitting.
fn parse(input: &[u8]) -> Result<PnpmLockStreams, io::Error> {
    let text =
        std::str::from_utf8(input).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let (header, packages_body, footer) = split_sections(text)?;

    let mut streams = PnpmLockStreams {
        header: header.to_owned(),
        footer: footer.to_owned(),
        ..Default::default()
    };

    parse_packages(packages_body, &mut streams);

    Ok(streams)
}

/// Split raw text into header (before `packages:\n`), packages body, and
/// footer (`snapshots:` line through EOF).
fn split_sections(text: &str) -> Result<(&str, &str, &str), io::Error> {
    let pkg_line_start = find_top_level_key(text, "packages:")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing packages: key"))?;

    let header = &text[..pkg_line_start];

    let after_pkg_key = &text[pkg_line_start + "packages:\n".len()..];

    let snap_offset = find_top_level_key(after_pkg_key, "snapshots:");

    let (packages_body, footer) = match snap_offset {
        Some(offset) => {
            let abs = pkg_line_start + "packages:\n".len() + offset;
            (
                &text[pkg_line_start + "packages:\n".len()..abs],
                &text[abs..],
            )
        }
        None => (after_pkg_key, ""),
    };

    Ok((header, packages_body, footer))
}

/// Find the byte offset of a top-level YAML key (line starts at column 0).
fn find_top_level_key(text: &str, key: &str) -> Option<usize> {
    let mut offset = 0;
    for line in text.split('\n') {
        if line.starts_with(key) {
            return Some(offset);
        }
        offset += line.len() + 1; // +1 for the \n
    }
    None
}

/// Sentinel byte used to encode newlines within residual strings.
///
/// Column serialization uses `\n` as the entry delimiter, so residuals (which
/// are multi-line YAML fragments) must not contain literal newlines. We replace
/// `\n` with `\0` on parse and reverse it on reconstruct.
const RESIDUAL_LINE_SEP: char = '\0';

/// Extract package entries from the packages section body.
///
/// Each entry starts with a 2-space-indented key line (`  'pkg@ver':\n`)
/// followed by a `    resolution: ...` line and optional residual lines.
/// Entries are separated by blank lines.
fn parse_packages(body: &str, streams: &mut PnpmLockStreams) {
    let mut lines = body.split('\n').peekable();

    while let Some(line) = lines.next() {
        if !line.starts_with("  ") || line.starts_with("    ") {
            continue;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() || !trimmed.ends_with(':') {
            continue;
        }

        let key = &trimmed[..trimmed.len() - 1];
        streams.pkg_keys.push(key.to_owned());

        let mut resolution = String::new();
        let mut residual_lines: Vec<&str> = Vec::new();

        while let Some(&next) = lines.peek() {
            if next.is_empty() {
                break;
            }
            if !next.starts_with("    ") {
                break;
            }

            let field_line = lines.next().unwrap();
            let field_trimmed = field_line.trim_start();

            if let Some(res) = field_trimmed.strip_prefix("resolution: ") {
                resolution = res.to_owned();
            } else {
                residual_lines.push(field_line);
            }
        }

        streams.pkg_resolutions.push(resolution);

        if residual_lines.is_empty() {
            streams.pkg_residuals.push(String::new());
        } else {
            let mut encoded = residual_lines.join(&RESIDUAL_LINE_SEP.to_string());
            encoded.push(RESIDUAL_LINE_SEP);
            streams.pkg_residuals.push(encoded);
        }
    }
}

/// Split a string on `\n`, preserving trailing empty segments that `str::lines()` drops.
fn split_preserving_trailing(s: &str) -> Vec<String> {
    s.split('\n').map(String::from).collect()
}

/// Columnar representation of a pnpm-lock.yaml file.
#[derive(Debug, Clone, Default)]
pub struct PnpmLockStreams {
    pub header: String,
    pub footer: String,
    pub pkg_keys: Vec<String>,
    pub pkg_resolutions: Vec<String>,
    pub pkg_residuals: Vec<String>,
}

impl PnpmLockStreams {
    /// Convert into a format-agnostic ColumnSet for compression.
    pub fn into_column_set(self) -> ColumnSet {
        ColumnSet {
            columns: vec![
                (
                    "header".into(),
                    ColumnData::Strings(split_preserving_trailing(&self.header)),
                ),
                (
                    "footer".into(),
                    ColumnData::Strings(split_preserving_trailing(&self.footer)),
                ),
                ("pkg_keys".into(), ColumnData::Strings(self.pkg_keys)),
                (
                    "pkg_resolutions".into(),
                    ColumnData::Strings(self.pkg_resolutions),
                ),
                (
                    "pkg_residuals".into(),
                    ColumnData::Strings(self.pkg_residuals),
                ),
            ],
        }
    }

    /// Reconstruct from a ColumnSet.
    pub fn from_column_set(columns: &ColumnSet) -> Self {
        let mut streams = PnpmLockStreams::default();
        for (label, data) in &columns.columns {
            match (label.as_str(), data) {
                ("header", ColumnData::Strings(v)) => streams.header = v.join("\n"),
                ("footer", ColumnData::Strings(v)) => streams.footer = v.join("\n"),
                ("pkg_keys", ColumnData::Strings(v)) => streams.pkg_keys = v.clone(),
                ("pkg_resolutions", ColumnData::Strings(v)) => {
                    streams.pkg_resolutions = v.clone();
                }
                ("pkg_residuals", ColumnData::Strings(v)) => streams.pkg_residuals = v.clone(),
                _ => {}
            }
        }
        streams
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ASTRO: &[u8] = include_bytes!("../../../tests/fixtures/astro/pnpm-lockfile.yml");
    const NUXT: &[u8] = include_bytes!("../../../tests/fixtures/nuxt/pnpm-lockfile.yml");
    const VITE: &[u8] = include_bytes!("../../../tests/fixtures/vite/pnpm-lockfile.yml");

    fn roundtrip_check(name: &str, input: &[u8]) {
        let columns = PnpmLock::parse_to_columns(input).unwrap();
        let output = PnpmLock::reconstruct(columns).unwrap();
        if output != input {
            let pos = output
                .iter()
                .zip(input.iter())
                .position(|(a, b)| a != b)
                .unwrap_or(output.len().min(input.len()));
            let context_start = pos.saturating_sub(80);
            let context_end = (pos + 80).min(output.len()).min(input.len());
            panic!(
                "{name} roundtrip mismatch at byte {pos} (output len={}, input len={})\n  expected: {:?}\n    actual: {:?}",
                output.len(),
                input.len(),
                String::from_utf8_lossy(&input[context_start..context_end]),
                String::from_utf8_lossy(&output[context_start..context_end]),
            );
        }
    }

    #[test]
    fn roundtrip_astro() {
        roundtrip_check("astro", ASTRO);
    }

    #[test]
    fn roundtrip_nuxt() {
        roundtrip_check("nuxt", NUXT);
    }

    #[test]
    fn roundtrip_vite() {
        roundtrip_check("vite", VITE);
    }

    #[test]
    fn parses_package_count() {
        let streams = parse(ASTRO).unwrap();
        assert!(
            streams.pkg_keys.len() > 1000,
            "expected >1000 packages, got {}",
            streams.pkg_keys.len()
        );
    }

    #[test]
    fn column_lengths_consistent() {
        let streams = parse(VITE).unwrap();
        let len = streams.pkg_keys.len();
        assert_eq!(streams.pkg_resolutions.len(), len);
        assert_eq!(streams.pkg_residuals.len(), len);
    }
}
