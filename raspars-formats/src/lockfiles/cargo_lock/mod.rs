use std::io::{self, BufRead, Write};

use raspars_core::models::{ColumnData, ColumnSet};

use super::format::LockfileFormat;

pub struct CargoLock;

impl LockfileFormat for CargoLock {
    fn parse_to_columns(input: &[u8]) -> Result<ColumnSet, io::Error> {
        let streams = parse(io::Cursor::new(input))?;
        Ok(streams.into_column_set())
    }

    fn reconstruct(columns: ColumnSet) -> Result<Vec<u8>, io::Error> {
        let streams = CargoLockStreams::from_column_set(&columns);
        let mut output = Vec::new();

        output.extend_from_slice(streams.header.as_bytes());
        if !streams.header.is_empty() && !streams.header.ends_with('\n') {
            writeln!(output)?;
        }

        for i in 0..streams.names.len() {
            writeln!(output)?;
            writeln!(output, "[[package]]")?;
            writeln!(output, "name = \"{}\"", streams.names[i])?;
            writeln!(output, "version = \"{}\"", streams.versions[i])?;

            if let Some(source) = &streams.sources[i] {
                writeln!(output, "source = \"{}\"", source)?;
            }

            if let Some(checksum) = &streams.checksums[i] {
                writeln!(output, "checksum = \"{}\"", checksum)?;
            }

            if !streams.dependencies[i].is_empty() {
                let deps = &streams.dependencies[i];
                writeln!(output, "dependencies = [")?;
                for dep in deps {
                    writeln!(output, " \"{}\",", dep)?;
                }
                writeln!(output, "]")?;
            }
        }

        Ok(output)
    }
}

/// Parse a Cargo.lock file into a stream of packages.
/// Files could be thousands, if not millions of lines long.
pub fn parse<R: BufRead>(reader: R) -> io::Result<CargoLockStreams> {
    let mut streams = CargoLockStreams::default();
    let mut current = PackageBuilder::default();
    let mut section = Section::None;
    let mut in_deps = false;
    let mut header_lines: Vec<String> = Vec::new();
    let mut past_header = false;

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();

        if trimmed == "[[package]]" {
            if !past_header {
                past_header = true;
                streams.header = header_lines.join("\n") + "\n";
            }
            flush(&mut current, &mut streams);
            current = PackageBuilder::default();
            section = Section::Package;
            in_deps = false;
            continue;
        }

        if trimmed.starts_with("[[") || trimmed.starts_with("[") {
            flush(&mut current, &mut streams);
            current = PackageBuilder::default();
            section = Section::None;
            in_deps = false;
            continue;
        }

        if !past_header {
            header_lines.push(line.clone());
            continue;
        }

        if let Section::Package = section {
            if in_deps {
                if trimmed == "]" {
                    in_deps = false;
                    continue;
                }
                let dep = trimmed.trim_matches(|c| c == '"' || c == ',');
                if !dep.is_empty() {
                    current.dependencies.push(dep.to_owned());
                }
                continue;
            }

            if trimmed.starts_with("dependencies") {
                if let Some(pos) = trimmed.find('[') {
                    let rest = &trimmed[pos..];
                    if rest.trim_end().ends_with(']') {
                        let inner = &rest[1..rest.len() - 1];
                        for dep in inner.split(',') {
                            let dep = dep.trim().trim_matches('"');
                            if !dep.is_empty() {
                                current.dependencies.push(dep.to_owned());
                            }
                        }
                    } else {
                        in_deps = true;
                        let inner = &rest[1..];
                        for dep in inner.split(',') {
                            let dep = dep.trim().trim_matches('"');
                            if !dep.is_empty() {
                                current.dependencies.push(dep.to_owned());
                            }
                        }
                    }
                }
                continue;
            }

            if let Some((key, value)) = parse_kv(trimmed) {
                match key {
                    "name" => current.name = Some(value),
                    "version" => current.version = Some(value),
                    "source" => current.source = Some(value),
                    "checksum" => current.checksum = Some(value),
                    _ => {}
                }
            }
        }
    }

    flush(&mut current, &mut streams);
    Ok(streams)
}

/// Flush a package builder into a cargo lock streams, logging any errors instead of returning them.
fn flush(pkg: &mut PackageBuilder, streams: &mut CargoLockStreams) {
    if !pkg.is_populated() {
        return;
    }

    let name = match pkg.name.take() {
        Some(name) => name,
        None => {
            eprintln!("Warning: Tried to flush a package without a name.");
            return;
        }
    };
    streams.names.push(name);

    let version = match pkg.version.take() {
        Some(version) => version,
        None => {
            eprintln!("Warning: Tried to flush a package without a version.");
            return;
        }
    };
    streams.versions.push(version);

    streams.sources.push(pkg.source.take());
    streams.checksums.push(pkg.checksum.take());
    streams
        .dependencies
        .push(std::mem::take(&mut pkg.dependencies));
}

/// Parse a key-value pair from a line.
fn parse_kv(line: &str) -> Option<(&str, String)> {
    let (key, rest) = line.split_once('=')?;
    let key = key.trim();
    let value = rest.trim().trim_matches('"');
    Some((key, value.to_owned()))
}

/// A stream of packages in a Cargo.lock file.
#[derive(Debug, Clone, Default)]
pub struct CargoLockStreams {
    pub header: String,
    pub names: Vec<String>,
    pub versions: Vec<String>,
    pub sources: Vec<Option<String>>,
    pub checksums: Vec<Option<String>>,
    pub dependencies: Vec<Vec<String>>,
}

impl CargoLockStreams {
    pub fn into_column_set(self) -> ColumnSet {
        ColumnSet {
            columns: vec![
                (
                    "header".into(),
                    ColumnData::Strings(self.header.lines().map(String::from).collect()),
                ),
                ("names".into(), ColumnData::Strings(self.names)),
                ("versions".into(), ColumnData::Strings(self.versions)),
                ("sources".into(), ColumnData::OptionalStrings(self.sources)),
                (
                    "checksums".into(),
                    ColumnData::OptionalStrings(self.checksums),
                ),
                (
                    "dependencies".into(),
                    ColumnData::StringLists(self.dependencies),
                ),
            ],
        }
    }

    pub fn from_column_set(columns: &ColumnSet) -> Self {
        let mut streams = CargoLockStreams::default();
        for (label, data) in &columns.columns {
            match (label.as_str(), data) {
                ("header", ColumnData::Strings(v)) => {
                    streams.header = v.join("\n");
                }
                ("names", ColumnData::Strings(v)) => streams.names = v.clone(),
                ("versions", ColumnData::Strings(v)) => streams.versions = v.clone(),
                ("sources", ColumnData::OptionalStrings(v)) => streams.sources = v.clone(),
                ("checksums", ColumnData::OptionalStrings(v)) => streams.checksums = v.clone(),
                ("dependencies", ColumnData::StringLists(v)) => streams.dependencies = v.clone(),
                _ => {}
            }
        }
        streams
    }
}

/// A builder for a package in a Cargo.lock file.
#[derive(Debug, Clone, Default)]
struct PackageBuilder {
    name: Option<String>,
    version: Option<String>,
    source: Option<String>,
    checksum: Option<String>,
    dependencies: Vec<String>,
}

impl PackageBuilder {
    fn is_populated(&self) -> bool {
        self.name.is_some()
    }
}

enum Section {
    None,
    Package,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    const FIXTURE: &str = include_str!("../../../tests/fixtures/rustup/Cargo.lock");

    fn fixture_streams() -> CargoLockStreams {
        parse(Cursor::new(FIXTURE)).unwrap()
    }

    #[test]
    fn parses_all_packages() {
        let streams = fixture_streams();
        assert_eq!(streams.names.len(), 388);
    }

    #[test]
    fn first_package_is_adler2() {
        let streams = fixture_streams();
        assert_eq!(streams.names[0], "adler2");
        assert_eq!(streams.versions[0], "2.0.1");
        assert_eq!(
            streams.sources[0].as_deref(),
            Some("registry+https://github.com/rust-lang/crates.io-index")
        );
        assert_eq!(
            streams.checksums[0].as_deref(),
            Some("320119579fcad9c21884f5c4861d16174d0e06250625266f50fe6898340abefa")
        );
        assert!(streams.dependencies[0].is_empty());
    }

    #[test]
    fn last_package_is_zstd_sys() {
        let streams = fixture_streams();
        let last = streams.names.len() - 1;
        assert_eq!(streams.names[last], "zstd-sys");
        assert_eq!(streams.versions[last], "2.0.16+zstd.1.5.7");
        assert_eq!(streams.dependencies[last], vec!["cc", "pkg-config"]);
    }

    #[test]
    fn rustup_has_no_source_or_checksum() {
        let streams = fixture_streams();
        let idx = streams.names.iter().position(|n| n == "rustup").unwrap();
        assert_eq!(streams.versions[idx], "1.29.0");
        assert!(streams.sources[idx].is_none());
        assert!(streams.checksums[idx].is_none());
    }

    #[test]
    fn rustup_dependencies() {
        let streams = fixture_streams();
        let idx = streams.names.iter().position(|n| n == "rustup").unwrap();
        let deps = &streams.dependencies[idx];
        assert_eq!(deps.len(), 74);
        assert!(deps.contains(&"anyhow".to_owned()));
        assert!(deps.contains(&"tokio".to_owned()));
        assert!(deps.contains(&"zstd".to_owned()));
    }

    #[test]
    fn aho_corasick_depends_on_memchr() {
        let streams = fixture_streams();
        let idx = streams
            .names
            .iter()
            .position(|n| n == "aho-corasick")
            .unwrap();
        assert_eq!(streams.dependencies[idx], vec!["memchr"]);
    }

    #[test]
    fn handles_versioned_dep_references() {
        let streams = fixture_streams();
        let idx = streams.names.iter().position(|n| n == "chacha20").unwrap();
        assert_eq!(
            streams.dependencies[idx],
            vec!["cfg-if 1.0.4", "cpufeatures 0.3.0", "rand_core 0.10.0"]
        );
    }

    #[test]
    fn duplicate_package_names_both_present() {
        let streams = fixture_streams();
        let anstream_indices: Vec<_> = streams
            .names
            .iter()
            .enumerate()
            .filter(|(_, n)| n.as_str() == "anstream")
            .map(|(i, _)| i)
            .collect();
        assert_eq!(anstream_indices.len(), 2);
        assert_eq!(streams.versions[anstream_indices[0]], "0.6.21");
        assert_eq!(streams.versions[anstream_indices[1]], "1.0.0");
    }

    #[test]
    fn all_registry_sources_share_prefix() {
        let streams = fixture_streams();
        for src in streams.sources.iter().flatten() {
            assert!(
                src.starts_with("registry+"),
                "unexpected source format: {src}"
            );
        }
    }

    #[test]
    fn only_rustup_lacks_source() {
        let streams = fixture_streams();
        let missing: Vec<_> = streams
            .names
            .iter()
            .zip(&streams.sources)
            .filter(|(_, s)| s.is_none())
            .map(|(n, _)| n.as_str())
            .collect();
        assert_eq!(missing, vec!["rustup"]);
    }

    #[test]
    fn stream_lengths_are_consistent() {
        let streams = fixture_streams();
        let len = streams.names.len();
        assert_eq!(streams.versions.len(), len);
        assert_eq!(streams.sources.len(), len);
        assert_eq!(streams.checksums.len(), len);
        assert_eq!(streams.dependencies.len(), len);
    }

    #[test]
    fn empty_input() {
        let streams = parse(Cursor::new("")).unwrap();
        assert!(streams.names.is_empty());
    }

    #[test]
    fn captures_header() {
        let streams = fixture_streams();
        assert!(streams.header.contains("version = 4"));
        assert!(streams.header.contains("@generated by Cargo"));
    }
}
