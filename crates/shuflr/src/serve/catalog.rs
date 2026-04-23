//! Dataset catalog for `serve`.
//!
//! The CLI passes `--dataset id=path` args; we parse them into a
//! lookup table keyed by `dataset_id`. Clients reference datasets by
//! this server-assigned ID; they never transmit filesystem paths
//! (005 §4.1, 002 §7). Catalog is fixed at startup; dynamic reload
//! is deferred (005 §9).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::index::Fingerprint;

/// One entry in the server's dataset catalog.
#[derive(Debug, Clone)]
pub struct DatasetEntry {
    pub id: String,
    pub path: PathBuf,
    /// Fingerprint at startup. Re-fingerprinting per-request would
    /// give stronger guarantees against mid-serve input mutation, but
    /// pays `Fingerprint::from_metadata` on the request path
    /// (microseconds per open, fine if it proves useful).
    pub fingerprint: Fingerprint,
}

/// Immutable snapshot of the catalog. Shared across transports via `Arc`.
#[derive(Debug, Clone)]
pub struct Catalog {
    entries: Arc<HashMap<String, DatasetEntry>>,
}

impl Catalog {
    /// Parse a slice of `id=path` strings into a catalog. Each entry's
    /// path is resolved and its fingerprint computed. Fails if any path
    /// doesn't exist, any ID is duplicated, or any arg is malformed.
    pub fn from_args<S: AsRef<str>>(args: &[S]) -> Result<Self> {
        let mut map = HashMap::new();
        for raw in args {
            let raw = raw.as_ref();
            let (id, path) = raw.split_once('=').ok_or_else(|| {
                Error::Input(format!(
                    "--dataset must be 'ID=PATH', got '{raw}' (missing '=')"
                ))
            })?;
            let id = id.trim();
            if id.is_empty() {
                return Err(Error::Input(format!("--dataset '{raw}' has an empty ID")));
            }
            // Reject characters that would be ambiguous in URL paths
            // (a very narrow allow-list — we'd rather be strict than
            // hand out "/../" risk).
            if !id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
            {
                return Err(Error::Input(format!(
                    "--dataset id '{id}' has disallowed chars; only [A-Za-z0-9._-] allowed"
                )));
            }
            let path = PathBuf::from(path);
            if !path.exists() {
                return Err(Error::NotFound {
                    path: path.display().to_string(),
                });
            }
            let fp = Fingerprint::from_metadata(&path)?;
            if map.contains_key(id) {
                return Err(Error::Input(format!("--dataset id '{id}' declared twice")));
            }
            map.insert(
                id.to_string(),
                DatasetEntry {
                    id: id.to_string(),
                    path,
                    fingerprint: fp,
                },
            );
        }
        Ok(Self {
            entries: Arc::new(map),
        })
    }

    pub fn get(&self, id: &str) -> Option<&DatasetEntry> {
        self.entries.get(id)
    }

    pub fn ids(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(|s| s.as_str())
    }

    pub fn iter(&self) -> impl Iterator<Item = &DatasetEntry> {
        self.entries.values()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Server-local absolute path for a dataset. Logged but never exposed
/// over the wire — keeps the 005 §4.1 "no paths on the wire" invariant.
pub fn resolved_path<'c>(cat: &'c Catalog, id: &str) -> Option<&'c Path> {
    cat.get(id).map(|e| e.path.as_path())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_dataset() {
        let tf = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tf.path(), b"{}\n").unwrap();
        let arg = format!("main={}", tf.path().display());
        let cat = Catalog::from_args(&[arg.as_str()]).unwrap();
        assert_eq!(cat.len(), 1);
        assert!(cat.get("main").is_some());
    }

    #[test]
    fn rejects_empty_id() {
        let tf = tempfile::NamedTempFile::new().unwrap();
        let arg = format!("={}", tf.path().display());
        assert!(Catalog::from_args(&[arg.as_str()]).is_err());
    }

    #[test]
    fn rejects_missing_equals() {
        let tf = tempfile::NamedTempFile::new().unwrap();
        let arg = tf.path().display().to_string();
        assert!(Catalog::from_args(&[arg.as_str()]).is_err());
    }

    #[test]
    fn rejects_weird_id_chars() {
        let tf = tempfile::NamedTempFile::new().unwrap();
        let arg = format!("has/slash={}", tf.path().display());
        assert!(Catalog::from_args(&[arg.as_str()]).is_err());
    }

    #[test]
    fn rejects_missing_path() {
        assert!(Catalog::from_args(&["main=/does/not/exist/xyz.jsonl"]).is_err());
    }

    #[test]
    fn rejects_duplicate_id() {
        let tf1 = tempfile::NamedTempFile::new().unwrap();
        let tf2 = tempfile::NamedTempFile::new().unwrap();
        let a = format!("main={}", tf1.path().display());
        let b = format!("main={}", tf2.path().display());
        assert!(Catalog::from_args(&[a.as_str(), b.as_str()]).is_err());
    }
}
