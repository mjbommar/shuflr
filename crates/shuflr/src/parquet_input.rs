//! Parquet → JSONL `Read` adapter, plus HF Hub URL resolution.
//!
//! Exposes a [`ParquetJsonlReader`] implementing `std::io::Read`: it reads
//! parquet row groups (local file or HF Hub) and emits one JSON object per
//! input row. Slotted into `shuflr convert` ahead of the existing
//! entropy/sample-rate filter + seekable-zstd writer.
//!
//! Feature-gated (`parquet`). The rest of the crate has no awareness of
//! columnar data.

use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;
use hf_hub::{
    Repo, RepoType,
    api::sync::{ApiBuilder, ApiRepo},
};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use serde_json::{Map, Value};

use crate::error::{Error, Result};

/// Parse `hf://<namespace>/<repo>[@<revision>]` into `(namespace/repo, revision)`.
/// Returns `None` if the string is not an `hf://` URL.
pub fn parse_hf_url(s: &str) -> Option<(String, Option<String>)> {
    let rest = s.strip_prefix("hf://")?;
    if rest.is_empty() {
        return None;
    }
    let (repo, rev) = match rest.split_once('@') {
        Some((r, v)) if !v.is_empty() => (r.to_string(), Some(v.to_string())),
        _ => (rest.to_string(), None),
    };
    if repo.contains(' ') || repo.split('/').count() < 2 {
        return None;
    }
    Some((repo, rev))
}

/// Handle for lazy-downloading parquet shards from an HF dataset.
///
/// `list_shards()` hits the HF info endpoint once to get the shard manifest;
/// individual shards are downloaded only when `fetch_shard()` is called. For
/// `--limit N` runs that only need the first few shards, this avoids the
/// 154 GB upfront download that an eager approach would incur.
pub struct HfShardSource {
    repo_handle: ApiRepo,
    repo_id: String,
    shards: Vec<String>,
}

impl HfShardSource {
    /// Open an HF dataset; returns a lazy handle (no downloads yet).
    pub fn open(url: &str) -> Result<Self> {
        let (repo_id, revision) = parse_hf_url(url).ok_or_else(|| {
            Error::Input(format!(
                "hf-hub: malformed URL {url:?}; expected hf://<namespace>/<repo>[@<revision>]"
            ))
        })?;

        let api = ApiBuilder::new()
            .with_progress(false)
            .build()
            .map_err(|e| Error::Input(format!("hf-hub: build api failed: {e}")))?;
        let repo_handle = match revision {
            Some(rev) => api.repo(Repo::with_revision(
                repo_id.clone(),
                RepoType::Dataset,
                rev,
            )),
            None => api.dataset(repo_id.clone()),
        };

        let info = repo_handle.info().map_err(|e| {
            Error::Input(format!("hf-hub: fetch info for {repo_id:?} failed: {e}"))
        })?;
        let mut shards: Vec<String> = info
            .siblings
            .into_iter()
            .map(|s| s.rfilename)
            .filter(|n: &String| n.starts_with("data/") && n.ends_with(".parquet"))
            .collect();
        if shards.is_empty() {
            return Err(Error::Input(format!(
                "hf-hub: no data/*.parquet shards found in {repo_id:?}"
            )));
        }
        shards.sort();

        tracing::info!(
            repo = %repo_id,
            shards = shards.len(),
            "HF dataset manifest loaded (shards will download lazily)",
        );

        Ok(Self {
            repo_handle,
            repo_id,
            shards,
        })
    }

    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// Download (or hit cache for) a specific shard by index. Only called
    /// when the reader actually needs it. Cached under
    /// `~/.cache/huggingface/hub/...` on subsequent runs.
    pub fn fetch_shard(&self, idx: usize) -> Result<PathBuf> {
        let name = self.shards.get(idx).ok_or_else(|| {
            Error::Input(format!(
                "hf-hub: shard index {idx} out of range (have {} shards)",
                self.shards.len()
            ))
        })?;
        tracing::info!(
            repo = %self.repo_id,
            shard_idx = idx,
            shard = %name,
            "downloading HF shard (or cache-hit)",
        );
        self.repo_handle
            .get(name)
            .map_err(|e| Error::Input(format!("hf-hub: get {name:?} failed: {e}")))
    }
}

/// Streams rows from one or more parquet files as JSONL bytes.
///
/// Owns a list of input paths. For each, opens a `ParquetRecordBatchReader`,
/// pulls one `RecordBatch` at a time, and serializes each row to a
/// single-line JSON object (terminated by `\n`). The row-to-JSON buffer is
/// drained into `read()`; when empty, the next row (or batch, or file) is
/// pulled on demand.
///
/// Column projection happens at the parquet-reader level (via
/// `ParquetRecordBatchReaderBuilder::with_projection`) so columns we don't
/// care about are never decoded — saves ~50% on wide datasets.
/// Source of parquet file paths. Either a fixed list (local files) or an
/// HF handle that downloads shards lazily one at a time.
enum PathSource {
    Local(Vec<PathBuf>),
    Hf(HfShardSource),
}

pub struct ParquetJsonlReader {
    source: PathSource,
    next_file: usize,
    projection: Option<Vec<String>>,
    reader: Option<ParquetRecordBatchReader>,
    current_batch: Option<RecordBatch>,
    next_row_in_batch: usize,
    out_buf: Vec<u8>,
    out_pos: usize,
}

impl ParquetJsonlReader {
    /// Build from a list of local file paths + optional column projection.
    pub fn new(files: Vec<PathBuf>, projection: Option<Vec<String>>) -> Self {
        Self {
            source: PathSource::Local(files),
            next_file: 0,
            projection,
            reader: None,
            current_batch: None,
            next_row_in_batch: 0,
            out_buf: Vec::with_capacity(16 * 1024),
            out_pos: 0,
        }
    }

    /// Build from a lazy HF shard source. Shards are downloaded on demand
    /// as `read()` advances — critical for `--limit N` runs that should
    /// only need the first shard or two of a 739-shard dataset.
    pub fn from_hf(source: HfShardSource, projection: Option<Vec<String>>) -> Self {
        Self {
            source: PathSource::Hf(source),
            next_file: 0,
            projection,
            reader: None,
            current_batch: None,
            next_row_in_batch: 0,
            out_buf: Vec::with_capacity(16 * 1024),
            out_pos: 0,
        }
    }

    /// Number of shards remaining. Useful for progress / debug logging.
    fn total_files(&self) -> usize {
        match &self.source {
            PathSource::Local(v) => v.len(),
            PathSource::Hf(h) => h.num_shards(),
        }
    }

    /// Resolve the next file path, downloading it (for HF) if needed.
    fn next_file_path(&mut self) -> Result<Option<PathBuf>> {
        if self.next_file >= self.total_files() {
            return Ok(None);
        }
        let idx = self.next_file;
        self.next_file += 1;
        let path = match &self.source {
            PathSource::Local(v) => v[idx].clone(),
            PathSource::Hf(h) => h.fetch_shard(idx)?,
        };
        Ok(Some(path))
    }

    fn open_next_file(&mut self) -> Result<bool> {
        let Some(path) = self.next_file_path()? else {
            return Ok(false);
        };
        let path_ref = &path;

        let f = File::open(path_ref).map_err(Error::Io)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(f)
            .map_err(|e| Error::Input(format!("parquet open {path_ref:?}: {e}")))?;

        // Build projection mask if the user asked for a subset of columns.
        let builder = if let Some(cols) = &self.projection {
            let schema = builder.parquet_schema();
            let mut indices = Vec::new();
            for name in cols {
                let idx = (0..schema.num_columns())
                    .find(|i| schema.column(*i).name() == name);
                match idx {
                    Some(i) => indices.push(i),
                    None => {
                        return Err(Error::Input(format!(
                            "parquet {path_ref:?}: projected column {name:?} not found. Available: {:?}",
                            (0..schema.num_columns())
                                .map(|i| schema.column(i).name().to_string())
                                .collect::<Vec<_>>()
                        )));
                    }
                }
            }
            let mask = parquet::arrow::ProjectionMask::leaves(schema, indices);
            builder.with_projection(mask)
        } else {
            builder
        };

        let reader = builder
            .build()
            .map_err(|e| Error::Input(format!("parquet build reader {path_ref:?}: {e}")))?;

        tracing::debug!(path = %path_ref.display(), "opened parquet file");
        self.reader = Some(reader);
        self.current_batch = None;
        self.next_row_in_batch = 0;
        Ok(true)
    }

    /// Try to pull the next non-empty batch from the current file.
    /// Returns Ok(true) if a batch is ready, Ok(false) if file exhausted.
    fn pull_next_batch(&mut self) -> Result<bool> {
        let Some(reader) = self.reader.as_mut() else {
            return Ok(false);
        };
        loop {
            match reader.next() {
                Some(Ok(batch)) if batch.num_rows() == 0 => continue,
                Some(Ok(batch)) => {
                    self.current_batch = Some(batch);
                    self.next_row_in_batch = 0;
                    return Ok(true);
                }
                Some(Err(e)) => {
                    return Err(Error::Input(format!("parquet decode: {e}")));
                }
                None => return Ok(false),
            }
        }
    }

    /// Serialize one row into `self.out_buf`, resetting `out_pos`.
    /// Returns Ok(true) on success, Ok(false) if nothing left to serialize.
    fn prepare_next_row(&mut self) -> Result<bool> {
        loop {
            // Do we have a ready batch with rows remaining?
            if let Some(batch) = self.current_batch.as_ref() {
                if self.next_row_in_batch < batch.num_rows() {
                    let row = serialize_row(batch, self.next_row_in_batch)?;
                    self.next_row_in_batch += 1;
                    self.out_buf.clear();
                    self.out_buf.extend_from_slice(row.as_bytes());
                    self.out_buf.push(b'\n');
                    self.out_pos = 0;
                    return Ok(true);
                }
                // Batch drained.
                self.current_batch = None;
            }
            // Need a new batch — or a new file.
            if self.reader.is_none() && !self.open_next_file()? {
                return Ok(false);
            }
            if !self.pull_next_batch()? {
                // Current file drained; move to the next on the next iteration.
                self.reader = None;
            }
        }
    }
}

impl Read for ParquetJsonlReader {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        let mut written = 0;
        while written < dst.len() {
            if self.out_pos >= self.out_buf.len() {
                match self.prepare_next_row() {
                    Ok(true) => {}
                    Ok(false) => return Ok(written),
                    Err(e) => {
                        return Err(io::Error::other(e.to_string()));
                    }
                }
            }
            let remaining = &self.out_buf[self.out_pos..];
            let n = remaining.len().min(dst.len() - written);
            dst[written..written + n].copy_from_slice(&remaining[..n]);
            self.out_pos += n;
            written += n;
        }
        Ok(written)
    }
}

/// Serialize a single RecordBatch row to a JSON object string.
///
/// Handles the primitive Arrow types we expect in HF datasets. Complex /
/// nested types (List, Struct) fall back to `null` with a tracing warn — we
/// can extend as needed.
fn serialize_row(batch: &RecordBatch, row: usize) -> Result<String> {
    use arrow_array::cast::AsArray;

    let schema = batch.schema();
    let mut map: Map<String, Value> = Map::with_capacity(batch.num_columns());
    for (col_idx, col) in batch.columns().iter().enumerate() {
        let field = schema.field(col_idx);
        let name = field.name().to_string();
        let value: Value = if col.is_null(row) {
            Value::Null
        } else {
            match field.data_type() {
                DataType::Utf8 => Value::String(col.as_string::<i32>().value(row).to_string()),
                DataType::LargeUtf8 => {
                    Value::String(col.as_string::<i64>().value(row).to_string())
                }
                DataType::Int8 => Value::from(col.as_primitive::<arrow_array::types::Int8Type>().value(row)),
                DataType::Int16 => Value::from(col.as_primitive::<arrow_array::types::Int16Type>().value(row)),
                DataType::Int32 => Value::from(col.as_primitive::<arrow_array::types::Int32Type>().value(row)),
                DataType::Int64 => Value::from(col.as_primitive::<arrow_array::types::Int64Type>().value(row)),
                DataType::UInt8 => Value::from(col.as_primitive::<arrow_array::types::UInt8Type>().value(row)),
                DataType::UInt16 => Value::from(col.as_primitive::<arrow_array::types::UInt16Type>().value(row)),
                DataType::UInt32 => Value::from(col.as_primitive::<arrow_array::types::UInt32Type>().value(row)),
                DataType::UInt64 => Value::from(col.as_primitive::<arrow_array::types::UInt64Type>().value(row)),
                DataType::Float32 => serde_json::Number::from_f64(
                    col.as_primitive::<arrow_array::types::Float32Type>().value(row) as f64,
                )
                .map(Value::Number)
                .unwrap_or(Value::Null),
                DataType::Float64 => serde_json::Number::from_f64(
                    col.as_primitive::<arrow_array::types::Float64Type>().value(row),
                )
                .map(Value::Number)
                .unwrap_or(Value::Null),
                DataType::Boolean => {
                    Value::Bool(col.as_boolean().value(row))
                }
                DataType::Binary => {
                    // Emit as base64 string so the JSON is legible. Rare in HF text datasets.
                    let bytes = col.as_binary::<i32>().value(row);
                    Value::String(hex_lowercase(bytes))
                }
                other => {
                    tracing::warn!(
                        column = %name,
                        dtype = ?other,
                        "unsupported arrow type for JSON serialization; emitting null"
                    );
                    Value::Null
                }
            }
        };
        map.insert(name, value);
    }

    serde_json::to_string(&Value::Object(map))
        .map_err(|e| Error::Input(format!("json serialize row: {e}")))
}

fn hex_lowercase(bytes: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(H[(b >> 4) as usize] as char);
        s.push(H[(b & 0xF) as usize] as char);
    }
    s
}

/// Recognize a path as a parquet input. Both local `.parquet` and `hf://` URLs.
pub fn looks_like_parquet_input(path: &Path) -> bool {
    let as_str = path.to_string_lossy();
    if as_str.starts_with("hf://") {
        return true;
    }
    path.extension().is_some_and(|e| e == "parquet")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_hf_urls() {
        assert_eq!(
            parse_hf_url("hf://alea-institute/kl3m-data-sample-006-medium"),
            Some(("alea-institute/kl3m-data-sample-006-medium".to_string(), None))
        );
        assert_eq!(
            parse_hf_url("hf://ns/repo@v1"),
            Some(("ns/repo".to_string(), Some("v1".to_string())))
        );
    }

    #[test]
    fn rejects_malformed_hf_urls() {
        assert_eq!(parse_hf_url("hf://"), None);
        assert_eq!(parse_hf_url("hf://no-slash"), None);
        assert_eq!(parse_hf_url("https://huggingface.co/x"), None);
        assert_eq!(parse_hf_url("hf://with space/repo"), None);
    }

    #[test]
    fn looks_like_parquet() {
        assert!(looks_like_parquet_input(Path::new("hf://a/b")));
        assert!(looks_like_parquet_input(Path::new("data.parquet")));
        assert!(looks_like_parquet_input(Path::new("/x/y.parquet")));
        assert!(!looks_like_parquet_input(Path::new("data.jsonl")));
        assert!(!looks_like_parquet_input(Path::new("data.jsonl.zst")));
    }
}
