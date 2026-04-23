//! Rust core for the `shuflr-client` Python package.
//!
//! PR-34a scope: HTTP transport only. The `Dataset` class wraps a
//! blocking HTTP/1.1 NDJSON connection to a `shuflr serve` endpoint;
//! `__iter__` returns raw `bytes` records, one per `__next__` call,
//! terminating on server close or `--sample` exhaustion.
//!
//! The PR-36 follow-up adds the custom `shuflr-wire/1` binary transport
//! and negotiates between the two based on URL scheme. Until then,
//! schemes `shuflr://` / `shuflrs://` are accepted for forward
//! compatibility but raise `NotImplementedError`.

#![allow(clippy::unwrap_used)]

use std::io::{BufRead, BufReader, Read};
use std::sync::Mutex;
use std::time::Duration;

use pyo3::exceptions::{PyIOError, PyNotImplementedError, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Supported URL schemes as of PR-34a.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transport {
    Http,
    Https,
    /// `shuflr://` — custom binary (PR-36).
    WireTcp,
    /// `shuflrs://` — wire + TLS (PR-36).
    WireTls,
    /// `shuflr+unix://` — wire over UDS (PR-36).
    WireUnix,
}

fn transport_from_scheme(scheme: &str) -> Option<Transport> {
    match scheme {
        "http" => Some(Transport::Http),
        "https" => Some(Transport::Https),
        "shuflr" => Some(Transport::WireTcp),
        "shuflrs" => Some(Transport::WireTls),
        "shuflr+unix" => Some(Transport::WireUnix),
        _ => None,
    }
}

/// Parsed connection target. Produced from the URL; passed to the
/// appropriate transport's `open` function.
#[derive(Debug, Clone)]
struct Target {
    transport: Transport,
    /// For HTTP: the full URL to GET /v1/streams/{id} against.
    /// For wire: the scheme-less host:port + dataset_id.
    raw: String,
    dataset_id: String,
}

fn parse_target(url_str: &str) -> PyResult<Target> {
    let parsed = url::Url::parse(url_str)
        .map_err(|e| PyValueError::new_err(format!("invalid URL '{url_str}': {e}")))?;
    let transport = transport_from_scheme(parsed.scheme()).ok_or_else(|| {
        PyValueError::new_err(format!("unsupported URL scheme '{}'", parsed.scheme()))
    })?;

    // Dataset id is the last non-empty path segment.
    let dataset_id = parsed
        .path_segments()
        .and_then(|mut it| it.next_back())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "URL '{url_str}' has no dataset_id (expected .../{{dataset_id}})"
            ))
        })?
        .to_string();

    Ok(Target {
        transport,
        raw: url_str.to_string(),
        dataset_id,
    })
}

/// Iterator over records. Holds the underlying BufReader and yields
/// `bytes` line by line (without the trailing `\n`).
enum Stream {
    Http(HttpStream),
}

struct HttpStream {
    reader: Mutex<BufReader<Box<dyn Read + Send>>>,
    exhausted: bool,
}

impl HttpStream {
    fn next_line(&mut self) -> Option<PyResult<Vec<u8>>> {
        if self.exhausted {
            return None;
        }
        let mut line = Vec::new();
        let n = {
            let mut guard = self.reader.lock().ok()?;
            match guard.read_until(b'\n', &mut line) {
                Ok(n) => n,
                Err(e) => {
                    self.exhausted = true;
                    return Some(Err(PyIOError::new_err(format!("read error: {e}"))));
                }
            }
        };
        if n == 0 {
            self.exhausted = true;
            return None;
        }
        if line.ends_with(b"\n") {
            line.pop();
            if line.ends_with(b"\r") {
                line.pop();
            }
        }
        // Empty trailing newline at the end of a stream is benign; skip it.
        if line.is_empty() {
            return self.next_line();
        }
        // Server sentinel: surface as an error for the Python caller.
        if line.starts_with(b"{\"_shuflr_error\":") {
            self.exhausted = true;
            let msg = String::from_utf8_lossy(&line).into_owned();
            return Some(Err(PyIOError::new_err(format!("server error: {msg}"))));
        }
        Some(Ok(line))
    }
}

/// `shuflr_client.Dataset(url, ...)` — see module docstring.
#[pyclass(module = "shuflr_client._shuflr_client_native", unsendable)]
struct Dataset {
    target: Target,
    // Populated on __iter__; None before first iteration.
    stream: Option<Stream>,
    // Query-string settings.
    seed: u64,
    shuffle: String,
    sample: Option<u64>,
    rank: Option<u32>,
    world_size: Option<u32>,
    timeout_secs: f64,
}

#[pymethods]
impl Dataset {
    #[new]
    #[pyo3(signature = (
        url,
        *,
        seed = 0,
        shuffle = "chunk-shuffled".to_string(),
        sample = None,
        rank = None,
        world_size = None,
        timeout = 30.0,
    ))]
    fn new(
        url: String,
        seed: u64,
        shuffle: String,
        sample: Option<u64>,
        rank: Option<u32>,
        world_size: Option<u32>,
        timeout: f64,
    ) -> PyResult<Self> {
        let target = parse_target(&url)?;
        if rank.is_some() && world_size.is_none() {
            return Err(PyValueError::new_err("rank requires world_size to be set"));
        }
        Ok(Self {
            target,
            stream: None,
            seed,
            shuffle,
            sample,
            rank,
            world_size,
            timeout_secs: timeout,
        })
    }

    /// Open the HTTP response (if not already open) and return self.
    fn __iter__(slf: PyRefMut<'_, Self>) -> PyResult<Py<Self>> {
        let py = slf.py();
        let owner: Py<Self> = slf.into();
        {
            let mut b = owner.borrow_mut(py);
            if b.stream.is_none() {
                b.open_stream()?;
            }
        }
        Ok(owner)
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Py<PyBytes>> {
        if slf.stream.is_none() {
            slf.open_stream()?;
        }
        let py = slf.py();
        let line = match &mut slf.stream {
            Some(Stream::Http(s)) => s.next_line(),
            None => None,
        };
        match line {
            Some(Ok(bytes)) => Ok(PyBytes::new(py, &bytes).into()),
            Some(Err(e)) => Err(e),
            None => Err(PyStopIteration::new_err(())),
        }
    }

    #[getter]
    fn dataset_id(&self) -> &str {
        &self.target.dataset_id
    }

    #[getter]
    fn url(&self) -> &str {
        &self.target.raw
    }

    #[getter]
    fn transport(&self) -> &'static str {
        match self.target.transport {
            Transport::Http => "http",
            Transport::Https => "https",
            Transport::WireTcp => "shuflr-wire/1",
            Transport::WireTls => "shuflr-wire/1+tls",
            Transport::WireUnix => "shuflr-wire/1+unix",
        }
    }
}

impl Dataset {
    fn open_stream(&mut self) -> PyResult<()> {
        match self.target.transport {
            Transport::Http | Transport::Https => {
                let stream = open_http(
                    &self.target.raw,
                    self.seed,
                    &self.shuffle,
                    self.sample,
                    self.rank,
                    self.world_size,
                    self.timeout_secs,
                )?;
                self.stream = Some(Stream::Http(stream));
                Ok(())
            }
            Transport::WireTcp | Transport::WireTls | Transport::WireUnix => {
                Err(PyNotImplementedError::new_err(
                    "shuflr-wire/1 transport lands in PR-36; use http:// or https:// for now",
                ))
            }
        }
    }
}

fn open_http(
    base: &str,
    seed: u64,
    shuffle: &str,
    sample: Option<u64>,
    rank: Option<u32>,
    world_size: Option<u32>,
    timeout_secs: f64,
) -> PyResult<HttpStream> {
    let parsed = url::Url::parse(base)
        .map_err(|e| PyValueError::new_err(format!("invalid URL '{base}': {e}")))?;
    let dataset_id = parsed
        .path_segments()
        .and_then(|mut it| it.next_back())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| PyValueError::new_err("URL has no dataset_id"))?;

    let mut stream_url = parsed.clone();
    stream_url.set_path(&format!("/v1/streams/{dataset_id}"));
    {
        let mut q = stream_url.query_pairs_mut();
        q.clear();
        if seed != 0 {
            q.append_pair("seed", &seed.to_string());
        }
        q.append_pair("shuffle", shuffle);
        if let Some(n) = sample {
            q.append_pair("sample", &n.to_string());
        }
        if let (Some(r), Some(w)) = (rank, world_size) {
            q.append_pair("rank", &r.to_string());
            q.append_pair("world_size", &w.to_string());
        }
    }

    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs_f64(timeout_secs))
        // No per-read timeout: we want to hold the stream open for
        // long-running shuffles. If the user wants a deadline they
        // can wrap with signal.alarm / threading.Timer.
        .build();
    let resp = agent
        .get(stream_url.as_str())
        .call()
        .map_err(|e| PyIOError::new_err(format!("HTTP GET {stream_url}: {e}")))?;

    if resp.status() != 200 {
        return Err(PyIOError::new_err(format!(
            "HTTP {} from {stream_url}",
            resp.status()
        )));
    }
    let reader: Box<dyn Read + Send> = Box::new(resp.into_reader());
    Ok(HttpStream {
        reader: Mutex::new(BufReader::with_capacity(256 * 1024, reader)),
        exhausted: false,
    })
}

/// Python module entry point. Exposes `Dataset` to the Python side.
#[pymodule]
fn _shuflr_client_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Dataset>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

// Unit tests for URL parsing live on the Python side (`tests/test_http.py`)
// — pyo3's `extension-module` feature skips libpython linking, so Rust
// test binaries inside this crate can't link. Any pure-logic tests that
// don't need pyo3 should move to their own crate if we want Rust-side
// coverage.
