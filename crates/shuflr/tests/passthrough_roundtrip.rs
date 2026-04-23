//! Library-level end-to-end roundtrip tests for `--shuffle=none`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::Write as _;
use std::path::PathBuf;

use shuflr::io::Input;
use shuflr::pipeline::{PassthroughConfig, passthrough};

/// Resolves `tests/corpora/<name>` from the workspace root.
fn corpus_path(name: &str) -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    // `CARGO_MANIFEST_DIR` for this crate is `crates/shuflr/`; walk up.
    let workspace = std::path::Path::new(manifest_dir)
        .ancestors()
        .nth(2)
        .expect("at least 2 ancestors");
    workspace.join("tests/corpora").join(name)
}

#[test]
fn tiny_fixture_roundtrips_exactly() {
    let path = corpus_path("tiny.jsonl");
    let input = Input::open(&path).unwrap();
    let mut out = Vec::new();
    let stats = passthrough(input, &mut out, &PassthroughConfig::default()).unwrap();

    assert_eq!(stats.records_in, 5);
    assert_eq!(stats.records_out, 5);
    let expected = std::fs::read(&path).unwrap();
    assert_eq!(out, expected);
}

#[test]
fn tiny_fixture_sample_two_takes_first_two() {
    let path = corpus_path("tiny.jsonl");
    let input = Input::open(&path).unwrap();
    let mut out = Vec::new();
    let cfg = PassthroughConfig {
        sample: Some(2),
        ..PassthroughConfig::default()
    };
    let stats = passthrough(input, &mut out, &cfg).unwrap();

    assert_eq!(stats.records_out, 2);
    let text = String::from_utf8(out).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("alpha"));
    assert!(lines[1].contains("bravo"));
}

#[test]
fn synthetic_1mb_preserves_record_count() {
    // Generate a 1 MiB synthetic file with uniform records; confirm roundtrip.
    let tmpdir = tempfile::tempdir().unwrap();
    let path = tmpdir.path().join("synth.jsonl");

    let mut file = std::fs::File::create(&path).unwrap();
    let record = b"{\"a\":1234567890,\"b\":\"the quick brown fox jumps over the lazy dog\"}\n";
    let mut written = 0;
    let target = 1 << 20; // 1 MiB
    let mut n_records = 0u64;
    while written < target {
        file.write_all(record).unwrap();
        written += record.len();
        n_records += 1;
    }
    file.flush().unwrap();
    drop(file);

    let input = Input::open(&path).unwrap();
    let mut out = Vec::new();
    let stats = passthrough(input, &mut out, &PassthroughConfig::default()).unwrap();

    assert_eq!(stats.records_in, n_records);
    assert_eq!(stats.records_out, n_records);
    assert_eq!(out, std::fs::read(&path).unwrap());
}
