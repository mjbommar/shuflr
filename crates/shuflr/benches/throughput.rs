//! Throughput benchmarks for the `--shuffle=none` passthrough pipeline.
//!
//! Target per 002 §11.5: ≥ 2 GB/s single-thread for plain-file passthrough.
//! We run at three fixture sizes (1 MiB / 16 MiB / 128 MiB) with three
//! representative record-length distributions so regressions are visible
//! across the distribution space we actually care about.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::Write as _;
use std::path::PathBuf;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use shuflr::io::Input;
use shuflr::pipeline::{PassthroughConfig, passthrough};

/// Write a deterministic JSONL fixture of approximately `target_bytes` bytes,
/// where each record is roughly `avg_len` bytes long. Uses a fixed RNG seed
/// so benches are reproducible.
fn build_fixture(path: &std::path::Path, target_bytes: u64, avg_len: usize) -> u64 {
    use rand::{Rng, SeedableRng};
    let mut rng = rand_chacha::ChaCha20Rng::seed_from_u64(0xdead_beef);
    let mut file = std::fs::File::create(path).expect("fixture create");
    let mut written = 0u64;
    let mut rec = Vec::with_capacity(avg_len * 2);
    while written < target_bytes {
        rec.clear();
        rec.extend_from_slice(b"{\"i\":");
        let i: u64 = rng.gen_range(0..1_000_000_000);
        rec.extend_from_slice(i.to_string().as_bytes());
        rec.extend_from_slice(b",\"t\":\"");
        // Body length chosen uniform around `avg_len - 20` (header + trailer ≈ 20).
        let body = rng.gen_range(avg_len.saturating_sub(20)..avg_len.saturating_add(20).max(1));
        for _ in 0..body {
            let b = rng.gen_range(b'a'..=b'z');
            rec.push(b);
        }
        rec.extend_from_slice(b"\"}\n");
        file.write_all(&rec).expect("fixture write");
        written += rec.len() as u64;
    }
    file.flush().expect("fixture flush");
    written
}

fn bench_passthrough(c: &mut Criterion) {
    // Keep fixtures under target/bench-data so they're git-ignored and survive
    // between bench invocations (saves ~3s of fixture-generation).
    let root: PathBuf = std::env::var_os("CARGO_TARGET_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/bench-data"));
    std::fs::create_dir_all(&root).expect("bench tmpdir");

    let cases: &[(&str, u64, usize)] = &[
        ("1MiB_64B", 1 << 20, 64),
        ("16MiB_1KiB", 16 << 20, 1024),
        ("128MiB_8KiB", 128 << 20, 8192),
    ];

    let mut group = c.benchmark_group("passthrough");
    for (name, bytes, avg_len) in cases {
        let path = root.join(format!("fixture_{name}.jsonl"));
        let actual = if path.exists() {
            std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
        } else {
            0
        };
        // Rebuild if absent or size is far from target.
        let want_rebuild = actual == 0 || actual.abs_diff(*bytes) > bytes / 10;
        let written = if want_rebuild {
            build_fixture(&path, *bytes, *avg_len)
        } else {
            actual
        };
        group.throughput(Throughput::Bytes(written));
        group.bench_with_input(BenchmarkId::from_parameter(name), &path, |b, path| {
            b.iter(|| {
                let input = Input::open(path).expect("open");
                let mut sink = std::io::sink();
                let stats =
                    passthrough(input, &mut sink, &PassthroughConfig::default()).expect("run");
                criterion::black_box(stats);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_passthrough);
criterion_main!(benches);
