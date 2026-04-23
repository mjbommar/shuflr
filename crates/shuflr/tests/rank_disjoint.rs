//! Property tests for distributed partitioning (002 §11.1 property 4):
//! `∀ r ≠ r' ∈ [0, W): emitted(r) ∩ emitted(r') = ∅`  AND
//! `⋃_r emitted(r) == multiset(input)`  (disjoint + complete cover).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{BTreeSet, HashSet};

use shuflr::io::Input;
use shuflr::pipeline::{
    BufferConfig, PassthroughConfig, buffer as run_buffer, passthrough as run_passthrough,
};

fn build_input(n: usize) -> Vec<u8> {
    (0..n)
        .map(|i| format!("rec_{i:04}\n"))
        .collect::<String>()
        .into_bytes()
}

fn run_mode<R>(raw: &[u8], world_size: u32, mut per_rank: R) -> Vec<Vec<String>>
where
    R: FnMut(Input, u32) -> Vec<u8>,
{
    (0..world_size)
        .map(|rank| {
            let inp = Input::from_reader(Box::new(std::io::Cursor::new(raw.to_vec())), None, None)
                .unwrap();
            let out = per_rank(inp, rank);
            String::from_utf8(out)
                .unwrap()
                .lines()
                .map(|s| s.to_string())
                .collect()
        })
        .collect()
}

fn assert_disjoint_and_complete(per_rank: &[Vec<String>], expected_count: usize) {
    // 1) Every record appears in some rank.
    let union: HashSet<&String> = per_rank.iter().flatten().collect();
    assert_eq!(
        union.len(),
        expected_count,
        "union of ranks should cover every record"
    );

    // 2) No record in two ranks.
    let mut seen = HashSet::new();
    for rank_out in per_rank {
        for rec in rank_out {
            assert!(
                seen.insert(rec.clone()),
                "record {rec} appears in multiple ranks"
            );
        }
    }

    // 3) Each rank's records are unique within that rank.
    for (r, rank_out) in per_rank.iter().enumerate() {
        let u: BTreeSet<&String> = rank_out.iter().collect();
        assert_eq!(u.len(), rank_out.len(), "rank {r} has duplicates");
    }
}

#[test]
fn passthrough_partitions_disjointly_across_ranks() {
    let n = 1_000usize;
    let raw = build_input(n);
    let world_sizes = [1u32, 2, 3, 4, 7, 8, 16];
    for &w in &world_sizes {
        let per_rank = run_mode(&raw, w, |input, rank| {
            let mut out = Vec::new();
            let cfg = PassthroughConfig {
                partition: if w > 1 { Some((rank, w)) } else { None },
                ..PassthroughConfig::default()
            };
            run_passthrough(input, &mut out, &cfg).unwrap();
            out
        });
        assert_disjoint_and_complete(&per_rank, n);
        // For passthrough, each rank's size should be ≈ n / w (differ by ≤ 1).
        for rank_out in &per_rank {
            let sz = rank_out.len();
            let target = n / w as usize;
            assert!(
                sz == target || sz == target + 1,
                "rank size {sz} not close to target {target}"
            );
        }
    }
}

#[test]
fn buffer_partitions_disjointly_across_ranks() {
    let n = 400usize;
    let raw = build_input(n);
    let world_sizes = [1u32, 2, 4, 8];
    for &w in &world_sizes {
        let per_rank = run_mode(&raw, w, |input, rank| {
            let mut out = Vec::new();
            let cfg = BufferConfig {
                buffer_size: 16,
                seed: 42,
                partition: if w > 1 { Some((rank, w)) } else { None },
                ..BufferConfig::default()
            };
            run_buffer(input, &mut out, &cfg).unwrap();
            out
        });
        assert_disjoint_and_complete(&per_rank, n);
    }
}

#[cfg(feature = "zstd")]
#[test]
fn chunk_shuffled_partitions_disjointly_across_ranks() {
    use shuflr::io::zstd_seekable::SeekableReader;
    use shuflr::io::zstd_seekable::writer::{Writer, WriterConfig};
    use shuflr::pipeline::{ChunkShuffledConfig, chunk_shuffled};

    let n = 500usize;
    let raw = build_input(n);
    let tmp = tempfile::NamedTempFile::new().unwrap();
    {
        let file = std::fs::File::create(tmp.path()).unwrap();
        let mut w = Writer::new(
            file,
            WriterConfig {
                level: 3,
                frame_size: 256,
                checksums: true,
                record_aligned: true,
            },
        );
        w.write_block(&raw).unwrap();
        w.finish().unwrap();
    }

    let world_sizes = [1u32, 2, 4, 8];
    for &w in &world_sizes {
        let per_rank: Vec<Vec<String>> = (0..w)
            .map(|rank| {
                let reader = SeekableReader::open(tmp.path()).unwrap();
                let mut out = Vec::new();
                let cfg = ChunkShuffledConfig {
                    seed: 7,
                    partition: if w > 1 { Some((rank, w)) } else { None },
                    ..ChunkShuffledConfig::default()
                };
                chunk_shuffled(reader, &mut out, &cfg).unwrap();
                String::from_utf8(out)
                    .unwrap()
                    .lines()
                    .map(|s| s.to_string())
                    .collect()
            })
            .collect();
        assert_disjoint_and_complete(&per_rank, n);
    }
}

#[test]
fn index_perm_partitions_disjointly_across_ranks() {
    use shuflr::pipeline::{IndexPermConfig, index_perm};
    use shuflr::{Fingerprint, IndexFile};

    let n = 300usize;
    let raw = build_input(n);
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), &raw).unwrap();
    let idx = IndexFile::build(&raw[..], Fingerprint([0; 32])).unwrap();

    let world_sizes = [1u32, 2, 4, 8];
    for &w in &world_sizes {
        let per_rank: Vec<Vec<String>> = (0..w)
            .map(|rank| {
                let mut out = Vec::new();
                let cfg = IndexPermConfig {
                    seed: 31,
                    partition: if w > 1 { Some((rank, w)) } else { None },
                    ..IndexPermConfig::default()
                };
                index_perm(tmp.path(), &idx, &mut out, &cfg).unwrap();
                String::from_utf8(out)
                    .unwrap()
                    .lines()
                    .map(|s| s.to_string())
                    .collect()
            })
            .collect();
        assert_disjoint_and_complete(&per_rank, n);
    }
}
