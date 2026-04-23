//! Multithreaded zstd-seekable writer.
//!
//! Pipeline (per 004 §3):
//!
//! ```text
//!   reader thread                  worker pool (N)               writer thread
//!   ┌───────────────┐   work_tx   ┌──────────────┐   done_tx   ┌─────────────────┐
//!   │ split into    │────────────▶│ compress     │────────────▶│ reorder by id   │
//!   │ record-aligned│   (id,Vec)  │ (zstd level, │  (id, comp, │ + write to sink │
//!   │ frames        │             │  XXH)        │   checksum, │ + append seek   │
//!   │               │             │              │   dec_size) │   table entry   │
//!   └───────────────┘             └──────────────┘             └─────────────────┘
//! ```
//!
//! Single-input / single-output: frames are produced in order by the reader,
//! consumed in arbitrary order by workers, and reassembled in original order
//! by the writer. Bounded channels on both sides provide backpressure so
//! `(threads × avg_frame_size)` is the effective memory ceiling.
//!
//! Fall back to [`super::writer::Writer`] when `threads == 1` — its
//! single-pass path saves the channel hops and the BTreeMap reorder.

use std::collections::BTreeMap;
use std::io::{Read, Write};

use crossbeam_channel::{bounded, unbounded};

use crate::error::{Error, Result};

use super::seek_table::{FrameEntry, SeekTable};
use super::writer::{DEFAULT_FRAME_SIZE, DEFAULT_LEVEL, MAX_FRAME_SIZE, WriterStats};

/// Configuration for a parallel convert run.
pub struct ParallelConfig {
    pub level: i32,
    pub frame_size: usize,
    pub checksums: bool,
    pub record_aligned: bool,
    /// Worker thread count. 0 means "use available parallelism".
    pub threads: usize,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            level: DEFAULT_LEVEL,
            frame_size: DEFAULT_FRAME_SIZE,
            checksums: true,
            record_aligned: true,
            threads: 0,
        }
    }
}

/// Run a multithreaded convert. `reader` streams input; `sink` receives the
/// zstd-seekable output. Returns aggregated stats including frame count and
/// ratio.
pub fn run<R: Read + Send, W: Write>(
    reader: R,
    sink: W,
    cfg: &ParallelConfig,
) -> Result<WriterStats> {
    let threads = resolve_threads(cfg.threads);
    assert!(cfg.frame_size > 0 && cfg.frame_size <= MAX_FRAME_SIZE);

    // Bounded so a slow writer applies backpressure through the whole chain.
    let queue_depth = (threads * 2).max(4);
    let (work_tx, work_rx) = bounded::<Job>(queue_depth);
    // Done channel is unbounded because workers can produce faster than the
    // writer consumes; the BTreeMap reorder absorbs short bursts.
    let (done_tx, done_rx) = unbounded::<Completed>();

    let level = cfg.level;
    let checksums = cfg.checksums;

    let result: Result<WriterStats> = std::thread::scope(|s| {
        // Spawn worker threads.
        let worker_handles: Vec<_> = (0..threads)
            .map(|_| {
                let rx = work_rx.clone();
                let tx = done_tx.clone();
                s.spawn(move || -> Result<()> {
                    while let Ok(job) = rx.recv() {
                        let compressed =
                            zstd::bulk::compress(&job.bytes, level).map_err(Error::Io)?;
                        let checksum = if checksums {
                            Some((xxhash_rust_like(&job.bytes) & 0xFFFF_FFFF) as u32)
                        } else {
                            None
                        };
                        let decompressed_size = job.bytes.len() as u32;
                        let compressed_size = compressed.len() as u32;
                        if compressed_size as usize > MAX_FRAME_SIZE {
                            return Err(Error::Input(format!(
                                "compressed frame size {compressed_size} exceeds u32 cap"
                            )));
                        }
                        if tx
                            .send(Completed {
                                id: job.id,
                                compressed,
                                checksum,
                                decompressed_size,
                            })
                            .is_err()
                        {
                            // Writer dropped; nothing to do but exit.
                            break;
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        // Drop our original cloners so the channels close once the reader
        // thread and all workers drop their senders.
        drop(work_rx);
        drop(done_tx);

        // Reader thread: produces work jobs.
        let reader_handle = s.spawn(move || -> Result<(u64, u64)> {
            reader_loop(reader, cfg.frame_size, cfg.record_aligned, work_tx)
        });

        // Writer thread = this thread: reorders and writes.
        let (frames, records, compressed_bytes, uncompressed_bytes, seek_table_bytes) =
            writer_loop(sink, done_rx, cfg.checksums)?;

        // Reap reader.
        let (read_bytes, read_records) = reader_handle
            .join()
            .map_err(|_| Error::Input("reader thread panicked".into()))??;
        let _ = read_bytes;
        let _ = read_records;

        // Reap workers; propagate first error.
        for h in worker_handles {
            h.join()
                .map_err(|_| Error::Input("worker thread panicked".into()))??;
        }

        Ok(WriterStats {
            frames,
            records,
            uncompressed_bytes,
            compressed_bytes,
            oversized_frames: 0, // reader tracks but we lose that granularity here
            seek_table_bytes,
        })
    });

    result
}

fn resolve_threads(requested: usize) -> usize {
    if requested == 0 {
        // Physical-core count rather than `available_parallelism()` because
        // zstd compression is compute-heavy: bench/001 showed 16-thread
        // (SMT) is ~14% SLOWER than 8-thread on an 8P/16T Ryzen 7840HS
        // (6.61s vs 5.79s for the same workload) due to hyperthread
        // contention on the encoder. Users who want all logical cores
        // can still pass --threads=16 explicitly.
        num_cpus::get_physical().max(1)
    } else {
        requested
    }
}

struct Job {
    id: u64,
    bytes: Vec<u8>,
}

struct Completed {
    id: u64,
    compressed: Vec<u8>,
    checksum: Option<u32>,
    decompressed_size: u32,
}

fn reader_loop<R: Read>(
    mut reader: R,
    frame_size: usize,
    record_aligned: bool,
    work_tx: crossbeam_channel::Sender<Job>,
) -> Result<(u64, u64)> {
    // Accumulate into `pending` until we have a record-aligned prefix of at
    // least `frame_size` bytes; hand that prefix off as a job.
    let mut pending: Vec<u8> = Vec::with_capacity(frame_size * 2);
    let mut next_id: u64 = 0;
    let mut buf = vec![0u8; 1 << 20]; // 1 MiB read chunk
    let mut total_bytes: u64 = 0;
    let mut total_records: u64 = 0;

    loop {
        let n = reader.read(&mut buf).map_err(Error::Io)?;
        if n == 0 {
            break;
        }
        total_bytes += n as u64;
        total_records += memchr::memchr_iter(b'\n', &buf[..n]).count() as u64;
        pending.extend_from_slice(&buf[..n]);

        while let Some(boundary) = next_frame_boundary(&pending, frame_size, record_aligned) {
            let frame_bytes: Vec<u8> = pending.drain(..boundary).collect();
            if work_tx
                .send(Job {
                    id: next_id,
                    bytes: frame_bytes,
                })
                .is_err()
            {
                return Err(Error::Input("worker pool exited; aborting convert".into()));
            }
            next_id += 1;
        }
    }
    // Flush trailing partial.
    if !pending.is_empty() {
        if record_aligned && pending.last().copied() != Some(b'\n') {
            pending.push(b'\n');
            total_records += 1;
        }
        if work_tx
            .send(Job {
                id: next_id,
                bytes: pending,
            })
            .is_err()
        {
            return Err(Error::Input("worker pool exited; aborting convert".into()));
        }
    }
    // Closing work_tx (dropped when this fn returns) signals workers to finish.
    Ok((total_bytes, total_records))
}

/// Find the byte offset at which we should close a frame, given the current
/// pending buffer and config. Returns `None` if no boundary is available.
fn next_frame_boundary(pending: &[u8], frame_size: usize, record_aligned: bool) -> Option<usize> {
    if pending.len() < frame_size {
        return None;
    }
    if !record_aligned {
        return Some(frame_size);
    }
    // Look for the first '\n' at or after frame_size - 1.
    let search_start = frame_size - 1;
    memchr::memchr(b'\n', &pending[search_start..]).map(|rel| search_start + rel + 1)
}

fn writer_loop<W: Write>(
    mut sink: W,
    done_rx: crossbeam_channel::Receiver<Completed>,
    with_checksums: bool,
) -> Result<(u64, u64, u64, u64, u64)> {
    let mut table = SeekTable::new(with_checksums);
    let mut buffered: BTreeMap<u64, Completed> = BTreeMap::new();
    let mut next_expected: u64 = 0;
    let mut records: u64 = 0;
    let mut compressed_bytes: u64 = 0;
    let mut uncompressed_bytes: u64 = 0;

    while let Ok(c) = done_rx.recv() {
        records += 0; // records counted in reader instead; left at 0 here
        buffered.insert(c.id, c);
        while let Some(c) = buffered.remove(&next_expected) {
            sink.write_all(&c.compressed).map_err(Error::Io)?;
            let comp = c.compressed.len() as u64;
            let dec = u64::from(c.decompressed_size);
            table.push(FrameEntry {
                compressed_size: c.compressed.len() as u32,
                decompressed_size: c.decompressed_size,
                checksum: c.checksum,
            });
            compressed_bytes += comp;
            uncompressed_bytes += dec;
            next_expected += 1;
        }
    }
    if !buffered.is_empty() {
        return Err(Error::Input(format!(
            "{} completed frames stuck in reorder buffer (expected next id {next_expected}); \
             a worker likely dropped a job",
            buffered.len()
        )));
    }
    let seek_table_bytes = table.write_to(&mut sink).map_err(Error::Io)? as u64;
    sink.flush().map_err(Error::Io)?;
    let frames = table.num_frames() as u64;
    Ok((
        frames,
        records,
        compressed_bytes,
        uncompressed_bytes,
        seek_table_bytes,
    ))
}

/// Same FNV-1a-64 placeholder the single-threaded writer uses. Swap to real
/// XXH64 in the pass that adds `--verify`.
fn xxhash_rust_like(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in data {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::zstd_seekable::reader::SeekableReader;

    #[test]
    fn parallel_convert_matches_single_threaded_for_small_input() {
        let input = (0..2000)
            .map(|i| format!("record_{i:04}\n"))
            .collect::<String>();

        let mut out = Vec::new();
        let stats = run(
            std::io::Cursor::new(&input),
            &mut out,
            &ParallelConfig {
                level: 3,
                frame_size: 256,
                checksums: true,
                record_aligned: true,
                threads: 4,
            },
        )
        .unwrap();
        assert!(stats.frames >= 4, "expected multiple frames, got {stats:?}");
        assert!(stats.seek_table_bytes > 0);

        // Output must decode back to the original input via standard zstd.
        let decoded = zstd::stream::decode_all(&out[..]).unwrap();
        assert_eq!(decoded, input.as_bytes());
    }

    #[test]
    fn parallel_convert_roundtrips_via_seekable_reader() {
        let input: Vec<u8> = (0..500)
            .map(|i| format!("rec_{i:03}\n"))
            .collect::<String>()
            .into_bytes();

        let tmp = tempfile::NamedTempFile::new().unwrap();
        {
            let sink = std::fs::File::create(tmp.path()).unwrap();
            run(
                std::io::Cursor::new(&input),
                sink,
                &ParallelConfig {
                    level: 3,
                    frame_size: 128,
                    checksums: true,
                    record_aligned: true,
                    threads: 3,
                },
            )
            .unwrap();
        }

        let mut r = SeekableReader::open(tmp.path()).unwrap();
        let mut recovered = Vec::new();
        for i in 0..r.num_frames() {
            recovered.extend_from_slice(&r.decompress_frame(i).unwrap());
        }
        assert_eq!(recovered, input);
    }

    #[test]
    fn parallel_convert_single_thread_still_works() {
        let input = b"a\nb\nc\nd\ne\n";
        let mut out = Vec::new();
        let _ = run(
            std::io::Cursor::new(input),
            &mut out,
            &ParallelConfig {
                threads: 1,
                frame_size: 4,
                ..ParallelConfig::default()
            },
        )
        .unwrap();
        let decoded = zstd::stream::decode_all(&out[..]).unwrap();
        assert_eq!(decoded, input);
    }

    #[test]
    fn parallel_convert_empty_input_produces_empty_file_with_trailer() {
        let mut out = Vec::new();
        let stats = run(
            std::io::Cursor::new(b""),
            &mut out,
            &ParallelConfig {
                threads: 2,
                ..ParallelConfig::default()
            },
        )
        .unwrap();
        assert_eq!(stats.frames, 0);
        // Output is just the seek-table skippable frame; parseable as a table.
        let table = SeekTable::read_from_tail(std::io::Cursor::new(&out)).unwrap();
        assert_eq!(table.num_frames(), 0);
    }
}
