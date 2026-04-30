# shuflr (library)

Library crate for [shuflr](https://github.com/mjbommar/shuflr) — streaming
shuffled JSONL.

This crate holds the engine: chunk pool, shuffle algorithms (passthrough,
buffer, chunk-shuffled, index-perm, reservoir), I/O (`pread` + streaming
decoders + zstd-seekable reader/writer + parallel writer), index builder,
analyzer, and the optional service edge (HTTP + `shuflr-wire/1` + future
gRPC, all behind feature flags).

The CLI binary lives in the [`shuflr-cli`](https://crates.io/crates/shuflr-cli)
crate; the Python client lives in
[`shuflr-client`](https://pypi.org/project/shuflr-client/).

## Library use

```rust
use std::io;
use shuflr::pipeline::{ChunkShuffledConfig, chunk_shuffled};
use shuflr::io::zstd_seekable::SeekableReader;

let reader = SeekableReader::open("corpus.jsonl.zst")?;
let cfg = ChunkShuffledConfig {
    seed: 42,
    epoch: 0,
    max_line: 16 * 1024 * 1024,
    on_error: shuflr::OnError::Skip,
    sample: None,
    ensure_trailing_newline: true,
    partition: None,
    emit_threads: 1,
    emit_prefetch: 32,
};
let stats = chunk_shuffled(reader, &mut io::stdout().lock(), &cfg)?;
# Ok::<_, shuflr::Error>(())
```

Sinks accept `impl Write`. Stdout is treated as the data channel — library
code never `println!`s; logging goes through `tracing` to stderr.

## Features

| Feature | Adds |
|---|---|
| `zstd` | zstd streaming input + seekable-zstd reader/writer/parallel |
| `gzip` | streaming gzip input |
| `bzip2`, `xz` | additional streaming-input codecs |
| `parquet` | parquet + HuggingFace Hub input |
| `serve` | HTTP/1.1 NDJSON listener (rustls TLS, bearer/mTLS auth) |
| `grpc` | gRPC listener (PR-35) |
| `prom`, `otlp` | metrics export |
| `uring` | Linux io\_uring fast path |

## Design

`docs/design/002-revised-plan.md` (in the parent repo) is the v1
authoritative spec. Amendments: 003 (compression), 004 (convert +
seekable invariants), 005 (serve transports).

## License

MIT OR Apache-2.0.
