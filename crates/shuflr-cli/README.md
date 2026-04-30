# shuflr-cli

Command-line interface for [shuflr](https://github.com/mjbommar/shuflr).
Produces the `shuflr` binary.

```sh
cargo install shuflr-cli
shuflr --help
```

`shuflr` is a single-binary tool for streaming records out of large JSONL
corpora in seed-deterministic shuffled order, on the CLI or over the
network. Convert a corpus to seekable-zstd once; shuffle out of it forever.

```sh
shuflr convert corpus.jsonl.gz -o corpus.jsonl.zst
shuflr stream --shuffle=chunk-shuffled --seed=42 corpus.jsonl.zst | ./train.py
```

Subcommands: `stream`, `convert`, `serve`, `info`, `analyze`, `index`,
`verify`, `completions`, `man`.

## Features

| Feature | Adds |
|---|---|
| `gzip` (default) | Streaming gzip input |
| `zstd` (default) | zstd input + `convert` / `info` (seekable-zstd) |
| `serve` | `serve` subcommand: HTTP/1.1 NDJSON + `shuflr-wire/1` listeners, TLS, bearer/mTLS auth |
| `parquet` | parquet + HuggingFace Hub input for `convert` |
| `bzip2`, `xz` | extra streaming-input codecs |
| `uring` | Linux io\_uring fast path |
| `full` | everything currently implemented |

```sh
cargo install shuflr-cli --features serve,parquet
```

See the [top-level README](https://github.com/mjbommar/shuflr) for the
full subcommand reference, the production-use story, and pointers to the
authoritative design docs (`docs/design/002-revised-plan.md` is v1).

## License

MIT OR Apache-2.0.
