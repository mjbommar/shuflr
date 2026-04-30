# shuflr-wire

Codec for the `shuflr-wire/1` binary protocol used by
[shuflr](https://github.com/mjbommar/shuflr) — framing, parsing, and
xxh3 checksums. No tokio, no TLS, no I/O — pure bytes-in / frames-out.

This crate is consumed by:

- [`shuflr`](https://crates.io/crates/shuflr) on the server side (behind
  the `serve` feature flag), and
- [`shuflr-client`](https://pypi.org/project/shuflr-client/) on the client
  side (Rust core of the Python wheel).

If you're not implementing a third-party `shuflr-wire/1` peer, you don't
need this crate directly — pull in `shuflr` or `shuflr-client` instead.

## Protocol

`shuflr-wire/1` is a length-prefixed, kind-tagged binary frame format
with xxh3 checksums on every frame. It carries the same dataset stream
contract as the HTTP NDJSON transport but with explicit framing,
ordered delivery, and a `RawFrames` mode that lets a server hand a
client compressed seekable-zstd frames it then re-shuffles locally
with a server-derived seed (~3.6× wire savings vs NDJSON for
chunk-shuffled streams).

See `docs/design/005-serve-multi-transport.md` §3 in the parent repo
for the full wire spec.

## Status

Shipped. Used in production over plain TCP today; TLS (`shuflrs://`)
and UDS (`shuflr+unix://`) variants are next.

## License

MIT OR Apache-2.0.
