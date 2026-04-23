# Review 07 — CLI / UX Surface

*Reviewer: CLI / product designer. Lens: would a new user love this after 10 minutes, or close the tab?*

## TL;DR

Today's plan lists **~30 flags across four tables** with no subcommand discipline. That's a flag swamp. The core idea is great and the taxonomy is mostly sane, but the surface is built as if all modes are equal — they aren't. 80% of users will type `shuflr file.jsonl | jq ...` and never touch `--active-chunks` or `--framing`. The one truly load-bearing split — **pipe vs. serve** — is hidden inside a flag (`--grpc ADDR`) instead of being the thing the user picks first.

Land with: subcommands (`stream` default, `serve` explicit), **12 v1 flags**, a one-screen `--help`, TTY auto-detect for `--progress`, shell completions from day one. Defer the rest behind `--help-full` and a man page.

## Strengths

- **Stdout is sacred** is already in CLAUDE.md. That alone puts shuflr ahead of most data tools.
- Flag *naming* is mostly consistent: `--batch-size` / `--batch-bytes`, `--chunks` / `--chunk-size` read as sibling pairs.
- `--seed` is first-class and reproducibility is built in, not retrofitted.
- `--dry-run` and `--verify` were thought of up front.
- The instinct to copy `jq`/`netcat` composability over `git`-style verb sprawl is correct for the CLI mode.

## Concerns and Gaps

### 1. Default with one arg

Plan implies `shuflr file.jsonl` works with `--shuffle=chunk-shuffled` default. Good — keep it. Do *not* require `--shuffle` for the common case. Zero-arg (`shuflr` with stdin) should auto-select `reservoir` with a sane default `K=100_000` and warn once on stderr that quality depends on buffer size.

### 2. Flag count: cut hard

Counting the plan: **I/O 5, ordering 8, output 9, ops 6 = 28 flags.** That fails the `--help` screen test. Only these earn a slot in v1:

Keep: `--shuffle`, `--seed`, `--chunk-size`, `--active-chunks`, `--epochs`, `--sample`, `--batch-size`, `--progress`, `--on-error`, `--dry-run`, `--verify`, `--log-level`. Plus positional `[INPUT...]`.

Defer: `--framing`, `--framing-out`, `--delim`, `--max-line`, `--mmap/--pread/--direct`, `--chunks` (redundant with `--chunk-size`), `--jitter-boundaries` (always on when epochs > 1), `--reservoir-size` (see below), `--batch-bytes`, `--flush-interval`, `--filter`, `--project`, `--stats`, `--sample-rate`, weighted `--input a:0.7`. All behind feature flags or `--help-full`.

`--filter` and `--project` in particular violate the CLAUDE.md non-goal: "Not a JSON transformation tool. Real transforms belong in `jq` downstream." Delete them. `| jq` is two extra keystrokes.

### 3. Subcommands vs. flags

**Adopt two subcommands.** The cost/benefit tips hard once "serve over gRPC" enters the picture. Compare:

```
shuflr data.jsonl                        # pipe (implicit `stream`)
shuflr stream data.jsonl                 # pipe, explicit
shuflr serve --grpc :50051 data.jsonl    # long-lived service
```

vs. today's implicit `shuflr --grpc :50051 data.jsonl` which reads as "pipe mode that secretly became a daemon." Subcommands also let `serve` grow its own flags (`--max-clients`, `--tls-cert`, `--fanout`) without bloating stream mode's `--help`.

Keep `stream` as the default when the first positional looks like a path and no `serve`-only flag is present. This preserves the `netcat`-style one-liner. Two-subcommand CLIs (`docker run` / `docker build`) are the sweet spot; avoid the `git`-plugin-bazaar trajectory.

Drop `--listen` / `--grpc` at top level — they become `serve --tcp`, `serve --grpc`, `serve --uds`.

### 4. Flag taxonomy cleanup

`--shuffle` is a mode selector; `--reservoir-size` is a mode-specific parameter. They should nest. Two clean options:

- **Colon syntax:** `--shuffle=reservoir:100000` (ripgrep-style, one flag).
- **Scoped flags:** `--shuffle=reservoir --reservoir-size=100000` (current plan; fine but verbose).

Pick the first. It mirrors `--input a:0.7` already in the plan and it collapses a flag. Apply the same to `--shuffle=chunk-shuffled:256MiB` as an alternative to `--chunk-size`.

### 5. Error messages and exit codes

Adopt `sysexits.h` with `ripgrep`-style tone: short, actionable, no stack noise. All errors to stderr, prefix with `shuflr:`.

| Condition | Exit | Message |
|---|---|---|
| Input not found | 66 (EX_NOINPUT) | `shuflr: no such file: 'data.jsonl'` |
| Permission denied | 77 (EX_NOPERM) | `shuflr: cannot read 'data.jsonl': Permission denied` |
| File smaller than chunk-size | 0 + warn | `shuflr: warn: file (12 MiB) smaller than --chunk-size (256 MiB); using single chunk` |
| Malformed JSONL + `--on-error=fail` | 65 (EX_DATAERR) | `shuflr: invalid JSON at byte 104857623 (line ~482193): expected ',' or '}'` |
| gRPC port in use | 73 (EX_CANTCREAT) | `shuflr: cannot bind :50051: address in use (try --grpc :0 for a random port)` |
| Client disconnected mid-stream | 0 | info log to stderr: `client disconnected after 1.2M records`; don't crash the server |
| Seed parse fail | 64 (EX_USAGE) | `shuflr: --seed must be a u64, got 'abc'` |

No Rust panic traces. Ever. `RUST_BACKTRACE` stays an internal tool.

### 6. `--help` on one screen

See mock below. Categorized, aligned, under 24 lines. Everything else to `--help-full` and `man shuflr`.

### 7. Interop

The CLI exists to feed pipelines. Ship a **Cookbook** in the README and the design doc — it doubles as the smoke test for whether the flag surface is actually sufficient.

### 8. Observability

`--progress` must:

- auto-enable only if **stderr is a TTY** (never stdout-detection; stdout is data)
- render: `[━━━━━━━━    ] 62%  1.3 GiB / 2.1 GiB  487 MiB/s  ETA 00:03`
- disable automatically under `--log-level=debug` (lines would fight the bar)
- accept `--progress=never|auto|always` (default `auto`)

### 9. `--dry-run` vs `--verify`

Clarify in help:

- `--dry-run` — scan chunk boundaries, compute expected record count, emit summary to stderr, exit 0. No records to stdout.
- `--verify` — full pass, validate JSONL, count malformed lines, exit 0 on clean / 65 on any malformed (overrides `--on-error`).

Make them mutually exclusive and say so.

### 10. Env vars and config file

**No `.shuflrrc` in v1.** Config files are a trap for a streaming tool — invocation-specific flags don't cache well. Do support:

- `SHUFLR_SEED` — equivalent to `--seed`
- `SHUFLR_LOG` — equivalent to `--log-level` (matches `RUST_LOG` idiom)
- `NO_COLOR` — respect it

That's it. Revisit config files only if v2 grows a `serve` mode with persistent deployment needs.

### 11. Shuffle-mode discoverability

In `--help`, show one line per mode:

```
  --shuffle MODE      none           pass-through, fastest
                      chunk-rr       round-robin across chunks, low randomness
                      chunk-shuffled intra-chunk shuffle + interleave (default)
                      reservoir:K    K-sized buffer; for stdin streams
```

The default is right. Most users never choose.

### 12. Man page and completions

Ship from v1. `clap_mangen` + `clap_complete` are ~30 lines of `build.rs`. Completions for bash/zsh/fish generated via `shuflr completions <shell>`. Cost: trivial. UX: enormous for `--shuffle=<TAB>`.

## Proposed v1 minimal flag set

```
POSITIONAL
  [INPUT...]              file(s) or glob; '-' or omit for stdin

COMMON
  -s, --shuffle MODE      none | chunk-rr | chunk-shuffled | reservoir[:K]
                          (default: chunk-shuffled; reservoir if stdin)
      --seed U64          reproducibility
  -n, --sample N          stop after N records
  -e, --epochs N|inf      passes over the input (default 1)
      --progress[=WHEN]   never | auto | always (default auto)

TUNING (most users never touch)
      --chunk-size BYTES  default 256MiB
      --active-chunks M   default 8
      --batch-size N      stdout 1, network 128
      --on-error POLICY   skip | fail | passthrough (default skip)

MODES
      --dry-run           scan + report, emit nothing
      --verify            validate JSONL; exit 65 on malformed

DIAGNOSTIC
      --log-level LEVEL   error|warn|info|debug|trace (or $SHUFLR_LOG)
  -h, --help              this screen
      --help-full         all flags including serve subcommand
  -V, --version
```

Twelve primary flags + six tuning/diagnostic. `serve` subcommand owns its own network flags.

## Mock `shuflr --help` (fits 24 lines)

```
shuflr 0.1 — stream large JSONL in shuffled order

USAGE:
  shuflr [OPTIONS] [INPUT...]           stream to stdout
  shuflr serve --grpc ADDR [INPUT...]   serve over network
  shuflr completions <SHELL>            shell completions

COMMON:
  -s, --shuffle MODE       none|chunk-rr|chunk-shuffled|reservoir[:K]  [chunk-shuffled]
      --seed U64           reproducible shuffle
  -n, --sample N           stop after N records
  -e, --epochs N|inf       passes over input                            [1]
      --progress[=WHEN]    never|auto|always                            [auto]

TUNING:
      --chunk-size BYTES   per-chunk granularity                        [256MiB]
      --active-chunks M    mmap concurrency                             [8]
      --on-error POLICY    skip|fail|passthrough                        [skip]

MODES:
      --dry-run            scan + report, no output
      --verify             check JSONL validity

See 'shuflr --help-full' for all flags, 'man shuflr' for details.
```

## Cookbook (add to design doc)

```bash
# 1. Shuffle a corpus into jq
shuflr corpus.jsonl | jq -r '.text' > texts.txt

# 2. Sample 10k random records reproducibly
shuflr --seed 42 --sample 10000 corpus.jsonl > sample.jsonl

# 3. Infinite epoch stream for a training loop
shuflr --epochs inf --seed 7 corpus.jsonl | ./train.py

# 4. Parallelize downstream work
shuflr corpus.jsonl | parallel --pipe -j 8 ./process.py

# 5. Shuffle stdin from another generator
zstdcat big.jsonl.zst | shuflr --shuffle reservoir:200000

# 6. Serve to remote trainers
shuflr serve --grpc 0.0.0.0:50051 --epochs inf /data/corpus.jsonl

# 7. Verify a corpus without emitting data
shuflr --verify corpus.jsonl
```

## Open questions

1. **Implicit `stream` subcommand:** is `shuflr file.jsonl` sugar for `shuflr stream file.jsonl`, or is `stream` never required? Leaning "never required; `serve` is the only named subcommand."
2. **`--sample N` + `--epochs inf`:** interaction needs a one-line policy (sample *per epoch* or *total*?). Document as *per epoch*, since that matches training usage.
3. **`--shuffle=reservoir:K` vs. `--shuffle=reservoir --reservoir-size=K`:** the colon form is cleaner but less greppable in scripts. Accept both, promote the colon form in docs.
4. **`serve` auth:** any v1 story (mTLS? bearer token?), or explicitly "bind to localhost / run behind a sidecar"? The latter is fine, but say so in `--help-full`.
5. **Progress output when `--log-level` is JSON:** suppress the bar entirely, or emit progress as periodic JSON log lines? Recommend the former — users who asked for JSON logs didn't ask for a bar.
