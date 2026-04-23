//! End-to-end CLI integration tests.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use assert_cmd::Command;
use predicates::prelude::*;

fn shuflr() -> Command {
    Command::cargo_bin("shuflr").expect("binary must build")
}

#[test]
fn bare_invocation_prints_help_and_exits_nonzero() {
    shuflr()
        .assert()
        .failure()
        .stderr(predicate::str::contains("Stream large JSONL"));
}

#[test]
fn help_flag_lists_all_subcommands() {
    let assert = shuflr().arg("--help").assert().success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    for sub in ["stream", "analyze", "index", "verify", "completions"] {
        assert!(
            out.contains(sub),
            "--help output missing subcommand '{sub}':\n{out}"
        );
    }
}

#[test]
fn version_flag_emits_a_version() {
    shuflr()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("shuflr"));
}

#[test]
fn implicit_stream_dispatch_on_bare_path() {
    // A path-like positional with no subcommand should route to `stream`,
    // which in PR-1 is stubbed. Exit code 69 (EX_UNAVAILABLE) is how we
    // identify a stubbed-but-not-yet-implemented subcommand.
    shuflr()
        .arg("nonexistent-file.jsonl")
        .assert()
        .code(69)
        .stderr(predicate::str::contains("'stream' is not yet implemented"));
}

#[test]
fn explicit_stream_subcommand_also_stubbed() {
    shuflr()
        .args(["stream", "-"])
        .assert()
        .code(69)
        .stderr(predicate::str::contains("'stream' is not yet implemented"));
}

#[test]
fn completions_subcommand_emits_a_script() {
    let assert = shuflr().args(["completions", "bash"]).assert().success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(
        out.contains("_shuflr"),
        "bash completion missing function marker:\n{out}"
    );
    assert!(
        out.contains("stream"),
        "bash completion missing subcommand:\n{out}"
    );
}

#[test]
fn completions_supports_zsh_and_fish() {
    for shell in ["zsh", "fish"] {
        shuflr()
            .args(["completions", shell])
            .assert()
            .success()
            .stdout(predicate::str::is_empty().not());
    }
}

#[test]
fn unknown_subcommand_fails_cleanly() {
    shuflr()
        .arg("--not-a-real-flag")
        .assert()
        .failure()
        .stderr(predicate::str::contains("error:"));
}

#[test]
fn byte_suffix_parsing_in_convert_help() {
    // Convert is feature-gated on `zstd`, enabled by default.
    let assert = shuflr().args(["convert", "--help"]).assert().success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(
        out.contains("frame-size"),
        "convert --help missing --frame-size:\n{out}"
    );
    assert!(
        out.contains("2MiB"),
        "convert --help missing default 2MiB:\n{out}"
    );
}

#[test]
fn seed_env_var_is_documented() {
    let assert = shuflr().args(["stream", "--help"]).assert().success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(
        out.contains("SHUFLR_SEED"),
        "stream --help missing SHUFLR_SEED env doc:\n{out}"
    );
}

#[test]
fn rank_requires_world_size() {
    shuflr()
        .args(["stream", "--rank", "0", "data.jsonl"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("world-size"));
}

fn tiny_corpus() -> std::path::PathBuf {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest.ancestors().nth(2).unwrap();
    workspace.join("tests/corpora/tiny.jsonl")
}

#[test]
fn stream_none_roundtrips_tiny_fixture() {
    let path = tiny_corpus();
    let assert = shuflr()
        .args(["stream", "--shuffle", "none"])
        .arg(&path)
        .assert()
        .success();
    let expected = std::fs::read(&path).unwrap();
    assert.stdout(expected);
}

#[test]
fn implicit_stream_still_errors_on_default_mode_stub() {
    // Default --shuffle is chunk-shuffled which is stubbed; user gets a clear message.
    shuflr()
        .arg(tiny_corpus())
        .assert()
        .code(69)
        .stderr(predicate::str::contains("not yet implemented"));
}

#[test]
fn stream_none_honors_sample() {
    let path = tiny_corpus();
    let assert = shuflr()
        .args(["stream", "--shuffle", "none", "--sample", "2"])
        .arg(&path)
        .assert()
        .success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert_eq!(out.lines().count(), 2, "expected exactly 2 records:\n{out}");
}

#[test]
fn stream_none_decodes_gzip_transparently() {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write as _;

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("data.jsonl.gz");
    let mut enc = GzEncoder::new(
        std::fs::File::create(&path).unwrap(),
        Compression::default(),
    );
    enc.write_all(b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n").unwrap();
    enc.finish().unwrap();

    shuflr()
        .args(["stream", "--shuffle", "none", "--log-level", "warn"])
        .arg(&path)
        .assert()
        .success()
        .stdout("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
}

#[test]
fn stream_none_decodes_zstd_transparently() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("data.jsonl.zst");
    let bytes = b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n";
    let compressed = zstd::stream::encode_all(&bytes[..], 3).unwrap();
    std::fs::write(&path, compressed).unwrap();

    shuflr()
        .args(["stream", "--shuffle", "none", "--log-level", "warn"])
        .arg(&path)
        .assert()
        .success()
        .stdout("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
}

#[test]
fn stream_none_decodes_gzip_via_stdin() {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write as _;

    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(b"one\ntwo\n").unwrap();
    let compressed = enc.finish().unwrap();

    shuflr()
        .args(["stream", "--shuffle", "none", "--log-level", "warn"])
        .write_stdin(compressed)
        .assert()
        .success()
        .stdout("one\ntwo\n");
}

#[test]
fn stream_none_stdin_works() {
    shuflr()
        .args(["stream", "--shuffle", "none"])
        .write_stdin("one\ntwo\nthree\n")
        .assert()
        .success()
        .stdout("one\ntwo\nthree\n");
}

#[test]
fn stream_none_patches_missing_trailing_newline() {
    shuflr()
        .args(["stream", "--shuffle", "none"])
        .write_stdin("a\nb")
        .assert()
        .success()
        .stdout("a\nb\n");
}

#[test]
fn stream_none_exit_65_on_fail_policy() {
    shuflr()
        .args([
            "stream",
            "--shuffle",
            "none",
            "--max-line",
            "5",
            "--on-error",
            "fail",
        ])
        .write_stdin("ok\nWAY_TOO_LONG\n")
        .assert()
        .code(65) // EX_DATAERR
        .stderr(predicate::str::contains("oversized"));
}

#[test]
fn convert_plain_jsonl_roundtrips_via_zstdcat() {
    let tmp = tempfile::tempdir().unwrap();
    let input_path = tmp.path().join("in.jsonl");
    let output_path = tmp.path().join("out.jsonl.zst");

    let content = (0..500)
        .map(|i| format!("{{\"i\":{i},\"t\":\"record number {i}\"}}\n"))
        .collect::<String>();
    std::fs::write(&input_path, &content).unwrap();

    shuflr()
        .args(["convert", "--log-level", "warn", "-o"])
        .arg(&output_path)
        .arg(&input_path)
        .assert()
        .success();

    // Output must be decodable by any standard zstd reader and reproduce the input.
    let compressed = std::fs::read(&output_path).unwrap();
    let decoded = zstd::stream::decode_all(&compressed[..]).unwrap();
    assert_eq!(decoded, content.as_bytes());
}

#[test]
fn info_reports_seekable_table_of_converted_file() {
    let tmp = tempfile::tempdir().unwrap();
    let input_path = tmp.path().join("in.jsonl");
    let output_path = tmp.path().join("out.jsonl.zst");

    // Larger than default 2 MiB frame size so the output has multiple frames.
    let mut content = String::new();
    for i in 0..100_000 {
        content.push_str(&format!(
            "{{\"i\":{i},\"pad\":\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"}}\n"
        ));
    }
    std::fs::write(&input_path, &content).unwrap();

    shuflr()
        .args(["convert", "--log-level", "warn", "-o"])
        .arg(&output_path)
        .arg(&input_path)
        .assert()
        .success();

    let assert = shuflr().args(["info"]).arg(&output_path).assert().success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(out.contains("format:"), "info missing 'format:':\n{out}");
    assert!(
        out.contains("zstd-seekable"),
        "info missing codec name:\n{out}"
    );
    assert!(out.contains("frames:"), "info missing frames count:\n{out}");
    assert!(
        out.contains("XXH64"),
        "checksums should be on by default:\n{out}"
    );
}

#[test]
fn info_json_mode_parses_cleanly() {
    let tmp = tempfile::tempdir().unwrap();
    let input_path = tmp.path().join("in.jsonl");
    let output_path = tmp.path().join("out.jsonl.zst");
    std::fs::write(&input_path, "a\nb\nc\n").unwrap();

    shuflr()
        .args(["convert", "--log-level", "warn", "-o"])
        .arg(&output_path)
        .arg(&input_path)
        .assert()
        .success();

    let assert = shuflr()
        .args(["info", "--json"])
        .arg(&output_path)
        .assert()
        .success();
    let out = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(out.trim().starts_with('{') && out.trim().ends_with('}'));
    assert!(out.contains("\"format\":\"zstd-seekable\""));
    assert!(out.contains("\"frames\":"));
}

#[test]
fn convert_rejects_multi_input_in_pr4() {
    let tmp = tempfile::tempdir().unwrap();
    let a = tmp.path().join("a.jsonl");
    let b = tmp.path().join("b.jsonl");
    let o = tmp.path().join("o.zst");
    std::fs::write(&a, "x\n").unwrap();
    std::fs::write(&b, "y\n").unwrap();

    shuflr()
        .args(["convert", "-o"])
        .arg(&o)
        .arg(&a)
        .arg(&b)
        .assert()
        .failure()
        .stderr(predicate::str::contains("PR-4"));
}

#[test]
fn convert_preserves_crlf_and_nul() {
    let tmp = tempfile::tempdir().unwrap();
    let input_path = tmp.path().join("in.jsonl");
    let output_path = tmp.path().join("out.jsonl.zst");
    // Nasty bytes: CRLF, embedded NUL, multi-byte UTF-8.
    let original: &[u8] = b"one\r\nt\0wo\n\xe2\x98\x83snowman\n";
    std::fs::write(&input_path, original).unwrap();

    shuflr()
        .args(["convert", "--log-level", "warn", "-o"])
        .arg(&output_path)
        .arg(&input_path)
        .assert()
        .success();

    let compressed = std::fs::read(&output_path).unwrap();
    let decoded = zstd::stream::decode_all(&compressed[..]).unwrap();
    assert_eq!(decoded, original);
}

#[test]
fn stream_none_exit_66_on_missing_input() {
    shuflr()
        .args(["stream", "--shuffle", "none", "/does/not/exist.jsonl"])
        .assert()
        .code(66) // EX_NOINPUT
        .stderr(predicate::str::contains("no such file"));
}
