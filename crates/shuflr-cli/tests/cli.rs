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
fn stream_none_rejects_compressed_input_clearly() {
    // Create a fake .gz in a tempdir with just the magic bytes — enough to trip the sniffer.
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("oops.jsonl.gz");
    std::fs::write(&path, [0x1f, 0x8b, 0x08, 0x00, 0, 0, 0, 0]).unwrap();
    shuflr()
        .args(["stream", "--shuffle", "none"])
        .arg(&path)
        .assert()
        .code(64) // EX_USAGE
        .stderr(predicate::str::contains("gzip"))
        .stderr(predicate::str::contains("gunzip -c"));
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
fn stream_none_exit_66_on_missing_input() {
    shuflr()
        .args(["stream", "--shuffle", "none", "/does/not/exist.jsonl"])
        .assert()
        .code(66) // EX_NOINPUT
        .stderr(predicate::str::contains("no such file"));
}
