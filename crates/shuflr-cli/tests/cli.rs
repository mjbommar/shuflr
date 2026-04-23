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
