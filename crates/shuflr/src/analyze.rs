//! `shuflr analyze` — detect source-order locality in a seekable-zstd file
//! that would make `--shuffle=chunk-shuffled` a bad choice (ML review 02 §1,
//! 002 §6.4).
//!
//! Approach:
//! 1. Sample up to `sample_chunks` frames uniformly at random (seeded).
//! 2. For each sampled frame, compute a 256-bin byte histogram and count
//!    records + mean record length.
//! 3. Compute the *global* byte distribution as the length-weighted mean
//!    of per-frame histograms.
//! 4. For each per-frame distribution, compute its KL divergence against
//!    the global, i.e. `Σ p_i · log(p_i / q_i)` over non-zero bins.
//! 5. Verdict: if `max KL > BYTE_KL_THRESHOLD_UNSAFE` **or** record-length
//!    coefficient of variation exceeds `RECLEN_CV_THRESHOLD_UNSAFE`, the
//!    corpus shows strong source-order locality and `chunk-shuffled` will
//!    preserve it. Recommend `index-perm`.
//!
//! Both thresholds are tuned against real EDGAR corpora (near-random byte
//! distributions, heavy record-length variance). A sorted-by-length
//! synthetic corpus trips the record-length check trivially; a sorted-by-
//! attribute synthetic corpus trips the byte-KL check.

#[cfg(feature = "zstd")]
use crate::error::Result;
#[cfg(feature = "zstd")]
use crate::io::zstd_seekable::SeekableReader;
#[cfg(feature = "zstd")]
use rand::{SeedableRng, seq::SliceRandom};
#[cfg(feature = "zstd")]
use rand_chacha::ChaCha20Rng;

/// Flag the corpus as unsafe if the max per-frame byte-KL vs global
/// exceeds this threshold (in nats). Empirically: uniform-random text
/// hits ~0.05 with small-sample noise; sorted-by-alphabet corpora
/// exceed 0.2. 0.10 separates them reliably on moderate sample sizes.
pub const BYTE_KL_THRESHOLD_UNSAFE: f64 = 0.10;

/// Coefficient of variation for per-frame mean record length. EDGAR's
/// natural variance is ~30–50%; a length-sorted corpus pushes this
/// above 0.6. 0.50 catches the latter without tripping on the former.
pub const RECLEN_CV_THRESHOLD_UNSAFE: f64 = 0.50;

/// Jensen–Shannon divergence threshold in nats. JS is symmetric and
/// bounded in `[0, ln 2]` (≈ [0, 0.693]), so thresholds port cleanly
/// across corpora. Empirically: random text hits ~0.01–0.03; source-
/// sorted corpora exceed 0.15.
pub const BYTE_JS_THRESHOLD_UNSAFE: f64 = 0.10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// No significant source-order locality detected. `chunk-shuffled` is OK.
    Safe,
    /// Frames differ enough that `chunk-shuffled` would preserve the
    /// clustering. Recommend `index-perm` (via `shuflr index`).
    Unsafe,
}

#[derive(Debug, Clone)]
pub struct AnalysisReport {
    pub total_frames: usize,
    pub sampled_frames: usize,
    pub total_records_sampled: u64,
    pub mean_record_len_bytes: f64,
    /// Max KL divergence (nats) of any sampled frame's byte distribution vs the global.
    pub byte_kl_max: f64,
    pub byte_kl_mean: f64,
    /// Max Jensen–Shannon divergence (nats, bounded `[0, ln 2]`) vs the global.
    pub byte_js_max: f64,
    pub byte_js_mean: f64,
    /// Mean per-frame byte-distribution Shannon entropy (nats, max = ln 256 ≈ 5.545).
    pub frame_entropy_mean: f64,
    /// Coefficient of variation of per-frame mean record length.
    pub reclen_cv: f64,
    pub verdict: Verdict,
}

#[cfg(feature = "zstd")]
pub fn run(reader: &mut SeekableReader, sample_chunks: usize, seed: u64) -> Result<AnalysisReport> {
    let total_frames = reader.num_frames();
    if total_frames == 0 {
        return Ok(AnalysisReport {
            total_frames: 0,
            sampled_frames: 0,
            total_records_sampled: 0,
            mean_record_len_bytes: 0.0,
            byte_kl_max: 0.0,
            byte_kl_mean: 0.0,
            byte_js_max: 0.0,
            byte_js_mean: 0.0,
            frame_entropy_mean: 0.0,
            reclen_cv: 0.0,
            verdict: Verdict::Safe,
        });
    }
    // Pick up to `sample_chunks` distinct frames uniformly at random.
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let mut ids: Vec<usize> = (0..total_frames).collect();
    ids.shuffle(&mut rng);
    ids.truncate(sample_chunks.min(total_frames));

    let mut global = [0u64; 256];
    let mut per_frame_hists: Vec<[u64; 256]> = Vec::with_capacity(ids.len());
    let mut per_frame_mean_len: Vec<f64> = Vec::with_capacity(ids.len());
    let mut total_records: u64 = 0;
    let mut total_bytes: u64 = 0;

    for id in &ids {
        let bytes = reader.decompress_frame(*id)?;
        let mut hist = [0u64; 256];
        for b in &bytes {
            hist[*b as usize] += 1;
        }
        let records: u64 = memchr::memchr_iter(b'\n', &bytes).count() as u64;
        let mean_len = if records > 0 {
            bytes.len() as f64 / records as f64
        } else {
            bytes.len() as f64
        };
        total_records += records;
        total_bytes += bytes.len() as u64;
        for i in 0..256 {
            global[i] += hist[i];
        }
        per_frame_hists.push(hist);
        per_frame_mean_len.push(mean_len);
    }

    // Global distribution p_i = global[i] / total_bytes.
    let global_total: u64 = global.iter().sum();
    let mut global_p = [0f64; 256];
    if global_total > 0 {
        for i in 0..256 {
            global_p[i] = global[i] as f64 / global_total as f64;
        }
    }

    // Per-frame metrics: KL(p || q), JS(p || q), entropy H(p), all in nats.
    let mut kls: Vec<f64> = Vec::with_capacity(per_frame_hists.len());
    let mut jss: Vec<f64> = Vec::with_capacity(per_frame_hists.len());
    let mut ents: Vec<f64> = Vec::with_capacity(per_frame_hists.len());
    for hist in &per_frame_hists {
        let total: u64 = hist.iter().sum();
        if total == 0 {
            continue;
        }
        let mut p = [0f64; 256];
        for i in 0..256 {
            p[i] = hist[i] as f64 / total as f64;
        }
        kls.push(kl_divergence(&p, &global_p));
        jss.push(js_divergence(&p, &global_p));
        ents.push(entropy(&p));
    }
    let (kl_max, kl_mean) = summarize(&kls);
    let (js_max, js_mean) = summarize(&jss);
    let (_, entropy_mean) = summarize(&ents);

    // Record-length coefficient of variation across sampled frames.
    let reclen_cv = coefficient_of_variation(&per_frame_mean_len);

    let verdict = if kl_max > BYTE_KL_THRESHOLD_UNSAFE
        || js_max > BYTE_JS_THRESHOLD_UNSAFE
        || reclen_cv > RECLEN_CV_THRESHOLD_UNSAFE
    {
        Verdict::Unsafe
    } else {
        Verdict::Safe
    };

    Ok(AnalysisReport {
        total_frames,
        sampled_frames: ids.len(),
        total_records_sampled: total_records,
        mean_record_len_bytes: if total_records == 0 {
            0.0
        } else {
            total_bytes as f64 / total_records as f64
        },
        byte_kl_max: kl_max,
        byte_kl_mean: kl_mean,
        byte_js_max: js_max,
        byte_js_mean: js_mean,
        frame_entropy_mean: entropy_mean,
        reclen_cv,
        verdict,
    })
}

/// `KL(p || q) = Σ p_i · ln(p_i / q_i)` over bins where p_i > 0. Bins
/// where q_i = 0 are skipped (infinity by convention; shouldn't happen
/// when p contributes to q via the global aggregation).
#[cfg(feature = "zstd")]
fn kl_divergence(p: &[f64; 256], q: &[f64; 256]) -> f64 {
    let mut kl = 0.0;
    for i in 0..256 {
        if p[i] > 0.0 && q[i] > 0.0 {
            kl += p[i] * (p[i] / q[i]).ln();
        }
    }
    kl
}

/// `JS(p, q) = 0.5 · KL(p || m) + 0.5 · KL(q || m)`, `m = 0.5·(p + q)`.
/// Symmetric in p/q, bounded in `[0, ln 2]`.
#[cfg(feature = "zstd")]
fn js_divergence(p: &[f64; 256], q: &[f64; 256]) -> f64 {
    let mut js = 0.0;
    for i in 0..256 {
        let (pi, qi) = (p[i], q[i]);
        let mi = 0.5 * (pi + qi);
        if mi == 0.0 {
            continue;
        }
        if pi > 0.0 {
            js += 0.5 * pi * (pi / mi).ln();
        }
        if qi > 0.0 {
            js += 0.5 * qi * (qi / mi).ln();
        }
    }
    js
}

/// Shannon entropy `H(p) = -Σ p_i · ln(p_i)` in nats. Max for 256 bins is ln 256 ≈ 5.545.
#[cfg(feature = "zstd")]
fn entropy(p: &[f64; 256]) -> f64 {
    let mut h = 0.0;
    for &pi in p {
        if pi > 0.0 {
            h -= pi * pi.ln();
        }
    }
    h
}

#[cfg(feature = "zstd")]
fn summarize(xs: &[f64]) -> (f64, f64) {
    if xs.is_empty() {
        return (0.0, 0.0);
    }
    let sum: f64 = xs.iter().sum();
    let max = xs.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    (max, sum / xs.len() as f64)
}

#[cfg(feature = "zstd")]
fn coefficient_of_variation(xs: &[f64]) -> f64 {
    if xs.len() < 2 {
        return 0.0;
    }
    let mean: f64 = xs.iter().sum::<f64>() / xs.len() as f64;
    if mean == 0.0 {
        return 0.0;
    }
    let var: f64 = xs.iter().map(|x| (*x - mean).powi(2)).sum::<f64>() / xs.len() as f64;
    var.sqrt() / mean
}

#[cfg(all(test, feature = "zstd"))]
mod tests {
    use super::*;
    use crate::io::zstd_seekable::writer::{Writer, WriterConfig};

    fn build_seekable(records: &[&[u8]]) -> tempfile::NamedTempFile {
        let tf = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::File::create(tf.path()).unwrap();
        let mut w = Writer::new(
            file,
            WriterConfig {
                level: 3,
                frame_size: 256,
                checksums: true,
                record_aligned: true,
            },
        );
        for r in records {
            w.write_block(r).unwrap();
        }
        w.finish().unwrap();
        tf
    }

    #[test]
    fn random_corpus_verdicts_safe() {
        use rand::Rng;
        let mut rng = ChaCha20Rng::seed_from_u64(1);
        let mut records: Vec<Vec<u8>> = Vec::new();
        for _ in 0..600 {
            let len = rng.gen_range(20..200);
            let mut s = String::new();
            for _ in 0..len {
                let c = (rng.gen_range(b'a'..=b'z')) as char;
                s.push(c);
            }
            s.push('\n');
            records.push(s.into_bytes());
        }
        let record_refs: Vec<&[u8]> = records.iter().map(|v| v.as_slice()).collect();
        let tmp = build_seekable(&record_refs);
        let mut r = SeekableReader::open(tmp.path()).unwrap();
        let report = run(&mut r, 16, 42).unwrap();
        assert_eq!(
            report.verdict,
            Verdict::Safe,
            "random byte distribution should be safe (kl_max={}, cv={})",
            report.byte_kl_max,
            report.reclen_cv
        );
    }

    #[test]
    fn length_sorted_corpus_verdicts_unsafe() {
        // Records grouped by length: early frames hold short records,
        // later frames hold long ones. Frame size is deliberately large
        // so each frame holds many records and the per-frame mean length
        // stabilizes near the homogeneous group's record length.
        let mut records: Vec<Vec<u8>> = Vec::new();
        for _ in 0..400 {
            records.push(b"short_record\n".to_vec()); // 13 bytes
        }
        for _ in 0..400 {
            let mut s = vec![b'x'; 300];
            s.push(b'\n');
            records.push(s); // 301 bytes
        }
        let record_refs: Vec<&[u8]> = records.iter().map(|v| v.as_slice()).collect();
        let tf = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::File::create(tf.path()).unwrap();
        let mut w = Writer::new(
            file,
            WriterConfig {
                level: 3,
                frame_size: 2048, // big enough to hold ~150 short or ~7 long records
                checksums: true,
                record_aligned: true,
            },
        );
        for r in &record_refs {
            w.write_block(r).unwrap();
        }
        w.finish().unwrap();

        let mut r = SeekableReader::open(tf.path()).unwrap();
        let report = run(&mut r, 32, 42).unwrap();
        assert_eq!(
            report.verdict,
            Verdict::Unsafe,
            "length-sorted corpus should be flagged (cv={}, kl_max={})",
            report.reclen_cv,
            report.byte_kl_max
        );
    }

    #[test]
    fn byte_sorted_corpus_verdicts_unsafe() {
        // Records partitioned by alphabet: first half is all 'a'..'m',
        // second half is all 'n'..'z'. Byte-KL per frame should exceed
        // threshold because early and late frames have disjoint supports.
        let mut records: Vec<Vec<u8>> = Vec::new();
        for i in 0..400 {
            let mut s = vec![b'a' + (i % 13) as u8; 80]; // 'a'..'m'
            s.push(b'\n');
            records.push(s);
        }
        for i in 0..400 {
            let mut s = vec![b'n' + (i % 13) as u8; 80]; // 'n'..'z'
            s.push(b'\n');
            records.push(s);
        }
        let record_refs: Vec<&[u8]> = records.iter().map(|v| v.as_slice()).collect();
        let tmp = build_seekable(&record_refs);
        let mut r = SeekableReader::open(tmp.path()).unwrap();
        let report = run(&mut r, 16, 42).unwrap();
        assert_eq!(
            report.verdict,
            Verdict::Unsafe,
            "byte-sorted corpus should be flagged (kl_max={}, cv={})",
            report.byte_kl_max,
            report.reclen_cv
        );
        assert!(report.byte_kl_max > BYTE_KL_THRESHOLD_UNSAFE);
    }
}
