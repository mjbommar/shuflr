//! PRF hierarchy rooted at a master seed (002 §3).
//!
//! Every random choice in the shuflr engine derives its seed from this
//! tree, so any sub-computation (frame `c` within epoch `e`, etc.) is
//! addressable without replaying the RNG stream. That's what lets
//! resumable cursors be exact without a cursor token equal in size to
//! the RNG state.
//!
//! Current scope (PR-6): we wire up the handful of nodes used by
//! `chunk-shuffled` on seekable-zstd (epoch → frame-permutation +
//! per-frame shuffles). Rank / client branches per 002 §3 come with
//! `--rank` / `serve`.

use blake3::Hasher;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;

/// A 32-byte derived key used to seed a `ChaCha20Rng`.
pub type Key = [u8; 32];

/// Root of the PRF tree: the user's `--seed` value.
#[derive(Copy, Clone, Debug)]
pub struct Seed {
    pub master: u64,
}

impl Seed {
    pub fn new(master: u64) -> Self {
        Self { master }
    }

    /// Derive a child key labeled `(domain, index...)`. Keeps labels
    /// explicit so future call sites can't silently collide.
    fn derive(&self, domain: &[u8], indices: &[u64]) -> Key {
        let mut h = Hasher::new();
        h.update(b"shuflr-v1\0");
        h.update(&self.master.to_le_bytes());
        h.update(b"\0");
        h.update(domain);
        for idx in indices {
            h.update(b"\0");
            h.update(&idx.to_le_bytes());
        }
        *h.finalize().as_bytes()
    }

    /// Epoch-level seed: parent of frame / interleave / jitter seeds.
    pub fn epoch(&self, epoch: u64) -> Key {
        self.derive(b"epoch", &[epoch])
    }

    /// Frame-permutation seed for the given epoch: drives the order in
    /// which frames are visited.
    pub fn perm(&self, epoch: u64) -> Key {
        self.derive(b"perm", &[epoch])
    }

    /// Per-frame shuffle seed: drives Fisher-Yates over the records
    /// inside a single frame.
    pub fn frame(&self, epoch: u64, frame_id: u64) -> Key {
        self.derive(b"frame", &[epoch, frame_id])
    }

    /// RNG for a sub-computation labeled by a key.
    pub fn rng_from(key: Key) -> ChaCha20Rng {
        ChaCha20Rng::from_seed(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn different_epochs_produce_different_keys() {
        let s = Seed::new(42);
        assert_ne!(s.epoch(0), s.epoch(1));
        assert_ne!(s.perm(0), s.perm(1));
        assert_ne!(s.frame(0, 0), s.frame(0, 1));
        assert_ne!(s.frame(0, 0), s.frame(1, 0));
    }

    #[test]
    fn determinism() {
        let s1 = Seed::new(7);
        let s2 = Seed::new(7);
        assert_eq!(s1.epoch(3), s2.epoch(3));
        assert_eq!(s1.frame(5, 17), s2.frame(5, 17));
    }

    #[test]
    fn different_masters_produce_different_keys() {
        let a = Seed::new(1);
        let b = Seed::new(2);
        assert_ne!(a.epoch(0), b.epoch(0));
        assert_ne!(a.frame(3, 5), b.frame(3, 5));
    }
}
