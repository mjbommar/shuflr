//! Engine pipelines. Each module here is a complete shuffle mode or
//! orchestrated flow. v1 modes arrive in the order documented by 002
//! §2 and 004 §9.

pub mod buffer;
#[cfg(feature = "zstd")]
pub mod chunk_shuffled;
pub mod passthrough;

pub use buffer::{Config as BufferConfig, run as buffer};
#[cfg(feature = "zstd")]
pub use chunk_shuffled::{Config as ChunkShuffledConfig, run as chunk_shuffled};
pub use passthrough::{Config as PassthroughConfig, run as passthrough};
