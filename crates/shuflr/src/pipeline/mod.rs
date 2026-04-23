//! Engine pipelines. Each module here is a complete shuffle mode or
//! orchestrated flow. v1 modes arrive in the order documented by 002
//! §2 and 004 §9.

pub mod buffer;
#[cfg(feature = "zstd")]
pub mod chunk_shuffled;
pub mod index_perm;
#[cfg(feature = "zstd")]
pub mod index_perm_zstd;
pub mod passthrough;
pub mod reservoir;

pub use buffer::{Config as BufferConfig, run as buffer};
#[cfg(feature = "zstd")]
pub use chunk_shuffled::{Config as ChunkShuffledConfig, run as chunk_shuffled};
pub use index_perm::{Config as IndexPermConfig, run as index_perm};
#[cfg(feature = "zstd")]
pub use index_perm_zstd::{
    Config as IndexPermZstdConfig, RunMetrics as IndexPermZstdMetrics, run as index_perm_zstd,
};
pub use passthrough::{Config as PassthroughConfig, run as passthrough};
pub use reservoir::{Config as ReservoirConfig, run as reservoir};
