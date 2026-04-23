//! zstd-seekable format support (004 §2).
//!
//! Currently ships the [`Writer`] + trailing seek-table parsing; the
//! seek-aware reader lands in PR-7 alongside `--shuffle=chunk-shuffled`
//! on compressed inputs.

pub mod seek_table;
pub mod writer;

pub use seek_table::{FrameEntry, SEEKABLE_MAGIC, SKIPPABLE_MAGIC, SeekTable};
pub use writer::{
    DEFAULT_FRAME_SIZE, DEFAULT_LEVEL, MAX_FRAME_SIZE, Writer, WriterConfig, WriterStats,
};
