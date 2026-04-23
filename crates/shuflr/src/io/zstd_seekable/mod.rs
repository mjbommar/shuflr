//! zstd-seekable format support (004 §2).
//!
//! Ships the [`Writer`] (PR-4), [`SeekTable`] parsing (PR-4), and the
//! random-access [`SeekableReader`] (PR-6). `--shuffle=chunk-shuffled`
//! on `.jsonl.zst` inputs is PR-6.

pub mod reader;
pub mod seek_table;
pub mod writer;

pub use reader::SeekableReader;
pub use seek_table::{FrameEntry, SEEKABLE_MAGIC, SKIPPABLE_MAGIC, SeekTable};
pub use writer::{
    DEFAULT_FRAME_SIZE, DEFAULT_LEVEL, MAX_FRAME_SIZE, Writer, WriterConfig, WriterStats,
};
