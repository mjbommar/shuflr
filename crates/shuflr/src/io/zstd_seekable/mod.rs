//! zstd-seekable format support (004 §2).
//!
//! Ships the [`Writer`] (PR-4), [`SeekTable`] parsing (PR-4), and the
//! random-access [`SeekableReader`] (PR-6). `--shuffle=chunk-shuffled`
//! on `.jsonl.zst` inputs is PR-6.

pub mod parallel;
pub mod reader;
pub mod record_index;
pub mod seek_table;
pub mod verify;
pub mod writer;

pub use parallel::{ParallelConfig, run as convert_parallel};
pub use reader::SeekableReader;
pub use record_index::{FrameCache, RecordIndex, RecordLocation};
pub use seek_table::{FrameEntry, SEEKABLE_MAGIC, SKIPPABLE_MAGIC, SeekTable};
pub use verify::{VerifyReport, run as verify, run_strict as verify_strict};
pub use writer::{
    DEFAULT_FRAME_SIZE, DEFAULT_LEVEL, MAX_FRAME_SIZE, Writer, WriterConfig, WriterStats,
};
