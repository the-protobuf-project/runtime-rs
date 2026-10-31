//! When an open segment is closed and the next write starts a new file.

use std::time::Duration;

/// Rotation limits, checked after each write. Either limit closes the segment;
/// `None` disables that limit. Age counts from the segment's first write.
#[derive(Clone, Copy, Debug)]
pub struct Rotation {
    /// Close a segment once its file reaches this many bytes.
    pub max_bytes: Option<u64>,
    /// Close a segment once it has been open this long.
    pub max_age: Option<Duration>,
}

impl Default for Rotation {
    /// 1 GiB or 15 minutes, whichever comes first.
    fn default() -> Self {
        Self {
            max_bytes: Some(1 << 30),
            max_age: Some(Duration::from_secs(15 * 60)),
        }
    }
}

impl Rotation {
    /// Never rotates: one segment per table for the life of the selection.
    pub fn never() -> Self {
        Self {
            max_bytes: None,
            max_age: None,
        }
    }
}
