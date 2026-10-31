//! Per-segment time bounds kept beside each cleanly closed file.
//!
//! The sidecar `<segment>.arrows.bounds` is one line: `min max rows`, with
//! times in epoch nanoseconds. It is written once, after the segment's
//! end-of-stream marker, through a temporary file and a rename, so a reader
//! sees either a complete sidecar or none. A missing or unreadable sidecar
//! only costs a full scan; it never hides rows.

use std::path::{Path, PathBuf};

/// Inclusive time range and row count of one closed segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bounds {
    /// Earliest observation time in the segment.
    pub min: i64,
    /// Latest observation time in the segment.
    pub max: i64,
    /// Rows in the segment.
    pub rows: u64,
}

impl Bounds {
    /// True when no row can fall inside the half-open window [start, end).
    pub(super) fn outside(&self, start: i64, end: i64) -> bool {
        self.max < start || self.min >= end
    }

    pub(super) fn save(&self, segment: &Path) -> std::io::Result<()> {
        let path = sidecar(segment);
        let staging = path.with_extension("bounds.tmp");
        std::fs::write(
            &staging,
            format!("{} {} {}\n", self.min, self.max, self.rows),
        )?;
        std::fs::rename(staging, path)
    }

    /// Reads a segment's sidecar, or None when it is absent or malformed.
    pub fn read(segment: &Path) -> Option<Self> {
        let text = std::fs::read_to_string(sidecar(segment)).ok()?;
        let mut parts = text.split_whitespace();
        let bounds = Self {
            min: parts.next()?.parse().ok()?,
            max: parts.next()?.parse().ok()?,
            rows: parts.next()?.parse().ok()?,
        };
        (parts.next().is_none() && bounds.min <= bounds.max).then_some(bounds)
    }
}

fn sidecar(segment: &Path) -> PathBuf {
    let mut name = segment.as_os_str().to_owned();
    name.push(".bounds");
    PathBuf::from(name)
}
