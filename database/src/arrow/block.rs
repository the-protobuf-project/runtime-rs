//! A run of whole batches in a segment, as the index records it.

/// A run of whole batches.
#[derive(Clone, Copy, Debug)]
pub(super) struct Block {
    /// Where its first batch's message starts.
    pub offset: u64,
    /// Where its last batch's message ends.
    pub end: u64,
    /// The ordinal of its first row within the segment.
    pub first_row: u64,
    /// Rows in the block.
    pub rows: u64,
    /// Rows with a timestamp; foreign files may hold nulls, which no window matches.
    pub valid: u64,
    /// Earliest timestamp, or `i64::MAX` with none.
    pub min: i64,
    /// Latest timestamp, or `i64::MIN` with none.
    pub max: i64,
}

impl Block {
    pub fn new(offset: u64, first_row: u64) -> Self {
        Self {
            offset,
            end: offset,
            first_row,
            rows: 0,
            valid: 0,
            min: i64::MAX,
            max: i64::MIN,
        }
    }

    /// The ordinal of its last row.
    pub fn last_row(&self) -> u64 {
        self.first_row + self.rows.saturating_sub(1)
    }

    /// True when every timestamped row is inside the half-open window.
    pub fn inside(&self, start: i64, end: i64) -> bool {
        self.valid > 0 && self.min >= start && self.max < end
    }

    /// True when no row can be inside the half-open window.
    pub fn outside(&self, start: i64, end: i64) -> bool {
        self.valid == 0 || self.max < start || self.min >= end
    }
}
