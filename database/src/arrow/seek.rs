//! Keyset paging over the segment indexes.
//!
//! Rows are ordered by time, ties by write order, which is what a stable sort of the whole
//! table gives: the key is (time, segment file name, row within the segment). A page token
//! holds the last key returned, and the next page is the `limit` smallest keys after it.
//!
//! Two bounds make a full read linear. Every block whose keys are all at or before the
//! cursor is skipped, and every block whose keys all lose to a full page is skipped, so for
//! a table written roughly in time order a page reads only the blocks it returns. Rows out
//! of order cost overlapping blocks and so more reads; the answer is the same. And the
//! total is counted once, by the first page, then carried in the token: recounting it per
//! page would alone be quadratic. Pages after the first therefore report the total as of the
//! first, while `next_page_token` still sees rows appended since.
//!
//! Memory is one page, one batch, and the indexes.

use super::bounds::Bounds;
use super::errors::invalid;
use super::index::Indexes;
use super::key::{Cursor, scope};
use super::load::segments;
use super::schema::matches;
use super::search::Search;
use crate::store::window::Range;
use crate::store::{ListResult, Resource, Result};
use std::path::Path;
use std::sync::Arc;

/// One page after `token`, counting every match when `total` asks and no token carries it.
/// Blocking: it runs on Tokio's blocking pool.
pub(super) fn page(
    indexes: &Arc<Indexes>,
    dir: &Path,
    resource: &Resource,
    range: &Range,
    token: &str,
    total: bool,
) -> Result<ListResult> {
    let scope = scope(resource, range);
    let cursor = match token {
        "" => None,
        token => Some(Cursor::decode(token, &scope)?),
    };
    let carried = cursor.as_ref().map(|cursor| cursor.total);
    let mut search = Search::new(
        resource,
        range,
        cursor.map(|cursor| cursor.after),
        carried.is_none() && total,
    )?;

    for path in segments(dir)? {
        if Bounds::read(&path).is_some_and(|b| b.outside(range.start, range.end)) {
            continue;
        }
        let index = indexes.get(&path)?;
        if !matches(&index.resource, resource) {
            return Err(invalid(format!(
                "{} was written with other columns",
                path.display()
            )));
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid("a segment name is not UTF-8"))?
            .to_owned();
        for block in &index.blocks {
            search.block(indexes, &path, &index, block, &name)?;
        }
    }

    let (best, matched) = search.finish();
    let total = carried.or(matched).unwrap_or(-1);
    let mut kept = best.into_sorted_vec();
    let more = kept.len() > range.limit;
    kept.truncate(range.limit);
    let next_page_token = match (more, kept.last()) {
        (true, Some(last)) => Cursor {
            scope,
            total,
            after: last.key.clone(),
        }
        .encode()?,
        _ => String::new(),
    };
    Ok(ListResult {
        items: kept.into_iter().map(|entry| entry.record).collect(),
        next_page_token,
        total,
    })
}

#[cfg(test)]
#[path = "seek_tests.rs"]
mod tests;
