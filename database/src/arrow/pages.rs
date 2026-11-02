//! Pages of a table, by key or by the offsets earlier versions issued.

use super::errors::joined;
use super::{ArrowDriver, filter, seek};
use crate::store::window::Range;
use crate::store::{ListResult, Resource, Result};

impl ArrowDriver {
    /// One page of the window after `token`; see the `seek` module. `total` asks the
    /// first page to count every match; later pages carry that count.
    ///
    /// A token of bare digits is the offset an earlier version issued, honoured
    /// so a read in progress across an upgrade finishes, at that version's cost.
    pub(super) async fn page(
        &self,
        res: &Resource,
        range: &Range,
        token: &str,
        total: bool,
    ) -> Result<ListResult> {
        if let Ok(offset) = token.parse::<usize>() {
            return self.page_at_offset(res, range, offset).await;
        }
        let (inner, resource) = (self.inner.clone(), res.clone());
        let (range, token) = (range.clone(), token.to_owned());
        tokio::task::spawn_blocking(move || {
            let dir = inner.dir.join(resource.table());
            seek::page(&inner.indexes, &dir, &resource, &range, &token, total)
        })
        .await
        .map_err(joined)?
    }

    /// Batches queries have read back from disk.
    #[cfg(test)]
    pub(super) fn batches_read(&self) -> u64 {
        self.inner
            .indexes
            .reads
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// One page after a row offset, decoding the table to reach it.
    async fn page_at_offset(
        &self,
        res: &Resource,
        range: &Range,
        offset: usize,
    ) -> Result<ListResult> {
        let top = filter::Top::new(res, range, offset)?;
        let (items, total) = self
            .window(res, range, top, |top, rows| top.push(rows))
            .await?
            .finish();
        let next = offset + items.len();
        Ok(ListResult {
            next_page_token: if (next as i64) < total {
                next.to_string()
            } else {
                String::new()
            },
            items,
            total,
        })
    }
}
