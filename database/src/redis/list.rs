//! Listing: a page of the sorted key set, cut and ordered by the server, then the records it
//! names in one bulk read. Go's `Driver.List`.

use super::RedisDriver;
use super::codec::{descending, failed, refuse_filter};
use crate::Result;
use crate::records::{ListOptions, ListResult, Resource, core};

impl RedisDriver {
    /// A page of records in primary-key order, either direction, at a cost independent of
    /// the table's size: the index is a sorted set, so the server hands back only the page.
    ///
    /// The total and the page are read in one pipeline, where Go spends a round trip on each;
    /// with `omit_total` the total is -1 and one key past the page says whether more follow.
    /// A key whose record is gone, from a delete that failed partway, is dropped from the
    /// index and skipped, as Go does. A filter is refused.
    pub(super) async fn list_page(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult> {
        refuse_filter(&opts.filter, !opts.restrict.is_empty())?;
        let limit = core::page_size(opts.page_size);
        let offset = core::decode_token(&opts.page_token);
        let fetch = core::fetch_limit(limit, opts.omit_total);
        let ids = self.keys.ids(res);

        let mut pipe = redis::pipe();
        if !opts.omit_total {
            pipe.cmd("ZCARD").arg(&ids);
        }
        let range = if descending(&opts.order_by, res) {
            "ZREVRANGE"
        } else {
            "ZRANGE"
        };
        pipe.cmd(range)
            .arg(&ids)
            .arg(offset)
            .arg(offset + fetch - 1);
        let read = |e| failed(format!("cannot read the index of {}", res.name), e);
        let (total, page): (i64, Vec<String>) = if opts.omit_total {
            let (page,): (Vec<String>,) = pipe.query_async(&mut self.conn()).await.map_err(read)?;
            (core::NO_TOTAL, page)
        } else {
            pipe.query_async(&mut self.conn()).await.map_err(read)?
        };
        if page.is_empty() {
            return Ok(ListResult {
                items: Vec::new(),
                next_page_token: String::new(),
                total,
            });
        }

        let (page, next_page_token) = core::trim_page(page, offset, limit, total, opts.omit_total);
        let found = self.fetch(res, &page).await?;
        let mut items = Vec::with_capacity(found.len());
        let mut gone = Vec::new();
        for (key, msg) in page.iter().zip(found) {
            match msg {
                Some(msg) => items.push(msg),
                None => gone.push(key),
            }
        }
        if !gone.is_empty() {
            // Best effort, as in Go: the page is right without it, and the next list retries.
            let _: redis::RedisResult<i64> = redis::cmd("ZREM")
                .arg(&ids)
                .arg(&gone)
                .query_async(&mut self.conn())
                .await;
        }
        Ok(ListResult {
            items,
            next_page_token,
            total,
        })
    }
}
