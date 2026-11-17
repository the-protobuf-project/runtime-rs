//! `list` and `count`: [`ListInput`] as records [`ListOptions`], with soft-deleted records
//! excluded by a clause ANDed onto the caller's filter, so the provider filters, orders and
//! pages them as it does any other filter. Go's `list`, `filtered` and `count`.

use std::collections::BTreeSet;

use prost::Message;
use prost_reflect::{DynamicMessage, ReflectMessage};

use super::{ListInput, Page, RecordsRepository};
use crate::records::{ListOptions, core};
use crate::{Error, Result};

/// `filter AND clause`. No parentheses are needed, now or under full AIP-160: `AND` binds
/// loosest there (`a OR b AND c` is `(a OR b) AND c`), so a clause ANDed at the end applies
/// to the whole of the caller's filter.
pub(super) fn and(filter: &str, clause: &str) -> String {
    match filter.trim() {
        "" => clause.to_owned(),
        f => format!("{f} AND {clause}"),
    }
}

impl<M> RecordsRepository<M>
where
    M: ReflectMessage + Message + Default + Send + Sync + 'static,
{
    /// The filter the provider runs: the caller's, and under soft delete without
    /// `show_deleted`, `delete_time = null` (Go's `delete_time IS NULL`).
    fn filter(&self, input: &ListInput) -> String {
        match (&self.inner.shape.delete_time, input.show_deleted) {
            (Some((_, column)), false) => and(&input.filter, &format!("{column} = null")),
            _ => input.filter.clone(),
        }
    }

    /// Whether a refusal of `filter` is the provider refusing any filter at all (Redis) when
    /// the only filter is the one excluding deleted records, which the repository can then
    /// apply itself.
    fn scans(&self, input: &ListInput, err: &Error) -> bool {
        matches!(err, Error::Unimplemented { .. })
            && input.filter.trim().is_empty()
            && self.filter(input) != input.filter
    }

    pub(super) async fn list_records(&self, input: &ListInput) -> Result<Page<M>> {
        let inner = &self.inner;
        let opts = ListOptions {
            page_size: input.page_size,
            page_token: input.page_token.clone(),
            order_by: input.order_by.clone(),
            filter: self.filter(input),
            omit_total: true,
        };
        let (items, next_page_token) = match self.db.list(&inner.res, &opts).await {
            Ok(page) => (page.items, page.next_page_token),
            Err(e) if self.scans(input, &e) => {
                let live = self.live(input).await?;
                let offset = core::decode_token(&input.page_token);
                let limit = core::page_size(input.page_size);
                let page = live.into_iter().skip(offset as usize).collect::<Vec<_>>();
                core::trim_page(page, offset, limit, core::NO_TOTAL, true)
            }
            Err(e) => return Err(e),
        };
        let mut out = Vec::with_capacity(items.len());
        for msg in &items {
            out.push(inner.read(msg).await?);
        }
        Ok(Page {
            items: out,
            next_page_token,
        })
    }

    pub(super) async fn count_records(&self, input: &ListInput) -> Result<i64> {
        let opts = ListOptions::default().filter(self.filter(input));
        match self.db.count(&self.inner.res, &opts).await {
            Err(e) if self.scans(input, &e) => Ok(self.live(input).await?.len() as i64),
            other => other,
        }
    }

    /// Every live record, in `input`'s order, read page by page: how a provider without
    /// filters lists a soft-delete resource. It reads every record for every page, so it
    /// suits the small sets such a provider holds; a repeated token is refused, not looped.
    async fn live(&self, input: &ListInput) -> Result<Vec<DynamicMessage>> {
        let mut opts = ListOptions::default()
            .order_by(&input.order_by)
            .page_size(500)
            .omit_total(true);
        let (mut out, mut seen) = (Vec::new(), BTreeSet::new());
        loop {
            let page = self.db.list(&self.inner.res, &opts).await?;
            out.extend(
                page.items
                    .into_iter()
                    .filter(|m| !self.inner.shape.deleted(m)),
            );
            if page.next_page_token.is_empty() {
                return Ok(out);
            }
            if !seen.insert(page.next_page_token.clone()) {
                return Err(Error::Invalid(format!(
                    "{} repeated page token {:?}",
                    self.inner.res.name, page.next_page_token
                )));
            }
            opts.page_token = page.next_page_token;
        }
    }
}
