//! `list` and `count`: [`ListInput`] as records [`ListOptions`]. Soft-deleted records are
//! excluded, and a parent's records kept, by clauses ANDed onto the caller's filter, so the
//! provider filters, orders and pages them as any other filter. Go's `list`, `filtered`,
//! `count` and `parentScope`.

use std::collections::BTreeSet;

use prost::Message;
use prost_reflect::{DynamicMessage, ReflectMessage};

use super::scope::{Scope, under};
use super::{ListInput, Page, RecordsRepository};
use crate::records::{ListOptions, core, key_of};
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
    /// The caller's filter and, under soft delete without `show_deleted`,
    /// `delete_time = null` (Go's `delete_time IS NULL`).
    fn live_filter(&self, input: &ListInput) -> String {
        match (&self.inner.shape.delete_time, input.show_deleted) {
            (Some((_, column)), false) => and(&input.filter, &format!("{column} = null")),
            _ => input.filter.clone(),
        }
    }

    /// Whether the repository keeps `msg` when it filters itself.
    fn keeps(&self, input: &ListInput, scope: &Scope, msg: &DynamicMessage) -> bool {
        let live = input.show_deleted || !self.inner.shape.deleted(msg);
        let name = key_of(&self.inner.res, msg).unwrap_or_default();
        live && (scope.parent.is_empty() || under(&scope.parent, &name))
    }

    /// The records of `input` and the next token: through the provider when the filter it
    /// runs says it all, else by the repository over a scan (a parent matched by name, or a
    /// provider refusing filters, Redis, when the caller set none).
    async fn select(
        &self,
        input: &ListInput,
        opts: ListOptions,
    ) -> Result<(Vec<DynamicMessage>, String)> {
        let scope = self.inner.names.scope(&input.parent)?;
        let base = self.live_filter(input);
        let direct = scope.parent.is_empty() || scope.clause.is_some();
        let filter = match &scope.clause {
            Some(c) => and(&base, c),
            None => base.clone(),
        };
        let scan_filter = match direct {
            true => {
                let opts = ListOptions {
                    filter,
                    ..opts.clone()
                };
                match self.db.list(&self.inner.res, &opts).await {
                    Ok(page) => return Ok((page.items, page.next_page_token)),
                    Err(Error::Unimplemented { .. }) if input.filter.trim().is_empty() => {
                        String::new()
                    }
                    Err(e) => return Err(e),
                }
            }
            false => base,
        };
        let kept = self.scan(input, &scope, scan_filter).await?;
        let offset = core::decode_token(&input.page_token);
        let limit = core::page_size(input.page_size);
        let page = kept.into_iter().skip(offset as usize).collect::<Vec<_>>();
        Ok(core::trim_page(page, offset, limit, core::NO_TOTAL, true))
    }

    pub(super) async fn list_records(&self, input: &ListInput) -> Result<Page<M>> {
        let opts = ListOptions {
            page_size: input.page_size,
            page_token: input.page_token.clone(),
            order_by: input.order_by.clone(),
            omit_total: true,
            ..ListOptions::default()
        };
        let (items, next_page_token) = self.select(input, opts).await?;
        let mut out = Vec::with_capacity(items.len());
        for msg in &items {
            out.push(self.inner.read(msg).await?);
        }
        Ok(Page {
            items: out,
            next_page_token,
        })
    }

    pub(super) async fn count_records(&self, input: &ListInput) -> Result<i64> {
        let scope = self.inner.names.scope(&input.parent)?;
        let base = self.live_filter(input);
        if scope.parent.is_empty() || scope.clause.is_some() {
            let filter = scope
                .clause
                .as_deref()
                .map_or(base.clone(), |c| and(&base, c));
            match self
                .db
                .count(&self.inner.res, &ListOptions::default().filter(filter))
                .await
            {
                Err(Error::Unimplemented { .. }) if input.filter.trim().is_empty() => {}
                other => return other,
            }
            return Ok(self.scan(input, &scope, String::new()).await?.len() as i64);
        }
        Ok(self.scan(input, &scope, base).await?.len() as i64)
    }

    /// Every record `filter` selects that the repository keeps, in `input`'s order, read
    /// page by page. It reads every match for every page, so it suits small sets; a
    /// repeated token is refused, not looped.
    async fn scan(
        &self,
        input: &ListInput,
        scope: &Scope,
        filter: String,
    ) -> Result<Vec<DynamicMessage>> {
        let mut opts = ListOptions::default()
            .filter(filter)
            .order_by(&input.order_by)
            .page_size(500)
            .omit_total(true);
        let (mut out, mut seen) = (Vec::new(), BTreeSet::new());
        loop {
            let page = self.db.list(&self.inner.res, &opts).await?;
            out.extend(
                page.items
                    .into_iter()
                    .filter(|m| self.keeps(input, scope, m)),
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
