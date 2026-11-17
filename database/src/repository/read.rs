//! `list` and `count`: [`ListInput`] as records [`ListOptions`]. The caller's filter goes to
//! the provider as written; soft-deleted records are excluded, and a parent's records kept,
//! by restrictions added in code ([`ListOptions::restrict`]), as Go's repository adds
//! `delete_time IS NULL` and the parent's `Where` outside `filterx.Parse`.

use std::collections::BTreeSet;

use prost::Message;
use prost_reflect::{DynamicMessage, ReflectMessage};

use super::scope::{Scope, under};
use super::{ListInput, Page, RecordsRepository};
use crate::records::core::filter::{Comparison, Expr, Op};
use crate::records::{Kind, ListOptions, Value, core, key_of};
use crate::{Error, Result};

impl<M> RecordsRepository<M>
where
    M: ReflectMessage + Message + Default + Send + Sync + 'static,
{
    /// The restrictions of `input` under `scope`: `delete_time IS NULL` under soft delete
    /// without `show_deleted`, and `<parent column> = <id>` when the scope has one.
    fn restrict(&self, input: &ListInput, scope: &Scope) -> Vec<Expr> {
        let mut out = Vec::new();
        if let (Some((_, column)), false) = (&self.inner.shape.delete_time, input.show_deleted) {
            out.push(Expr::Null(column.clone()));
        }
        if let Some((column, id)) = &scope.clause {
            let kind = self
                .inner
                .res
                .lookup_column(column)
                .map_or(Kind::String, |c| c.kind);
            let value = core::coerce(kind, id).unwrap_or_else(|_| Value::String(id.clone()));
            let (column, op) = (column.clone(), Op::Eq);
            out.push(Expr::Compare(Comparison {
                column,
                kind,
                op,
                value,
            }));
        }
        out
    }

    /// Whether the repository keeps `msg` when it filters itself.
    fn keeps(&self, input: &ListInput, scope: &Scope, msg: &DynamicMessage) -> bool {
        let live = input.show_deleted || !self.inner.shape.deleted(msg);
        let name = key_of(&self.inner.res, msg).unwrap_or_default();
        live && (scope.parent.is_empty() || under(&scope.parent, &name))
    }

    /// The options a scan reads with: the provider's filter and restrictions where it takes
    /// them, nothing where it refuses them (Redis), which the repository then applies.
    fn scan_options(&self, input: &ListInput, refused: bool) -> ListOptions {
        match refused {
            true => ListOptions::default(),
            false => ListOptions {
                filter: input.filter.clone(),
                restrict: self.restrict(input, &Scope::default()),
                ..ListOptions::default()
            },
        }
        .order_by(&input.order_by)
        .omit_total(true)
    }

    /// The records of `input` and the next token: through the provider when its filter and
    /// restrictions say it all, else by the repository over a scan (a parent matched by
    /// name, or a provider refusing restrictions, Redis, when the caller set no filter).
    async fn select(
        &self,
        input: &ListInput,
        opts: ListOptions,
    ) -> Result<(Vec<DynamicMessage>, String)> {
        let scope = self.inner.names.scope(&input.parent)?;
        let direct = scope.parent.is_empty() || scope.clause.is_some();
        let refused = match direct {
            true => {
                let restrict = self.restrict(input, &scope);
                let opts = ListOptions {
                    filter: input.filter.clone(),
                    restrict,
                    ..opts
                };
                match self.db.list(&self.inner.res, &opts).await {
                    Ok(page) => return Ok((page.items, page.next_page_token)),
                    Err(Error::Unimplemented { .. }) if input.filter.trim().is_empty() => true,
                    Err(e) => return Err(e),
                }
            }
            false => false,
        };
        let kept = self
            .scan(input, &scope, self.scan_options(input, refused))
            .await?;
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
        let mut refused = false;
        if scope.parent.is_empty() || scope.clause.is_some() {
            let opts = ListOptions {
                filter: input.filter.clone(),
                restrict: self.restrict(input, &scope),
                ..ListOptions::default()
            };
            match self.db.count(&self.inner.res, &opts).await {
                Err(Error::Unimplemented { .. }) if input.filter.trim().is_empty() => {
                    refused = true
                }
                other => return other,
            }
        }
        let opts = self.scan_options(input, refused);
        Ok(self.scan(input, &scope, opts).await?.len() as i64)
    }

    /// Every record `opts` selects that the repository keeps, read page by page. It reads
    /// every match for every page, so it suits small sets; a repeated token is refused, not
    /// looped.
    async fn scan(
        &self,
        input: &ListInput,
        scope: &Scope,
        opts: ListOptions,
    ) -> Result<Vec<DynamicMessage>> {
        let mut opts = opts.page_size(500);
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
