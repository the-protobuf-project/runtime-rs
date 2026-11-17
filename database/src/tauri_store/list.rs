//! Listing and counting. The store is an in-memory map, so a listing reads the resource's
//! records and filters, orders and pages them here, with the same `core` filter tree,
//! `order_by` resolution and offset tokens as every other backend.

use std::cmp::Ordering;

use super::codec;
use super::driver::Inner;
use crate::Result;
use crate::records::core::{self, filter};
use crate::records::{ListOptions, ListResult, Resource, Row, Value, columns_to_message};

impl<R: tauri::Runtime> Inner<R> {
    /// Every record of `res` matching `filter`, in store order: the [`filter::parse`] tree
    /// evaluated over each row by [`filter::Expr::matches`], the reference the translating
    /// providers agree with.
    pub(super) fn matching(&self, res: &Resource, filter: &str) -> Result<Vec<Row>> {
        let expr = filter::parse(res, filter)?;
        let prefix = self.keys.records(res);
        let mut rows = Vec::new();
        for (key, json) in self.store.entries() {
            if !key.starts_with(&prefix) {
                continue;
            }
            let row = codec::decode(res, &json)?;
            if expr.as_ref().is_none_or(|e| e.matches(&row)) {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    pub(super) fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult> {
        let mut rows = self.matching(res, &opts.filter)?;
        let order = core::order_by(res, &opts.order_by);
        rows.sort_by(|a, b| {
            order.iter().fold(Ordering::Equal, |acc, (col, desc)| {
                acc.then_with(|| {
                    let o = sort_key(a.get(col), b.get(col));
                    if *desc { o.reverse() } else { o }
                })
            })
        });
        let total = rows.len() as i64;
        let offset = core::decode_token(&opts.page_token);
        let limit = core::page_size(opts.page_size);
        let fetch = core::fetch_limit(limit, opts.omit_total) as usize;
        let page: Vec<Row> = rows.into_iter().skip(offset as usize).take(fetch).collect();
        let (page, next) = core::trim_page(page, offset, limit, total, opts.omit_total);
        Ok(ListResult {
            items: page
                .iter()
                .map(|r| columns_to_message(res, r))
                .collect::<Result<_>>()?,
            next_page_token: next,
            total: if opts.omit_total {
                core::NO_TOTAL
            } else {
                total
            },
        })
    }
}

/// The order of two values in a listing: nulls first, as Postgres orders them descending
/// and SQLite and MySQL ascending; incomparable values tie, leaving the key to decide.
fn sort_key(a: Option<&Value>, b: Option<&Value>) -> Ordering {
    let null = |v: Option<&Value>| v.is_none_or(|v| matches!(v, Value::Null));
    match (null(a), null(b)) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => filter::compare(a.unwrap_or(&Value::Null), b.unwrap_or(&Value::Null))
            .unwrap_or(Ordering::Equal),
    }
}
