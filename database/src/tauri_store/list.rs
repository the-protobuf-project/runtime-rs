//! Listing and counting. The store is an in-memory map, so a listing reads the resource's
//! records and filters, orders and pages them here, with the same `core` filter subset,
//! `order_by` resolution and offset tokens as every other backend.

use std::cmp::Ordering;

use super::codec;
use super::driver::Inner;
use crate::records::{ListOptions, ListResult, Resource, Row, Value, columns_to_message, core};
use crate::{Error, Result};

impl<R: tauri::Runtime> Inner<R> {
    /// Every record of `res` matching `filter`, in store order.
    pub(super) fn matching(&self, res: &Resource, filter: &str) -> Result<Vec<Row>> {
        let clauses = core::parse_filter(filter)?
            .into_iter()
            .map(|c| {
                let col = res.lookup_column(&c.column).ok_or_else(|| {
                    Error::Invalid(format!(
                        "filter names {:?}, which {} does not have",
                        c.column, res.name
                    ))
                })?;
                let v = core::coerce(col.kind, &c.value)
                    .map_err(|e| Error::Invalid(format!("filter on {}: {e}", c.column)))?;
                Ok((col.name.clone(), c.op, v))
            })
            .collect::<Result<Vec<_>>>()?;
        let prefix = self.keys.records(res);
        let mut rows = Vec::new();
        for (key, json) in self.store.entries() {
            if !key.starts_with(&prefix) {
                continue;
            }
            let row = codec::decode(res, &json)?;
            let keep = clauses.iter().all(|(col, op, want)| {
                row.get(col)
                    .and_then(|have| compare(have, want))
                    .is_some_and(|o| holds(op, o))
            });
            if keep {
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

/// How two values of one column compare; `None` when either is null or they are of different
/// kinds, which no filter matches, as SQL's comparisons with `NULL` match nothing.
fn compare(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Int(x), Value::Int(y)) => Some(x.cmp(y)),
        (Value::Uint(x), Value::Uint(y)) => Some(x.cmp(y)),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        (Value::Bytes(x), Value::Bytes(y)) => Some(x.cmp(y)),
        (Value::Timestamp(x), Value::Timestamp(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

fn holds(op: &str, o: Ordering) -> bool {
    match op {
        "=" => o == Ordering::Equal,
        "!=" => o != Ordering::Equal,
        ">" => o == Ordering::Greater,
        ">=" => o != Ordering::Less,
        "<" => o == Ordering::Less,
        "<=" => o != Ordering::Greater,
        _ => false,
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
        _ => {
            compare(a.unwrap_or(&Value::Null), b.unwrap_or(&Value::Null)).unwrap_or(Ordering::Equal)
        }
    }
}
