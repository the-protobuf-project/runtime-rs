//! Request builders for operation arguments.
//!
//! These types are shared (not generated per resource) because every resource's
//! optional arguments have the same shape: a where filter, ordering, and paging for
//! reads; pre/post-check row filters for writes. A generated resource handler can then
//! satisfy the generic [`QueryHandler`](crate::QueryHandler)/[`MutationHandler`](crate::MutationHandler)
//! traits, so a single adapter can drive CRUD for every entity.
//!
//! Builder methods take `&mut self` and return `&mut Self` (rather than a consuming
//! `self -> Self`) so callers can both chain fluently and build conditionally, e.g.
//! `if let Some(f) = filter { r.where_(f); }`.

use crate::columns::OrderTerm;
use crate::keyset::after;
use crate::predicate::{Predicate, and};

/// The optional arguments shared by list, find, aggregate, and live-list operations: a
/// where filter, result ordering, and limit/offset paging.
#[derive(Debug, Clone, Default)]
pub struct ListRequest {
    limit: i64,
    offset: i64,
    order_by: Vec<OrderTerm>,
    where_: Predicate,
}

impl ListRequest {
    /// Sets the maximum number of rows to return.
    pub fn limit(&mut self, v: i64) -> &mut Self {
        self.limit = v;
        self
    }

    /// Sets how many leading rows to skip.
    pub fn offset(&mut self, v: i64) -> &mut Self {
        self.offset = v;
        self
    }

    /// Sets the result ordering (build terms with a field handle's `asc`/`desc`).
    pub fn order_by(&mut self, v: Vec<OrderTerm>) -> &mut Self {
        self.order_by = v;
        self
    }

    /// Sets the row filter predicate.
    pub fn where_(&mut self, v: Predicate) -> &mut Self {
        self.where_ = v;
        self
    }

    /// Turns this request into the next keyset page: orders by `term` and keeps only
    /// rows after `last` (see [`after`]), composing with any predicate already set via
    /// [`ListRequest::where_`]. Pair with [`ListRequest::limit`] for the page size; the
    /// cursor for the following page is `term`'s column value from the last row
    /// returned.
    pub fn keyset_after(&mut self, term: OrderTerm, last: impl serde::Serialize) -> &mut Self {
        let ks = after(&term, last);
        self.order_by.push(term);
        self.where_ = if self.where_.is_omitted() {
            ks
        } else {
            and(&[std::mem::take(&mut self.where_), ks])
        };
        self
    }

    /// The configured row limit.
    pub fn get_limit(&self) -> i64 {
        self.limit
    }

    /// The configured row offset.
    pub fn get_offset(&self) -> i64 {
        self.offset
    }

    /// The configured result ordering.
    pub fn get_order_by(&self) -> &[OrderTerm] {
        &self.order_by
    }

    /// The configured row filter predicate.
    pub fn get_where(&self) -> &Predicate {
        &self.where_
    }
}

/// The optional arguments for an insert: a post-check row filter the inserted rows must
/// satisfy (a permission/consistency guard).
#[derive(Debug, Clone, Default)]
pub struct CreateRequest {
    post_check: Predicate,
}

impl CreateRequest {
    /// Sets the post-insert guard predicate.
    pub fn post_check(&mut self, v: Predicate) -> &mut Self {
        self.post_check = v;
        self
    }

    /// The configured post-insert guard.
    pub fn get_post_check(&self) -> &Predicate {
        &self.post_check
    }
}

/// The optional arguments for an update: a pre-check guard (the row must match before
/// the write — the basis of optimistic concurrency) and a post-check guard (the row
/// must match after).
#[derive(Debug, Clone, Default)]
pub struct UpdateRequest {
    pre_check: Predicate,
    post_check: Predicate,
}

impl UpdateRequest {
    /// Sets the pre-update guard predicate (e.g. an etag equality).
    pub fn pre_check(&mut self, v: Predicate) -> &mut Self {
        self.pre_check = v;
        self
    }

    /// Sets the post-update guard predicate.
    pub fn post_check(&mut self, v: Predicate) -> &mut Self {
        self.post_check = v;
        self
    }

    /// The configured pre-update guard.
    pub fn get_pre_check(&self) -> &Predicate {
        &self.pre_check
    }

    /// The configured post-update guard.
    pub fn get_post_check(&self) -> &Predicate {
        &self.post_check
    }
}

/// The optional arguments for a delete: a pre-check guard the row must match before
/// removal.
#[derive(Debug, Clone, Default)]
pub struct DeleteRequest {
    pre_check: Predicate,
}

impl DeleteRequest {
    /// Sets the pre-delete guard predicate.
    pub fn pre_check(&mut self, v: Predicate) -> &mut Self {
        self.pre_check = v;
        self
    }

    /// The configured pre-delete guard.
    pub fn get_pre_check(&self) -> &Predicate {
        &self.pre_check
    }
}
