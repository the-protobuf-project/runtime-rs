//! The per-resource data a generated plan supplies.

use std::time::Duration;

use super::Keyspace;

/// The strategy the resource was annotated with. Every strategy reads by name through the
/// read-through (Aside) entry, as Go's decorator does; `Indexed` additionally files each
/// written record under its [`Index`] fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Strategy {
    /// `STRATEGY_ASIDE`.
    Aside,
    /// `STRATEGY_INDEXED`.
    Indexed,
    /// `STRATEGY_DOCUMENT`.
    Document,
    /// `STRATEGY_VOLATILE`.
    Volatile,
}

/// A secondary index field of an `INDEXED` resource.
pub struct Index<M> {
    /// The index name in the cache's keyspace.
    pub field: &'static str,
    /// The record's value for it; `None` leaves the record unfiled under this index.
    pub value: fn(&M) -> Option<&str>,
}

/// A declared invalidation edge: writing this resource makes `target`'s entry for the
/// record `via` names stale.
pub struct Edge<M> {
    /// The resource whose entry is dropped.
    pub target: &'static Keyspace,
    /// The reference field: the target's name, or its id when its pattern has one variable.
    pub via: fn(&M) -> Option<&str>,
}

/// One declared invalidation edge, as data: Go's `Edge`. The generated
/// `INVALIDATION_EDGES` lists them so the blast radius of a write is readable, and so a
/// consumer acting on a change feed can use them without regenerating anything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeclaredEdge {
    /// The resource written.
    pub from: &'static str,
    /// The resource whose entry the write makes stale.
    pub to: &'static str,
    /// The reference column carrying the relationship.
    pub via: &'static str,
    /// Why, in a sentence.
    pub reason: &'static str,
}

/// The record cache of one resource.
pub struct Record<M> {
    /// Where its entries live.
    pub keyspace: &'static Keyspace,
    /// The annotated strategy.
    pub strategy: Strategy,
    /// How long an entry is fresh; `None` takes the cache's default.
    pub ttl: Option<Duration>,
    /// How long past `ttl` an entry may be served while it refreshes.
    pub stale: Option<Duration>,
    /// The index fields, for `Strategy::Indexed`.
    pub indexes: Vec<Index<M>>,
    /// The edges a write fires, in order.
    pub edges: Vec<Edge<M>>,
}

/// The list cache of one resource: Go's `<Model>ListCache`.
#[derive(Clone, Debug)]
pub struct List {
    /// The proto message, for diagnostics.
    pub resource: &'static str,
    /// The fixed namespace pages are cached under (the record namespace's base and `.list`).
    pub namespace: &'static str,
    /// The pattern's collections, which a written record's name is matched against.
    pub collections: &'static [&'static str],
    /// Whether the pattern has a parent.
    pub parented: bool,
    /// How long a page may be served.
    pub ttl: Duration,
}

/// Everything resource-specific about one cached repository; generated.
pub struct Plan<M> {
    /// The record's resource name.
    pub name: fn(&M) -> &str,
    /// The record cache, when `(cache.v1.cache).enabled`.
    pub record: Option<Record<M>>,
    /// The list cache, when `(cache.v1.cache).list.enabled`.
    pub list: Option<List>,
}
