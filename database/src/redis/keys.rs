//! Every Redis key one driver uses. Go's `database/redis/keys.go`, byte for byte, so a Go and a
//! Rust program over one Redis read each other's records.
//!
//! The prefix and the selected database are baked in at construction, so an operation asks
//! for "the key for this record" and cannot reach another database's keyspace. Writing
//! `{head}` for those two:
//!
//! ```text
//! {head}{table}:rec:{key}          one record, proto wire format
//! {head}{table}:ids.z              the sorted set of primary keys, for list and count
//! {head}{table}:uniq:{col}:{value} a reservation on a unique column, holding the key
//! ```

use crate::records::Resource;

/// The key layout for one prefix and database. Go's `keys`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Keys {
    head: String,
}

impl Keys {
    /// The layout under `prefix` and `database`, either of which may be empty. The database
    /// is a key segment rather than a numbered Redis database: those are a property of the
    /// connection, so one would need a second connection, and Redis ships sixteen. Go's
    /// `newKeys`.
    pub(super) fn new(prefix: &str, database: &str) -> Self {
        let mut head = String::new();
        for part in [prefix, database] {
            if !part.is_empty() {
                head.push_str(part);
                head.push(':');
            }
        }
        Self { head }
    }

    /// One resource's keyspace, so two resources never share one.
    pub(super) fn table(&self, res: &Resource) -> String {
        format!("{}{}:", self.head, res.table)
    }

    /// The key holding one encoded record.
    pub(super) fn record(&self, res: &Resource, key: &str) -> String {
        format!("{}rec:{key}", self.table(res))
    }

    /// The sorted set of the resource's primary keys, all scored 0 so the server orders them
    /// by key. The `.z` names the type: a plain set once lived under `ids`, and reusing that
    /// name would meet `WRONGTYPE` rather than a migration.
    pub(super) fn ids(&self, res: &Resource) -> String {
        format!("{}ids.z", self.table(res))
    }

    /// The reservation of `value` on unique `column`, holding the key that owns it. The value
    /// is [`escape`]d here, so a claim and a release cannot disagree on the spelling.
    pub(super) fn unique(&self, res: &Resource, column: &str, value: &str) -> String {
        format!("{}uniq:{column}:{}", self.table(res), escape(value))
    }

    /// A `SCAN MATCH` pattern for every key of one resource, for dropping it. Unlike Go, the
    /// literal part is glob-escaped, so a `*` in a prefix cannot reach another keyspace.
    pub(super) fn pattern(&self, res: &Resource) -> String {
        let mut out = String::new();
        for c in self.table(res).chars() {
            if matches!(c, '*' | '?' | '[' | ']' | '\\') {
                out.push('\\');
            }
            out.push(c);
        }
        out.push('*');
        out
    }
}

/// A value made safe for a key position: a colon in it could otherwise make one column's
/// reservation another's. Percent goes first so the encoding stays reversible. Go's `escape`.
pub(super) fn escape(v: &str) -> String {
    v.replace('%', "%25").replace(':', "%3A")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{Column, Kind};

    fn res() -> Resource {
        let pool = prost_reflect::DescriptorPool::global();
        let ts = pool
            .get_message_by_name("google.protobuf.Timestamp")
            .unwrap();
        Resource::new(ts, "users", "id", vec![Column::new("id", Kind::String)])
    }

    #[test]
    fn the_layout_is_go_s() {
        let k = Keys::new("app", "tenant_a");
        assert_eq!(
            k.record(&res(), "users/ada"),
            "app:tenant_a:users:rec:users/ada"
        );
        assert_eq!(k.ids(&res()), "app:tenant_a:users:ids.z");
        assert_eq!(
            k.unique(&res(), "email", "a:b%c"),
            "app:tenant_a:users:uniq:email:a%3Ab%25c"
        );
        assert_eq!(Keys::new("", "").record(&res(), "x"), "users:rec:x");
        assert_eq!(Keys::new("", "db").ids(&res()), "db:users:ids.z");
        assert_eq!(Keys::new("a*", "").pattern(&res()), "a\\*:users:*");
    }
}
