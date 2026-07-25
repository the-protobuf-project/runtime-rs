//! Filter expressions for a resource's Find/where argument.

use serde::{Serialize, Serializer};

/// A filter expression for a resource's Find/where argument. Built from the generated
/// per-resource field handles (e.g. `resource.id.eq("x")`) and combined with
/// [`and`]/[`or`]/[`not`]. The default `Predicate` is empty; generated code omits it
/// from the operation entirely rather than sending an empty object.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Predicate(pub(crate) serde_json::Value);

impl Predicate {
    /// Reports whether this is the default, empty predicate that generated code should
    /// omit from the operation entirely.
    pub fn is_omitted(&self) -> bool {
        self.0.is_null()
    }
}

impl Serialize for Predicate {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

/// Builds a single-column predicate of the form `{col: {op: v}}`. Shared constructor
/// used by every field handle's operator method.
pub(crate) fn pred(col: &str, op: &str, v: impl Serialize) -> Predicate {
    let value = serde_json::to_value(v).unwrap_or(serde_json::Value::Null);
    let mut inner = serde_json::Map::new();
    inner.insert(op.to_string(), value);
    let mut outer = serde_json::Map::new();
    outer.insert(col.to_string(), serde_json::Value::Object(inner));
    Predicate(serde_json::Value::Object(outer))
}

/// Combines predicates so that all must match (`{_and: [...]}`). An empty slice returns
/// the empty predicate.
pub fn and(ps: &[Predicate]) -> Predicate {
    combine("_and", ps)
}

/// Combines predicates so that any may match (`{_or: [...]}`). An empty slice returns
/// the empty predicate.
pub fn or(ps: &[Predicate]) -> Predicate {
    combine("_or", ps)
}

/// Negates a predicate (`{_not: ...}`).
pub fn not(p: Predicate) -> Predicate {
    let mut outer = serde_json::Map::new();
    outer.insert("_not".to_string(), p.0);
    Predicate(serde_json::Value::Object(outer))
}

/// Nests a related resource's predicate under a relationship field, so a row can be
/// filtered by its relations, e.g. `resource.organisation_members(members.email.eq("x"))`.
pub fn relation(col: &str, p: Predicate) -> Predicate {
    let mut outer = serde_json::Map::new();
    outer.insert(col.to_string(), p.0);
    Predicate(serde_json::Value::Object(outer))
}

fn combine(op: &str, ps: &[Predicate]) -> Predicate {
    if ps.is_empty() {
        return Predicate::default();
    }
    let nodes: Vec<serde_json::Value> = ps.iter().map(|p| p.0.clone()).collect();
    let mut outer = serde_json::Map::new();
    outer.insert(op.to_string(), serde_json::Value::Array(nodes));
    Predicate(serde_json::Value::Object(outer))
}
