//! Column ordering and the update-patch trait implemented by generated code.
//!
//! Go's `columns.go` also defines a generic `IsOmitted(v any) bool` built on
//! `reflect.Value.IsZero()`. Rust has no equivalent without per-type impls, so it is not
//! ported; callers check omission the semantically appropriate way inline
//! (`Predicate::is_omitted()`, `Vec::is_empty()`, `== 0`, ...).

use serde::{Serialize, Serializer};

/// The sort direction for an order_by term. The standard ascending/descending enum
/// shared by every GraphQL CRUD resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum OrderBy {
    /// Ascending order.
    Asc,
    /// Descending order.
    Desc,
}

/// One sort key (column + direction) produced by a field handle's `asc`/`desc`. A list
/// request's order_by takes a list of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderTerm {
    pub(crate) col: String,
    pub(crate) dir: OrderBy,
}

impl Serialize for OrderTerm {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serde_json::Map::new();
        map.insert(
            self.col.clone(),
            serde_json::to_value(self.dir).unwrap_or(serde_json::Value::Null),
        );
        map.serialize(serializer)
    }
}

/// Implemented by hand-written generated update-patch structs. Turns the patch into a
/// Hasura-style update-columns map: each instructed field becomes
/// `{jsonName: {"set": value}}`.
///
/// ```
/// use network_graphql::{ColumnPatch, Nullable};
///
/// struct UpdateOrganisationInput {
///     display_name: Nullable<String>,
///     description: Nullable<String>,
/// }
///
/// impl ColumnPatch for UpdateOrganisationInput {
///     fn set_columns(&self) -> serde_json::Map<String, serde_json::Value> {
///         let mut out = serde_json::Map::new();
///         if let Some(v) = self.display_name.to_set_entry() {
///             out.insert("displayName".into(), v);
///         }
///         if let Some(v) = self.description.to_set_entry() {
///             out.insert("description".into(), v);
///         }
///         out
///     }
/// }
/// ```
pub trait ColumnPatch {
    /// Returns the Hasura-style `{jsonName: {"set": value}}` map for every instructed
    /// field.
    fn set_columns(&self) -> serde_json::Map<String, serde_json::Value>;
}
