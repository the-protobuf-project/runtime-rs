use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::GraphQLClient;
use crate::error::{Error, Result};

/// One named, typed GraphQL operation argument. Go's `QueryFields`/`MutateFields` infer each
/// variable's declared GraphQL type via reflection over the argument's Go type; Rust has no
/// runtime reflection, so the type is supplied explicitly here instead.
#[derive(Debug, Clone)]
pub struct FieldArg {
    /// The GraphQL variable/argument name (without the leading `$`).
    pub name: String,
    /// The argument's value, already converted to JSON.
    pub value: Value,
    /// The argument's declared GraphQL type (e.g. `"ID!"`, `"[String!]"`), used to render the
    /// operation's variable declarations.
    pub gql_type: String,
}

impl FieldArg {
    /// Builds a `FieldArg`, converting `value` to JSON via [`serde::Serialize`].
    pub fn new(
        name: impl Into<String>,
        value: impl Serialize,
        gql_type: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            value: serde_json::to_value(value).unwrap_or(Value::Null),
            gql_type: gql_type.into(),
        }
    }
}

impl GraphQLClient {
    /// Runs a query selecting `selection` under `field` with the given arguments, declaring only
    /// the arguments present (so optional filters are omitted rather than sent as explicit
    /// nulls). The response's `data` object is decoded into `T`.
    pub async fn query_fields<T: DeserializeOwned>(
        &self,
        field: &str,
        args: &[FieldArg],
        selection: &str,
    ) -> Result<T> {
        self.exec_fields(false, field, args, selection).await
    }

    /// Runs a mutation selecting `selection` under `field` with the given arguments. See
    /// [`GraphQLClient::query_fields`].
    pub async fn mutate_fields<T: DeserializeOwned>(
        &self,
        field: &str,
        args: &[FieldArg],
        selection: &str,
    ) -> Result<T> {
        self.exec_fields(true, field, args, selection).await
    }

    async fn exec_fields<T: DeserializeOwned>(
        &self,
        mutation: bool,
        field: &str,
        args: &[FieldArg],
        selection: &str,
    ) -> Result<T> {
        let sorted = sorted_args(args);
        let op_kw = if mutation { "mutation" } else { "query" };
        let head = build_field_tag(field, &sorted);
        let var_decls = build_variable_declarations(&sorted);
        let document = format!("{op_kw}{var_decls} {{ {head} {selection} }}");

        let mut variables = serde_json::Map::new();
        for a in &sorted {
            variables.insert(a.name.clone(), a.value.clone());
        }
        let variables = if variables.is_empty() {
            None
        } else {
            Some(Value::Object(variables))
        };

        let data = self
            .exec_raw(&document, variables.as_ref())
            .await
            .map_err(|e| Error::GraphQLOperation(Box::new(e)))?;
        // `data` is the whole operation's result, keyed by field (`{"<field>": ...}`); the
        // caller's `T` describes only the selected field's value, not the wrapper — mirrors Go's
        // execFields, which assigns just the wrapper struct's single decoded field to `result`.
        let inner = match data {
            Value::Object(mut map) => map.remove(field).unwrap_or(Value::Null),
            other => other,
        };
        serde_json::from_value(inner).map_err(Error::GraphQLDecode)
    }
}

pub(crate) fn sorted_args(args: &[FieldArg]) -> Vec<&FieldArg> {
    let mut sorted: Vec<&FieldArg> = args.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    sorted
}

/// Renders `field(name: $name, ...)` for the arguments present (sorted for determinism), or just
/// `field` when there are none.
pub(crate) fn build_field_tag(field: &str, sorted_args: &[&FieldArg]) -> String {
    if sorted_args.is_empty() {
        return field.to_string();
    }
    let parts: Vec<String> = sorted_args
        .iter()
        .map(|a| format!("{}: ${}", a.name, a.name))
        .collect();
    format!("{field}({})", parts.join(", "))
}

/// Renders the outer operation's `($name: Type, ...)` variable declarations from the same
/// arguments, or an empty string when there are none.
pub(crate) fn build_variable_declarations(sorted_args: &[&FieldArg]) -> String {
    if sorted_args.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = sorted_args
        .iter()
        .map(|a| format!("${}: {}", a.name, a.gql_type))
        .collect();
    format!("({})", parts.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_tag_no_args() {
        assert_eq!(build_field_tag("users", &[]), "users");
    }

    #[test]
    fn field_tag_sorted_args() {
        let args = vec![
            FieldArg::new("id", "1", "ID!"),
            FieldArg::new("active", true, "Boolean"),
        ];
        let sorted = sorted_args(&args);
        assert_eq!(
            build_field_tag("user", &sorted),
            "user(active: $active, id: $id)"
        );
        assert_eq!(
            build_variable_declarations(&sorted),
            "($active: Boolean, $id: ID!)"
        );
    }
}
