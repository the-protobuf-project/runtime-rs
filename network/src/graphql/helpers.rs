use serde::Serialize;
use serde_json::Value;

use crate::error::{Error, Result};

/// Converts a serializable value into a JSON object map. Useful for building inline GraphQL
/// arguments from structs.
pub fn struct_to_map<T: Serialize>(input: &T) -> Result<serde_json::Map<String, Value>> {
    match serde_json::to_value(input)? {
        Value::Object(map) => Ok(map),
        _ => Err(Error::InputNotAnObject),
    }
}

/// Formats a map of variables as an inline GraphQL arguments string (e.g. `id: "1", name:
/// "foo"`) for use in dynamically-built mutation/query strings. Iteration order follows the
/// map's own order (unordered for a `serde_json::Map` backed by the default feature set), which
/// matches Go's `BuildGraphQLArgs`: it iterates a Go map, whose order was never deterministic
/// either.
pub fn build_graphql_args(variables: &serde_json::Map<String, Value>) -> String {
    variables
        .iter()
        .map(|(key, value)| format!("{key}: {}", graphql_literal(value)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn graphql_literal(value: &Value) -> String {
    match value {
        Value::String(s) => format!("{s:?}"),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_args_from_object() {
        let mut map = serde_json::Map::new();
        map.insert("id".to_string(), Value::String("1".to_string()));
        let args = build_graphql_args(&map);
        assert_eq!(args, r#"id: "1""#);
    }

    #[test]
    fn rejects_non_object_input() {
        assert!(matches!(struct_to_map(&42), Err(Error::InputNotAnObject)));
    }
}
