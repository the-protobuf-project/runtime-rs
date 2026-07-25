use serde_json::Value;

use super::named::{sorted_args, FieldArg};
use super::GraphQLClient;
use crate::error::{Error, Result};

/// One mutation in a transactional batch: a root mutation field, its arguments, and the
/// selection set for its result. See [`GraphQLClient::batch_mutate`].
#[derive(Debug, Clone)]
pub struct BatchOp {
    /// The root mutation field name (e.g. `"insertUser"`).
    pub field: String,
    /// The field's arguments; only the ones present are declared.
    pub args: Vec<FieldArg>,
    /// The GraphQL selection set for this op's result (e.g. `"{ id }"`).
    pub selection: String,
}

impl GraphQLClient {
    /// Runs every op as one GraphQL mutation document, executed by the engine in a single
    /// transaction (all commit or all roll back). Each op's arguments are namespaced (`m0_`,
    /// `m1_`, ...) so fields sharing an argument name do not collide, and each op's selection is
    /// aliased (`m0`, `m1`, ...) so the results can be read back in order. Returns one decoded
    /// JSON value per op, in input order; on error nothing is returned.
    pub async fn batch_mutate(&self, ops: &[BatchOp]) -> Result<Vec<Value>> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }

        let mut variables = serde_json::Map::new();
        let mut var_decl_parts = Vec::new();
        let mut heads = Vec::with_capacity(ops.len());

        for (i, op) in ops.iter().enumerate() {
            let alias = format!("m{i}");
            let sorted = sorted_args(&op.args);
            heads.push(format!("{} {}", build_batch_tag(&alias, &op.field, &sorted), op.selection));
            for a in &sorted {
                let var_name = format!("{alias}_{}", a.name);
                variables.insert(var_name.clone(), a.value.clone());
                var_decl_parts.push(format!("${var_name}: {}", a.gql_type));
            }
        }

        let var_decls = if var_decl_parts.is_empty() {
            String::new()
        } else {
            format!("({})", var_decl_parts.join(", "))
        };
        let document = format!("mutation{var_decls} {{ {} }}", heads.join(" "));
        let variables = if variables.is_empty() { None } else { Some(Value::Object(variables)) };

        let data = self
            .exec_raw(&document, variables.as_ref())
            .await
            .map_err(|e| Error::GraphQLBatch(Box::new(e)))?;

        Ok((0..ops.len())
            .map(|i| data.get(format!("m{i}")).cloned().unwrap_or(Value::Null))
            .collect())
    }
}

/// Renders one batched field as `alias: field(arg: $alias_arg, ...)` (arguments sorted for
/// determinism, variables namespaced by alias), or `alias: field` when it has none.
pub(crate) fn build_batch_tag(alias: &str, field: &str, sorted_args: &[&FieldArg]) -> String {
    if sorted_args.is_empty() {
        return format!("{alias}: {field}");
    }
    let parts: Vec<String> = sorted_args
        .iter()
        .map(|a| format!("{}: ${alias}_{}", a.name, a.name))
        .collect();
    format!("{alias}: {field}({})", parts.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_tag_no_args() {
        assert_eq!(build_batch_tag("m0", "insertThing", &[]), "m0: insertThing");
    }

    #[test]
    fn batch_tag_sorted_namespaced_args() {
        let args = vec![
            FieldArg::new("postCheck", Value::Null, "ThingBoolExp"),
            FieldArg::new("objects", Value::Null, "[ThingInsertInput!]!"),
        ];
        let sorted = sorted_args(&args);
        assert_eq!(
            build_batch_tag("m1", "insertThing", &sorted),
            "m1: insertThing(objects: $m1_objects, postCheck: $m1_postCheck)"
        );
    }
}
