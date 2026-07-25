use serde_json::Value;

use super::GraphQLClient;
use crate::error::{Error, Result};

impl GraphQLClient {
    /// Sends a raw GraphQL query (or mutation) string with optional variables and returns the
    /// `data` object as a map.
    pub async fn execute_raw_query(
        &self,
        query: &str,
        variables: Option<&Value>,
    ) -> Result<serde_json::Map<String, Value>> {
        let data = self
            .exec_raw(query, variables)
            .await
            .map_err(|e| Error::GraphQLRawQuery(Box::new(e)))?;
        Ok(as_object(data))
    }

    /// Sends a raw GraphQL mutation string with optional variables and returns the `data` object
    /// as a map.
    pub async fn exec_raw_mutation(
        &self,
        mutation: &str,
        variables: Option<&Value>,
    ) -> Result<serde_json::Map<String, Value>> {
        let data = self
            .exec_raw(mutation, variables)
            .await
            .map_err(|e| Error::GraphQLRawMutation(Box::new(e)))?;
        Ok(as_object(data))
    }
}

fn as_object(v: Value) -> serde_json::Map<String, Value> {
    match v {
        Value::Object(m) => m,
        _ => serde_json::Map::new(),
    }
}
