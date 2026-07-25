use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::{GraphQLClient, helpers};
use crate::error::{Error, Result};

impl GraphQLClient {
    /// Runs a GraphQL query. `query` is the full, caller-supplied query text; `variables` is
    /// sent alongside it. The response's `data` object is decoded into `T`.
    pub async fn query<T: DeserializeOwned>(
        &self,
        query: &str,
        variables: Option<&Value>,
    ) -> Result<T> {
        self.exec_typed(query, variables).await
    }

    /// Runs a GraphQL mutation. `mutation` is the full, caller-supplied mutation text;
    /// `variables` is sent alongside it. The response's `data` object is decoded into `T`.
    pub async fn mutation<T: DeserializeOwned>(
        &self,
        mutation: &str,
        variables: Option<&Value>,
    ) -> Result<T> {
        self.exec_typed(mutation, variables).await
    }

    async fn exec_typed<T: DeserializeOwned>(
        &self,
        document: &str,
        variables: Option<&Value>,
    ) -> Result<T> {
        let data = self
            .exec_raw(document, variables)
            .await
            .map_err(|e| Error::GraphQLOperation(Box::new(e)))?;
        serde_json::from_value(data).map_err(Error::GraphQLDecode)
    }

    /// Runs a mutation built from an input struct's fields, flattened inline as arguments (not
    /// as a `$input` variable) — e.g. `input: CreateUserInput{name: "John", ...}` becomes
    /// `createUser(name: "John", ...)`. `selection` is the mutation's return-field selection set
    /// (e.g. `"{ id name }"`).
    pub async fn mutation_with_input<In: Serialize, Out: DeserializeOwned>(
        &self,
        mutation_name: &str,
        input: &In,
        selection: &str,
    ) -> Result<Out> {
        let map = helpers::struct_to_map(input)?;
        let args = helpers::build_graphql_args(&map);
        let document = if args.is_empty() {
            format!("mutation {{ {mutation_name} {selection} }}")
        } else {
            format!("mutation {{ {mutation_name}({args}) {selection} }}")
        };
        self.mutation(&document, None).await
    }
}
