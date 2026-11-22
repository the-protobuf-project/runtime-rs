//! The client's SQL, with no proto in it: run statements, read a database, and find one by
//! name. The records driver is built on these three calls.

use std::collections::HashMap;

use network::HttpMethod;
use serde_json::{Value as Json, json};

use super::BACKEND;
use super::client::D1Client;
use super::wire::{Database, Outcome, QueryResult, Statement};
use crate::{Error, Result};

impl D1Client {
    /// Runs statements against database `id`, one request for all of them; D1 applies a
    /// batch as a single unit.
    pub async fn query(&self, id: &str, stmts: &[Statement]) -> Result<Vec<Outcome>> {
        let body = match stmts {
            [one] => one.body(),
            many => json!({ "batch": many.iter().map(Statement::body).collect::<Vec<_>>() }),
        };
        let url = self.at(&format!("/{id}/query"), HashMap::new());
        let results: Vec<QueryResult> = self
            .call(HttpMethod::Post, url, serde_json::to_vec(&body)?)
            .await?;
        if results.len() != stmts.len() {
            return Err(Error::Backend {
                backend: BACKEND,
                source: format!(
                    "sent {} statements and got {} results back",
                    stmts.len(),
                    results.len()
                )
                .into(),
            });
        }
        results
            .into_iter()
            .enumerate()
            .map(|(i, r)| match r.success {
                true => Ok(Outcome {
                    rows: r.results,
                    changes: r.meta.changes,
                }),
                false => Err(Error::Backend {
                    backend: BACKEND,
                    source: format!("statement {} did not succeed", i + 1).into(),
                }),
            })
            .collect()
    }

    /// Reads database `id`, proving it is there and the token reaches it.
    pub async fn ping(&self, id: &str) -> Result<()> {
        let url = self.at(&format!("/{id}"), HashMap::new());
        self.call::<Json>(HttpMethod::Get, url, Vec::new())
            .await
            .map(|_| ())
            .map_err(|e| Error::Backend {
                backend: BACKEND,
                source: format!("database {id} is not reachable: {e}").into(),
            })
    }

    /// The id of the database named `name`. The list endpoint filters by name loosely, so
    /// the result is checked for an exact match: `tenant_a` must not select `tenant_ab`.
    pub async fn lookup(&self, name: &str) -> Result<String> {
        let mut page = 1;
        loop {
            let params = HashMap::from([
                ("name".to_owned(), name.to_owned()),
                ("page".to_owned(), page.to_string()),
                ("per_page".to_owned(), "100".to_owned()),
            ]);
            let found: Vec<Database> = self
                .call(HttpMethod::Get, self.at("", params), Vec::new())
                .await?;
            if let Some(db) = found.iter().find(|db| db.name == name) {
                return Ok(db.uuid.clone());
            }
            if found.len() < 100 {
                return Err(Error::Invalid(format!(
                    "d1: no database named {name:?} in the account; create it with `wrangler d1 create {name}`"
                )));
            }
            page += 1;
        }
    }
}
