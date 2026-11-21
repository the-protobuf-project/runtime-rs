//! What crosses the wire to D1: the statements sent, and the envelope, results and errors
//! that come back.

use serde::Deserialize;
use serde_json::{Map, Value as Json, json};

use super::BACKEND;
use crate::Error;

/// One statement and the text bound into it.
#[derive(Clone, Debug)]
pub(crate) struct Statement {
    pub(crate) sql: String,
    pub(crate) args: Vec<String>,
}

impl Statement {
    pub(crate) fn new(sql: impl Into<String>, args: Vec<String>) -> Self {
        Self {
            sql: sql.into(),
            args,
        }
    }

    pub(crate) fn body(&self) -> Json {
        match self.args.is_empty() {
            true => json!({ "sql": self.sql }),
            false => json!({ "sql": self.sql, "params": self.args }),
        }
    }
}

/// What one statement produced: its rows as JSON objects, and how many it changed.
#[derive(Debug, Default)]
pub(crate) struct Outcome {
    pub(crate) rows: Vec<Map<String, Json>>,
    pub(crate) changes: i64,
}

#[derive(Deserialize)]
pub(crate) struct Envelope<T> {
    pub(crate) result: Option<T>,
    #[serde(default)]
    pub(crate) errors: Vec<ApiError>,
}

#[derive(Deserialize)]
pub(crate) struct ApiError {
    #[serde(default)]
    pub(crate) code: i64,
    #[serde(default)]
    pub(crate) message: String,
}

#[derive(Deserialize)]
pub(crate) struct QueryResult {
    #[serde(default)]
    pub(crate) results: Vec<Map<String, Json>>,
    #[serde(default)]
    pub(crate) success: bool,
    #[serde(default)]
    pub(crate) meta: Meta,
}

#[derive(Deserialize, Default)]
pub(crate) struct Meta {
    #[serde(default)]
    pub(crate) changes: i64,
}

#[derive(Deserialize)]
pub(crate) struct Database {
    #[serde(default)]
    pub(crate) uuid: String,
    #[serde(default)]
    pub(crate) name: String,
}

/// The messages of an API error list, joined.
pub(crate) fn reasons(errors: &[ApiError]) -> String {
    errors
        .iter()
        .map(|e| format!("{} ({})", e.message, e.code))
        .collect::<Vec<_>>()
        .join("; ")
}

/// A refusal as the error a caller tests for. SQLite says UNIQUE for a duplicate primary key
/// and a duplicate unique column alike, and D1 passes its message through, so the text is the
/// only signal there is.
pub(crate) fn classify(why: String) -> Error {
    match why.contains("UNIQUE constraint failed") {
        true => Error::AlreadyExists,
        false => Error::Backend {
            backend: BACKEND,
            source: why.into(),
        },
    }
}
