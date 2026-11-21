//! A stand-in for D1's REST API over in-process SQLite, for `tests/d1.rs`: the list, read
//! and query endpoints, a batch in one transaction as D1 applies it, and a refusal carrying
//! SQLite's own message, as D1 passes it through.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use database::d1::{D1Client, D1Config, D1Provider};
use database::orm::sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use database::orm::sqlx::{self, Column as _, Row as _, TypeInfo as _, ValueRef as _};
use serde_json::{Map, Value as Json, json};

const ACCOUNT: &str = "acct";
pub const MAIN: &str = "11111111-1111-1111-1111-111111111111";

#[derive(Clone)]
struct Fake {
    /// name -> (id, database)
    dbs: Arc<HashMap<String, (String, SqlitePool)>>,
}

impl Fake {
    fn by_id(&self, id: &str) -> Option<&SqlitePool> {
        self.dbs.values().find(|(i, _)| i == id).map(|(_, p)| p)
    }
}

fn fail(status: StatusCode, code: i64, message: &str) -> Response {
    let body = json!({
        "success": false, "result": null, "messages": [],
        "errors": [{"code": code, "message": message}],
    });
    (status, axum::Json(body)).into_response()
}

fn ok(result: Json) -> Response {
    axum::Json(json!({"success": true, "errors": [], "messages": [], "result": result}))
        .into_response()
}

async fn list(State(f): State<Fake>, Query(q): Query<HashMap<String, String>>) -> Response {
    let name = q.get("name").cloned().unwrap_or_default();
    let found: Vec<Json> = f
        .dbs
        .iter()
        .filter(|(n, _)| n.contains(&name))
        .map(|(n, (id, _))| json!({"name": n, "uuid": id}))
        .collect();
    ok(Json::Array(found))
}

async fn one(State(f): State<Fake>, Path((_, id)): Path<(String, String)>) -> Response {
    match f.by_id(&id) {
        Some(_) => ok(json!({"uuid": id})),
        None => fail(StatusCode::NOT_FOUND, 7404, "database not found"),
    }
}

#[derive(serde::Deserialize, Clone)]
struct Stmt {
    sql: String,
    #[serde(default)]
    params: Vec<String>,
}

#[derive(serde::Deserialize)]
struct Body {
    #[serde(flatten)]
    single: Option<Stmt>,
    batch: Option<Vec<Stmt>>,
}

/// One statement or a batch, a batch in one transaction, as D1 applies it.
async fn query(
    State(f): State<Fake>,
    Path((_, id)): Path<(String, String)>,
    axum::Json(body): axum::Json<Body>,
) -> Response {
    let Some(pool) = f.by_id(&id) else {
        return fail(StatusCode::NOT_FOUND, 7404, "database not found");
    };
    let stmts = match (body.batch, body.single) {
        (Some(b), _) => b,
        (None, Some(s)) => vec![s],
        _ => return fail(StatusCode::BAD_REQUEST, 7400, "no statement"),
    };
    let mut tx = pool.begin().await.unwrap();
    let mut results = Vec::new();
    for s in stmts {
        match run(&mut tx, &s).await {
            Ok(r) => results.push(r),
            Err(e) => return fail(StatusCode::BAD_REQUEST, 7500, &e.to_string()),
        }
    }
    tx.commit().await.unwrap();
    ok(Json::Array(results))
}

async fn run(tx: &mut sqlx::SqliteConnection, s: &Stmt) -> sqlx::Result<Json> {
    // The statement is the driver under test's, which is the thing being checked.
    let mut q = sqlx::query(sqlx::AssertSqlSafe(s.sql.clone()));
    for p in &s.params {
        q = q.bind(p.clone());
    }
    if !s.sql.trim_start().to_uppercase().starts_with("SELECT") {
        let done = q.execute(&mut *tx).await?;
        return Ok(
            json!({"success": true, "results": [], "meta": {"changes": done.rows_affected()}}),
        );
    }
    let mut rows = Vec::new();
    for r in q.fetch_all(&mut *tx).await? {
        let mut obj = Map::new();
        for (i, c) in r.columns().iter().enumerate() {
            let raw = r.try_get_raw(i)?;
            // Null first: a null in a TEXT column reports the column's declared type.
            let v = match raw.type_info().name() {
                _ if raw.is_null() => Json::Null,
                "INTEGER" => json!(r.try_get::<i64, _>(i)?),
                "REAL" => json!(r.try_get::<f64, _>(i)?),
                _ => json!(r.try_get::<String, _>(i)?),
            };
            obj.insert(c.name().to_owned(), v);
        }
        rows.push(Json::Object(obj));
    }
    Ok(json!({"success": true, "results": rows, "meta": {"changes": 0}}))
}

/// A stand-in serving the named databases, and a provider over it.
pub async fn serve(names: &[(&str, &str)]) -> (D1Provider, Arc<D1Client>) {
    let mut dbs = HashMap::new();
    for (name, id) in names {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        dbs.insert((*name).to_owned(), ((*id).to_owned(), pool));
    }
    let fake = Fake { dbs: Arc::new(dbs) };
    let api = Router::new()
        .route("/client/v4/accounts/{acct}/d1/database", get(list))
        .route("/client/v4/accounts/{acct}/d1/database/{id}", get(one))
        .route(
            "/client/v4/accounts/{acct}/d1/database/{id}/query",
            post(query),
        )
        .with_state(fake);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, api).await.unwrap() });

    let client = Arc::new(
        D1Client::connect(D1Config {
            account_id: ACCOUNT.into(),
            api_token: "test".into(),
            database_id: MAIN.into(),
            base_url: format!("http://{addr}/client/v4"),
            ..D1Config::default()
        })
        .await
        .unwrap(),
    );
    (D1Provider::new(client.clone()), client)
}
