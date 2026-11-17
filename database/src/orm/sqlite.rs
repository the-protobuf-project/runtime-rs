//! SQLite: in-process, double-quoted identifiers, a selected database is an attached one,
//! and storage classes rather than column types, so a value reads back as its kind asks.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use sea_query::SqliteQueryBuilder;
use sea_query_sqlx::SqlxValues;
use sqlx::sqlite::{Sqlite, SqliteRow};

use super::Dialect;
use super::engine::{Engine, run_statements, strings};
use super::rows::{first_of, locate, undecodable};
use crate::Result;
use crate::records::{Column, Kind, Value};

impl Engine for Sqlite {
    const DIALECT: Dialect = Dialect::Sqlite;
    type Builder = SqliteQueryBuilder;

    run_statements!();

    fn decode(row: &SqliteRow, col: &Column) -> Result<Option<Value>> {
        let Some((i, null)) = locate(row, &col.name)? else {
            return Ok(None);
        };
        if null {
            return Ok(Some(Value::Null));
        }
        let v = match col.kind {
            Kind::Int | Kind::Enum => first_of!(row, i; i64 => Value::Int),
            Kind::Uint => first_of!(row, i; i64 => |n| Value::Uint(n as u64)),
            Kind::Float => first_of!(row, i; f64 => Value::Float),
            Kind::Bool => first_of!(row, i; bool => Value::Bool),
            Kind::Bytes => first_of!(row, i; Vec<u8> => Value::Bytes),
            Kind::Timestamp => first_of!(row, i;
                DateTime<Utc> => Value::Timestamp,
                NaiveDateTime => |t: NaiveDateTime| Value::Timestamp(t.and_utc()),
                NaiveDate => |d: NaiveDate| Value::Timestamp(d.and_time(Default::default()).and_utc()),
                String => Value::String),
            Kind::String | Kind::Unknown => first_of!(row, i; String => Value::String),
        };
        v.map(Some).ok_or_else(|| undecodable(col))
    }

    fn count(row: &SqliteRow) -> Result<i64> {
        sqlx::Row::try_get::<i64, _>(row, 0).map_err(super::engine::map)
    }

    fn has_table(schema: &str, table: &str) -> (String, SqlxValues) {
        let master = if schema.is_empty() {
            "sqlite_master".to_owned()
        } else {
            format!("{}.sqlite_master", Dialect::Sqlite.quote(schema))
        };
        let sql = format!("SELECT COUNT(*) FROM {master} WHERE type = 'table' AND name = ?");
        (sql, strings([table]))
    }
}
