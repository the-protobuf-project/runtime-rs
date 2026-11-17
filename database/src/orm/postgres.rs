//! PostgreSQL: double-quoted identifiers, a selected database is a schema, and strict
//! parameter types, so each column kind is read back as the widths Postgres may declare.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use sea_query::PostgresQueryBuilder;
use sea_query_sqlx::SqlxValues;
use sqlx::postgres::{PgRow, Postgres};

use super::Dialect;
use super::engine::{Engine, run_statements, strings};
use super::rows::{first_of, locate, undecodable};
use crate::Result;
use crate::records::{Column, Kind, Value};

impl Engine for Postgres {
    const DIALECT: Dialect = Dialect::Postgres;
    type Builder = PostgresQueryBuilder;

    run_statements!();

    fn decode(row: &PgRow, col: &Column) -> Result<Option<Value>> {
        let Some((i, null)) = locate(row, &col.name)? else {
            return Ok(None);
        };
        if null {
            return Ok(Some(Value::Null));
        }
        let v = match col.kind {
            Kind::Int | Kind::Enum => first_of!(row, i;
                i64 => Value::Int, i32 => |n| Value::Int(n.into()), i16 => |n| Value::Int(n.into())),
            Kind::Uint => first_of!(row, i;
                i64 => |n| Value::Uint(n as u64), i32 => |n: i32| Value::Uint(n as u64)),
            Kind::Float => {
                first_of!(row, i; f64 => Value::Float, f32 => |f| Value::Float(f.into()))
            }
            Kind::Bool => first_of!(row, i; bool => Value::Bool),
            Kind::Bytes => first_of!(row, i; Vec<u8> => Value::Bytes),
            Kind::Timestamp => first_of!(row, i;
                DateTime<Utc> => Value::Timestamp,
                NaiveDateTime => |t: NaiveDateTime| Value::Timestamp(t.and_utc()),
                NaiveDate => |d: NaiveDate| Value::Timestamp(d.and_time(Default::default()).and_utc())),
            Kind::String | Kind::Unknown => first_of!(row, i; String => Value::String),
        };
        v.map(Some).ok_or_else(|| undecodable(col))
    }

    fn count(row: &PgRow) -> Result<i64> {
        sqlx::Row::try_get::<i64, _>(row, 0).map_err(super::engine::map)
    }

    fn has_table(schema: &str, table: &str) -> (String, SqlxValues) {
        let base = "SELECT COUNT(*) FROM information_schema.tables \
                    WHERE table_type = 'BASE TABLE' AND table_name = $1 AND table_schema = ";
        if schema.is_empty() {
            (format!("{base}CURRENT_SCHEMA()"), strings([table]))
        } else {
            (format!("{base}$2"), strings([table, schema]))
        }
    }
}
