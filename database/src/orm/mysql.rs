//! MySQL: backticked identifiers, a selected database is a database, unsigned integers are
//! native, and booleans are `TINYINT(1)`.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use sea_query::MysqlQueryBuilder;
use sea_query_sqlx::SqlxValues;
use sqlx::mysql::{MySql, MySqlRow};

use super::Dialect;
use super::engine::{Engine, run_statements, strings};
use super::rows::{first_of, locate, undecodable};
use crate::Result;
use crate::records::{Column, Kind, Value};

impl Engine for MySql {
    const DIALECT: Dialect = Dialect::MySql;
    type Builder = MysqlQueryBuilder;

    run_statements!();

    fn decode(row: &MySqlRow, col: &Column) -> Result<Option<Value>> {
        let Some((i, null)) = locate(row, &col.name)? else {
            return Ok(None);
        };
        if null {
            return Ok(Some(Value::Null));
        }
        let v = match col.kind {
            Kind::Int | Kind::Enum => first_of!(row, i;
                i64 => Value::Int, i32 => |n| Value::Int(n.into()), u64 => |n| Value::Int(n as i64)),
            Kind::Uint => first_of!(row, i; u64 => Value::Uint, i64 => |n| Value::Uint(n as u64)),
            Kind::Float => {
                first_of!(row, i; f64 => Value::Float, f32 => |f| Value::Float(f.into()))
            }
            Kind::Bool => first_of!(row, i; bool => Value::Bool, i8 => |n| Value::Bool(n != 0)),
            Kind::Bytes => first_of!(row, i; Vec<u8> => Value::Bytes),
            Kind::Timestamp => first_of!(row, i;
                DateTime<Utc> => Value::Timestamp,
                NaiveDateTime => |t: NaiveDateTime| Value::Timestamp(t.and_utc()),
                NaiveDate => |d: NaiveDate| Value::Timestamp(d.and_time(Default::default()).and_utc())),
            Kind::String | Kind::Unknown => first_of!(row, i;
                String => Value::String,
                Vec<u8> => |b: Vec<u8>| Value::String(String::from_utf8_lossy(&b).into_owned())),
        };
        v.map(Some).ok_or_else(|| undecodable(col))
    }

    fn count(row: &MySqlRow) -> Result<i64> {
        sqlx::Row::try_get::<i64, _>(row, 0).map_err(super::engine::map)
    }

    fn has_table(schema: &str, table: &str) -> (String, SqlxValues) {
        let base = "SELECT COUNT(*) FROM information_schema.tables \
                    WHERE table_type = 'BASE TABLE' AND table_name = ? AND table_schema = ";
        if schema.is_empty() {
            (format!("{base}DATABASE()"), strings([table]))
        } else {
            (format!("{base}?"), strings([table, schema]))
        }
    }
}
