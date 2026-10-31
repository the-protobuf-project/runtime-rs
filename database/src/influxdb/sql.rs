//! SQL is assembled only from validated descriptors and fixed operators.

use crate::store::validate_value;
use crate::store::window::{Comparison, Range};
use crate::store::{Kind, Resource, Result, Role, Value};
use chrono::{DateTime, SecondsFormat};
use std::collections::BTreeMap;

pub(super) struct Query {
    pub sql: String,
    pub params: BTreeMap<String, Value>,
}

pub(super) fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(super) fn column(resource: &Resource, name: &str) -> Result<String> {
    Ok(quote(if resource.column(name)?.role == Role::Timestamp {
        "time"
    } else {
        name
    }))
}

pub(super) fn timestamp(value: i64) -> Value {
    Value::from(DateTime::from_timestamp_nanos(value).to_rfc3339_opts(SecondsFormat::Nanos, true))
}

pub(super) fn bounds(resource: &Resource, range: &Range) -> Result<Query> {
    range.validate()?;
    let mut params = BTreeMap::from([
        ("start".into(), timestamp(range.start)),
        ("end".into(), timestamp(range.end)),
    ]);
    let mut parts = vec![
        "\"time\" >= $start".to_owned(),
        "\"time\" < $end".to_owned(),
    ];
    for (index, predicate) in range.predicates.iter().enumerate() {
        let col = resource.column(&predicate.column)?;
        validate_value(col.kind, &predicate.value)?;
        let op = match predicate.comparison {
            Comparison::Eq => "=",
            Comparison::Ne => "!=",
            Comparison::Lt => "<",
            Comparison::Le => "<=",
            Comparison::Gt => ">",
            Comparison::Ge => ">=",
        };
        let key = format!("p{index}");
        let value = if col.kind == Kind::Timestamp {
            timestamp(predicate.value.as_i64().expect("validated time predicate"))
        } else {
            predicate.value.clone()
        };
        params.insert(key.clone(), value);
        parts.push(format!("{} {op} ${key}", column(resource, &col.name)?));
    }
    Ok(Query {
        sql: parts.join(" AND "),
        params,
    })
}

pub(super) fn range(resource: &Resource, range: &Range) -> Result<Query> {
    let mut query = bounds(resource, range)?;
    let select = resource
        .columns()
        .iter()
        .map(|col| {
            if col.role == Role::Timestamp {
                format!("\"time\" AS {}", quote(&col.name))
            } else {
                quote(&col.name)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let direction = if range.descending { "DESC" } else { "ASC" };
    let mut order = vec![format!("\"time\" {direction}")];
    for tag in resource.columns().iter().filter(|c| c.role == Role::Tag) {
        order.push(format!("{} ASC", quote(&tag.name)));
    }
    query.sql = format!(
        "SELECT {select} FROM {} WHERE {} ORDER BY {} LIMIT {}",
        quote(resource.table()),
        query.sql,
        order.join(", "),
        range.limit
    );
    Ok(query)
}

/// Matching points in the window, as one row with column `n`.
pub(super) fn count(resource: &Resource, range: &Range) -> Result<Query> {
    let mut query = bounds(resource, range)?;
    query.sql = format!(
        "SELECT count(*) AS n FROM {} WHERE {}",
        quote(resource.table()),
        query.sql
    );
    Ok(query)
}
