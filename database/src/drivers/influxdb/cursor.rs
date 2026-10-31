//! Scoped cursor encoding and lexicographic continuation predicates.
use super::sql::{Query, column, timestamp};
use crate::{Error, Range, Record, Resource, Result, Role, Value};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize)]
struct Cursor {
    version: u8,
    scope: String,
    position: Vec<Value>,
}

pub(super) fn scope(host: &str, database: &str, resource: &Resource, query: &Query) -> String {
    let input = format!(
        "{host:?}{database:?}{:?}{:?}{:?}",
        resource.columns(),
        query.sql,
        query.params
    );
    format!("{:x}", Sha256::digest(input.as_bytes()))
}

fn keys(resource: &Resource) -> Result<Vec<&str>> {
    let mut keys = vec![resource.time_column()?];
    keys.extend(
        resource
            .columns()
            .iter()
            .filter(|c| c.role == Role::Tag)
            .map(|c| c.name.as_str()),
    );
    Ok(keys)
}

pub(super) fn apply(
    query: &mut Query,
    resource: &Resource,
    range: &Range,
    scope: &str,
    token: &str,
) -> Result<()> {
    if token.len() > 32_768 {
        return Err(Error::Invalid("page token too large".into()));
    }
    let invalid = || Error::Invalid("invalid page token or token belongs to another query".into());
    let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?;
    let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    let keys = keys(resource)?;
    if cursor.version != 1 || cursor.scope != scope || cursor.position.len() != keys.len() {
        return Err(invalid());
    }
    let mut clauses = Vec::new();
    let mut equals = Vec::new();
    for (i, (key, value)) in keys.iter().zip(cursor.position).enumerate() {
        let value = if i == 0 {
            timestamp(
                value
                    .as_i64()
                    .filter(|v| *v >= range.start && *v < range.end)
                    .ok_or_else(invalid)?,
            )
        } else {
            if value.as_str().is_none() {
                return Err(invalid());
            }
            value
        };
        let parameter = format!("cursor{i}");
        query.params.insert(parameter.clone(), value);
        let name = column(resource, key)?;
        let op = if i == 0 && range.descending { "<" } else { ">" };
        let mut branch = equals.clone();
        branch.push(format!("{name} {op} ${parameter}"));
        clauses.push(format!("({})", branch.join(" AND ")));
        equals.push(format!("{name} = ${parameter}"));
    }
    let order = query
        .sql
        .find(" ORDER BY ")
        .expect("range SQL has ORDER BY");
    query
        .sql
        .insert_str(order, &format!(" AND ({})", clauses.join(" OR ")));
    Ok(())
}

pub(super) fn encode(resource: &Resource, row: &Record, scope: String) -> Result<String> {
    let position = keys(resource)?
        .into_iter()
        .map(|key| {
            row.get(key)
                .filter(|v| !v.is_null())
                .cloned()
                .ok_or_else(|| {
                    Error::Invalid(format!("paging needs every identity column, missing {key}"))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let body = serde_json::to_vec(&Cursor {
        version: 1,
        scope,
        position,
    })?;
    let token = URL_SAFE_NO_PAD.encode(body);
    if token.len() > 32_768 {
        return Err(Error::Invalid("page identity too large".into()));
    }
    Ok(token)
}
