//! Rows to BSON documents and back. Go's `mongodb/codec.go`.

use bson::{Binary, Bson, DateTime, Decimal128, Document, spec::BinarySubtype};
use chrono::{TimeZone, Utc};
use prost_reflect::DynamicMessage;

use super::error::invalid;
use super::filter::coerce;
use crate::Result;
use crate::records::{Kind, Resource, Row, Value, columns_to_message};

/// The stored document for a row: the primary key moves to `_id`, every other column keeps
/// its name, so the document reads naturally in any MongoDB tool. Go's `toDocument`.
pub(super) fn to_document(res: &Resource, cols: &Row) -> Result<Document> {
    let mut doc = Document::new();
    if let Some(pk) = cols.get(&res.pk_column) {
        let id = to_bson(kind_of(res, &res.pk_column), pk)?;
        if id == Bson::Null {
            return Err(invalid(format!(
                "resource {:?} has no primary key value",
                res.name
            )));
        }
        doc.insert("_id", id);
    }
    for (name, value) in cols.iter().filter(|(n, _)| **n != res.pk_column) {
        doc.insert(name.clone(), to_bson(kind_of(res, name), value)?);
    }
    if !doc.contains_key("_id") {
        return Err(invalid(format!(
            "resource {:?} has no primary key value",
            res.name
        )));
    }
    Ok(doc)
}

/// The message a stored document holds: `_id` back to the primary-key column. Go's
/// `fromDocument`.
pub(super) fn from_document(res: &Resource, doc: Document) -> Result<DynamicMessage> {
    let row: Row = doc
        .into_iter()
        .map(|(name, value)| {
            let name = if name == "_id" {
                res.pk_column.clone()
            } else {
                name
            };
            (name, from_bson(value))
        })
        .collect();
    columns_to_message(res, &row)
}

/// The `_id` a key string selects, typed as the primary key is stored, so an integer key
/// matches an integer `_id`. A key the kind cannot read stays a string, and matches nothing.
pub(super) fn key_bson(res: &Resource, key: &str) -> Bson {
    coerce(kind_of(res, &res.pk_column), key).unwrap_or_else(|_| Bson::String(key.to_owned()))
}

/// A stored `_id` as the key string the contract carries. Go's `fmt.Sprint(id)`.
pub(super) fn key_string(id: &Bson) -> String {
    match id {
        Bson::String(s) => s.clone(),
        other => from_bson(other.clone()).key_string(),
    }
}

fn kind_of(res: &Resource, column: &str) -> Kind {
    res.lookup_column(column)
        .map(|c| c.kind)
        .unwrap_or_default()
}

/// A bridge value narrowed to what BSON carries. Go's `toBSON` and the Go driver's own
/// encoding of what that leaves alone.
pub(super) fn to_bson(kind: Kind, value: &Value) -> Result<Bson> {
    Ok(match value {
        Value::Null => Bson::Null,
        Value::String(s) => Bson::String(s.clone()),
        // Go's bridge reads an enum as an int32, which its driver stores as one.
        Value::Int(n) if kind == Kind::Enum => match i32::try_from(*n) {
            Ok(n) => Bson::Int32(n),
            Err(_) => Bson::Int64(*n),
        },
        Value::Int(n) => Bson::Int64(*n),
        // BSON has no unsigned 64-bit integer: past i64::MAX the driver would store a
        // negative int64 that reads back as a different number. Decimal128 holds it exactly
        // and still sorts and compares as a number on the server.
        Value::Uint(u) => match i64::try_from(*u) {
            Ok(n) => Bson::Int64(n),
            Err(_) => Bson::Decimal128(
                u.to_string()
                    .parse::<Decimal128>()
                    .map_err(|e| invalid(format!("cannot store {u}: {e}")))?,
            ),
        },
        Value::Float(f) => Bson::Double(*f),
        Value::Bool(b) => Bson::Boolean(*b),
        // Go's driver stores a nil byte slice, which is what an empty bytes field reads as,
        // as null; it reads back as empty all the same.
        Value::Bytes(b) if b.is_empty() => Bson::Null,
        Value::Bytes(b) => Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: b.clone(),
        }),
        // BSON dates are milliseconds; UTC, so a round trip does not depend on a zone.
        Value::Timestamp(t) => Bson::DateTime(DateTime::from_millis(t.timestamp_millis())),
    })
}

/// A stored value widened back to what the bridge reads. Go's `fromBSON`.
pub(super) fn from_bson(value: Bson) -> Value {
    match value {
        Bson::Null | Bson::Undefined => Value::Null,
        Bson::String(s) => Value::String(s),
        Bson::Int32(n) => Value::Int(i64::from(n)),
        Bson::Int64(n) => Value::Int(n),
        Bson::Double(f) => Value::Float(f),
        Bson::Boolean(b) => Value::Bool(b),
        Bson::Binary(b) => Value::Bytes(b.bytes),
        Bson::DateTime(t) => Value::Timestamp(
            Utc.timestamp_millis_opt(t.timestamp_millis())
                .single()
                .unwrap_or_default(),
        ),
        // Only a uint64 past i64::MAX is stored this way; see `to_bson`.
        Bson::Decimal128(d) => match d.to_string().parse::<u64>() {
            Ok(u) => Value::Uint(u),
            Err(_) => Value::String(d.to_string()),
        },
        Bson::ObjectId(id) => Value::String(id.to_hex()),
        other => Value::String(other.to_string()),
    }
}
