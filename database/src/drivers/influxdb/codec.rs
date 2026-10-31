//! Descriptor-directed SDK point encoding and Arrow row decoding.

use crate::{Error, Kind, Record, Resource, Result, Role, Value};
use influxdb3_client::{FieldValue, Point, Row, Value as SdkValue};

pub(super) fn point(resource: &Resource, record: &Record) -> Result<Point> {
    resource.validate(record)?;
    let mut point = Point::new(resource.table());
    for col in resource.columns() {
        let value = &record[&col.name];
        point = match col.role {
            Role::Key => return Err(Error::Invalid("InfluxDB cannot enforce record keys".into())),
            Role::Timestamp => point.timestamp_nanos(value.as_i64().expect("validated time")),
            Role::Tag => point.tag(&col.name, value.as_str().expect("validated tag")),
            Role::Field => {
                let field = match col.kind {
                    Kind::String => FieldValue::String(value.as_str().unwrap().to_owned()),
                    Kind::Int => FieldValue::Integer(value.as_i64().unwrap()),
                    Kind::Uint => FieldValue::UInteger(value.as_u64().unwrap()),
                    Kind::Float => FieldValue::Float(value.as_f64().unwrap()),
                    Kind::Bool => FieldValue::Boolean(value.as_bool().unwrap()),
                    Kind::Timestamp => unreachable!("Resource rejects timestamp fields"),
                };
                point.field(&col.name, field)
            }
        };
    }
    Ok(point)
}

pub(super) fn row(row: Row) -> Result<Record> {
    row.into_map()
        .into_iter()
        .map(|(name, value)| {
            let value = match value {
                SdkValue::Bool(v) => Value::from(v),
                SdkValue::I8(v) => Value::from(v),
                SdkValue::I16(v) => Value::from(v),
                SdkValue::I32(v) => Value::from(v),
                SdkValue::I64(v) | SdkValue::Timestamp(v) => Value::from(v),
                SdkValue::U8(v) => Value::from(v),
                SdkValue::U16(v) => Value::from(v),
                SdkValue::U32(v) => Value::from(v),
                SdkValue::U64(v) => Value::from(v),
                SdkValue::F32(v) => finite(f64::from(v))?,
                SdkValue::F64(v) => finite(v)?,
                SdkValue::String(v) => Value::from(v),
                SdkValue::Null => Value::Null,
                SdkValue::Binary(_) => {
                    return Err(Error::Invalid("unexpected binary query value".into()));
                }
            };
            Ok((name, value))
        })
        .collect()
}

fn finite(value: f64) -> Result<Value> {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| Error::Invalid("server returned a non-finite float".into()))
}
