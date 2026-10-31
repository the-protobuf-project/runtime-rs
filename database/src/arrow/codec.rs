//! Validated records to columnar batches and back.

use super::errors::{backend, invalid};
use super::schema::to_schema;
use crate::store::{Kind, Record, Resource, Result, Value};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt64Array,
};
use std::sync::Arc;

/// Builds one batch. Callers validate records first, so every value is present
/// and of its declared kind; a mismatch here is reported, never coerced.
pub(super) fn encode(resource: &Resource, records: &[Record]) -> Result<RecordBatch> {
    let columns = resource
        .columns()
        .iter()
        .map(|col| {
            let values = records.iter().map(|r| &r[&col.name]);
            let array: ArrayRef = match col.kind {
                Kind::String => Arc::new(StringArray::from_iter_values(
                    values.map(|v| v.as_str().unwrap_or_default()),
                )),
                Kind::Int => Arc::new(Int64Array::from_iter_values(
                    values.map(|v| v.as_i64().unwrap_or_default()),
                )),
                Kind::Enum => Arc::new(Int32Array::from_iter_values(values.map(|v| {
                    v.as_i64()
                        .and_then(|n| i32::try_from(n).ok())
                        .unwrap_or_default()
                }))),
                Kind::Uint => Arc::new(UInt64Array::from_iter_values(
                    values.map(|v| v.as_u64().unwrap_or_default()),
                )),
                Kind::Float => Arc::new(Float64Array::from_iter_values(
                    values.map(|v| v.as_f64().unwrap_or_default()),
                )),
                Kind::Bool => Arc::new(BooleanArray::from(
                    values
                        .map(|v| v.as_bool().unwrap_or_default())
                        .collect::<Vec<_>>(),
                )),
                Kind::Timestamp => Arc::new(
                    TimestampNanosecondArray::from_iter_values(
                        values.map(|v| v.as_i64().unwrap_or_default()),
                    )
                    .with_timezone("UTC"),
                ),
            };
            array
        })
        .collect();
    RecordBatch::try_new(Arc::new(to_schema(resource)), columns).map_err(backend)
}

/// Decodes a batch written for `resource`, by column name.
pub(super) fn decode(resource: &Resource, batch: &RecordBatch) -> Result<Vec<Record>> {
    let rows: Vec<usize> = (0..batch.num_rows()).collect();
    decode_rows(resource, batch, &rows)
}

/// Decodes only the listed row indices, in the order given.
pub(super) fn decode_rows(
    resource: &Resource,
    batch: &RecordBatch,
    rows: &[usize],
) -> Result<Vec<Record>> {
    let mut records = vec![Record::new(); rows.len()];
    for col in resource.columns() {
        let array = batch
            .column_by_name(&col.name)
            .ok_or_else(|| invalid(format!("arrow batch has no column {}", col.name)))?;
        let values = column_values(col.kind, array.as_ref(), rows)
            .ok_or_else(|| invalid(format!("arrow column {} has the wrong type", col.name)))?;
        for (record, value) in records.iter_mut().zip(values) {
            record.insert(col.name.clone(), value);
        }
    }
    Ok(records)
}

/// Reads one column at the given rows. Columns are written non-null; a null
/// from a foreign file decodes as JSON null rather than a default value.
fn column_values(kind: Kind, array: &dyn Array, rows: &[usize]) -> Option<Vec<Value>> {
    fn pick<A: Array>(array: &A, rows: &[usize], get: impl Fn(&A, usize) -> Value) -> Vec<Value> {
        rows.iter()
            .map(|&i| match array.is_null(i) {
                true => Value::Null,
                false => get(array, i),
            })
            .collect()
    }
    let any = array.as_any();
    Some(match kind {
        Kind::String => pick(any.downcast_ref::<StringArray>()?, rows, |a, i| {
            a.value(i).into()
        }),
        Kind::Int => pick(any.downcast_ref::<Int64Array>()?, rows, |a, i| {
            a.value(i).into()
        }),
        Kind::Enum => pick(any.downcast_ref::<Int32Array>()?, rows, |a, i| {
            a.value(i).into()
        }),
        Kind::Uint => pick(any.downcast_ref::<UInt64Array>()?, rows, |a, i| {
            a.value(i).into()
        }),
        Kind::Float => pick(any.downcast_ref::<Float64Array>()?, rows, |a, i| {
            a.value(i).into()
        }),
        Kind::Bool => pick(any.downcast_ref::<BooleanArray>()?, rows, |a, i| {
            a.value(i).into()
        }),
        Kind::Timestamp => pick(
            any.downcast_ref::<TimestampNanosecondArray>()?,
            rows,
            |a, i| a.value(i).into(),
        ),
    })
}
