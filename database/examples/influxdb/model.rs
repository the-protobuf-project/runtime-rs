//! One model shared by all examples, written as the store generator emits it:
//! a static descriptor and `Model` conversions, with no serde or reflection.
use database::store::{Column, Error, Kind, Model, Record, Resource, Role};
use std::sync::LazyLock;

#[derive(Debug, PartialEq)]
pub struct Reading {
    pub device: String,
    pub value: f64,
    /// Explicit epoch nanoseconds make retries preserve point identity.
    pub observed_at: i64,
}

static READING: LazyLock<Resource> = LazyLock::new(|| {
    Resource::new(
        "readings",
        vec![
            Column::new("device", Kind::String, Role::Tag),
            Column::new("value", Kind::Float, Role::Field),
            Column::new("observed_at", Kind::Timestamp, Role::Timestamp),
        ],
    )
    .expect("generated descriptor is valid")
    .with_name("Reading")
});

impl Model for Reading {
    fn resource() -> &'static Resource {
        &READING
    }

    fn to_record(&self) -> database::Result<Record> {
        Ok(Record::from([
            ("device".into(), self.device.as_str().into()),
            ("value".into(), self.value.into()),
            ("observed_at".into(), self.observed_at.into()),
        ]))
    }

    fn from_record(mut record: Record) -> database::Result<Self> {
        let mut take = |name: &str| {
            record
                .remove(name)
                .ok_or_else(|| Error::Invalid(format!("missing {name}")))
        };
        Ok(Self {
            device: take("device")?.as_str().unwrap_or_default().to_owned(),
            value: take("value")?.as_f64().unwrap_or_default(),
            observed_at: take("observed_at")?.as_i64().unwrap_or_default(),
        })
    }
}

/// A window over the readings table, in epoch nanoseconds.
pub fn window(start: i64, end: i64) -> database::store::RangeOptions {
    database::store::RangeOptions {
        start,
        end,
        ..Default::default()
    }
}
