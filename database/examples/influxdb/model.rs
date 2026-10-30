//! One model/descriptor shared by all examples in this folder.
use database::{Column, Kind, Resource, Role};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct Reading {
    pub device: String,
    pub value: f64,
    /// Explicit epoch nanoseconds make retries preserve point identity.
    pub observed_at: i64,
}

pub fn resource() -> database::Result<Resource> {
    Resource::new(
        "readings",
        vec![
            Column::new("device", Kind::String, Role::Tag),
            Column::new("value", Kind::Float, Role::Field),
            Column::new("observed_at", Kind::Timestamp, Role::Timestamp),
        ],
    )
}
