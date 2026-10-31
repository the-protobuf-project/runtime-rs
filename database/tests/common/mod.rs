//! Shared telemetry model and descriptors for adapter tests.

use database::store::{Column, Kind, Record, Resource, Role};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Reading {
    pub device: String,
    pub temperature: f64,
    pub observed_at: i64,
}

pub fn resource() -> Resource {
    Resource::new(
        "readings",
        vec![
            Column::new("device", Kind::String, Role::Tag),
            Column::new("temperature", Kind::Float, Role::Field),
            Column::new("observed_at", Kind::Timestamp, Role::Timestamp),
        ],
    )
    .unwrap()
}

pub fn record() -> Record {
    serde_json::from_value(json!({
        "device": "boiler", "temperature": 22.5, "observed_at": 1234567890,
    }))
    .unwrap()
}
