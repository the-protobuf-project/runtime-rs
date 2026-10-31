//! The BMS point model and capture helpers shared by Arrow tests.
#![allow(dead_code)]
use database::arrow::ArrowProvider;
use database::collection;
use database::store::{Column, For, Kind, Provider, RangeOptions, Resource, Role};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub device: String,
    pub object: String,
    pub value: f64,
    pub alarm: bool,
    pub observed_at: i64,
}

pub fn points() -> Resource {
    Resource::new(
        "points",
        vec![
            Column::new("device", Kind::String, Role::Tag),
            Column::new("object", Kind::String, Role::Tag),
            Column::new("value", Kind::Float, Role::Field),
            Column::new("alarm", Kind::Bool, Role::Field),
            Column::new("observed_at", Kind::Timestamp, Role::Timestamp),
        ],
    )
    .unwrap()
}

pub fn point(object: &str, value: f64, observed_at: i64) -> Point {
    Point {
        device: "ahu1".into(),
        object: object.into(),
        value,
        alarm: value > 30.0,
        observed_at,
    }
}

pub fn root(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tpp-arrow-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

pub async fn open(provider: &ArrowProvider) -> For<Point> {
    provider.create_database("bms").await.unwrap();
    collection(&provider.set_database("bms").await.unwrap(), points())
}

pub fn window(start: i64, end: i64) -> RangeOptions {
    RangeOptions {
        start,
        end,
        ..RangeOptions::default()
    }
}
