//! Shared descriptor, rows and selection for Arrow segment tests.
#![allow(dead_code)]
use database::arrow::{ArrowProvider, Rotation};
use database::store::{Column, DB, Kind, Provider, RangeOptions, Record, Resource, Role};
use std::path::PathBuf;

pub const SECOND: i64 = 1_000_000_000;

pub fn readings() -> Resource {
    Resource::new(
        "readings",
        vec![
            Column::new("object", Kind::String, Role::Tag),
            Column::new("value", Kind::Float, Role::Field),
            Column::new("pulses", Kind::Uint, Role::Field),
            Column::new("observed_at", Kind::Timestamp, Role::Timestamp),
        ],
    )
    .unwrap()
}

pub fn reading(object: &str, value: f64, pulses: u64, at: i64) -> Record {
    Record::from([
        ("object".into(), object.into()),
        ("value".into(), value.into()),
        ("pulses".into(), pulses.into()),
        ("observed_at".into(), at.into()),
    ])
}

pub async fn select(name: &str, rotation: Rotation) -> (ArrowProvider, DB) {
    let root: PathBuf =
        std::env::temp_dir().join(format!("tpp-arrow-seg-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let provider = ArrowProvider::new(root).with_rotation(rotation);
    provider.create_database("bms").await.unwrap();
    let db = provider.set_database("bms").await.unwrap();
    (provider, db)
}

pub fn every_write() -> Rotation {
    Rotation {
        max_bytes: Some(1),
        max_age: None,
    }
}

pub fn window(start: i64, end: i64) -> RangeOptions {
    RangeOptions {
        start,
        end,
        page_size: 10_000,
        ..RangeOptions::default()
    }
}
