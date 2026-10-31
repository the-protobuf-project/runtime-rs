//! Descriptors, refusals, registries and generated-model views, without a server.
mod common;
use common::{Reading, record, resource};
use database::store::{
    Capabilities, Column, DB, Error, For, Kind, ListOptions, Model, RangeOptions, Record, Registry,
    Resource, Role, Scan,
};
use serde_json::json;
use std::sync::LazyLock;

#[test]
fn invalid_schema_is_rejected_at_construction() {
    let columns = resource().columns().to_vec();
    assert!(Resource::new("readings; DROP TABLE x", columns.clone()).is_err());
    let mut duplicate = columns.clone();
    duplicate.push(columns[0].clone());
    assert!(Resource::new("readings", duplicate).is_err());
    assert!(Resource::new("readings", vec![Column::new("x", Kind::Int, Role::Tag)]).is_err());
    assert!(Resource::new("readings", columns[..2].to_vec()).is_err());
}

#[test]
fn records_are_complete_typed_and_do_not_silently_drop_data() {
    let res = resource();
    res.validate(&record()).unwrap();
    let mut row = record();
    row.insert("temperature".into(), json!("22.5"));
    assert!(res.validate(&row).is_err());
    row = record();
    row.insert("device".into(), json!(""));
    assert!(res.validate(&row).is_err());
    row.insert("device".into(), json!("injected\npoint"));
    assert!(res.validate(&row).is_err());
    row = record();
    row.remove("observed_at");
    assert!(res.validate(&row).is_err());
    row = record();
    row.insert("undeclared".into(), json!(1));
    assert!(res.validate(&row).is_err());
}

#[test]
fn enums_are_i32_numbers() {
    let res = Resource::new(
        "states",
        vec![
            Column::new("mode", Kind::Enum, Role::Field),
            Column::new("at", Kind::Timestamp, Role::Timestamp),
        ],
    )
    .unwrap();
    let row = |mode: i64| Record::from([("mode".into(), mode.into()), ("at".into(), 1.into())]);
    res.validate(&row(3)).unwrap();
    assert!(res.validate(&row(i64::from(i32::MAX) + 1)).is_err());
}

#[tokio::test]
async fn missing_capabilities_are_present_and_refuse_by_name() {
    let db = DB::build("empty", "tenant", Capabilities::default());
    let unimplemented = |e: Error| {
        matches!(
            e,
            Error::Unimplemented {
                backend: "empty",
                ..
            }
        )
    };
    assert!(unimplemented(
        db.create(&resource(), &record()).await.unwrap_err()
    ));
    assert!(unimplemented(db.get(&resource(), "k").await.unwrap_err()));
    assert!(unimplemented(
        db.list(&resource(), &ListOptions::default())
            .await
            .unwrap_err()
    ));
    let range = RangeOptions::default();
    assert!(unimplemented(
        db.series.range(&resource(), &range).await.unwrap_err()
    ));
    assert!(unimplemented(
        db.schema.has_schema(&resource()).await.unwrap_err()
    ));
    assert!(unimplemented(
        db.batch.create_many(&resource(), &[]).await.unwrap_err()
    ));
    let streamed = db.streams.stream(&resource(), &Scan::new(0, 1)).await;
    assert!(unimplemented(streamed.err().unwrap()));
    db.close().await.unwrap();
}

static READINGS: LazyLock<Resource> = LazyLock::new(|| resource().with_name("Reading"));

/// What the generator emits for a message.
impl Model for Reading {
    fn resource() -> &'static Resource {
        &READINGS
    }
    fn to_record(&self) -> database::Result<Record> {
        Ok(Record::from([
            ("device".into(), self.device.clone().into()),
            ("temperature".into(), self.temperature.into()),
            ("observed_at".into(), self.observed_at.into()),
        ]))
    }
    fn from_record(mut r: Record) -> database::Result<Self> {
        let mut take = |c: &str| r.remove(c).ok_or(Error::Invalid(format!("missing {c}")));
        Ok(Self {
            device: take("device")?.as_str().unwrap_or_default().to_owned(),
            temperature: take("temperature")?.as_f64().unwrap_or_default(),
            observed_at: take("observed_at")?.as_i64().unwrap_or_default(),
        })
    }
}

#[tokio::test]
async fn generated_models_bind_through_for_and_registries_find_them() {
    let registry = Registry::new([READINGS.clone()]);
    assert_eq!(registry.resource("Reading").unwrap().table(), "readings");
    assert!(registry.resource("Book").is_err());
    assert_eq!(registry.names(), ["Reading"]);

    let db = DB::build("empty", "tenant", Capabilities::default());
    let readings = For::<Reading>::new(&db);
    assert_eq!(readings.resource().name(), "Reading");
    let reading = Reading {
        device: "boiler".into(),
        temperature: 22.5,
        observed_at: 1,
    };
    // The view validates the generated record before the backend sees it.
    let bad = Reading {
        device: String::new(),
        ..reading
    };
    assert!(matches!(
        readings.create(&bad).await,
        Err(Error::Invalid(_))
    ));
}

#[test]
fn record_descriptors_do_not_require_a_time_series_schema() {
    let resource = Resource::new(
        "books",
        vec![
            Column::new("id", Kind::String, Role::Key),
            Column::new("title", Kind::String, Role::Field),
        ],
    )
    .unwrap();
    assert_eq!(resource.key_column(), Some("id"));
    assert!(resource.time_column().is_err());
}
