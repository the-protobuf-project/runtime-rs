//! Opt-in real InfluxDB test. Writes observations to the supplied database.
use database::influxdb::{ClientConfig, InfluxProvider};
use database::store::{
    AggregateOptions, Column, Kind, Provider, RangeOptions, Reducer, Reduction, Resource, Role,
};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
#[ignore = "requires INFLUX_HOST, INFLUX_DATABASE and a token; writes real points"]
async fn influxdb3_round_trip_and_duplicate_point_semantics() {
    let config = ClientConfig::from_env().expect("set InfluxDB environment variables");
    let name = config.database.clone();
    let db = InfluxProvider::new(config)
        .set_database(&name)
        .await
        .unwrap();
    let resource = Resource::new(
        "runtime_database_test",
        vec![
            Column::new("device", Kind::String, Role::Tag),
            Column::new("value", Kind::Float, Role::Field),
            Column::new("observed_at", Kind::Timestamp, Role::Timestamp),
        ],
    )
    .unwrap();
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let device = format!("rust_{now}");
    let start = now - now.rem_euclid(60_000_000_000);
    let row = |time, value| {
        serde_json::from_value(json!({
            "device": device, "value": value, "observed_at": time,
        }))
        .unwrap()
    };
    db.batch
        .create_many(&resource, &[row(start + 1, 10.0), row(start + 2, 20.0)])
        .await
        .unwrap();
    db.create(&resource, &row(start + 1, 30.0)).await.unwrap();
    let filter = format!("device = \"{device}\"");
    let range = RangeOptions {
        start,
        end: start + 3,
        filter: filter.clone(),
        ..RangeOptions::default()
    };
    let records = db.series.range(&resource, &range).await.unwrap().items;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["value"], 30.0);
    assert_eq!(records[1]["value"], 20.0);
    let buckets = db
        .series
        .aggregate(
            &resource,
            &AggregateOptions {
                start,
                end: start + 3,
                every: Duration::from_secs(60),
                reduce: vec![Reduction::new(Reducer::Avg, "value", "mean")],
                group_by: vec!["device".into()],
                filter,
                ..AggregateOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].values["mean"], 25.0);
    assert_eq!(buckets[0].start, start);
    assert_eq!(buckets[0].group["device"], device);
    assert!(db.get(&resource, "k").await.is_err());
    assert!(db.schema.has_schema(&resource).await.unwrap());
}
