//! Opt-in real InfluxDB test. Writes observations to the supplied database.
use database::drivers::influxdb::{ClientConfig, InfluxProvider};
use database::{
    Aggregate, Column, Comparison, Kind, Predicate, Provider, Range, Reduce, Reduction, Resource,
    Role,
};
use serde_json::json;

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
    let series = db.series().unwrap();
    series
        .write(&resource, &[row(start + 1, 10.0), row(start + 2, 20.0)])
        .await
        .unwrap();
    series
        .write(&resource, &[row(start + 1, 30.0)])
        .await
        .unwrap();
    let mut range = Range::new(start, start + 3);
    range.predicates.push(Predicate {
        column: "device".into(),
        comparison: Comparison::Eq,
        value: json!(device),
    });
    let records = series.range(&resource, &range).await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["value"], 30.0);
    assert_eq!(records[1]["value"], 20.0);
    let buckets = series
        .aggregate(
            &resource,
            &Aggregate {
                range,
                every_seconds: 60,
                reduce: vec![Reduce {
                    column: "value".into(),
                    function: Reduction::Mean,
                    alias: "mean".into(),
                }],
                group_by: vec!["device".into()],
            },
        )
        .await
        .unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0]["mean"], 25.0);
    assert_eq!(buckets[0]["observed_at"], start);
    assert!(db.records().is_err());
}
