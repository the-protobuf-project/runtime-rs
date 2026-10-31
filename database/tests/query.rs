//! End-to-end SDK Arrow Flight query and database-selection tests.
mod common;
#[path = "common/flight.rs"]
mod flight;
use common::{Reading, record, resource};
use database::influxdb::{ClientConfig, InfluxProvider, RetryConfig};
use database::store::{AggregateOptions, Error, Provider, RangeOptions, Reducer, Reduction};
use std::time::Duration;

fn window() -> RangeOptions {
    RangeOptions {
        start: 0,
        end: 2_000_000_000,
        ..RangeOptions::default()
    }
}

fn provider(host: &str) -> InfluxProvider {
    InfluxProvider::new(
        ClientConfig::builder()
            .host(host)
            .token("test-token")
            .database("template")
            .retry(RetryConfig::disabled())
            .build()
            .unwrap(),
    )
}

#[tokio::test]
async fn selection_and_typed_reads_send_database_and_decode_arrow_timestamps() {
    let fixture = flight::Fixture::start("normal").await;
    let provider = provider(&fixture.host);
    let first = provider.set_database("tenant_a").await.unwrap();
    let second = provider.set_database("tenant_b").await.unwrap();
    let (rows, next) = database::collection::<Reading>(&first, resource())
        .range(&window())
        .await
        .unwrap();
    assert!(next.is_empty());
    assert_eq!(
        rows,
        vec![Reading {
            device: "boiler".into(),
            temperature: 22.5,
            observed_at: 1234567890
        }]
    );
    resource().validate(&record()).unwrap();
    assert!(first.get(&resource(), "k").await.is_err());
    assert_eq!(first.name, "tenant_a");
    assert_eq!(second.name, "tenant_b");
    let tickets = fixture.tickets.lock().unwrap();
    assert_eq!(tickets[0]["database"], "tenant_a");
    assert_eq!(tickets[1]["database"], "tenant_b");
    assert_eq!(tickets[2]["database"], "tenant_a");
    assert_eq!(
        tickets[2]["params"]["start"],
        "1970-01-01T00:00:00.000000000Z"
    );
}

#[tokio::test]
async fn aggregation_uses_the_same_sdk_and_returns_named_buckets() {
    let fixture = flight::Fixture::start("normal").await;
    let db = provider(&fixture.host)
        .set_database("sensors")
        .await
        .unwrap();
    let query = AggregateOptions {
        start: 0,
        end: 2_000_000_000,
        every: Duration::from_secs(60),
        reduce: vec![Reduction::new(Reducer::Avg, "temperature", "mean")],
        ..AggregateOptions::default()
    };
    let buckets = db.series.aggregate(&resource(), &query).await.unwrap();
    assert_eq!(buckets[0].start, 0);
    assert_eq!(buckets[0].values["mean"], 22.5);
    assert!(
        fixture.tickets.lock().unwrap()[1]["sql_query"]
            .as_str()
            .unwrap()
            .contains("date_bin")
    );
}

#[tokio::test]
async fn selection_propagates_query_permission_failure() {
    let fixture = flight::Fixture::start("denied").await;
    let result = provider(&fixture.host).set_database("sensors").await;
    assert!(matches!(result, Err(Error::Backend { .. })));
    assert_eq!(fixture.tickets.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn streamed_rows_decode_incrementally_and_midstream_failures_are_not_retried() {
    use database::influxdb::{Client, InfluxDriver};
    use database::store::{Scan, SeriesReader};
    use futures::TryStreamExt;
    use std::{sync::Arc, time::Duration};
    for mode in ["stall", "broken"] {
        let fixture = flight::Fixture::start(mode).await;
        let config = ClientConfig::builder()
            .host(&fixture.host)
            .token("test-token")
            .database("sensors")
            .retry(RetryConfig::disabled())
            .build()
            .unwrap();
        let driver = InfluxDriver::new(
            Arc::new(Client::new(config).await.unwrap()),
            Duration::from_millis(250),
        )
        .unwrap();
        let mut stream = driver
            .stream(&resource(), &Scan::new(0, 2_000_000_000))
            .await
            .unwrap();
        assert_eq!(
            stream.try_next().await.unwrap().unwrap()["device"],
            "boiler"
        );
        let error = stream.try_next().await.unwrap_err();
        if mode == "stall" {
            assert!(matches!(error, Error::Timeout));
        } else {
            assert!(matches!(error, Error::Backend { .. }));
        }
        let tickets = fixture.tickets.lock().unwrap();
        assert_eq!(tickets.len(), 1);
        assert!(!tickets[0]["sql_query"].as_str().unwrap().contains("LIMIT"));
    }
}
