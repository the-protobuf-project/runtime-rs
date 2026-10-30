//! Real-server lifecycle, paged/streamed reads, and typed background ingestion.
use database::drivers::influxdb::{ClientConfig, InfluxAdmin, InfluxProvider};
use database::{
    Collection, Column, Error, IngestConfig, Kind, Provider, Range, Resource, Retention, Role, Scan,
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
struct Reading {
    device: String,
    value: f64,
    observed_at: i64,
}

#[tokio::test]
#[ignore = "requires local InfluxDB credentials; creates and deletes its own disposable database"]
async fn retention_ingestion_streaming_pagination_and_table_deletion() {
    let config = ClientConfig::from_env().unwrap();
    let admin = InfluxAdmin::new(&config, Duration::from_secs(30)).unwrap();
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let name = format!("rust_lifecycle_{now}");
    admin
        .create_database(&name, Retention::For(Duration::from_secs(3600)))
        .await
        .unwrap();
    assert!(matches!(
        admin.create_database(&name, Retention::Infinite).await,
        Err(Error::AlreadyExists)
    ));
    let db = InfluxProvider::new(config)
        .set_database(&name)
        .await
        .unwrap();
    assert!(matches!(db.row_deletion(), Err(Error::Unsupported { .. })));
    let resource = Resource::new(
        "readings",
        vec![
            Column::new("device", Kind::String, Role::Tag),
            Column::new("value", Kind::Float, Role::Field),
            Column::new("observed_at", Kind::Timestamp, Role::Timestamp),
        ],
    )
    .unwrap();
    let readings = Collection::<Reading>::new(db.clone(), resource.clone());
    let old = now - 7_200_000_000_000;
    readings
        .write(&[Reading {
            device: "expired".into(),
            value: -1.0,
            observed_at: old,
        }])
        .await
        .unwrap();
    assert!(
        readings
            .range(&Range::new(old, old + 1))
            .await
            .unwrap()
            .is_empty()
    );
    let writer = readings
        .batch_writer(IngestConfig {
            batch_size: 3,
            queue_capacity: 2,
            flush_interval: Duration::from_millis(50),
            ..IngestConfig::default()
        })
        .unwrap();
    for n in 0..10 {
        writer
            .write(&Reading {
                device: format!("sensor_{}", n % 5),
                value: f64::from(n),
                observed_at: now + i64::from(n / 5),
            })
            .await
            .unwrap();
    }
    writer.flush().await.unwrap();
    writer.close().await.unwrap();
    let rows: Vec<Reading> = readings
        .stream(&Scan::new(now, now + 2))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(rows.len(), 10);
    assert_eq!(
        rows.iter().map(|r| r.value).collect::<Vec<_>>(),
        (0..10).map(f64::from).collect::<Vec<_>>()
    );
    for descending in [false, true] {
        let mut range = Range::new(now, now + 2);
        range.limit = 2;
        range.descending = descending;
        let mut token = None;
        let mut values = Vec::new();
        loop {
            let page = readings.page(&range, token.as_deref()).await.unwrap();
            values.extend(page.items.into_iter().map(|row| row.value));
            token = page.next_page_token;
            if token.is_none() {
                break;
            }
            assert!(values.len() <= 10, "paging repeated a cursor");
        }
        let expected: Vec<f64> = if descending {
            (5..10).chain(0..5).map(f64::from).collect()
        } else {
            (0..10).map(f64::from).collect()
        };
        assert_eq!(values, expected);
    }
    db.maintenance()
        .unwrap()
        .drop_table(&resource)
        .await
        .unwrap();
    assert!(readings.range(&Range::new(now, now + 2)).await.is_err());
    admin.drop_database(&name).await.unwrap();
}
