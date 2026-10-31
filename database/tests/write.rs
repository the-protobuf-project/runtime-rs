//! Exercise the real SDK HTTP path against controlled server responses.
mod common;
use common::{Reading, record, resource};
use database::drivers::influxdb::{
    Client, ClientConfig, InfluxDriver, InfluxProvider, RetryConfig,
};
use database::{Capabilities, Collection, DB, Error, Provider, TimeSeries};
use std::{sync::Arc, time::Duration};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_string_contains, header, method, path, query_param},
};

async fn driver(server: &MockServer) -> InfluxDriver {
    let config = ClientConfig::builder()
        .host(server.uri())
        .token("test-token")
        .database("sensors")
        .retry(RetryConfig::disabled())
        .build()
        .unwrap();
    InfluxDriver::new(
        Arc::new(Client::new(config).await.unwrap()),
        Duration::from_secs(1),
    )
    .unwrap()
}

#[tokio::test]
async fn typed_write_uses_v3_auth_precision_and_durable_policy() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v3/write_lp"))
        .and(header("Authorization", "Bearer test-token"))
        .and(query_param("db", "sensors"))
        .and(query_param("precision", "nanosecond"))
        .and(query_param("accept_partial", "false"))
        .and(body_string_contains(
            "readings,device=boiler temperature=22.5 1234567890",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    let db = DB::new(
        "influxdb3",
        "sensors".into(),
        Capabilities {
            series: Some(Arc::new(driver(&server).await)),
            ..Capabilities::default()
        },
    );
    assert!(db.records().is_err());
    Collection::new(db, resource())
        .write(&[Reading {
            device: "boiler".into(),
            temperature: 22.5,
            observed_at: 1234567890,
        }])
        .await
        .unwrap();
    let requests = server.received_requests().await.unwrap();
    assert!(!requests[0].url.query_pairs().any(|(k, _)| k == "no_sync"));
}

#[tokio::test]
async fn invalid_later_record_prevents_the_entire_network_write() {
    let server = MockServer::start().await;
    let driver = driver(&server).await;
    let mut invalid = record();
    invalid.remove("device");
    assert!(matches!(
        driver.write(&resource(), &[record(), invalid]).await,
        Err(Error::Invalid(_))
    ));
    driver.write(&resource(), &[]).await.unwrap();
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn server_errors_keep_the_sdk_source_and_are_not_reported_as_success() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(401).set_body_json(serde_json::json!({"error": "unauthorized"})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let error = driver(&server)
        .await
        .write(&resource(), &[record()])
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Backend {
            backend: "influxdb3",
            ..
        }
    ));
    assert!(error.to_string().contains("401"));
}

#[tokio::test]
async fn timeout_bounds_the_complete_write() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(204).set_delay(Duration::from_secs(2)))
        .mount(&server)
        .await;
    let driver = driver(&server).await;
    let driver = InfluxDriver::new(driver.client().clone(), Duration::from_millis(25)).unwrap();
    assert!(matches!(
        driver.write(&resource(), &[record()]).await,
        Err(Error::Timeout)
    ));
}

#[tokio::test]
async fn sharing_a_client_cannot_silently_select_a_different_database() {
    let server = MockServer::start().await;
    let driver = driver(&server).await;
    let provider = InfluxProvider::from_client(driver.client().clone());
    assert!(matches!(
        provider.set_database("other").await,
        Err(Error::Invalid(_))
    ));
    assert!(provider.set_database("bad-name").await.is_err());
    assert!(server.received_requests().await.unwrap().is_empty());
}
