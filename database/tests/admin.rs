//! Management requests are explicit, authenticated, and never widen deletion.
use database::Error;
use database::influxdb::{ClientConfig, InfluxAdmin, Retention};
use serde_json::json;
use std::time::Duration;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, header, method, path, query_param},
};

fn admin(host: &str) -> InfluxAdmin {
    let config = ClientConfig::builder()
        .host(host)
        .database("template")
        .token("secret")
        .build()
        .unwrap();
    InfluxAdmin::new(&config, Duration::from_secs(1)).unwrap()
}

#[tokio::test]
async fn database_creation_sends_exact_retention_without_implicit_selection() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v3/configure/database"))
        .and(header("Authorization", "Bearer secret"))
        .and(body_json(json!({"db":"sensors", "retention_period":"60m"})))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    admin(&server.uri())
        .create_database("sensors", Retention::For(Duration::from_secs(3600)))
        .await
        .unwrap();
    assert!(
        admin(&server.uri())
            .create_database("bad name", Retention::Infinite)
            .await
            .is_err()
    );
    assert!(
        admin(&server.uri())
            .create_database("sensors", Retention::For(Duration::from_secs(1)))
            .await
            .is_err()
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn management_rejections_are_classified_without_echoing_secrets() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401).set_body_string("secret"))
        .mount(&server)
        .await;
    let error = admin(&server.uri())
        .create_database("sensors", Retention::Infinite)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::HttpStatus { status: 401 }));
    assert!(!error.to_string().contains("secret"));
}

#[tokio::test]
async fn database_drop_names_only_the_explicit_database() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/api/v3/configure/database"))
        .and(query_param("db", "disposable"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    admin(&server.uri())
        .drop_database("disposable")
        .await
        .unwrap();
}

#[tokio::test]
async fn infinite_retention_is_omitted_for_the_core_create_endpoint() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_json(json!({"db":"sensors"})))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    admin(&server.uri())
        .create_database("sensors", Retention::Infinite)
        .await
        .unwrap();
}
