use std::collections::HashMap;
use std::time::Duration;

use runtime::{BatchOp, ConnectionOptions, FieldArg, UrlOptions};
use serde_json::json;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn new_connection_tx_add_commit_end_to_end() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {"m0": {"id": "promo-1"}, "m1": {"id": "money-1"}}
        })))
        .mount(&server)
        .await;

    let url = url::Url::parse(&server.uri()).unwrap();
    let mut network = runtime::new_connection(runtime::GRAPHQL_CONN_CLIENT).unwrap();
    network
        .with_opts(ConnectionOptions {
            url: UrlOptions {
                scheme: runtime::HTTP,
                host: format!("{}:{}", url.host_str().unwrap(), url.port().unwrap()),
                paths: vec!["/graphql".to_string()],
                params: HashMap::new(),
            },
            timeout: Duration::from_secs(2),
            skip_connectivity_check: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let gql = network.as_graphql().unwrap();

    let mut tx = runtime::Tx::new(gql);
    tx.add(BatchOp { field: "insertPromocode".to_string(), args: vec![], selection: "{ id }".to_string() });
    tx.add(BatchOp {
        field: "insertBookingMoney".to_string(),
        args: vec![FieldArg::new("amount", 100, "Int!")],
        selection: "{ id }".to_string(),
    });
    assert_eq!(tx.len(), 2);

    let results = tx.commit().await.unwrap();
    assert_eq!(results[0]["id"], "promo-1");
    assert_eq!(results[1]["id"], "money-1");
}

#[tokio::test]
async fn empty_tx_commits_nothing() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(200)).expect(0).mount(&server).await;

    let url = url::Url::parse(&server.uri()).unwrap();
    let mut network = runtime::new_connection(runtime::GRAPHQL_CONN_CLIENT).unwrap();
    network
        .with_opts(ConnectionOptions {
            url: UrlOptions {
                scheme: runtime::HTTP,
                host: format!("{}:{}", url.host_str().unwrap(), url.port().unwrap()),
                paths: vec!["/graphql".to_string()],
                params: HashMap::new(),
            },
            timeout: Duration::from_secs(2),
            skip_connectivity_check: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let gql = network.as_graphql().unwrap();

    let tx = runtime::Tx::new(gql);
    assert!(tx.is_empty());
    let results = tx.commit().await.unwrap();
    assert!(results.is_empty());
    server.verify().await;
}
