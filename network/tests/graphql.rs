use std::collections::HashMap;
use std::time::Duration;

use network::{ConnectionOptions, FieldArg, GraphQLClient, UrlOptions, UrlScheme};
use serde::Deserialize;
use serde_json::json;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn opts_for(server: &MockServer) -> ConnectionOptions {
    let url = url::Url::parse(&server.uri()).unwrap();
    ConnectionOptions {
        url: UrlOptions {
            scheme: UrlScheme::Http,
            host: format!("{}:{}", url.host_str().unwrap(), url.port().unwrap()),
            paths: vec!["/graphql".to_string()],
            params: HashMap::new(),
        },
        timeout: Duration::from_secs(2),
        ..Default::default()
    }
}

#[tokio::test]
async fn connect_sends_default_connectivity_query() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .and(body_string_contains("__typename"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"data": {"__typename": "Query"}})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let mut gql = GraphQLClient::default();
    gql.connect(opts_for(&server)).await.unwrap();
    server.verify().await;
}

#[tokio::test]
async fn connect_fails_on_server_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let mut gql = GraphQLClient::default();
    let err = gql.connect(opts_for(&server)).await.unwrap_err();
    assert!(matches!(err, network::Error::GraphQLConnect { .. }));
}

#[tokio::test]
async fn connect_uses_custom_connectivity_query() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .and(body_string_contains("ping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": {"ping": true}})))
        .expect(1)
        .mount(&server)
        .await;

    let mut opts = opts_for(&server);
    opts.graphql_connectivity_query = Some("query { ping }".to_string());
    let mut gql = GraphQLClient::default();
    gql.connect(opts).await.unwrap();
    server.verify().await;
}

#[tokio::test]
async fn query_decodes_typed_response() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {"user": {"id": "1", "name": "Ada"}}
        })))
        .mount(&server)
        .await;

    let mut opts = opts_for(&server);
    opts.skip_connectivity_check = true;
    let mut gql = GraphQLClient::default();
    gql.connect(opts).await.unwrap();

    #[derive(Deserialize)]
    struct Data {
        user: User,
    }
    #[derive(Deserialize)]
    struct User {
        id: String,
        name: String,
    }

    let data: Data = gql
        .query("query { user(id: \"1\") { id name } }", None)
        .await
        .unwrap();
    assert_eq!(data.user.id, "1");
    assert_eq!(data.user.name, "Ada");
}

#[tokio::test]
async fn query_surfaces_graphql_errors() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "errors": [{"message": "not found"}]
        })))
        .mount(&server)
        .await;

    let mut opts = opts_for(&server);
    opts.skip_connectivity_check = true;
    let mut gql = GraphQLClient::default();
    gql.connect(opts).await.unwrap();

    let err = gql
        .query::<serde_json::Value>("query { missing }", None)
        .await
        .unwrap_err();
    assert!(matches!(err, network::Error::GraphQLOperation(_)));
}

#[tokio::test]
async fn query_fields_declares_only_present_sorted_args() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_string_contains("user(active: $active, id: $id)"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"data": {"user": {"id": "1"}}})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let mut opts = opts_for(&server);
    opts.skip_connectivity_check = true;
    let mut gql = GraphQLClient::default();
    gql.connect(opts).await.unwrap();

    let args = vec![
        FieldArg::new("id", "1", "ID!"),
        FieldArg::new("active", true, "Boolean"),
    ];
    let _: serde_json::Value = gql.query_fields("user", &args, "{ id }").await.unwrap();
    server.verify().await;
}

/// `query_fields`/`mutate_fields` must decode `T` from the *unwrapped* selected field
/// (`data["user"]`), not from the whole `data` object — a struct shaped like the field's own
/// selection (no `user` wrapper) must deserialize successfully.
#[tokio::test]
async fn query_fields_unwraps_the_selected_field_before_decoding() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {"user": {"id": "1", "name": "Ada"}}
        })))
        .mount(&server)
        .await;

    let mut opts = opts_for(&server);
    opts.skip_connectivity_check = true;
    let mut gql = GraphQLClient::default();
    gql.connect(opts).await.unwrap();

    #[derive(Deserialize)]
    struct UserFields {
        id: String,
        name: String,
    }

    let args = vec![FieldArg::new("id", "1", "ID!")];
    let user: UserFields = gql
        .query_fields("user", &args, "{ id name }")
        .await
        .unwrap();
    assert_eq!(user.id, "1");
    assert_eq!(user.name, "Ada");
}

#[tokio::test]
async fn batch_mutate_namespaces_args_and_returns_ordered_results() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {"m0": {"id": "a"}, "m1": {"id": "b"}}
        })))
        .mount(&server)
        .await;

    let mut opts = opts_for(&server);
    opts.skip_connectivity_check = true;
    let mut gql = GraphQLClient::default();
    gql.connect(opts).await.unwrap();

    let ops = vec![
        network::BatchOp {
            field: "insertThing".to_string(),
            args: vec![],
            selection: "{ id }".to_string(),
        },
        network::BatchOp {
            field: "insertThing".to_string(),
            args: vec![FieldArg::new("objects", json!([]), "[ThingInsertInput!]!")],
            selection: "{ id }".to_string(),
        },
    ];
    let results = gql.batch_mutate(&ops).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["id"], "a");
    assert_eq!(results[1]["id"], "b");
}
