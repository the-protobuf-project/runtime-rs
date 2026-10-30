//! Validation, problem details, idempotency and hints, end to end through a running
//! [`grpc::HybridServer`] and a generated tonic client.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use common::{SOLD_OUT, Shop, create, serve};
use grpc::idempotency::{self, MemoryStore, Store};
use grpc::problem::{Kind, Translator};
use grpc::{Interceptor, Next, UnaryCall, UnaryReply};
use tonic::{Code, Status};
use tonic_types::StatusExt;

/// Counts the calls that reach it.
#[derive(Clone, Default)]
struct Count(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl Interceptor for Count {
    async fn intercept(&self, call: UnaryCall, next: Next<'_>) -> Result<UnaryReply, Status> {
        self.0.fetch_add(1, Ordering::SeqCst);
        next.run(call).await
    }
}

// --- validation ------------------------------------------------------------------------

#[tokio::test]
async fn validation_rejects_a_request_before_any_interceptor_or_the_handler() {
    let shop = Shop::default();
    let count = Count::default();
    let reached = Arc::clone(&count.0);
    let (server, mut client) = serve(shop.clone(), |s| {
        s.with_interceptor(count).with_validation()
    })
    .await;

    let status = client
        .create_book(create("nope", "b1", ""))
        .await
        .expect_err("the request breaks its rules");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "parent must look like shelves/{shelf}; got \"nope\"."
    );
    let bad = status
        .get_details_bad_request()
        .expect("a BadRequest detail");
    assert_eq!(bad.field_violations[0].field, "parent");
    assert_eq!(
        status.get_details_error_info().map(|i| i.reason).as_deref(),
        Some(grpc::validation_hints::VALIDATION_REASON)
    );
    assert_eq!(reached.load(Ordering::SeqCst), 0, "validation ran first");
    assert_eq!(shop.runs(), 0, "the handler never ran");

    let book = client
        .create_book(create("shelves/s", "b1", ""))
        .await
        .expect("a valid request is served")
        .into_inner();
    assert_eq!(book.name, "shelves/s/books/b1");
    assert_eq!(reached.load(Ordering::SeqCst), 1);
    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn a_supplied_validator_replaces_the_engine() {
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let recorder = Arc::clone(&seen);
    let (server, mut client) = serve(Shop::default(), move |s| {
        s.with_validator(move |call: &UnaryCall| -> Result<(), Status> {
            recorder.lock().unwrap().push(call.method().to_string());
            let request = call.message()?;
            let id = request
                .get_field_by_name("book_id")
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default();
            if id == "forbidden" {
                return Err(Status::invalid_argument("book_id is reserved."));
            }
            Ok(())
        })
    })
    .await;

    let status = client
        .create_book(create("shelves/s", "forbidden", ""))
        .await
        .expect_err("rejected");
    assert_eq!(status.message(), "book_id is reserved.");
    // The default engine is not running: this parent breaks its pattern.
    client
        .create_book(create("nope", "fine", ""))
        .await
        .expect("served");
    assert_eq!(*seen.lock().unwrap(), ["CreateBook", "CreateBook"]);
    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn the_standalone_layer_validates_a_plain_tonic_server() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binds");
    let port = listener.local_addr().unwrap().port();
    let shop = Shop::default();
    let serving = tokio::spawn(
        tonic::transport::Server::builder()
            .layer(grpc::validation::standalone_layer(
                &[common::SHOP_DESCRIPTOR_SET],
                vec![],
            ))
            .add_service(common::shop_server(shop.clone()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    let mut client = common::client(port).await;
    let status = client
        .create_book(create("nope", "b", ""))
        .await
        .expect_err("rejected");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(shop.runs(), 0);
    client
        .create_book(create("shelves/s", "b", ""))
        .await
        .expect("served");
    serving.abort();
}

// --- problem details -------------------------------------------------------------------

fn translator() -> Translator {
    Translator::new(
        "shop.example.com",
        [
            Kind::new("not found", Code::NotFound, "NOT_FOUND"),
            Kind::new(SOLD_OUT, Code::FailedPrecondition, "SOLD_OUT"),
        ],
    )
}

#[tokio::test]
async fn problems_and_sentinels_become_actionable_statuses() {
    let (server, mut client) = serve(Shop::default(), |s| {
        s.with_interceptor(translator().interceptor())
    })
    .await;

    let status = client
        .create_book(create("shelves/s", "sold-out", ""))
        .await
        .expect_err("a problem");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(
        status.message(),
        "Shelf shelves/s is empty. Restock it first."
    );
    let info = status.get_details_error_info().expect("an ErrorInfo");
    assert_eq!(
        (info.reason.as_str(), info.domain.as_str()),
        ("SHELF_EMPTY", "shop.example.com")
    );
    assert_eq!(
        status
            .get_details_precondition_failure()
            .unwrap()
            .violations[0]
            .subject,
        "shelves/s"
    );

    let status = client
        .create_book(create("shelves/x", "plain", ""))
        .await
        .expect_err("a sentinel");
    assert_eq!(status.code(), Code::NotFound);
    assert_eq!(status.message(), "Shelf shelves/x has no room.");
    assert_eq!(
        grpc::problem::reason_of(&status).as_deref(),
        Some("NOT_FOUND")
    );

    // A status the handler meant passes through.
    let status = client
        .create_book(create("shelves/s", "fail", ""))
        .await
        .expect_err("a status");
    assert_eq!(
        (status.code(), status.message()),
        (Code::FailedPrecondition, "no")
    );
    server.stop().await.expect("the server stops");
}

// --- idempotency -----------------------------------------------------------------------

#[tokio::test]
async fn a_reused_request_id_replays_the_first_response() {
    let shop = Shop::default();
    let store = Arc::new(MemoryStore::new());
    let shared: Arc<dyn Store> = store.clone();
    let (server, mut client) = serve(shop.clone(), |s| {
        s.with_interceptor(idempotency::IdempotencyInterceptor::new(
            shared,
            idempotency::Options::default(),
        ))
    })
    .await;

    let first = client
        .create_book(create("shelves/s", "b", "r1"))
        .await
        .expect("served")
        .into_inner();
    let again = client
        .create_book(create("shelves/s", "b", "r1"))
        .await
        .expect("replayed")
        .into_inner();
    assert_eq!(first, again);
    assert_eq!(shop.runs(), 1, "the replay did not run the handler");

    client
        .create_book(create("shelves/s", "b", "r2"))
        .await
        .expect("a new id runs");
    assert_eq!(shop.runs(), 2);

    // No request_id, and a dry run, are never recorded.
    let before = store.len();
    client
        .create_book(create("shelves/s", "b", ""))
        .await
        .expect("served");
    let mut dry = create("shelves/s", "b", "r3");
    dry.validate_only = true;
    client.create_book(dry.clone()).await.expect("served");
    client.create_book(dry).await.expect("served");
    assert_eq!(store.len(), before);
    assert_eq!(shop.runs(), 5);
    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn a_failed_call_releases_its_request_id() {
    let shop = Shop::default();
    let store = Arc::new(MemoryStore::new());
    let shared: Arc<dyn Store> = store.clone();
    let (server, mut client) = serve(shop.clone(), |s| {
        s.with_interceptor(idempotency::IdempotencyInterceptor::new(
            shared,
            idempotency::Options::default(),
        ))
    })
    .await;

    let status = client
        .create_book(create("shelves/s", "fail", "r1"))
        .await
        .expect_err("fails");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(store.is_empty(), "a failed call kept its key");
    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn a_concurrent_duplicate_is_aborted_and_an_abandoned_claim_cleared() {
    let shop = Shop::default();
    let now = Arc::new(Mutex::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000),
    ));
    let clock = {
        let now = Arc::clone(&now);
        Arc::new(move || *now.lock().unwrap()) as idempotency::ClockFn
    };
    let store = Arc::new(MemoryStore::with_clock(Arc::clone(&clock)));
    let shared: Arc<dyn Store> = store.clone();
    let (server, mut client) = serve(shop.clone(), |s| {
        s.with_interceptor(idempotency::IdempotencyInterceptor::new(
            shared,
            idempotency::Options {
                domain: "test".into(),
                now: Some(clock),
                ..Default::default()
            },
        ))
    })
    .await;

    let mut first_client = client.clone();
    let first = tokio::spawn(async move {
        first_client
            .create_book(create("shelves/s", "wait", "r1"))
            .await
    });
    shop.entered().await;
    let status = client
        .create_book(create("shelves/s", "wait", "r1"))
        .await
        .expect_err("a concurrent duplicate");
    assert_eq!(status.code(), Code::Aborted);
    let info = status.get_details_error_info().unwrap();
    assert_eq!(
        (info.reason.as_str(), info.domain.as_str()),
        (idempotency::REASON_IN_FLIGHT, "test")
    );
    shop.release();
    let first = first
        .await
        .unwrap()
        .expect("the first call finishes")
        .into_inner();
    let replay = client
        .create_book(create("shelves/s", "wait", "r1"))
        .await
        .expect("replayed")
        .into_inner();
    assert_eq!(first, replay);
    assert_eq!(shop.runs(), 1);

    // A claim left IN_FLIGHT past the stale window is cleared, and the retry runs.
    let name = idempotency::key_name("/shop.v1.ShopService/CreateBook", "r2");
    store
        .create(idempotency::Key {
            name: name.clone(),
            method: String::new(),
            request_id: "r2".into(),
            state: idempotency::State::InFlight,
            response: Default::default(),
            response_type: String::new(),
            create_time: None,
        })
        .await
        .unwrap();
    *now.lock().unwrap() += Duration::from_secs(11 * 60);
    let status = client
        .create_book(create("shelves/s", "b", "r2"))
        .await
        .expect_err("abandoned");
    assert_eq!(status.code(), Code::Aborted);
    assert_eq!(
        grpc::problem::reason_of(&status).as_deref(),
        Some(idempotency::REASON_ABANDONED)
    );
    assert!(
        store.get(&name).await.is_err(),
        "the abandoned claim was cleared"
    );
    client
        .create_book(create("shelves/s", "b", "r2"))
        .await
        .expect("the retry runs");
    assert_eq!(shop.runs(), 2);
    server.stop().await.expect("the server stops");
}

// --- hints -----------------------------------------------------------------------------

#[tokio::test]
async fn hints_name_the_resource_and_hide_internal_errors() {
    let (server, mut client) = serve(Shop::default(), |s| {
        s.with_interceptor(
            grpc::hints::Hinter::new(grpc::hints::Options {
                product: "Shop".into(),
                ..Default::default()
            })
            .interceptor(),
        )
    })
    .await;

    let status = client
        .get_shelf(common::pb::GetShelfRequest {
            name: "shelves/missing".into(),
        })
        .await
        .expect_err("missing");
    assert_eq!(status.code(), Code::NotFound);
    assert_eq!(
        status.message(),
        "Shelf \"shelves/missing\" does not exist. Check the name, or list what does exist \
         with ListShelves (GET /v1/shelves)."
    );
    assert_eq!(
        status.get_details_resource_info().unwrap().resource_type,
        "shop.example.com/Shelf"
    );

    let mut request = tonic::Request::new(common::pb::GetShelfRequest {
        name: "shelves/boom".into(),
    });
    request.metadata_mut().insert(
        "traceparent",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            .parse()
            .unwrap(),
    );
    let status = client.get_shelf(request).await.expect_err("internal");
    assert_eq!(status.code(), Code::Internal);
    assert_eq!(
        status.message(),
        "Shop failed unexpectedly while handling GetShelf. This is a fault on our side, not \
         in your request: retry it, and if it keeps failing, report trace \
         4bf92f3577b34da6a3ce929d0e0e4736."
    );
    server.stop().await.expect("the server stops");
}
