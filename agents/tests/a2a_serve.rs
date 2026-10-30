//! Serving an agent for real: the gRPC binding, a standalone server, a shared mux, and a
//! service function — each driven over the wire rather than inspected.

#![cfg(feature = "a2a")]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use agents::a2a::{self, ServerConfig, Transport, pb};
use agents::{Config, GrpcRoutes, Mux, Runtime};
use common::{config, free_port};
use tokio_util::sync::CancellationToken;

/// An agent whose reply proves the request reached it.
fn replying_agent() -> Arc<dyn a2a::Executor> {
    Arc::new(a2a::TextAgent::new(|text: String| async move {
        Ok(format!("you said: {text}"))
    }))
}

/// A JSON-RPC `SendMessage` carrying `text`.
fn send_message(text: &str) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": ::a2a::methods::SEND_MESSAGE,
        "params": {"message": {"messageId": "m-1", "role": "ROLE_USER", "parts": [{"text": text}]}},
    })
}

/// Serves `routes` on a tonic server bound to a free port, returning the address and a stop.
async fn serve_grpc(routes: &GrpcRoutes) -> (String, CancellationToken) {
    let port = free_port().await;
    let addr = format!("127.0.0.1:{port}");
    let stop = CancellationToken::new();
    let server = tonic::transport::Server::builder()
        .add_routes(routes.routes())
        .serve_with_shutdown(addr.parse().unwrap(), stop.clone().cancelled_owned());
    tokio::spawn(server);
    // The listener binds inside the spawned task; give it a moment before dialling.
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    (addr, stop)
}

#[tokio::test]
async fn a_message_round_trips_over_the_grpc_binding() {
    let routes = GrpcRoutes::new();
    let runtime = Runtime::new(Config {
        grpc_routes: Some(routes.clone()),
        ..config(free_port().await)
    })
    .register(a2a::Service::from_arc(replying_agent(), vec![]).transports([Transport::Grpc]));

    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.expect("start");

    // A gRPC-only agent opens no listener of its own and reports a dial target.
    let endpoints = runtime.endpoints();
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].transport, "grpc");
    assert!(!endpoints[0].url.contains("://"), "{}", endpoints[0].url);

    let (addr, stop) = serve_grpc(&routes).await;
    let mut client = pb::a2a_service_client::A2aServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("dial");
    let reply = client
        .send_message(pb::SendMessageRequest {
            message: Some(pb::Message {
                message_id: "m-1".into(),
                role: pb::Role::User as i32,
                parts: vec![pb::Part {
                    content: Some(pb::part::Content::Text("hello".into())),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect("send_message")
        .into_inner();

    let Some(pb::send_message_response::Payload::Message(message)) = reply.payload else {
        panic!("a one-shot reply is a message: {reply:?}");
    };
    let Some(pb::part::Content::Text(text)) = &message.parts[0].content else {
        panic!("expected text: {message:?}");
    };
    assert_eq!(text, "you said: hello");
    assert_eq!(a2a::GRPC_SERVICE_NAME, "lf.a2a.v1.A2AService");

    stop.cancel();
    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_grpc_binding_without_routes_is_refused() {
    let err = a2a::start_server(
        CancellationToken::new(),
        ServerConfig {
            transports: vec![Transport::Grpc],
            ..Default::default()
        },
        replying_agent(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.to_string(), a2a::Error::NoGrpcRoutes.to_string());
}

#[tokio::test]
async fn a_standalone_server_owns_its_listener_and_always_serves_the_card() {
    let port = free_port().await;
    let ready: Arc<Mutex<Option<ServerConfig>>> = Arc::default();
    let seen = Arc::clone(&ready);
    let cfg = ServerConfig {
        name: "standalone".into(),
        version: "0.1.0".into(),
        addr: format!("127.0.0.1:{port}"),
        transports: vec![Transport::JsonRpc, Transport::Rest],
        base_path: "agent//v1/".into(),
        skills: vec![a2a::skill("echo", "Echo", "Repeats")],
        // Off, but a server that owns its mux serves the card regardless.
        serve_agent_card: false,
        on_ready: Some(Arc::new(move |resolved: &ServerConfig| {
            *seen.lock().unwrap() = Some(resolved.clone());
        })),
        ..Default::default()
    };

    let cancel = CancellationToken::new();
    let task = tokio::spawn(a2a::start_server(cancel.clone(), cfg, replying_agent()));
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let mut card = None;
    for _ in 0..50 {
        if let Ok(resp) = client
            .get(format!("{base}{}", a2a::AGENT_CARD_PATH))
            .send()
            .await
        {
            card = Some(resp.json::<serde_json::Value>().await.expect("card json"));
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let card = card.expect("the card was served");
    assert_eq!(card["name"], "standalone");
    assert_eq!(
        card["supportedInterfaces"][0]["url"],
        format!("{base}/agent/v1")
    );

    // The resolved config is what on_ready saw: the base path cleaned as Go's path.Clean does.
    let resolved = ready.lock().unwrap().clone().expect("on_ready ran");
    assert_eq!(resolved.base_path, "/agent/v1");
    assert!(resolved.serve_agent_card);

    // JSON-RPC answers at the base path with the agent's reply.
    let rpc: serde_json::Value = client
        .post(format!("{base}/agent/v1"))
        .header("A2A-Version", "1.0")
        .json(&send_message("hi"))
        .send()
        .await
        .expect("jsonrpc")
        .json()
        .await
        .expect("jsonrpc json");
    assert!(rpc.to_string().contains("you said: hi"), "{rpc}");

    // The extended card is the card, as Go's WithExtendedAgentCard attaches it.
    let extended: serde_json::Value = client
        .post(format!("{base}/agent/v1"))
        .header("A2A-Version", "1.0")
        .json(&serde_json::json!({
            "jsonrpc": "2.0", "id": 2,
            "method": ::a2a::methods::GET_EXTENDED_AGENT_CARD, "params": {},
        }))
        .send()
        .await
        .expect("extended card")
        .json()
        .await
        .expect("extended json");
    assert_eq!(extended["result"]["name"], "standalone", "{extended}");

    // REST is served beneath the same prefix, with the prefix stripped before it routes.
    let rest = client
        .post(format!("{base}/agent/v1/message:send"))
        .header("A2A-Version", "1.0")
        .json(&serde_json::json!({
            "message": {"messageId": "m-2", "role": "ROLE_USER", "parts": [{"text": "rest"}]},
        }))
        .send()
        .await
        .expect("rest");
    assert!(rest.status().is_success(), "{}", rest.status());
    assert!(rest.text().await.unwrap().contains("you said: rest"));

    cancel.cancel();
    task.await
        .expect("join")
        .expect("an ordinary shutdown is Ok");
}

#[tokio::test]
async fn two_agents_on_one_mux_share_the_card_path_without_panicking() {
    let mux = Mux::new();
    let mut tasks = Vec::new();
    let cancel = CancellationToken::new();
    for (name, base) in [("first", "/one"), ("second", "/two")] {
        let cfg = ServerConfig {
            name: name.into(),
            base_path: base.into(),
            mux: Some(mux.clone()),
            serve_agent_card: true,
            ..Default::default()
        };
        // Sequential, so "first" mounts first; both ask for the well-known path.
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx = Mutex::new(Some(tx));
        let cfg = ServerConfig {
            on_ready: Some(Arc::new(move |_: &ServerConfig| {
                if let Some(tx) = tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
            })),
            ..cfg
        };
        tasks.push(tokio::spawn(a2a::start_server(
            cancel.clone(),
            cfg,
            replying_agent(),
        )));
        rx.await.expect("mounted");
    }

    let port = free_port().await;
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap();
    let router = mux.router();
    let serving = tokio::spawn(async move { axum::serve(listener, router).await });

    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let card: serde_json::Value = client
        .get(format!("{base}{}", a2a::AGENT_CARD_PATH))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        card["name"], "first",
        "the first agent to mount keeps the card"
    );
    for path in ["/one", "/two"] {
        let rpc = client
            .post(format!("{base}{path}"))
            .header("A2A-Version", "1.0")
            .json(&send_message("x"))
            .send()
            .await
            .unwrap();
        assert!(rpc.status().is_success(), "{path} must be mounted");
    }

    cancel.cancel();
    for task in tasks {
        task.await.unwrap().expect("a shared-mux server returns Ok");
    }
    serving.abort();
}

#[tokio::test]
async fn a_service_function_receives_the_assembled_config() {
    let port = free_port().await;
    let seen: Arc<Mutex<Option<ServerConfig>>> = Arc::default();
    let record = Arc::clone(&seen);
    let layered = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&layered);

    let serve = a2a::service_fn(move |cancel, mut cfg: ServerConfig| {
        *record.lock().unwrap() = Some(cfg.clone());
        cfg.skills = vec![a2a::skill("echo", "Echo", "from the function")];
        let count = Arc::clone(&count);
        cfg.handler_options.layers.push(Arc::new(move |inner| {
            count.fetch_add(1, Ordering::SeqCst);
            inner
        }));
        a2a::start_server(cancel, cfg, replying_agent())
    });

    let runtime =
        Runtime::new(config(port)).register(a2a::Service::from_fn(serve).base_path("/fn"));
    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.expect("start");

    // The runtime's identity and placement arrive on the config, with on_ready already set.
    let cfg = seen.lock().unwrap().clone().expect("the function ran");
    assert_eq!(cfg.name, "test-service");
    assert_eq!(cfg.version, "1.2.3");
    assert_eq!(cfg.base_path, "/fn");
    assert!(cfg.on_ready.is_some() && cfg.mux.is_some());
    assert_eq!(
        layered.load(Ordering::SeqCst),
        1,
        "the handler layer was applied"
    );

    let card: serde_json::Value =
        reqwest::get(format!("http://127.0.0.1:{port}{}", a2a::AGENT_CARD_PATH))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(card["skills"][0]["description"], "from the function");
    let endpoints = runtime.endpoints();
    assert_eq!(endpoints[0].url, format!("http://127.0.0.1:{port}/fn"));

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}
