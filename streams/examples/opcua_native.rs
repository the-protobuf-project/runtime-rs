//! The native reference for the OPC UA throughput bench: async-opcua's own
//! client doing what `--backend opcua --message opcua --scenario fanout` does,
//! without this crate's driver.
//!
//! It opens one session, puts one data-change monitored item per node in one
//! subscription, then has `--publishers` tasks write Int64 sequence numbers
//! (with the send time as source timestamp) to the nodes round-robin, each
//! write waiting for the server's Write response, and counts the
//! notifications that come back. It prints writes/s, notifications/s, loss,
//! and latency in the bench's terms. There is no standard OPC UA load tool, so
//! this is the baseline the driver's overhead is measured against.
//!
//! ```text
//! docker compose -f streams/docker/compose.opcua.yaml up -d --wait
//! cargo run --release -p tpp-streams --features opcua --example opcua_native -- \
//!     --messages 20000 --publishers 32 --subjects 10
//! ```
//!
//! `OPCUA_URL` (default `opc.tcp://127.0.0.1:14840`), `OPCUA_PUBLISHING_MS`
//! (default 20) and `--batch N` (N values per Write request) shape it the
//! same way as the bench.

use std::{
    collections::HashSet,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use futures::StreamExt;
use opcua::{
    client::{ClientBuilder, DataChangeCallback, IdentityToken},
    crypto::SecurityPolicy,
    types::{
        AttributeId, DataChangeFilter, DataChangeTrigger, DataValue, DateTime, ExtensionObject,
        MessageSecurityMode, MonitoredItemCreateRequest, MonitoringMode, MonitoringParameters,
        NodeId, NumericRange, TimestampsToReturn, UserTokenPolicy, Variant, WriteValue,
    },
};

/// The number after `name` on the command line, or `default` when it is
/// missing or unparsable.
fn arg(name: &str, default: u64) -> u64 {
    let mut args = std::env::args().skip(1);
    while let Some(key) = args.next() {
        if key == name {
            return args.next().and_then(|v| v.parse().ok()).unwrap_or(default);
        }
    }
    default
}

/// Runs the fanout bench with the raw async-opcua client and prints a report
/// shaped like the bench's, for side-by-side comparison.
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("OPCUA_URL").unwrap_or_else(|_| "opc.tcp://127.0.0.1:14840".into());
    let publishing: u64 = std::env::var("OPCUA_PUBLISHING_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let messages = arg("--messages", 20_000);
    let publishers = arg("--publishers", 32);
    if publishers == 0 {
        return Err("--publishers must be at least 1: nothing would write".into());
    }
    let subjects = arg("--subjects", 10) as usize;
    let batch = arg("--batch", 1).max(1);
    let verbose = std::env::args().any(|a| a == "--verbose");

    let mut client = ClientBuilder::new()
        .application_name("tpp-streams native reference")
        .application_uri("urn:tpp-streams:native")
        .pki_dir(std::env::temp_dir().join("tpp-streams-opcua-native"))
        .create_sample_keypair(false)
        .trust_server_certs(true)
        .session_retry_limit(3)
        .client()
        .map_err(|e| e.join("; "))?;
    let (session, event_loop) = client
        .connect_to_matching_endpoint(
            (
                url.as_str(),
                SecurityPolicy::None.to_str(),
                MessageSecurityMode::None,
                UserTokenPolicy::anonymous(),
            ),
            IdentityToken::Anonymous,
        )
        .await?;
    tokio::spawn(async move {
        let mut events = std::pin::pin!(event_loop.enter());
        while let Some(event) = events.next().await {
            if verbose {
                eprintln!("event loop: {event:?}");
            }
            if event.is_err() {
                eprintln!("event loop ended: {event:?}");
                return;
            }
        }
    });
    session.wait_for_connection().await;
    let ns = session
        .get_namespace_index("http://microsoft.com/Opc/OpcPlc/")
        .await?;
    let nodes: Vec<NodeId> = (0..subjects)
        .map(|i| NodeId::from_str(&format!("ns={ns};s=bench.s{i:02}")).unwrap())
        .collect();

    // What arrives: sequence numbers and their latencies. Only values this
    // run wrote count: each carries its send time as source timestamp, and
    // an initial value, which the server reports whenever it gets to it,
    // carries an older one.
    let seen = Arc::new(Mutex::new((Vec::<u64>::new(), Vec::<u32>::new())));
    let received = Arc::new(AtomicU64::new(0));
    let since = Arc::new(AtomicI64::new(i64::MAX));
    let (seen_cb, received_cb, since_cb) = (seen.clone(), received.clone(), since.clone());
    let subscription = session
        .create_subscription(
            Duration::from_millis(publishing),
            60,
            10,
            // The driver's default cap, for the same reason: see
            // OpcUaConfig::max_notifications_per_publish.
            1000,
            0,
            true,
            DataChangeCallback::new(move |value: DataValue, _| {
                let Some(Variant::Int64(seq)) = value.value else {
                    return;
                };
                if value
                    .source_timestamp
                    .is_none_or(|t| t.ticks() < since_cb.load(Ordering::Relaxed))
                {
                    return;
                }
                let latency = value
                    .source_timestamp
                    .and_then(|t| (DateTime::now().as_chrono() - t.as_chrono()).to_std().ok())
                    .map(|d| d.as_micros().min(u128::from(u32::MAX)) as u32);
                let mut seen = seen_cb.lock().unwrap();
                seen.0.push(seq as u64);
                if let Some(l) = latency {
                    seen.1.push(l);
                }
                received_cb.fetch_add(1, Ordering::Relaxed);
            }),
        )
        .await?;
    let items: Vec<MonitoredItemCreateRequest> = nodes
        .iter()
        .map(|node| {
            MonitoredItemCreateRequest::new(
                node.clone().into(),
                MonitoringMode::Reporting,
                MonitoringParameters {
                    client_handle: 0,
                    sampling_interval: 0.0,
                    filter: ExtensionObject::from_message(DataChangeFilter {
                        trigger: DataChangeTrigger::StatusValueTimestamp,
                        deadband_type: 0,
                        deadband_value: 0.0,
                    }),
                    queue_size: 1024,
                    discard_oldest: true,
                },
            )
        })
        .collect();
    let created = session
        .create_monitored_items(subscription, TimestampsToReturn::Both, items)
        .await?;
    if created.len() != nodes.len() {
        return Err(format!("monitored {} of {} nodes", created.len(), nodes.len()).into());
    }
    for (node, item) in nodes.iter().zip(&created) {
        if item.result.status_code.is_bad() {
            return Err(format!("cannot monitor {node}: {}", item.result.status_code).into());
        }
    }

    let next = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let first_error = Arc::new(Mutex::new(None::<String>));
    since.store(DateTime::now().ticks(), Ordering::Relaxed);
    let start = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..publishers {
        let (session, nodes, next, errors, first_error) = (
            session.clone(),
            nodes.clone(),
            next.clone(),
            errors.clone(),
            first_error.clone(),
        );
        tasks.push(tokio::spawn(async move {
            loop {
                let first = next.fetch_add(batch, Ordering::Relaxed);
                if first >= messages {
                    return;
                }
                let last = (first + batch).min(messages);
                let node = &nodes[((first / batch) as usize) % nodes.len()];
                let writes: Vec<WriteValue> = (first..last)
                    .map(|seq| WriteValue {
                        node_id: node.clone(),
                        attribute_id: AttributeId::Value as u32,
                        index_range: NumericRange::None,
                        value: DataValue {
                            value: Some((seq as i64).into()),
                            source_timestamp: Some(DateTime::now()),
                            ..DataValue::default()
                        },
                    })
                    .collect();
                match session.write(&writes).await {
                    Ok(results) => {
                        let bad = results.iter().filter(|s| s.is_bad()).count() as u64;
                        errors.fetch_add(bad, Ordering::Relaxed);
                    }
                    Err(e) => {
                        errors.fetch_add(last - first, Ordering::Relaxed);
                        first_error.lock().unwrap().get_or_insert(e.to_string());
                    }
                }
            }
        }));
    }
    for t in tasks {
        t.await?;
    }
    let publish_secs = start.elapsed().as_secs_f64();
    let published = messages - errors.load(Ordering::Relaxed);
    let (mut last, mut quiet) = (0, Instant::now());
    loop {
        let got = received.load(Ordering::Relaxed);
        if got >= published || quiet.elapsed() > Duration::from_secs(5) {
            break;
        }
        if got != last {
            (last, quiet) = (got, Instant::now());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let e2e_secs = start.elapsed().as_secs_f64();
    let (seqs, mut latencies) = std::mem::take(&mut *seen.lock().unwrap());
    let unique = seqs.iter().collect::<HashSet<_>>().len() as u64;
    latencies.sort_unstable();
    let pct = |p: f64| {
        (!latencies.is_empty()).then(|| {
            latencies[((latencies.len() as f64 - 1.0) * p).round() as usize] as f64 / 1000.0
        })
    };
    println!(
        "{}",
        serde_json::json!({
            "label": "opcua-native",
            "messages": messages,
            "publishers": publishers,
            "subjects": subjects,
            "batch": batch,
            "published": published,
            "publish_errors": errors.load(Ordering::Relaxed),
            "first_error": *first_error.lock().unwrap(),
            "publish_rate": (published as f64 / publish_secs * 100.0).round() / 100.0,
            "delivered": seqs.len(),
            "unique": unique,
            "lost": published.saturating_sub(unique),
            "delivered_rate": (unique as f64 / e2e_secs * 100.0).round() / 100.0,
            "latency_ms": { "p50": pct(0.5), "p95": pct(0.95), "p99": pct(0.99), "max": pct(1.0) },
        })
    );
    let _ = session.delete_subscription(subscription).await;
    session.disconnect().await?;
    Ok(())
}
