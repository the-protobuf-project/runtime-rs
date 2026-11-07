//! Throughput and latency benchmark for every stream provider.
//!
//! One harness drives every backend through the `streams` contract, so the
//! numbers compare like with like: the same messages, the same subjects, the
//! same verification. Run from the workspace root, in release mode:
//!
//! ```text
//! cargo bench -p tpp-streams --features <backend>,protobuf,live-tests[,tls] \
//!     --bench throughput -- --backend redis --scenario fanout --messages 100000
//! ```
//!
//! # Messages
//!
//! Payloads are the messages runtime-go's `codec/protobuf` tests use:
//! `google.protobuf.StringValue` (a body of `--payload` bytes) or
//! `google.protobuf.Int64Value` (the sequence number). prost encodes `String`
//! and `i64` as exactly those wrapper types, so the bytes on the wire are what a
//! Go publisher using `protobuf.Codec` sends. Every message is decoded with the
//! protobuf codec on receipt, so decode cost is part of what is measured.
//!
//! Single publishes pin the message id to `<seq>.<nanos since start>`, which
//! carries the sequence and send time without changing the payload. A batch
//! cannot pin ids, so `batch` publishes Int64Value and reads the sequence back
//! out of the payload; it reports throughput and loss, not latency.
//!
//! # Scenarios
//!
//! - `fanout`: one plain subscriber per subject; publishers spread messages
//!   round-robin over the subjects. A plain subscription may lose under load:
//!   that is reported as `lost`, not hidden.
//! - `batch`: `fanout`, publishing through `Batch` in groups of `--batch`.
//! - `durable`: `--consumers` durable consumers per subject sharing one name,
//!   every message acked after it is read; every sequence must arrive.
//! - `group`: `--consumers` plain subscribers per subject in one group; every
//!   message must arrive once across the group.
//!
//! Each run prints one JSON object and appends it to
//! `target/bench-results.jsonl` at the workspace root.

mod targets;

use std::{
    collections::HashSet,
    io::Write as _,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use futures::{StreamExt, stream::BoxStream};
use streams::{
    Delivery, Manager, Message, Options, Payload, Position, Protobuf, Registry, Stream, as_batch,
    as_durable, as_positioned,
};

/// The instant every send time is measured from. Publisher and receiver share
/// one process, so one monotonic clock gives an honest end-to-end latency.
static START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// What a run is asked to do.
#[derive(Clone, Debug)]
struct Args {
    backend: String,
    profile: String,
    scenario: String,
    messages: u64,
    subjects: usize,
    publishers: usize,
    consumers: usize,
    batch: usize,
    payload: usize,
    message: String,
    rate: u64,
    timeout: Duration,
    label: String,
}

impl Args {
    fn parse() -> Self {
        let mut a = Args {
            backend: "redis".into(),
            profile: "plain".into(),
            scenario: "fanout".into(),
            messages: 20_000,
            subjects: 10,
            publishers: 32,
            consumers: 1,
            batch: 100,
            payload: 256,
            message: "string".into(),
            rate: 0,
            timeout: Duration::from_secs(300),
            label: String::new(),
        };
        let mut it = std::env::args().skip(1);
        while let Some(key) = it.next() {
            // cargo bench appends --bench to the binary's arguments.
            if key == "--bench" {
                continue;
            }
            let value = it.next().unwrap_or_else(|| panic!("{key} needs a value"));
            let n = || -> u64 {
                value
                    .replace('_', "")
                    .parse()
                    .unwrap_or_else(|_| panic!("{key}: {value:?} is not a number"))
            };
            match key.as_str() {
                "--backend" => a.backend = value.clone(),
                "--profile" => a.profile = value.clone(),
                "--scenario" => a.scenario = value.clone(),
                "--messages" => a.messages = n(),
                "--subjects" => a.subjects = n() as usize,
                "--publishers" => a.publishers = n() as usize,
                "--consumers" => a.consumers = n() as usize,
                "--batch" => a.batch = n() as usize,
                "--payload" => a.payload = n() as usize,
                "--message" => a.message = value.clone(),
                "--rate" => a.rate = n(),
                "--timeout" => a.timeout = Duration::from_secs(n()),
                "--label" => a.label = value.clone(),
                other => panic!("unknown argument {other}"),
            }
        }
        assert!(a.subjects > 0 && a.publishers > 0 && a.consumers > 0 && a.batch > 0);
        assert!(
            matches!(a.scenario.as_str(), "fanout" | "batch" | "durable" | "group"),
            "--scenario is fanout, batch, durable, or group"
        );
        if a.scenario == "batch" {
            a.message = "int64".into();
        }
        assert!(matches!(a.message.as_str(), "string" | "int64"), "--message is string or int64");
        a
    }

    /// Receivers per subject.
    fn receivers_per_subject(&self) -> usize {
        match self.scenario.as_str() {
            "durable" | "group" => self.consumers,
            _ => 1,
        }
    }
}

/// The codecs every bench receiver accepts: JSON and protobuf.
fn registry() -> Registry {
    Registry::new().with("protobuf")
}

/// Encodes message `seq` as one of runtime-go's codec test messages.
fn payload(args: &Args, seq: u64, body: &str) -> Payload {
    match args.message.as_str() {
        "int64" => Payload::encode(&Protobuf, &(seq as i64)).expect("encode Int64Value"),
        _ => Payload::encode(&Protobuf, &body.to_owned()).expect("encode StringValue"),
    }
}

/// The pinned id carrying sequence and send time.
fn pinned_id(seq: u64) -> String {
    format!("{seq}.{}", START.elapsed().as_nanos())
}

/// Decodes a delivered message as a real consumer would, returning its
/// sequence and, when the id carries a send time, its end-to-end latency.
fn read(args: &Args, msg: &Message) -> Option<(u64, Option<Duration>)> {
    let pinned = msg.id().split_once('.').and_then(|(seq, sent)| {
        Some((seq.parse::<u64>().ok()?, Duration::from_nanos(sent.parse().ok()?)))
    });
    match args.message.as_str() {
        "int64" => {
            let seq = u64::try_from(msg.decode::<i64, _>(&Protobuf).ok()?).ok()?;
            match pinned {
                Some((pseq, _)) if pseq != seq => None,
                Some((_, sent)) => Some((seq, Some(START.elapsed().saturating_sub(sent)))),
                None => Some((seq, None)),
            }
        }
        _ => {
            let body: String = msg.decode(&Protobuf).ok()?;
            let (seq, sent) = pinned?;
            (body.len() == args.payload).then(|| (seq, Some(START.elapsed().saturating_sub(sent))))
        }
    }
}

/// What one receiver saw. Kept behind a mutex the run holds, so the results
/// survive the receiver task being aborted at the end.
#[derive(Default)]
struct Received {
    seqs: Vec<u64>,
    latencies_us: Vec<u32>,
    undecodable: u64,
    ack_errors: u64,
}

/// Counters every task in a run shares.
#[derive(Default)]
struct Shared {
    published: AtomicU64,
    publish_errors: AtomicU64,
    received: AtomicU64,
    first_error: Mutex<Option<String>>,
}

impl Shared {
    fn error(&self, n: u64, e: impl std::fmt::Display) {
        self.publish_errors.fetch_add(n, Ordering::Relaxed);
        self.first_error.lock().unwrap().get_or_insert_with(|| e.to_string());
    }
}

fn subject(i: usize) -> String {
    format!("bench.s{i:02}")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    LazyLock::force(&START);
    let args = Args::parse();
    let report = run(&args).await;
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/bench-results.jsonl");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("open results file");
    writeln!(file, "{}", serde_json::to_string(&report).unwrap()).unwrap();
}

async fn run(args: &Args) -> serde_json::Value {
    let conn = targets::connect(&args.backend, &args.profile, registry())
        .await
        .unwrap_or_else(|e| panic!("connect {}/{}: {e}", args.backend, args.profile));
    let subjects: Vec<String> = (0..args.subjects).map(subject).collect();
    let stream = conn
        .publisher
        .create(Stream {
            name: format!("bench-{}", args.scenario),
            subjects: subjects.clone(),
            active: true,
            ..Stream::default()
        })
        .await
        .expect("create stream");
    if let Some(sub) = &conn.subscriber {
        // Brokerless backends keep stream declarations on each side.
        sub.create(stream.clone()).await.expect("declare stream on the subscriber side");
    }
    let publisher: Arc<dyn Manager> = conn.publisher.bind(&stream.id).await.expect("bind publisher");
    let receiver: Arc<dyn Manager> = match &conn.subscriber {
        Some(sub) => sub.bind(&stream.id).await.expect("bind subscriber"),
        None => publisher.clone(),
    };

    // Attach every receiver before anything is published.
    let shared = Arc::new(Shared::default());
    let mut slots = Vec::new();
    let mut tasks = Vec::new();
    for s in &subjects {
        for _ in 0..args.receivers_per_subject() {
            let slot = Arc::new(Mutex::new(Received::default()));
            let messages = attach(args, receiver.as_ref(), s).await;
            tasks.push(tokio::spawn(receive(args.clone(), messages, slot.clone(), shared.clone())));
            slots.push(slot);
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish.
    let body = "x".repeat(args.payload);
    let next = Arc::new(AtomicU64::new(0));
    let pub_start = Instant::now();
    let mut pubs = Vec::new();
    for _ in 0..args.publishers {
        let (args, publisher, shared, next, subjects, body) = (
            args.clone(),
            publisher.clone(),
            shared.clone(),
            next.clone(),
            subjects.clone(),
            body.clone(),
        );
        pubs.push(tokio::spawn(async move {
            let step = if args.scenario == "batch" { args.batch as u64 } else { 1 };
            loop {
                let first = next.fetch_add(step, Ordering::Relaxed);
                if first >= args.messages {
                    break;
                }
                let last = (first + step).min(args.messages);
                if args.rate > 0 {
                    let due = pub_start + Duration::from_secs_f64(first as f64 / args.rate as f64);
                    tokio::time::sleep_until(due.into()).await;
                }
                let subject = &subjects[((first / step) as usize) % subjects.len()];
                if args.scenario == "batch" {
                    let payloads = (first..last).map(|seq| payload(&args, seq, "")).collect();
                    let result = match as_batch(publisher.as_ref()) {
                        Ok(b) => b.publish_batch(subject, payloads, &Options::default()).await,
                        Err(e) => Err(e),
                    };
                    match result {
                        Ok(ids) => {
                            shared.published.fetch_add(ids.len() as u64, Ordering::Relaxed);
                        }
                        Err(e) => shared.error(last - first, e),
                    }
                } else {
                    let opts = Options::default().with_id(pinned_id(first));
                    match publisher.publish(subject, payload(&args, first, &body), &opts).await {
                        Ok(_) => {
                            shared.published.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => shared.error(1, e),
                    }
                }
            }
        }));
    }
    for p in pubs {
        p.await.expect("publisher task panicked");
    }
    let publish_secs = pub_start.elapsed().as_secs_f64();
    let published = shared.published.load(Ordering::Relaxed);

    // Wait until everything published has arrived, or nothing has for 5s.
    let deadline = pub_start + args.timeout;
    let (mut last_count, mut quiet_since) = (0, Instant::now());
    loop {
        let got = shared.received.load(Ordering::Relaxed);
        if got >= published || Instant::now() > deadline {
            break;
        }
        if got != last_count {
            (last_count, quiet_since) = (got, Instant::now());
        } else if quiet_since.elapsed() > Duration::from_secs(5) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let e2e_secs = pub_start.elapsed().as_secs_f64();

    for t in tasks {
        t.abort();
        let _ = t.await;
    }
    let mut all = Received::default();
    for slot in slots {
        let mut part = slot.lock().unwrap();
        all.seqs.append(&mut part.seqs);
        all.latencies_us.append(&mut part.latencies_us);
        all.undecodable += part.undecodable;
        all.ack_errors += part.ack_errors;
    }
    let cleanup = conn.publisher.delete(&stream.id).await.err().map(|e| e.to_string());
    let _ = conn.publisher.close().await;
    if let Some(sub) = &conn.subscriber {
        let _ = sub.close().await;
    }
    let first_error = shared.first_error.lock().unwrap().clone();
    summarize(args, &shared, published, publish_secs, e2e_secs, all, first_error, cleanup)
}

/// One received message: lent, for a durable consumer, or handed over.
enum Delivered {
    Plain(Message),
    Lent(Delivery),
}

async fn attach(args: &Args, m: &dyn Manager, subject: &str) -> BoxStream<'static, Delivered> {
    let opts = Options::default().with_prefetch(1024);
    match args.scenario.as_str() {
        "durable" => {
            let name = format!("bench-{}", subject.replace('.', "-"));
            let deliveries = match as_positioned(m) {
                Ok(p) => p.consume_from(subject, &name, Position::Earliest, &opts).await,
                Err(_) => match as_durable(m) {
                    Ok(d) => d.consume(subject, &name, &opts).await,
                    Err(e) => Err(e),
                },
            }
            .unwrap_or_else(|e| panic!("consume {subject}: {e}"));
            deliveries.map(Delivered::Lent).boxed()
        }
        "group" => m
            .subscribe(subject, &opts.with_group("bench-group"))
            .await
            .unwrap_or_else(|e| panic!("subscribe {subject} in a group: {e}"))
            .map(Delivered::Plain)
            .boxed(),
        _ => m
            .subscribe(subject, &opts)
            .await
            .unwrap_or_else(|e| panic!("subscribe {subject}: {e}"))
            .map(Delivered::Plain)
            .boxed(),
    }
}

async fn receive(
    args: Args,
    mut messages: BoxStream<'static, Delivered>,
    slot: Arc<Mutex<Received>>,
    shared: Arc<Shared>,
) {
    while let Some(d) = messages.next().await {
        let message = match &d {
            Delivered::Plain(m) => m,
            Delivered::Lent(d) => d.message(),
        };
        let read = read(&args, message);
        // Settle after the work, as a real consumer should.
        let acked = match d {
            Delivered::Lent(d) => d.ack().await.is_ok(),
            Delivered::Plain(_) => true,
        };
        {
            let mut s = slot.lock().unwrap();
            match read {
                Some((seq, latency)) => {
                    s.seqs.push(seq);
                    if let Some(l) = latency {
                        s.latencies_us.push(l.as_micros().min(u128::from(u32::MAX)) as u32);
                    }
                }
                None => s.undecodable += 1,
            }
            if !acked {
                s.ack_errors += 1;
            }
        }
        shared.received.fetch_add(1, Ordering::Relaxed);
    }
}

#[allow(clippy::too_many_arguments)]
fn summarize(
    args: &Args,
    shared: &Shared,
    published: u64,
    publish_secs: f64,
    e2e_secs: f64,
    mut all: Received,
    first_error: Option<String>,
    cleanup_error: Option<String>,
) -> serde_json::Value {
    let delivered = all.seqs.len() as u64;
    let unique = all.seqs.iter().collect::<HashSet<_>>().len() as u64;
    let lost = published.saturating_sub(unique);
    let publish_errors = shared.publish_errors.load(Ordering::Relaxed);
    all.latencies_us.sort_unstable();
    let pct = |p: f64| -> Option<f64> {
        let l = &all.latencies_us;
        (!l.is_empty()).then(|| l[((l.len() as f64 - 1.0) * p).round() as usize] as f64 / 1000.0)
    };
    let ok = lost == 0
        && publish_errors == 0
        && all.undecodable == 0
        && all.ack_errors == 0
        && (args.scenario != "group" || delivered == unique);
    serde_json::json!({
        "label": args.label,
        "backend": args.backend,
        "profile": args.profile,
        "scenario": args.scenario,
        "message": if args.message == "int64" { "google.protobuf.Int64Value" } else { "google.protobuf.StringValue" },
        "payload_bytes": if args.message == "int64" { 0 } else { args.payload },
        "messages": args.messages,
        "subjects": args.subjects,
        "publishers": args.publishers,
        "receivers_per_subject": args.receivers_per_subject(),
        "batch": if args.scenario == "batch" { args.batch } else { 1 },
        "rate_target": args.rate,
        "published": published,
        "publish_errors": publish_errors,
        "first_error": first_error,
        "publish_secs": round(publish_secs),
        "publish_rate": round(published as f64 / publish_secs.max(1e-9)),
        "delivered": delivered,
        "unique": unique,
        "duplicates": delivered - unique,
        "lost": lost,
        "undecodable": all.undecodable,
        "ack_errors": all.ack_errors,
        "e2e_secs": round(e2e_secs),
        "delivered_rate": round(unique as f64 / e2e_secs.max(1e-9)),
        "latency_ms": { "p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99), "max": pct(1.0) },
        "cleanup_error": cleanup_error,
        "ok": ok,
    })
}

fn round(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}
