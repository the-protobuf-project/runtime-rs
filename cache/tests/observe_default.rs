//! Every strategy of a built database reports through the process's default signals,
//! labeled with its backend, and observing it again replaces rather than stacks.
use cache::{
    DatabaseSpec, build_database,
    core::{Capabilities, MemoryDriver, Options},
    instrument,
};
use std::sync::{Arc, Mutex};
use telemetry::{Context, KeyValue, LogSink, Severity, Telemetry};

#[derive(Default)]
struct Logs(Mutex<Vec<String>>);
impl LogSink for Logs {
    fn emit(&self, _: &Context, _: Severity, event: &'static str, attrs: &[KeyValue]) {
        if event == "runtime.operation.completed" {
            let get = |k: &str| {
                attrs
                    .iter()
                    .find(|kv| kv.key.as_str() == k)
                    .map(|kv| kv.value.to_string())
                    .unwrap_or_default()
            };
            let line = format!("{} {} {}", get("backend"), get("operation"), get("outcome"));
            self.0.lock().unwrap().push(line);
        }
    }
}

fn signals() -> (Telemetry, Arc<Logs>) {
    let logs = Arc::new(Logs::default());
    (Telemetry::default().with_log_sink(logs.clone()), logs)
}

fn memory() -> cache::DB {
    let driver = Arc::new(MemoryDriver::new());
    build_database(driver, Capabilities::new(), DatabaseSpec::default())
}

#[tokio::test]
async fn every_strategy_reports_through_the_process_default() {
    let (default, logs) = signals();
    telemetry::set_process_default(default);
    let db = memory();
    telemetry::set_process_default(Telemetry::default());

    let opts = Options::default();
    let id = db.document.create(b"a", &opts).await.unwrap();
    db.document.get(&id, &mut Vec::new()).await.unwrap();
    assert!(db.document.get("missing", &mut Vec::new()).await.is_err());
    db.volatile.set("k", b"v", &opts).await.unwrap();
    let aside = db.aside(Arc::new(|_: String| {
        Box::pin(async { Ok(br#""loaded""#.to_vec()) })
    }));
    aside
        .get_or_load("x", &mut Vec::new(), &opts)
        .await
        .unwrap();

    let got = logs.0.lock().unwrap().clone();
    for want in [
        "memory create ok",
        "memory get ok",
        "memory get not_found",
        "memory set ok",
        "memory get_or_load ok",
    ] {
        assert!(got.iter().any(|l| l == want), "missing {want:?} in {got:?}");
    }
}

#[tokio::test]
async fn observing_again_replaces_rather_than_stacks() {
    let ((first, first_logs), (second, second_logs)) = (signals(), signals());
    let db = instrument(instrument(memory(), first), second);
    db.volatile
        .set("k", b"v", &Options::default())
        .await
        .unwrap();
    assert!(first_logs.0.lock().unwrap().is_empty());
    assert_eq!(*second_logs.0.lock().unwrap(), ["memory set ok"]);
}
