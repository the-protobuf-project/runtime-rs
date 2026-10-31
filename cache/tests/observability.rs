//! Shared telemetry records outcomes without exposing cache keys or bodies.
use cache::{
    CacheError, Result,
    core::{Document, Options},
    with_observability,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use telemetry::{Context, KeyValue, LogSink, Severity, Telemetry};

#[derive(Default)]
struct Logs(Mutex<Vec<String>>);
impl LogSink for Logs {
    fn emit(&self, _: &Context, _: Severity, event: &'static str, attrs: &[KeyValue]) {
        self.0.lock().unwrap().push(format!("{event} {attrs:?}"));
    }
}
struct Fake;
#[async_trait::async_trait]
impl Document for Fake {
    async fn create(&self, _: &[u8], _: &Options) -> Result<String> {
        Ok("secret-key".into())
    }
    async fn get(&self, _: &str, _: &mut Vec<u8>) -> Result<()> {
        Err(CacheError::NotFound)
    }
    async fn update(&self, _: &str, _: &[u8], _: &Options) -> Result<()> {
        Ok(())
    }
    async fn delete(&self, _: &str) -> Result<()> {
        Ok(())
    }
    async fn keys(&self) -> Result<Vec<String>> {
        Ok(vec!["secret-key".into()])
    }
    async fn list(&self) -> Result<Vec<Vec<u8>>> {
        Ok(vec![b"secret-body".to_vec()])
    }
    async fn ttl(&self, _: &str) -> Result<Duration> {
        Ok(Duration::from_secs(1))
    }
}
#[tokio::test]
async fn all_document_calls_share_the_runtime_policy() {
    let logs = Arc::new(Logs::default());
    let raw: Arc<dyn Document> = Arc::new(Fake);
    assert!(Arc::ptr_eq(
        &raw,
        &with_observability(raw.clone(), Telemetry::default())
    ));
    let document = with_observability(raw, Telemetry::default().with_log_sink(logs.clone()));
    let opts = Options::default();
    assert_eq!(
        document.create(b"secret-body", &opts).await.unwrap(),
        "secret-key"
    );
    assert!(matches!(
        document.get("secret-key", &mut Vec::new()).await,
        Err(CacheError::NotFound)
    ));
    document
        .update("secret-key", b"secret-body", &opts)
        .await
        .unwrap();
    document.delete("secret-key").await.unwrap();
    assert_eq!(document.keys().await.unwrap(), ["secret-key"]);
    assert_eq!(document.list().await.unwrap(), [b"secret-body".to_vec()]);
    assert_eq!(
        document.ttl("secret-key").await.unwrap(),
        Duration::from_secs(1)
    );
    let events = logs.0.lock().unwrap();
    assert_eq!(events.len(), 7);
    assert!(events.iter().any(|event| event.contains("not_found")));
    assert!(
        events
            .iter()
            .all(|event| !event.contains("secret-key") && !event.contains("secret-body"))
    );
}
