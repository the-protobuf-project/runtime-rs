use std::time::Duration;

use telemetry::{
    KeyValue, Observer, Operation, Outcome, SpanKind, Start, Telemetry, inject,
    opentelemetry::propagation::Injector,
};

use crate::error::Result;

/// Returns a [`reqwest::Client`] with a dedicated pooled connection pool sized for concurrent
/// single-host traffic (the normal shape for a GraphQL or REST backend), rather than sharing
/// reqwest's small process-wide default pool.
pub(crate) fn new_pooled_client(timeout: Duration) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(timeout)
        .pool_max_idle_per_host(100)
        .build()?)
}

/// Collects the trace headers a request carries to its server.
#[derive(Default)]
pub(crate) struct HeaderCarrier(pub Vec<(String, String)>);

impl Injector for HeaderCarrier {
    fn set(&mut self, key: &str, value: String) {
        self.0.push((key.to_string(), value));
    }
}

/// The observer a client reports through, built once when it connects;
/// `None` when the options carry no signal.
pub(crate) fn observer(telemetry: &Telemetry) -> Option<Observer> {
    telemetry
        .is_enabled()
        .then(|| telemetry.observer("network"))
}

/// One traced request: its operation and the headers carrying its trace.
pub(crate) struct Traced {
    op: Option<Operation>,
    /// The trace headers to send with the request.
    pub headers: Vec<(String, String)>,
}

impl Traced {
    /// Starts `operation` on `backend` for a request of `method` to `url`, a
    /// client span whose context the returned headers carry.
    pub(crate) fn start(
        observer: Option<&Observer>,
        backend: &'static str,
        operation: &'static str,
        method: &str,
        url: &str,
    ) -> Self {
        let Some(observer) = observer else {
            return Self {
                op: None,
                headers: Vec::new(),
            };
        };
        let mut attributes = vec![KeyValue::new("http.request.method", method.to_owned())];
        if let Ok(parsed) = url::Url::parse(url) {
            if let Some(host) = parsed.host_str() {
                attributes.push(KeyValue::new("server.address", host.to_owned()));
            }
            attributes.push(KeyValue::new("url.path", parsed.path().to_owned()));
        }
        let op = observer.start_with(
            backend,
            operation,
            SpanKind::Client,
            Start {
                attributes,
                ..Start::default()
            },
        );
        let mut carrier = HeaderCarrier::default();
        inject(&op.context(), &mut carrier);
        Self {
            op: Some(op),
            headers: carrier.0,
        }
    }

    /// Finishes with the server's answer.
    pub(crate) fn answered(self, status: u16) {
        if let Some(op) = self.op {
            op.set_attribute(KeyValue::new(
                "http.response.status_code",
                i64::from(status),
            ));
            op.finish(status_outcome(status));
        }
    }

    /// Finishes with a request that got no answer.
    pub(crate) fn failed(self, timed_out: bool) {
        if let Some(op) = self.op {
            op.finish(if timed_out {
                Outcome::Timeout
            } else {
                Outcome::Error
            });
        }
    }
}

/// Classifies a status the server sent: a 404 is a missing record, a 409 or
/// 412 a conflict, any other 4xx the caller's input, a 5xx the server's error.
pub(crate) fn status_outcome(status: u16) -> Outcome {
    match status {
        0..=399 => Outcome::Ok,
        404 => Outcome::NotFound,
        409 | 412 => Outcome::Conflict,
        408 | 504 => Outcome::Timeout,
        501 => Outcome::Unsupported,
        400..=499 => Outcome::Invalid,
        _ => Outcome::Error,
    }
}
