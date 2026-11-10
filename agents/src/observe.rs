//! Server spans for every request the runtime's own listeners serve.
//!
//! With [`Config::telemetry`](crate::Config::telemetry) set, each request is a
//! server span, `agents.request`, continuing the caller's trace from its W3C
//! `traceparent` header, and a share of `runtime.operation.count` and
//! `runtime.operation.duration`, labeled component `agents`, backend `http`,
//! operation and outcome. It is runtime-go's `agents/observe.go`. A host that
//! owns the server through [`Config::mux`](crate::Config::mux) instruments it
//! itself.

use axum::{Router, extract::Request, middleware::Next, response::Response};
use telemetry::{
    FutureExt, KeyValue, Observer, Outcome, SpanKind, Start, Telemetry, extract,
    opentelemetry::propagation::Extractor,
};

/// `router` with every request traced through `telemetry`, or `router` as it is
/// when `telemetry` carries no signal.
pub(crate) fn traced(router: Router, telemetry: &Telemetry) -> Router {
    if !telemetry.is_enabled() {
        return router;
    }
    let observer = telemetry.observer("agents");
    router.layer(axum::middleware::from_fn(
        move |request: Request, next: Next| {
            let observer = observer.clone();
            async move { serve(observer, request, next).await }
        },
    ))
}

async fn serve(observer: Observer, request: Request, next: Next) -> Response {
    let parent = extract(&Headers(request.headers()));
    let op = observer.start_with(
        "http",
        "request",
        SpanKind::Server,
        Start {
            parent: Some(parent),
            attributes: vec![
                KeyValue::new("http.request.method", request.method().as_str().to_owned()),
                KeyValue::new("url.path", request.uri().path().to_owned()),
            ],
            ..Start::default()
        },
    );
    let response = next.run(request).with_context(op.context()).await;
    let status = response.status().as_u16();
    op.set_attribute(KeyValue::new(
        "http.response.status_code",
        i64::from(status),
    ));
    op.finish(outcome(status));
    response
}

/// Classifies a status the server sent: the caller's mistakes are not the
/// server's failures.
fn outcome(status: u16) -> Outcome {
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

struct Headers<'a>(&'a axum::http::HeaderMap);

impl Extractor for Headers<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(axum::http::HeaderName::as_str).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, routing::post};
    use telemetry::testing::Recorder;
    use tower::ServiceExt as _;

    #[tokio::test]
    async fn a_request_is_a_server_span_under_the_callers_trace() {
        let rec = Recorder::new();
        let router = traced(
            Router::new().route("/mcp", post(|| async { axum::http::StatusCode::ACCEPTED })),
            &rec.signals(),
        );
        let caller = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let request = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("traceparent", caller)
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::ACCEPTED);

        let span = rec.span("agents.request").expect("request span");
        assert_eq!(span.kind, SpanKind::Server);
        assert_eq!(span.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(span.parent_span_id, "00f067aa0ba902b7");
        assert_eq!(span.attributes["http.response.status_code"], "202");
        assert_eq!(rec.outcomes()["request"], "ok");
    }

    #[test]
    fn statuses_map_to_outcomes() {
        assert_eq!(outcome(200), Outcome::Ok);
        assert_eq!(outcome(404), Outcome::NotFound);
        assert_eq!(outcome(400), Outcome::Invalid);
        assert_eq!(outcome(503), Outcome::Error);
    }
}
