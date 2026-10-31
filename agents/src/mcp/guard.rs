//! The `Host` check for the legacy SSE transport, which rmcp's Streamable HTTP server does for
//! itself: a request naming a host the server was not told it answers to is refused, so a page
//! on another site cannot reach this server through a DNS name it controls (DNS rebinding).

use axum::Router;
use axum::extract::Request;
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::IntoResponse;

/// `router`, answering only requests whose `Host` is in `allowed`: a name, matching any port,
/// or `name:port`, matching that port only. Names compare case-insensitively.
pub(crate) fn hosts(router: Router, allowed: Vec<String>) -> Router {
    let allowed: Vec<String> = allowed
        .into_iter()
        .map(|h| h.to_ascii_lowercase())
        .collect();
    router.layer(axum::middleware::from_fn(
        move |req: Request, next: Next| {
            let ok = req
                .headers()
                .get(header::HOST)
                .and_then(|h| h.to_str().ok())
                .is_some_and(|h| permitted(&allowed, &h.to_ascii_lowercase()));
            async move {
                if ok {
                    next.run(req).await
                } else {
                    (
                        StatusCode::FORBIDDEN,
                        "Host is not one this server answers to",
                    )
                        .into_response()
                }
            }
        },
    ))
}

/// Whether `host`, as a `Host` header gives it, is one of `allowed`.
fn permitted(allowed: &[String], host: &str) -> bool {
    let name = match host.rsplit_once(':') {
        // An IPv6 literal keeps its brackets and may carry a port after them.
        Some((name, port)) if !port.contains(']') && port.bytes().all(|b| b.is_ascii_digit()) => {
            name
        }
        _ => host,
    };
    allowed.iter().any(|a| a == host || a == name)
}

#[cfg(test)]
mod tests {
    use super::permitted;

    fn allowed() -> Vec<String> {
        vec![
            "localhost".into(),
            "bacnet.example.com:443".into(),
            "[::1]".into(),
        ]
    }

    #[test]
    fn a_name_allows_any_port_and_a_name_with_a_port_only_that_one() {
        assert!(permitted(&allowed(), "localhost"));
        assert!(permitted(&allowed(), "localhost:8787"));
        assert!(permitted(&allowed(), "bacnet.example.com:443"));
        assert!(!permitted(&allowed(), "bacnet.example.com:80"));
        assert!(!permitted(&allowed(), "evil.example.net"));
    }

    #[test]
    fn an_ipv6_literal_is_matched_with_its_brackets() {
        assert!(permitted(&allowed(), "[::1]:8080"));
        assert!(permitted(&allowed(), "[::1]"));
    }
}
