//! The `Host` and `Origin` checks for legacy SSE, which rmcp's Streamable HTTP server does for
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

/// Applies the configured origin restriction to every SSE method, allowing non-browser clients.
pub(crate) fn origins(router: Router, allowed: Vec<String>) -> Router {
    if allowed.is_empty() {
        return router;
    }
    router.layer(axum::middleware::from_fn(
        move |req: Request, next: Next| {
            let ok = req.headers().get(header::ORIGIN).is_none_or(|origin| {
                origin
                    .to_str()
                    .is_ok_and(|origin| origin_permitted(&allowed, origin))
            });
            async move {
                if ok {
                    next.run(req).await
                } else {
                    (StatusCode::FORBIDDEN, "Origin is not allowed").into_response()
                }
            }
        },
    ))
}

/// Matches rmcp's origin allowlist: schemes and hosts ignore case; `:*` allows any port.
fn origin_permitted(allowed: &[String], origin: &str) -> bool {
    let origin = origin.trim();
    if origin.eq_ignore_ascii_case("null") {
        return allowed
            .iter()
            .any(|a| a.trim().eq_ignore_ascii_case("null"));
    }
    let parse = |value: &str| {
        let uri: http::Uri = value.parse().ok()?;
        Some((
            uri.scheme_str()?.to_ascii_lowercase(),
            uri.host()?.trim_matches(['[', ']']).to_ascii_lowercase(),
            uri.port_u16(),
        ))
    };
    let Some((scheme, host, port)) = parse(origin) else {
        return false;
    };
    allowed.iter().any(|entry| {
        let entry = entry.trim();
        let wildcard = entry.strip_suffix(":*");
        let base = wildcard.unwrap_or(entry);
        let Some((a_scheme, a_host, a_port)) = parse(base) else {
            return false;
        };
        if wildcard.is_some() && a_port.is_some() {
            return false;
        }
        scheme == a_scheme
            && host == a_host
            && a_port.is_none_or(|allowed| {
                Some(allowed)
                    == port.or(match scheme.as_str() {
                        "http" => Some(80),
                        "https" => Some(443),
                        _ => None,
                    })
            })
    })
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
    use super::{origin_permitted, permitted};

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

    #[test]
    fn origins_match_schemes_hosts_default_ports_and_wildcard_ports() {
        let allowed = vec![
            "https://APP.example:443".into(),
            "http://localhost:*".into(),
        ];
        assert!(origin_permitted(&allowed, "https://app.example"));
        assert!(origin_permitted(&allowed, "https://app.example:443"));
        assert!(origin_permitted(&allowed, "http://localhost:8080"));
        assert!(!origin_permitted(&allowed, "https://app.example:8443"));
        assert!(!origin_permitted(&allowed, "http://app.example"));
        assert!(!origin_permitted(&allowed, "https://evil.example"));
        assert!(!origin_permitted(&allowed, "not an origin"));
        assert!(!origin_permitted(&allowed, "null"));
    }
}
