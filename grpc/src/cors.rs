//! Cross-origin resource sharing for the HTTP listeners. Go's `cors.go`.
//!
//! A [`CorsPolicy`] lets a browser on another origin call the REST gateway. The policy wraps
//! everything HTTP/1.1 and HTTP/3 answer, outermost: the gateways, a
//! [`HybridServer::with_http_handler`] replacement, and `/health` alike. A preflight is
//! answered before any routing. The agent protocols are separate servers and are not
//! covered, as in Go.
//!
//! This is written by hand rather than on `tower_http::cors`, because that layer answers a
//! disallowed preflight 200 with no CORS headers, where Go answers 403.
//!
//! # Differences from Go
//!
//! [`CorsPolicy::max_age`] is a [`Duration`], so Go's negative-age check has no counterpart.
//! [`CorsPolicy::validate`] also refuses a method or header list no HTTP header can carry,
//! which Go lets through for `net/http` to sanitise on the wire.
//!
//! [`HybridServer::with_http_handler`]: crate::HybridServer::with_http_handler

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use http::header::{
    ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
    ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_MAX_AGE,
    ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD, ORIGIN, VARY,
};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri};

use crate::error::{Error, Result};
use crate::hybrid::HybridServer;

/// The cross-origin policy [`HybridServer::with_cors`] applies to the HTTP listeners, so a
/// browser on another origin can call the REST gateway.
///
/// Every list takes its default when empty, except `allowed_origins`: a policy naming no
/// origin allows none, and [`CorsPolicy::validate`] refuses it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CorsPolicy {
    /// The origins allowed to call, each an exact `scheme://host[:port]`, or `*` for any
    /// origin. Matching ignores case and a trailing slash.
    pub allowed_origins: Vec<String>,
    /// What a preflight's `Access-Control-Request-Method` may name. Empty means
    /// [`default_cors_allowed_methods`].
    pub allowed_methods: Vec<String>,
    /// What a preflight's `Access-Control-Request-Headers` is answered with. Empty means
    /// [`default_cors_allowed_headers`]; `*` allows whatever a preflight asks for by echoing
    /// it back.
    pub allowed_headers: Vec<String>,
    /// The response headers a browser lets the calling script read, beyond the
    /// CORS-safelisted ones.
    pub exposed_headers: Vec<String>,
    /// Lets a browser send cookies and HTTP authentication and read the response. It cannot
    /// be combined with the `*` origin.
    pub allow_credentials: bool,
    /// How long a browser may cache a preflight's answer; zero sends no
    /// `Access-Control-Max-Age`, leaving the browser's own default.
    pub max_age: Duration,
}

/// The methods a policy allows when it names none: every verb the REST gateway's
/// `google.api.http` bindings use, plus `OPTIONS`.
#[must_use]
pub fn default_cors_allowed_methods() -> Vec<String> {
    ["GET", "POST", "PATCH", "PUT", "DELETE", "OPTIONS"]
        .map(String::from)
        .to_vec()
}

/// The request headers a policy allows when it names none: what a GAPIC client's browser
/// transport sends — `Content-Type`, the `x-goog-*` call metadata, and the credentials it
/// may carry.
#[must_use]
pub fn default_cors_allowed_headers() -> Vec<String> {
    [
        "Content-Type",
        "Authorization",
        "X-Goog-Api-Client",
        "X-Goog-Request-Params",
        "X-Goog-User-Project",
        "X-Api-Key",
    ]
    .map(String::from)
    .to_vec()
}

impl CorsPolicy {
    /// Reports whether the policy can be served: at least one origin, each `*` or an
    /// absolute `scheme://host[:port]` with no path, query or fragment, and no `*` origin
    /// when credentials are allowed — a browser refuses that pairing, and echoing any origin
    /// instead would hand every site the caller's credentials.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidCors`] naming what is wrong.
    pub fn validate(&self) -> Result<()> {
        if self.allowed_origins.is_empty() {
            return Err(invalid("no allowed origins"));
        }
        for origin in &self.allowed_origins {
            if origin == "*" {
                if self.allow_credentials {
                    return Err(invalid(
                        "the \"*\" origin cannot be combined with allowed credentials; list \
                         the origins instead",
                    ));
                }
                continue;
            }
            validate_origin(origin)?;
        }
        for list in [
            &self.allowed_methods,
            &self.allowed_headers,
            &self.exposed_headers,
        ] {
            let joined = list.join(", ");
            if HeaderValue::from_str(&joined).is_err() {
                return Err(invalid(&format!("{joined:?} is not a valid header value")));
            }
        }
        Ok(())
    }
}

fn invalid(message: &str) -> Error {
    Error::InvalidCors(message.to_string())
}

/// Checks one non-wildcard origin is `scheme://host[:port]`.
fn validate_origin(origin: &str) -> Result<()> {
    let want = || {
        invalid(&format!(
            "origin {origin:?}: want scheme://host[:port], such as https://app.example.com"
        ))
    };
    let trimmed = origin.strip_suffix('/').unwrap_or(origin);
    // `Uri` drops a fragment and keeps user info inside the authority, so both are checked
    // on the text.
    if trimmed.contains(['#', '@']) {
        return Err(want());
    }
    let uri: Uri = trimmed.parse().map_err(|_| want())?;
    let bare = uri
        .path_and_query()
        .is_none_or(|rest| matches!(rest.as_str(), "" | "/"));
    let host = uri.authority().is_some_and(|a| !a.host().is_empty());
    if uri.scheme().is_none() || !host || !bare || trimmed.ends_with('/') {
        return Err(want());
    }
    Ok(())
}

/// A validated policy with its header values precomputed.
#[derive(Debug)]
struct Compiled {
    any_origin: bool,
    origins: HashSet<String>,
    methods: HeaderValue,
    allow_methods: HashSet<String>,
    headers: HeaderValue,
    any_header: bool,
    exposed: Option<HeaderValue>,
    credentials: bool,
    max_age: Option<HeaderValue>,
}

/// Lower-cases `origin` and drops a trailing slash: scheme and host are case-insensitive,
/// and a configured `https://a.example/` means what a browser sends as `https://a.example`.
fn normalize_origin(origin: &str) -> String {
    let origin = origin.trim();
    origin.strip_suffix('/').unwrap_or(origin).to_lowercase()
}

impl Compiled {
    /// Precomputes the header values of a policy [`CorsPolicy::validate`] accepted.
    fn new(policy: &CorsPolicy) -> Result<Self> {
        let value = |text: &str| {
            HeaderValue::from_str(text).map_err(|_| invalid("a policy value is not a header"))
        };
        let methods = if policy.allowed_methods.is_empty() {
            default_cors_allowed_methods()
        } else {
            policy.allowed_methods.clone()
        };
        let headers = if policy.allowed_headers.is_empty() {
            default_cors_allowed_headers()
        } else {
            policy.allowed_headers.clone()
        };
        let upper: Vec<String> = methods.iter().map(|m| m.trim().to_uppercase()).collect();

        Ok(Self {
            any_origin: policy.allowed_origins.iter().any(|o| o == "*"),
            origins: policy
                .allowed_origins
                .iter()
                .filter(|o| *o != "*")
                .map(|o| normalize_origin(o))
                .collect(),
            methods: value(&upper.join(", "))?,
            allow_methods: upper.into_iter().collect(),
            headers: value(&headers.join(", "))?,
            any_header: headers.iter().any(|h| h == "*"),
            exposed: match policy.exposed_headers.as_slice() {
                [] => None,
                exposed => Some(value(&exposed.join(", "))?),
            },
            credentials: policy.allow_credentials,
            max_age: if policy.max_age.is_zero() {
                None
            } else {
                Some(HeaderValue::from(policy.max_age.as_secs()))
            },
        })
    }

    /// Whether `origin` may call.
    fn allowed(&self, origin: &HeaderValue) -> bool {
        self.any_origin
            || origin
                .to_str()
                .is_ok_and(|o| self.origins.contains(&normalize_origin(o)))
    }

    /// `Access-Control-Allow-Origin` for an allowed origin: `*` for a wildcard policy without
    /// credentials, else the origin itself.
    fn allow_origin(&self, origin: &HeaderValue) -> HeaderValue {
        if self.any_origin && !self.credentials {
            HeaderValue::from_static("*")
        } else {
            origin.clone()
        }
    }

    /// The headers an allowed origin's ordinary response gains.
    fn decorate(&self, headers: &mut HeaderMap, origin: &HeaderValue) {
        headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, self.allow_origin(origin));
        if self.credentials {
            headers.insert(
                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }
        if let Some(exposed) = &self.exposed {
            headers.insert(ACCESS_CONTROL_EXPOSE_HEADERS, exposed.clone());
        }
    }

    /// Answers a preflight without calling the wrapped service.
    fn preflight<R: Default>(&self, request: &HeaderMap, origin: &HeaderValue) -> Response<R> {
        let mut response = Response::new(R::default());
        let headers = response.headers_mut();
        for vary in [
            "Origin",
            "Access-Control-Request-Method",
            "Access-Control-Request-Headers",
        ] {
            headers.append(VARY, HeaderValue::from_static(vary));
        }

        let method = request
            .get(ACCESS_CONTROL_REQUEST_METHOD)
            .and_then(|m| m.to_str().ok())
            .unwrap_or_default()
            .to_uppercase();
        if !self.allowed(origin) || !self.allow_methods.contains(&method) {
            tracing::debug!(?origin, %method, "CORS: refused preflight");
            *response.status_mut() = StatusCode::FORBIDDEN;
            return response;
        }

        headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, self.allow_origin(origin));
        headers.insert(ACCESS_CONTROL_ALLOW_METHODS, self.methods.clone());
        if !self.any_header {
            headers.insert(ACCESS_CONTROL_ALLOW_HEADERS, self.headers.clone());
        } else if let Some(requested) = non_empty(request, &ACCESS_CONTROL_REQUEST_HEADERS) {
            headers.insert(ACCESS_CONTROL_ALLOW_HEADERS, requested.clone());
        }
        if self.credentials {
            headers.insert(
                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }
        if let Some(max_age) = &self.max_age {
            headers.insert(ACCESS_CONTROL_MAX_AGE, max_age.clone());
        }
        *response.status_mut() = StatusCode::NO_CONTENT;
        response
    }
}

/// A header's value, treating an empty one as absent, as Go's `Header.Get(...) == ""` does.
fn non_empty<'a>(headers: &'a HeaderMap, name: &HeaderName) -> Option<&'a HeaderValue> {
    headers.get(name).filter(|value| !value.is_empty())
}

/// Returns the tower layer applying `policy` to the service it wraps. It is what
/// [`HybridServer::with_cors`] installs, public for a caller composing its own handler.
/// Go's `CORS`.
///
/// A preflight — `OPTIONS` carrying `Origin` and `Access-Control-Request-Method` — is
/// answered here and never reaches the wrapped service: 204 with the policy's methods,
/// headers, credentials and max age for an allowed origin, 403 with no CORS headers
/// otherwise. Any other request from an allowed origin passes through with
/// `Access-Control-Allow-Origin` (and the exposed headers and credentials flag) added; from
/// a disallowed origin it passes through unchanged, and the browser withholds the response.
/// Every response carries `Vary: Origin`, so a shared cache never serves one origin's answer
/// to another.
///
/// # Errors
///
/// [`Error::InvalidCors`] when [`CorsPolicy::validate`] refuses the policy.
pub fn cors(policy: CorsPolicy) -> Result<CorsLayer> {
    policy.validate()?;
    Ok(CorsLayer {
        compiled: Arc::new(Compiled::new(&policy)?),
    })
}

/// The layer [`cors`] returns. Wrap a service in it with [`tower::Layer::layer`], or hand it
/// to anything that takes a layer, such as `ServiceBuilder`.
#[derive(Debug, Clone)]
pub struct CorsLayer {
    compiled: Arc<Compiled>,
}

impl<S> tower::Layer<S> for CorsLayer {
    type Service = Cors<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Cors {
            inner,
            compiled: Arc::clone(&self.compiled),
        }
    }
}

/// A service behind a [`CorsLayer`].
#[derive(Debug, Clone)]
pub struct Cors<S> {
    inner: S,
    compiled: Arc<Compiled>,
}

impl<S, B, R> tower::Service<Request<B>> for Cors<S>
where
    S: tower::Service<Request<B>, Response = Response<R>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    R: Default + Send + 'static,
{
    type Response = Response<R>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Response<R>, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let origin = non_empty(request.headers(), &ORIGIN).cloned();
        let compiled = Arc::clone(&self.compiled);

        if let Some(origin) = &origin
            && request.method() == Method::OPTIONS
            && non_empty(request.headers(), &ACCESS_CONTROL_REQUEST_METHOD).is_some()
        {
            let response = compiled.preflight(request.headers(), origin);
            return Box::pin(std::future::ready(Ok(response)));
        }

        let future = self.inner.call(request);
        Box::pin(async move {
            let mut response = future.await?;
            let headers = response.headers_mut();
            headers.append(VARY, HeaderValue::from_static("Origin"));
            if let Some(origin) = origin.filter(|origin| compiled.allowed(origin)) {
                compiled.decorate(headers, &origin);
            }
            Ok(response)
        })
    }
}

impl HybridServer {
    /// Applies `policy` to the HTTP/1.1 and HTTP/3 listeners — the REST gateway, a
    /// [`HybridServer::with_http_handler`] replacement, and `/health` alike — so browser
    /// clients on the allowed origins can call them. Go's `WithCORS`.
    ///
    /// Preflights are answered before any handler runs. An invalid policy (see
    /// [`CorsPolicy::validate`]) makes [`HybridServer::start`] fail. The MCP and A2A
    /// listeners are separate servers and are not covered.
    #[must_use]
    pub fn with_cors(mut self, policy: CorsPolicy) -> Self {
        tracing::debug!(
            origins = ?policy.allowed_origins,
            credentials = policy.allow_credentials,
            "with_cors: allowing origins"
        );
        self.cors = Some(policy);
        self
    }

    /// Refuses a policy that cannot be served, before anything binds.
    pub(crate) fn validate_cors(&self) -> Result<()> {
        self.cors.as_ref().map_or(Ok(()), CorsPolicy::validate)
    }

    /// Wraps the router in the policy, outermost.
    ///
    /// The policy is mounted as a fallback around the whole router rather than added with
    /// [`Router::layer`], because a layer goes around each route: the policy has to run
    /// before routing, or a preflight's verdict would depend on what the path matched.
    pub(crate) fn apply_cors(&self, router: Router) -> Result<Router> {
        let Some(policy) = &self.cors else {
            return Ok(router);
        };
        tracing::debug!("HTTP: wrapping the router in the CORS policy");
        let layer = cors(policy.clone())?;
        Ok(Router::new().fallback_service(tower::Layer::layer(&layer, router)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use tower::ServiceExt;

    fn policy(origins: &[&str]) -> CorsPolicy {
        CorsPolicy {
            allowed_origins: origins.iter().map(ToString::to_string).collect(),
            ..Default::default()
        }
    }

    fn router(policy: CorsPolicy) -> Router {
        let layer = cors(policy).expect("a valid policy");
        Router::new().fallback_service(tower::Layer::layer(
            &layer,
            Router::new().route("/x", get(|| async { "body" })),
        ))
    }

    async fn send(router: Router, request: Request<Body>) -> Response<Body> {
        router.oneshot(request).await.expect("infallible")
    }

    fn preflight(origin: &str, method: &str) -> http::request::Builder {
        Request::options("/x")
            .header("Origin", origin)
            .header("Access-Control-Request-Method", method)
    }

    fn vary(response: &Response<Body>) -> Vec<&str> {
        response
            .headers()
            .get_all(VARY)
            .iter()
            .map(|v| v.to_str().expect("ascii"))
            .collect()
    }

    #[test]
    fn validate_refuses_what_go_refuses() {
        assert!(policy(&[]).validate().is_err());
        let wildcard_credentials = CorsPolicy {
            allow_credentials: true,
            ..policy(&["*"])
        };
        assert!(wildcard_credentials.validate().is_err());
        for bad in [
            "app.example.com",
            "https://",
            "https://a.example/path",
            "https://a.example?x=1",
            "https://a.example#f",
            "https://user@a.example",
            "https://a.example//",
        ] {
            assert!(
                policy(&[bad]).validate().is_err(),
                "{bad} should be refused"
            );
        }
        for good in [
            "*",
            "https://a.example",
            "https://a.example/",
            "http://h:8080",
        ] {
            policy(&[good]).validate().expect(good);
        }
        let credentialed = CorsPolicy {
            allow_credentials: true,
            ..policy(&["https://a.example"])
        };
        credentialed
            .validate()
            .expect("a listed origin may take credentials");
    }

    #[tokio::test]
    async fn an_allowed_preflight_is_answered_204_with_the_policy() {
        let response = send(
            router(CorsPolicy {
                allow_credentials: true,
                max_age: Duration::from_secs(600),
                ..policy(&["https://App.Example/"])
            }),
            preflight("https://app.example", "post")
                .body(Body::empty())
                .expect("request"),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let headers = response.headers();
        assert_eq!(
            headers[ACCESS_CONTROL_ALLOW_ORIGIN], "https://app.example",
            "the caller's spelling is echoed"
        );
        assert_eq!(
            headers[ACCESS_CONTROL_ALLOW_METHODS],
            "GET, POST, PATCH, PUT, DELETE, OPTIONS"
        );
        assert_eq!(
            headers[ACCESS_CONTROL_ALLOW_HEADERS],
            "Content-Type, Authorization, X-Goog-Api-Client, X-Goog-Request-Params, \
             X-Goog-User-Project, X-Api-Key"
        );
        assert_eq!(headers[ACCESS_CONTROL_ALLOW_CREDENTIALS], "true");
        assert_eq!(headers[ACCESS_CONTROL_MAX_AGE], "600");
        assert_eq!(
            vary(&response),
            [
                "Origin",
                "Access-Control-Request-Method",
                "Access-Control-Request-Headers"
            ]
        );
    }

    #[tokio::test]
    async fn a_refused_preflight_is_403_with_no_cors_headers() {
        for (origin, method) in [
            ("https://evil.example", "GET"),
            ("https://app.example", "TRACE"),
        ] {
            let response = send(
                router(policy(&["https://app.example"])),
                preflight(origin, method)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            assert!(!response.headers().contains_key(ACCESS_CONTROL_ALLOW_ORIGIN));
            assert!(
                !response
                    .headers()
                    .contains_key(ACCESS_CONTROL_ALLOW_METHODS)
            );
        }
    }

    #[tokio::test]
    async fn a_star_header_policy_echoes_what_was_requested() {
        let echoing = || CorsPolicy {
            allowed_headers: vec!["*".into()],
            ..policy(&["*"])
        };
        let response = send(
            router(echoing()),
            preflight("https://a.example", "GET")
                .header("Access-Control-Request-Headers", "x-one, x-two")
                .body(Body::empty())
                .expect("request"),
        )
        .await;
        assert_eq!(
            response.headers()[ACCESS_CONTROL_ALLOW_HEADERS],
            "x-one, x-two"
        );
        assert_eq!(response.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], "*");

        let unasked = send(
            router(echoing()),
            preflight("https://a.example", "GET")
                .body(Body::empty())
                .expect("request"),
        )
        .await;
        assert!(!unasked.headers().contains_key(ACCESS_CONTROL_ALLOW_HEADERS));
    }

    #[tokio::test]
    async fn ordinary_requests_pass_through_decorated_only_when_allowed() {
        let exposing = || CorsPolicy {
            exposed_headers: vec!["X-One".into(), "X-Two".into()],
            allow_credentials: true,
            ..policy(&["https://app.example"])
        };
        let allowed = send(
            router(exposing()),
            Request::get("/x")
                .header("Origin", "https://app.example")
                .body(Body::empty())
                .expect("request"),
        )
        .await;
        assert_eq!(allowed.status(), StatusCode::OK);
        assert_eq!(
            allowed.headers()[ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://app.example"
        );
        assert_eq!(allowed.headers()[ACCESS_CONTROL_ALLOW_CREDENTIALS], "true");
        assert_eq!(
            allowed.headers()[ACCESS_CONTROL_EXPOSE_HEADERS],
            "X-One, X-Two"
        );
        assert_eq!(vary(&allowed), ["Origin"]);

        let refused = send(
            router(exposing()),
            Request::get("/x")
                .header("Origin", "https://evil.example")
                .body(Body::empty())
                .expect("request"),
        )
        .await;
        assert_eq!(refused.status(), StatusCode::OK, "the browser withholds it");
        assert!(!refused.headers().contains_key(ACCESS_CONTROL_ALLOW_ORIGIN));
        assert_eq!(vary(&refused), ["Origin"]);

        let no_origin = send(
            router(exposing()),
            Request::get("/x").body(Body::empty()).expect("request"),
        )
        .await;
        assert!(
            !no_origin
                .headers()
                .contains_key(ACCESS_CONTROL_ALLOW_ORIGIN)
        );
        assert_eq!(vary(&no_origin), ["Origin"]);
    }

    #[tokio::test]
    async fn options_without_a_request_method_is_not_a_preflight() {
        let response = send(
            router(policy(&["*"])),
            Request::options("/x")
                .header("Origin", "https://a.example")
                .body(Body::empty())
                .expect("request"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], "*");
    }
}
