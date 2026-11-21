//! Purges what Cloudflare's CDN has cached for a zone. runtime-go's `cache/cdn`, which
//! reaches the same endpoint through Cloudflare's Go SDK; Cloudflare's Rust SDK has no cache
//! purge, so this goes through the workspace's network client.
//!
//! It is the other end of a cache: what a [`DB`](crate::DB) stores the program reads, and
//! what the CDN stores every visitor reads, so a write that changes a page needs both
//! forgotten. A purge is how the second one is.
//!
//! ```no_run
//! # async fn example() -> cache::Result<()> {
//! use cache::cdn::{PurgeConfig, Purger};
//!
//! let edge = Purger::connect(PurgeConfig {
//!     zone_id: "zone".into(),
//!     api_token: "token".into(),
//!     ..PurgeConfig::default()
//! }).await?;
//! edge.tags(&["book-42"]).await?; // everything served with Cache-Tag: book-42
//! # Ok(())
//! # }
//! ```
//!
//! Tags are the form worth designing for: a response carrying `Cache-Tag: book-42,
//! author-7` can be dropped by either, so the code that changes a book need not know every
//! URL it appears under.
//!
//! Cloudflare acknowledging a purge means it accepted the request, not that every location
//! has dropped the content yet; propagation takes seconds. The API also rate-limits purges
//! per account (five a minute for tags, hosts and prefixes on the free plan), so purge per
//! write, not per read, and prefer one call with many tags over many with one.

use std::collections::HashMap;
use std::time::Duration;

use network::{ConnectionOptions, HttpClient, HttpMethod, UrlOptions, UrlScheme};
use serde_json::{Value as Json, json};

use crate::{CacheError, Result};

/// Cloudflare's API root.
const API: &str = "https://api.cloudflare.com/client/v4";

/// How many URLs, tags, hosts or prefixes go into one purge: the most every plan accepts.
/// Longer lists are sent in pieces.
const PER_REQUEST: usize = 100;

/// Which zone a [`Purger`] purges, and as whom.
#[derive(Clone, Debug, Default)]
pub struct PurgeConfig {
    /// The zone whose cache is purged.
    pub zone_id: String,
    /// An API token with the Cache Purge permission on the zone.
    pub api_token: String,
    /// The API root; empty is Cloudflare's. Tests point it at a stand-in.
    pub base_url: String,
    /// Bounds each request; zero takes the network client's default.
    pub timeout: Duration,
}

/// Drops cached content from one zone.
pub struct Purger {
    http: HttpClient,
    url: UrlOptions,
    headers: HashMap<String, String>,
}

impl Purger {
    /// A purger for the zone in `config`. Nothing is sent until a purge.
    pub async fn connect(config: PurgeConfig) -> Result<Self> {
        if config.zone_id.is_empty() || config.api_token.is_empty() {
            return Err(CacheError::Internal(
                "cdn: a purger needs a zone id and an API token".to_owned(),
            ));
        }
        let base = match config.base_url.is_empty() {
            true => API,
            false => config.base_url.trim_end_matches('/'),
        };
        let (scheme, rest) = base
            .split_once("://")
            .ok_or_else(|| CacheError::Internal(format!("cdn: {base:?} is not a URL")))?;
        let (host, root) = rest.split_once('/').unwrap_or((rest, ""));
        let path = match root.trim_matches('/') {
            "" => format!("/zones/{}/purge_cache", config.zone_id),
            root => format!("/{root}/zones/{}/purge_cache", config.zone_id),
        };
        let url = UrlOptions {
            scheme: scheme
                .parse::<UrlScheme>()
                .map_err(|e| CacheError::Internal(format!("cdn: {e}")))?,
            host: host.to_owned(),
            paths: vec![path],
            params: HashMap::new(),
        };
        let mut http = HttpClient::default();
        http.connect(ConnectionOptions {
            url: url.clone(),
            timeout: config.timeout,
            skip_connectivity_check: true,
            ..ConnectionOptions::default()
        })
        .await
        .map_err(|e| CacheError::Internal(format!("cdn: {e}")))?;
        let headers = HashMap::from([
            (
                "Authorization".to_owned(),
                format!("Bearer {}", config.api_token),
            ),
            ("Content-Type".to_owned(), "application/json".to_owned()),
        ]);
        Ok(Self { http, url, headers })
    }

    /// Purges the content cached for each full URL, such as
    /// `https://example.com/css/site.css`.
    pub async fn urls(&self, urls: &[&str]) -> Result<()> {
        self.each("files", urls).await
    }

    /// Purges everything served with one of these tags in its `Cache-Tag` header.
    pub async fn tags(&self, tags: &[&str]) -> Result<()> {
        self.each("tags", tags).await
    }

    /// Purges everything cached for these hostnames, such as `www.example.com`.
    pub async fn hosts(&self, hosts: &[&str]) -> Result<()> {
        self.each("hosts", hosts).await
    }

    /// Purges everything whose URL starts with one of these: a host and a path, such as
    /// `www.example.com/blog/`, with no scheme, query or fragment.
    pub async fn prefixes(&self, prefixes: &[&str]) -> Result<()> {
        self.each("prefixes", prefixes).await
    }

    /// Purges the whole zone. Every visitor's next request for anything goes to the origin,
    /// so this is for an incident or a deploy that changed everything, not routine
    /// invalidation.
    pub async fn everything(&self) -> Result<()> {
        self.send(json!({ "purge_everything": true })).await
    }

    /// Sends a list in pieces the API accepts. An empty list purges nothing rather than
    /// being sent: the API would refuse it, and nothing asked for is nothing to do.
    async fn each(&self, field: &str, items: &[&str]) -> Result<()> {
        for chunk in items.chunks(PER_REQUEST) {
            self.send(json!({ field: chunk })).await?;
        }
        Ok(())
    }

    async fn send(&self, body: Json) -> Result<()> {
        let resp = self
            .http
            .request(
                HttpMethod::Post,
                &self.url,
                body.to_string().into_bytes(),
                &self.headers,
                0,
                0,
                None,
            )
            .await;
        let Some(error) = resp.error else {
            return Ok(());
        };
        // The status says the purge was refused; the body says why (a rate limit, a field
        // the plan lacks).
        let why = serde_json::from_slice::<Json>(&resp.data)
            .ok()
            .and_then(|b| b.get("errors").cloned())
            .and_then(|e| e.as_array().cloned())
            .map(|errors| {
                errors
                    .iter()
                    .filter_map(|e| e.get("message").and_then(Json::as_str))
                    .collect::<Vec<_>>()
                    .join("; ")
            })
            .filter(|why| !why.is_empty())
            .unwrap_or_else(|| error.to_string());
        Err(CacheError::Internal(format!("cdn: cannot purge: {why}")))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    use super::*;

    #[derive(Clone, Default)]
    struct Recorder {
        bodies: Arc<Mutex<Vec<Json>>>,
        refuse: bool,
    }

    impl Respond for Recorder {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            self.bodies
                .lock()
                .unwrap()
                .push(serde_json::from_slice(&request.body).unwrap());
            match self.refuse {
                false => ResponseTemplate::new(200).set_body_json(
                    json!({"success": true, "errors": [], "messages": [], "result": {"id": "p1"}}),
                ),
                true => ResponseTemplate::new(429).set_body_json(json!({
                    "success": false, "messages": [], "result": null,
                    "errors": [{"code": 1134, "message": "Unable to purge, rate limit reached."}],
                })),
            }
        }
    }

    async fn zone(refuse: bool) -> (Recorder, MockServer, Purger) {
        let server = MockServer::start().await;
        let recorder = Recorder {
            refuse,
            ..Recorder::default()
        };
        Mock::given(method("POST"))
            .and(path("/client/v4/zones/zone1/purge_cache"))
            .respond_with(recorder.clone())
            .mount(&server)
            .await;
        let purger = Purger::connect(PurgeConfig {
            zone_id: "zone1".into(),
            api_token: "test".into(),
            base_url: format!("{}/client/v4", server.uri()),
            ..PurgeConfig::default()
        })
        .await
        .unwrap();
        (recorder, server, purger)
    }

    #[tokio::test]
    async fn test_each_form_sends_its_field() {
        let (got, _server, edge) = zone(false).await;
        edge.urls(&["https://example.com/a.css"]).await.unwrap();
        edge.tags(&["book-42"]).await.unwrap();
        edge.hosts(&["www.example.com"]).await.unwrap();
        edge.prefixes(&["www.example.com/blog/"]).await.unwrap();
        edge.everything().await.unwrap();
        let fields: Vec<String> = got
            .bodies
            .lock()
            .unwrap()
            .iter()
            .map(|b| {
                let o = b.as_object().unwrap();
                assert_eq!(o.len(), 1, "{b}");
                o.keys().next().unwrap().clone()
            })
            .collect();
        assert_eq!(
            fields,
            ["files", "tags", "hosts", "prefixes", "purge_everything"]
        );
    }

    #[tokio::test]
    async fn test_long_lists_go_in_pieces_and_empty_ones_not_at_all() {
        let (got, _server, edge) = zone(false).await;
        let tags: Vec<String> = (0..250).map(|i| format!("t{i}")).collect();
        let tags: Vec<&str> = tags.iter().map(String::as_str).collect();
        edge.tags(&tags).await.unwrap();
        edge.urls(&[]).await.unwrap();
        let bodies = got.bodies.lock().unwrap();
        assert_eq!(bodies.len(), 3);
        let sent: usize = bodies
            .iter()
            .map(|b| b["tags"].as_array().unwrap().len())
            .sum();
        assert_eq!(sent, 250);
    }

    #[tokio::test]
    async fn test_a_refusal_says_why() {
        let (_got, _server, edge) = zone(true).await;
        let err = edge.tags(&["a"]).await.unwrap_err().to_string();
        assert!(err.contains("rate limit"), "{err}");
    }
}
