//! The GraphQL client: connection lifecycle plus raw, typed, field-based, batch, and
//! subscription operations. Start with [`GraphQLClient`], obtained via
//! [`crate::Network::as_graphql`].

mod batch;
mod helpers;
mod named;
mod raw;
mod subscription;
mod subscription_task;
mod typed;
mod ws_protocol;

use crate::error::{Error, Result};
use crate::options::{ConnectionOptions, DEFAULT_TIMEOUT};
use crate::transport::{HeaderCarrier, new_pooled_client};
use crate::url::build_full_url;

pub use batch::BatchOp;
pub use named::FieldArg;
pub use subscription::Subscription;

/// Sent to verify the GraphQL server is reachable during `connect`/`reconnect`. Override with
/// [`ConnectionOptions::graphql_connectivity_query`] for strict servers that limit introspection.
pub const DEFAULT_GRAPHQL_CONNECTIVITY_QUERY: &str = "query { __typename }";

/// A GraphQL API client. Create via [`crate::Network::new_connection`] and
/// [`crate::Network::as_graphql`]. It embeds [`ConnectionOptions`] (URL, timeout, headers,
/// skip-connectivity-check, GraphQL connectivity query).
#[derive(Default)]
pub struct GraphQLClient {
    pub(crate) http: Option<reqwest::Client>,
    pub(crate) endpoint: Option<String>,
    /// The options this client was last connected with.
    pub options: ConnectionOptions,
}

impl GraphQLClient {
    /// Configures the GraphQL client and optionally verifies server reachability. If
    /// `opts.timeout` is zero, [`DEFAULT_TIMEOUT`] is used. If `skip_connectivity_check` is true,
    /// no connectivity query is sent; otherwise the connectivity query runs and `connect` returns
    /// an error on failure.
    pub async fn connect(&mut self, mut opts: ConnectionOptions) -> Result<()> {
        if opts.timeout.is_zero() {
            opts.timeout = DEFAULT_TIMEOUT;
        }

        let endpoint = build_full_url(&opts.url, 0).map_err(|e| Error::BuildUrl(Box::new(e)))?;
        let http = new_pooled_client(opts.timeout)?;

        self.http = Some(http);
        self.endpoint = Some(endpoint);
        self.options = opts;

        self.verify_connectivity().await
    }

    /// Tears down the current client and re-establishes it with the same options. If
    /// `skip_connectivity_check` is false, the connectivity query runs again.
    pub async fn reconnect(&mut self) -> Result<()> {
        if self.http.is_none() {
            return Err(Error::GraphQLNotInitialized);
        }
        self.connect(self.options.clone()).await
    }

    /// Clears the GraphQL client. It is not usable until `connect` is called again.
    pub async fn close(&mut self) -> Result<()> {
        self.http = None;
        self.endpoint = None;
        Ok(())
    }

    async fn verify_connectivity(&self) -> Result<()> {
        if self.options.skip_connectivity_check {
            return Ok(());
        }
        let query = self
            .options
            .graphql_connectivity_query
            .clone()
            .unwrap_or_else(|| DEFAULT_GRAPHQL_CONNECTIVITY_QUERY.to_string());
        self.exec_raw(&query, None)
            .await
            .map_err(|source| Error::GraphQLConnect {
                host: self.options.url.host.clone(),
                source: Box::new(source),
            })?;
        Ok(())
    }

    /// Posts a `{query, variables}` document to the endpoint and returns the `data` object,
    /// erroring on a transport failure, a non-2xx status with no GraphQL `errors` payload, or a
    /// non-empty `errors` array in an otherwise-200 response. Shared by raw/typed/named/batch
    /// operations and the connectivity check.
    pub(crate) async fn exec_raw(
        &self,
        document: &str,
        variables: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let client = self.http.as_ref().ok_or(Error::GraphQLNotInitialized)?;
        let endpoint = self.endpoint.as_ref().ok_or(Error::GraphQLNotInitialized)?;

        let mut body = serde_json::Map::new();
        body.insert(
            "query".to_string(),
            serde_json::Value::String(document.to_string()),
        );
        if let Some(v) = variables {
            body.insert("variables".to_string(), v.clone());
        }

        let mut builder = client.post(endpoint).json(&body);
        for (k, v) in &self.options.headers {
            builder = builder.header(k, v);
        }
        if let Some(propagator) = &self.options.trace_propagator {
            let mut carrier = HeaderCarrier::default();
            let cx = opentelemetry::Context::current();
            propagator.inject_context(&cx, &mut carrier);
            for (k, v) in carrier.0 {
                builder = builder.header(k, v);
            }
        }

        let send = builder.send();
        let resp = tokio::time::timeout(self.options.timeout, send)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::SendRequest)?;

        let status = resp.status();
        let payload: serde_json::Value = resp.json().await.map_err(Error::ReadBody)?;

        if let Some(errors) = payload.get("errors").and_then(|e| e.as_array()) {
            if !errors.is_empty() {
                return Err(Error::GraphQLErrors(errors.clone()));
            }
        } else if !status.is_success() {
            return Err(Error::UnexpectedStatus(status.as_u16()));
        }

        Ok(payload
            .get("data")
            .cloned()
            .unwrap_or(serde_json::Value::Null))
    }
}
