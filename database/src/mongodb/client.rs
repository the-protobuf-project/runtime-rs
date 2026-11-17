//! The connection you own: [`MongoConfig`] and [`MongoClient`]. Go's `mongodb/client.go`.

use std::time::Duration;

use mongodb::bson::doc;
use mongodb::options::{ClientOptions, Credential};

use super::error::{backend, invalid};
use crate::Result;

/// How to reach MongoDB. The default is not usable: an address or a URI is required, because
/// guessing localhost is how a program ends up writing to whatever is listening there. Go's
/// `mongodb.Config`.
#[derive(Clone, Default)]
pub struct MongoConfig {
    /// The full connection string, e.g. `mongodb://localhost:27017`. When set it is used as
    /// is and every other field but the pool and timeout settings is ignored: a URI is the one
    /// form that expresses a replica set, a read preference, TLS and an `authSource`.
    pub uri: String,
    /// `host:port`, used when `uri` is empty.
    pub address: String,
    /// The user to authenticate as, when the server asks.
    pub username: String,
    /// The password. Passed to the driver as a credential, never spliced into a URI, so a
    /// password holding `@`, `/` or `:` means itself rather than changing the host dialed.
    pub password: String,
    /// The database credentials are checked against; empty leaves it to the driver (`admin`).
    pub auth_source: String,
    /// Bounds both the dial and server selection; zero takes the driver's defaults, where
    /// selection alone is thirty seconds. Set it in anything that checks dependencies at
    /// startup.
    pub connect_timeout: Duration,
    /// Caps concurrent connections per server; zero takes the driver's 10. Past the cap
    /// operations queue rather than fail, so size it from the concurrency you expect.
    pub max_pool_size: u32,
    /// Connections kept open while idle, so a burst after a quiet spell does not pay to
    /// establish them; zero keeps none.
    pub min_pool_size: u32,
    /// Closes a connection idle longer than this; zero keeps them, which is wrong behind a
    /// load balancer that silently drops idle connections.
    pub max_conn_idle_time: Duration,
}

impl MongoConfig {
    /// A config dialing `host:port`.
    pub fn address(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            ..Self::default()
        }
    }

    /// A config using the connection string `uri` as is.
    pub fn uri(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            ..Self::default()
        }
    }

    /// The driver options this config describes. Go's `Config.uri` plus the option setters.
    pub(super) async fn options(&self) -> Result<ClientOptions> {
        let uri = if !self.uri.is_empty() {
            self.uri.clone()
        } else if !self.address.is_empty() {
            format!(
                "mongodb://{}",
                self.address.trim_start_matches("mongodb://")
            )
        } else {
            return Err(invalid("an address or a URI is required"));
        };
        let mut opts = ClientOptions::parse(&uri)
            .await
            .map_err(|e| backend("cannot parse the connection string", e))?;
        if self.uri.is_empty() && !self.username.is_empty() {
            let mut credential = Credential::default();
            credential.username = Some(self.username.clone());
            credential.password = Some(self.password.clone());
            credential.source = (!self.auth_source.is_empty()).then(|| self.auth_source.clone());
            opts.credential = Some(credential);
        }
        // Zero leaves each as the URI or the driver has it.
        let set = |d: Duration| (!d.is_zero()).then_some(d);
        let count = |n: u32| (n > 0).then_some(n);
        opts.max_pool_size = count(self.max_pool_size).or(opts.max_pool_size);
        opts.min_pool_size = count(self.min_pool_size).or(opts.min_pool_size);
        opts.max_idle_time = set(self.max_conn_idle_time).or(opts.max_idle_time);
        if let Some(t) = set(self.connect_timeout) {
            // Both: they bound different things, and the first alone leaves an unreachable
            // address hanging for the thirty-second selection default.
            opts.connect_timeout = Some(t);
            opts.server_selection_timeout = Some(t);
        }
        Ok(opts)
    }
}

impl std::fmt::Debug for MongoConfig {
    /// Everything but the password, and the URI only by whether it is set: either may carry
    /// a secret.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoConfig")
            .field("uri", &(!self.uri.is_empty()).then_some("<set>"))
            .field("address", &self.address)
            .field("username", &self.username)
            .field("auth_source", &self.auth_source)
            .field("connect_timeout", &self.connect_timeout)
            .field("max_pool_size", &self.max_pool_size)
            .field("min_pool_size", &self.min_pool_size)
            .field("max_conn_idle_time", &self.max_conn_idle_time)
            .finish_non_exhaustive()
    }
}

/// A connection to MongoDB that you own: nothing in this module closes it. It wraps the driver
/// so nothing above names one, and hands it back through [`MongoClient::unwrap`]. Cheap to
/// clone; clones share one pool. Go's `mongodb.Client`.
#[derive(Clone, Debug)]
pub struct MongoClient {
    inner: mongodb::Client,
}

impl MongoClient {
    /// Dials MongoDB and pings it before returning, so a bad address or a refused credential
    /// fails here, at startup, rather than on the first read in a request handler. Go's
    /// `NewClient`.
    pub async fn connect(config: MongoConfig) -> Result<Self> {
        let opts = config.options().await?;
        let inner =
            mongodb::Client::with_options(opts).map_err(|e| backend("cannot connect", e))?;
        if let Err(e) = ping(&inner).await {
            inner.shutdown().await;
            return Err(backend("cannot reach the server", e));
        }
        Ok(Self { inner })
    }

    /// Adopts a client built elsewhere, configured in ways [`MongoConfig`] does not reach.
    /// Neither dialed nor pinged here. Go's `Wrap`.
    pub fn wrap(inner: mongodb::Client) -> Self {
        Self { inner }
    }

    /// The driver client, for what this module does not do. Go's `Client.Unwrap`.
    pub fn unwrap(&self) -> &mongodb::Client {
        &self.inner
    }

    /// Disconnects, once every session and cursor from it is gone. Go's `Client.Close`.
    pub async fn close(self) {
        self.inner.shutdown().await;
    }
}

/// One round trip to the server.
pub(super) async fn ping(client: &mongodb::Client) -> mongodb::error::Result<()> {
    client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await
        .map(drop)
}
