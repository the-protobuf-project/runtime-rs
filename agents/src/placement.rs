//! Sorting services by the address they need, and opening the listeners that follow.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower_http::timeout::TimeoutLayer;

use crate::agents::{Mux, Service};
use crate::error::{Error, Result};
use crate::runtime::Runtime;

/// One listen address and the services that share it.
pub(crate) struct Group {
    pub(crate) addr: String,
    pub(crate) mux: Mux,
    pub(crate) services: Vec<Arc<dyn Service>>,
    /// Any service here mounts HTTP handlers.
    pub(crate) http: bool,
    /// This runtime opens the listener, rather than a host's router owning it.
    pub(crate) owned: bool,
}

/// A listener this runtime opened, and the task serving it.
pub(crate) struct Listener {
    pub(crate) addr: String,
    pub(crate) handle: JoinHandle<()>,
}

impl Runtime {
    /// Sorts services by the address they need. Everything that named none shares the
    /// runtime's, which is what makes one process serving two protocols bind one port.
    pub(crate) fn group(&self) -> Vec<Group> {
        let shared = format!("{}:{}", self.config().host, self.config().port);

        let mut by_addr: HashMap<String, Group> = HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for svc in self.services() {
            let req = svc.requires();
            let addr = req.addr.clone().unwrap_or_else(|| shared.clone());

            let group = by_addr.entry(addr.clone()).or_insert_with(|| {
                // A host's router is only the shared group's. A service that asked for an
                // address of its own asked to be somewhere else, and mounting it on the
                // host's router would put it on the host's port instead.
                let (mux, owned) = match &self.config().mux {
                    Some(host_mux) if addr == shared => (host_mux.clone(), false),
                    _ => (Mux::new(), true),
                };
                order.push(addr.clone());
                Group {
                    addr: addr.clone(),
                    mux,
                    services: Vec::new(),
                    http: false,
                    owned,
                }
            });

            group.services.push(Arc::clone(svc));
            group.http = group.http || req.http;
        }

        order.sort();
        order
            .into_iter()
            .filter_map(|addr| by_addr.remove(&addr))
            .collect()
    }

    /// Opens a server for every group that owns its listener and has something on it.
    pub(crate) async fn listen(
        &self,
        groups: &[Group],
        drain: &CancellationToken,
    ) -> Result<Vec<Listener>> {
        let mut listeners: Vec<Listener> = Vec::new();

        for group in groups {
            if !group.owned || !group.http {
                continue;
            }

            let listener = match TcpListener::bind(&group.addr).await {
                Ok(listener) => listener,
                Err(source) => {
                    // Close whatever already came up, so a failed start leaves no
                    // half-bound process behind.
                    drain.cancel();
                    for opened in listeners {
                        opened.handle.abort();
                    }
                    return Err(Error::Listen {
                        addr: group.addr.clone(),
                        source,
                    });
                }
            };

            let mut router = group.mux.router();
            // net/http bounds the read and the write phases separately; hyper does not
            // split them, so the nearest honest equivalent is one bound on the whole
            // exchange. The larger of the two is used so neither is tightened silently.
            if let Some(limit) =
                max_timeout(self.config().read_timeout, self.config().write_timeout)
            {
                router = router.layer(TimeoutLayer::with_status_code(
                    axum::http::StatusCode::REQUEST_TIMEOUT,
                    limit,
                ));
            }

            let shutdown = drain.clone();
            let addr = group.addr.clone();
            let serving = addr.clone();
            let handle = tokio::spawn(async move {
                let served = axum::serve(listener, router)
                    .with_graceful_shutdown(async move { shutdown.cancelled().await })
                    .await;
                if let Err(err) = served {
                    // The listener is gone and the caller is long past start. Shutdown
                    // reports the drain; this is the one path with no one to tell.
                    tracing::error!(addr = %serving, error = %err, "agents: listener stopped");
                }
            });

            listeners.push(Listener { addr, handle });
        }

        Ok(listeners)
    }
}

/// The larger of two optional durations, or `None` when neither is set.
fn max_timeout(
    a: Option<std::time::Duration>,
    b: Option<std::time::Duration>,
) -> Option<std::time::Duration> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}
