//! Sorting services by the address they need, and opening the listeners that follow.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower_http::timeout::TimeoutLayer;

use crate::agents::{Mux, Service};
use crate::error::{Error, Result};
use crate::listen::Bound;
use crate::runtime::Runtime;

/// One listen address and the services that share it.
pub(crate) struct Group {
    /// The address the services asked for, which is also what groups them.
    pub(crate) addr: String,
    /// The address the services are told they sit on: [`Group::addr`] until the runtime
    /// binds the group's listener, then the same address with the port the operating
    /// system chose when the request named port 0.
    pub(crate) listen: String,
    pub(crate) mux: Mux,
    pub(crate) services: Vec<Arc<dyn Service>>,
    /// Any service here mounts HTTP handlers.
    pub(crate) http: bool,
    /// This runtime opens the listener, rather than a host's router owning it.
    pub(crate) owned: bool,
}

/// A listener this runtime opened, and the task serving it.
pub(crate) struct Listener {
    /// The address the listener was asked for.
    pub(crate) requested: String,
    /// The address it bound, with the chosen port when `requested` named port 0.
    pub(crate) addr: String,
    pub(crate) handle: JoinHandle<()>,
}

/// A group's listener, bound but not yet accepting.
pub(crate) struct Opened {
    /// Index of the group in the slice [`Runtime::bind`] was given.
    group: usize,
    listener: Bound,
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
                    listen: addr.clone(),
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

    /// Binds a listener for every group that owns one and has something to put on it, and
    /// records in [`Group::listen`] the address each actually bound.
    ///
    /// Binding comes before any service mounts, so that a service asking for port 0 is
    /// placed on — and so advertises — the port it really got rather than `:0`. A bound
    /// socket does not accept yet: a client arriving before [`Runtime::serve_groups`] waits
    /// in the backlog rather than meeting a router with nothing on it, which is the
    /// ordering the start contract promises.
    ///
    /// On failure every listener already bound is dropped, so its port is free again.
    pub(crate) async fn bind(&self, groups: &mut [Group]) -> Result<Vec<Opened>> {
        let mut opened: Vec<Opened> = Vec::new();
        for (index, group) in groups.iter_mut().enumerate() {
            if !group.owned || !group.http {
                continue;
            }
            let listener = Bound::bind(&group.addr)
                .await
                .map_err(|source| Error::Listen {
                    addr: group.addr.clone(),
                    source,
                })?;
            if let Some(port) = listener.port() {
                group.listen = with_port(&group.addr, port);
            }
            opened.push(Opened {
                group: index,
                listener,
            });
        }
        Ok(opened)
    }

    /// Serves every listener [`Runtime::bind`] opened, once the services have mounted.
    pub(crate) fn serve_groups(
        &self,
        groups: &[Group],
        opened: Vec<Opened>,
        drain: &CancellationToken,
    ) -> Vec<Listener> {
        let mut listeners: Vec<Listener> = Vec::new();

        for Opened { group, listener } in opened {
            let Some(group) = groups.get(group) else {
                continue;
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
            let addr = group.listen.clone();
            let serving = addr.clone();
            let handle = tokio::spawn(async move {
                let served = listener
                    .serve(router, async move { shutdown.cancelled().await })
                    .await;
                if let Err(err) = served {
                    // The listener is gone and the caller is long past start. Shutdown
                    // reports the drain; this is the one path with no one to tell.
                    tracing::error!(addr = %serving, error = %err, "agents: listener stopped");
                }
            });

            listeners.push(Listener {
                requested: group.addr.clone(),
                addr,
                handle,
            });
        }

        listeners
    }
}

/// `addr` with its port replaced by `port` when it named port 0, and unchanged otherwise.
///
/// Only the ephemeral case is rewritten, and only the port: a host given as a name stays
/// that name, so an address a caller configured is advertised exactly as configured.
fn with_port(addr: &str, port: u16) -> String {
    match addr.rsplit_once(':') {
        Some((host, "0")) => format!("{host}:{port}"),
        _ => addr.to_owned(),
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

#[cfg(test)]
mod tests {
    use super::with_port;

    #[test]
    fn only_an_ephemeral_port_is_rewritten() {
        assert_eq!(with_port("127.0.0.1:0", 4321), "127.0.0.1:4321");
        assert_eq!(with_port(":0", 4321), ":4321");
        assert_eq!(with_port("[::1]:0", 4321), "[::1]:4321");
        assert_eq!(with_port("localhost:9000", 9000), "localhost:9000");
        assert_eq!(with_port("unix:/tmp/a.sock", 1), "unix:/tmp/a.sock");
    }
}
