//! [`Runtime::start`]: mounting every service, then opening what they need.
//!
//! The ordering here is the whole contract, which is why it is its own file rather than one
//! long method among the accessors.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::agents::{DEFAULT_READY_TIMEOUT, Endpoint, Placement, Protocol, Ready};
use crate::error::{Error, Result, ServiceError};
use crate::runtime::Runtime;

impl Runtime {
    /// Mounts every registered service, opens the listeners they need, and returns once
    /// they are all answering. It does not block.
    ///
    /// The ordering is not an implementation detail. Every service mounts first and
    /// listeners open second, because a port that is accepting before its handlers are
    /// registered answers 404 to whoever got there first. A gRPC binding is stricter still:
    /// a tonic server takes its routes by value when it starts, so a runtime sharing
    /// [`Config::grpc_routes`] has to complete `start` before the host reads them out.
    ///
    /// `cancel` governs the services' lifetime. Firing it stops them; it does not drain the
    /// listeners, which is [`Runtime::shutdown`]'s job.
    pub async fn start(&self, cancel: CancellationToken) -> Result<()> {
        {
            let mut state = self.state.lock().expect("agents: state poisoned");
            if state.started {
                return Err(Error::AlreadyStarted);
            }
            if self.services.is_empty() {
                return Err(Error::NoServices);
            }
            state.started = true;
        }

        for service in &self.services {
            if service.requires().grpc && self.cfg.grpc_routes.is_none() {
                return Err(Error::NeedsGrpcRoutes(service.protocol()));
            }
        }

        let service_cancel = cancel.child_token();
        let drain = CancellationToken::new();
        let groups = self.group();
        let ready_timeout = self.cfg.ready_timeout.unwrap_or(DEFAULT_READY_TIMEOUT);

        let mut endpoints: Vec<Endpoint> = Vec::new();
        let mut tasks: Vec<(Protocol, JoinHandle<Result<(), ServiceError>>)> = Vec::new();

        for group in &groups {
            for service in &group.services {
                let placement = Placement {
                    identity: self.cfg.identity.clone(),
                    addr: group.addr.clone(),
                    mux: group.mux.clone(),
                    grpc_routes: self.cfg.grpc_routes.clone(),
                    public_url: self.cfg.public_url.clone(),
                };

                let protocol = service.protocol();
                let (ready, rx) = Ready::new();
                let owned = Arc::clone(service);
                let token = service_cancel.clone();
                let handle =
                    tokio::spawn(async move { owned.serve(token, placement, ready).await });

                match wait_ready(rx, ready_timeout, &cancel).await {
                    Mounted::Ready(eps) => {
                        endpoints.extend(eps);
                        tasks.push((protocol, handle));
                    }
                    // The sender is dropped when the task ends without reporting ready,
                    // which is a service that failed or returned early rather than a stall.
                    Mounted::Dropped => {
                        service_cancel.cancel();
                        abort_all(tasks);
                        return Err(match handle.await {
                            Ok(Err(source)) => Error::FailedToStart { protocol, source },
                            _ => Error::StoppedBeforeMount(protocol),
                        });
                    }
                    Mounted::TimedOut => {
                        service_cancel.cancel();
                        handle.abort();
                        abort_all(tasks);
                        return Err(Error::MountTimeout {
                            protocol,
                            timeout: ready_timeout,
                        });
                    }
                    Mounted::Cancelled => {
                        service_cancel.cancel();
                        handle.abort();
                        abort_all(tasks);
                        return Err(Error::Cancelled);
                    }
                }
            }
        }

        let listeners = match self.listen(&groups, &drain).await {
            Ok(listeners) => listeners,
            Err(err) => {
                service_cancel.cancel();
                abort_all(tasks);
                return Err(err);
            }
        };

        let mut state = self.state.lock().expect("agents: state poisoned");
        state.endpoints = endpoints;
        state.listeners = listeners;
        state.service_cancel = Some(service_cancel);
        state.drain = Some(drain);
        state.tasks = tasks;

        Ok(())
    }
}

/// What became of a service the runtime was waiting on.
enum Mounted {
    Ready(Vec<Endpoint>),
    Dropped,
    TimedOut,
    Cancelled,
}

/// Waits for one service to report itself mounted, or to prove it never will.
async fn wait_ready(
    rx: tokio::sync::oneshot::Receiver<Vec<Endpoint>>,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Mounted {
    tokio::select! {
        biased;
        result = tokio::time::timeout(timeout, rx) => match result {
            Ok(Ok(endpoints)) => Mounted::Ready(endpoints),
            Ok(Err(_)) => Mounted::Dropped,
            Err(_) => Mounted::TimedOut,
        },
        () = cancel.cancelled() => Mounted::Cancelled,
    }
}

/// Drops the services that already mounted, so a failed start leaves nothing running.
fn abort_all(tasks: Vec<(Protocol, JoinHandle<Result<(), ServiceError>>)>) {
    for (_, handle) in tasks {
        handle.abort();
    }
}
