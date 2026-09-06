//! The single object: build it once, register the protocols it should speak, start it.

use std::sync::{Arc, Mutex};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::agents::{Config, DEFAULT_HOST, DEFAULT_READY_TIMEOUT, Endpoint, Protocol, Service};
use crate::error::{Result, ServiceError};
use crate::placement::Listener;

/// What a started runtime is holding.
#[derive(Default)]
pub(crate) struct State {
    pub(crate) started: bool,
    pub(crate) endpoints: Vec<Endpoint>,
    pub(crate) listeners: Vec<Listener>,
    /// Stops the services. Distinct from the drain, because cancelling a service is not
    /// the same as draining the listener it mounted on.
    pub(crate) service_cancel: Option<CancellationToken>,
    /// Stops the listeners this runtime owns.
    pub(crate) drain: Option<CancellationToken>,
    pub(crate) tasks: Vec<(Protocol, JoinHandle<Result<(), ServiceError>>)>,
}

/// Groups the agent-facing protocols one process speaks.
///
/// It exists because the alternative is every process wiring the same four things by hand —
/// identity that has to match across protocols, a router they can share, a listener somebody
/// has to own, and a shutdown that drains it. A runtime is not a layer over the protocol
/// crates so much as the place those four decisions are made once.
pub struct Runtime {
    pub(crate) cfg: Config,
    pub(crate) services: Vec<Arc<dyn Service>>,
    pub(crate) state: Mutex<State>,
}

impl Runtime {
    /// Builds a runtime from `cfg`. Nothing is bound or registered until
    /// [`Runtime::start`].
    pub fn new(mut cfg: Config) -> Self {
        if cfg.host.is_empty() {
            cfg.host = DEFAULT_HOST.to_string();
        }
        if cfg.ready_timeout.is_none() {
            cfg.ready_timeout = Some(DEFAULT_READY_TIMEOUT);
        }
        Self {
            cfg,
            services: Vec::new(),
            state: Mutex::new(State::default()),
        }
    }

    /// Adds a service and returns the runtime, so registration chains from
    /// [`Runtime::new`].
    ///
    /// Registering nothing is fine; starting with nothing is [`Error::NoServices`].
    /// Where Go panics on a registration after start, this consumes the runtime, so a late
    /// one cannot be written at all once [`Runtime::start`] has borrowed it.
    #[must_use]
    pub fn register<S: Service>(mut self, service: S) -> Self {
        self.services.push(Arc::new(service));
        self
    }

    /// [`Runtime::register`] for several services at once.
    #[must_use]
    pub fn register_all<I, S>(mut self, services: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Service,
    {
        for service in services {
            self.services.push(Arc::new(service));
        }
        self
    }

    /// Adds an already-boxed service, for a caller assembling a heterogeneous list.
    #[must_use]
    pub fn register_dyn(mut self, service: Arc<dyn Service>) -> Self {
        self.services.push(service);
        self
    }

    /// The settings this runtime was built with, with defaults filled in.
    pub fn config(&self) -> &Config {
        &self.cfg
    }

    /// The services registered so far.
    pub(crate) fn services(&self) -> &[Arc<dyn Service>] {
        &self.services
    }

    /// Where every registered protocol answers. Empty until [`Runtime::start`] returns,
    /// and stable afterwards.
    pub fn endpoints(&self) -> Vec<Endpoint> {
        self.state
            .lock()
            .expect("agents: state poisoned")
            .endpoints
            .clone()
    }
}
