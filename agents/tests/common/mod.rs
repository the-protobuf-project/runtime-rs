//! The stand-in service the runtime tests drive, so each test file states only its own
//! assertions.
//!
//! Not every test binary uses every helper, and an unused one here is not dead code — it is
//! used by a sibling.

#![allow(dead_code)]

use std::sync::Arc;

use agents::error::ServiceError;
use agents::{Config, Endpoint, Identity, Placement, Protocol, Ready, Requirements, Service};
use axum::Router;
use axum::routing::get;
use tokio_util::sync::CancellationToken;

/// What the fake service does when the runtime starts it.
#[derive(Clone, Copy)]
pub enum Behaviour {
    /// Mounts a route, reports ready, blocks.
    Mount,
    /// Blocks without ever reporting ready.
    NeverMount,
    /// Returns an error before reporting ready.
    FailBeforeReady,
    /// Returns cleanly before reporting ready.
    StopBeforeReady,
}

/// A stand-in for a protocol crate, so these tests exercise the runtime rather than MCP.
pub struct Fake {
    protocol: Protocol,
    requires: Requirements,
    behaviour: Behaviour,
    path: &'static str,
    body: &'static str,
}

impl Fake {
    pub fn mounting(protocol: Protocol, path: &'static str, body: &'static str) -> Self {
        Self {
            protocol,
            requires: Requirements {
                http: true,
                ..Default::default()
            },
            behaviour: Behaviour::Mount,
            path,
            body,
        }
    }

    pub fn at(mut self, addr: &str) -> Self {
        self.requires.addr = Some(addr.to_string());
        self
    }

    pub fn behaving(mut self, behaviour: Behaviour) -> Self {
        self.behaviour = behaviour;
        self
    }

    pub fn needing_grpc(mut self) -> Self {
        self.requires.grpc = true;
        self.requires.http = false;
        self
    }
}

#[async_trait::async_trait]
impl Service for Fake {
    fn protocol(&self) -> Protocol {
        self.protocol
    }

    fn requires(&self) -> Requirements {
        self.requires.clone()
    }

    async fn serve(
        &self,
        cancel: CancellationToken,
        placement: Placement,
        ready: Arc<Ready>,
    ) -> Result<(), ServiceError> {
        match self.behaviour {
            Behaviour::FailBeforeReady => return Err("fake: refused to mount".into()),
            Behaviour::StopBeforeReady => return Ok(()),
            Behaviour::NeverMount => {
                cancel.cancelled().await;
                return Ok(());
            }
            Behaviour::Mount => {}
        }

        let (path, body) = (self.path, self.body);
        placement
            .mux
            .mount(move |router: Router| router.route(path, get(move || async move { body })));

        ready.ready(vec![Endpoint {
            protocol: self.protocol,
            transport: "test".into(),
            url: format!("http://{}{}", placement.addr, path),
            detail: path.into(),
        }]);

        cancel.cancelled().await;
        Ok(())
    }
}

/// A port nothing is listening on, for a test that needs to name one in advance.
pub async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

pub fn config(port: u16) -> Config {
    Config {
        identity: Identity {
            name: "test-service".into(),
            description: "a service under test".into(),
            version: "1.2.3".into(),
        },
        host: "127.0.0.1".into(),
        port,
        ..Default::default()
    }
}

/// A do-nothing agent, for tests about placement and cards rather than behaviour.
#[cfg(feature = "a2a")]
pub fn echo_agent() -> impl agents::a2a::Executor {
    agents::a2a::TextAgent::new(|text: String| async move { Ok(text) })
}
