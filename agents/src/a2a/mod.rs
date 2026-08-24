//! The Agent2Agent runtime: an agent, the skills it advertises, and the transports it
//! answers on, as one [`crate::Service`].
//!
//! It wraps the official [`a2a-rs`](https://github.com/a2aproject/a2a-rs) SDK the same way
//! the Go package wraps `a2a-go` — the protocol is the SDK's, and what this module adds is
//! placement: an agent registered here gets its identity from the runtime and its address
//! from wherever the runtime decided to put it.
//!
//! ```no_run
//! # use agents::{Config, Identity, Runtime};
//! # use agents::a2a;
//! # use a2a_server::{AgentExecutor, ExecutorContext};
//! # use futures::stream::BoxStream;
//! # struct Echo;
//! # #[async_trait::async_trait]
//! # impl AgentExecutor for Echo {
//! #     fn execute(&self, _: ExecutorContext) -> BoxStream<'static, Result<::a2a::StreamResponse, ::a2a::A2AError>> { Box::pin(futures::stream::empty()) }
//! #     fn cancel(&self, _: ExecutorContext) -> BoxStream<'static, Result<::a2a::StreamResponse, ::a2a::A2AError>> { Box::pin(futures::stream::empty()) }
//! # }
//! # async fn example() -> agents::Result<()> {
//! let runtime = Runtime::new(Config {
//!     identity: Identity { name: "my-agent".into(), version: "1.0.0".into(), ..Default::default() },
//!     port: 9000,
//!     ..Default::default()
//! })
//! .register(a2a::Service::new(Echo, vec![a2a::skill("echo", "Echo", "Repeats the input")]));
//!
//! runtime.serve(tokio_util::sync::CancellationToken::new()).await
//! # }
//! ```

mod card;
mod serve;
mod service;
mod transport;

pub use service::Service;
pub use transport::{Transport, parse_transports};

use a2a::{AgentCapabilities, AgentSkill};
use a2a_server::WELL_KNOWN_AGENT_CARD_PATH;

/// One distinct capability an agent advertises on its card.
pub type Skill = AgentSkill;

/// The optional protocol features an agent supports.
pub type Capabilities = AgentCapabilities;

/// Where a client fetches an agent's card. Singular per host.
pub const AGENT_CARD_PATH: &str = WELL_KNOWN_AGENT_CARD_PATH;

/// Where JSON-RPC mounts, and the prefix REST is served under.
pub const DEFAULT_BASE_PATH: &str = "/a2a";

/// A skill with just the three fields a card cannot do without.
///
/// The SDK's [`AgentSkill`] has eight more, all optional; this is the shape most callers
/// actually write.
pub fn skill(
    id: impl Into<String>,
    name: impl Into<String>,
    description: impl Into<String>,
) -> Skill {
    Skill {
        id: id.into(),
        name: name.into(),
        description: description.into(),
        tags: Vec::new(),
        examples: None,
        input_modes: None,
        output_modes: None,
        security_requirements: None,
    }
}
