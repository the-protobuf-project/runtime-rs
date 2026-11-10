//! The backend-agnostic stream contract.
//!
//! [`Publisher`] and [`Subscriber`] are the honest intersection of every
//! backend: send a payload, receive it, no promise that it survives a restart.
//! [`Durable`], [`Positioned`], and [`Batch`] are everything past that, and a
//! provider exposes one only if its backend has it. A caller reaching for one
//! a provider lacks gets [`StreamError::Unsupported`](crate::StreamError)
//! rather than a silent downgrade to weaker delivery than it asked for, and
//! [`require`] asks for several at startup.

mod contract;
mod headers;
mod options;
mod types;

pub use contract::{
    Batch, Capability, Deliveries, Durable, Manager, Positioned, Publisher, Streams, Subscriber,
    Subscription, as_batch, as_durable, as_positioned, require,
};
pub use headers::{Headers, MAX_KEY, MAX_TOTAL, MAX_VALUE};
pub use options::Options;
pub use types::{Acker, Delivery, Message, Position, Stream};
