//! The backend-agnostic stream contract.
//!
//! [`Publisher`] and [`Subscriber`] are the honest intersection of every
//! backend: send a payload, receive it, no promise that it survives a restart.
//! [`Durable`], [`Positioned`], and [`Batch`] are everything past that, and a
//! provider exposes one only if its backend has it. A caller reaching for one
//! a provider lacks gets [`StreamError::Unsupported`](crate::StreamError)
//! rather than a silent downgrade to weaker delivery than it asked for.

mod contract;
mod options;
mod types;

pub use contract::{
    Batch, Deliveries, Durable, Manager, Positioned, Publisher, Streams, Subscriber, Subscription,
    as_batch, as_durable, as_positioned,
};
pub use options::Options;
pub use types::{Acker, Delivery, Message, Position, Stream};
