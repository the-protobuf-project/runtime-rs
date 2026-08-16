//! Strategy implementations
//!
//! Strategies build high-level cache logic using the low-level Driver.
//! Each strategy has a different access pattern:
//! - Volatile: Key/value only, no enumeration
//! - Document: Whole values with enumeration via index
//! - Indexed: Document + secondary field lookups
//! - Aside: Read-through with loader

pub mod aside;
pub mod document;
mod flight;
mod refresher;
pub mod volatile;
pub use aside::AsideImpl;
pub use document::DocumentImpl;
pub use volatile::VolatileImpl;
