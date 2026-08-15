//! Strategy implementations
//! 
//! Strategies build high-level cache logic using the low-level Driver.
//! Each strategy has a different access pattern:
//! - Volatile: Key/value only, no enumeration
//! - Document: Whole values with enumeration via index
//! - Indexed: Document + secondary field lookups
//! - Aside: Read-through with loader

pub mod volatile;
pub mod document;
mod aside;
pub use document::DocumentImpl;
pub use volatile::VolatileImpl;
