//! High-level cache policies composed over core Driver capabilities.
//!
//! Strategies build high-level cache logic using the low-level Driver.
//! Each strategy has a different access pattern:
//! - Volatile: Key/value only, no enumeration
//! - Document: Whole values with enumeration via index
//! - Indexed: Document + secondary field lookups
//! - Aside: Read-through with Loader and bounded refresh coordination.
//!
//! Strategy modules generate keys only through Keyspace and never own cached
//! application storage. Private Flight and Refresher state coordinates tasks;
//! values remain behind Driver implementations.

pub mod aside;
pub mod document;
pub(crate) mod flight;
pub mod indexed;
pub(crate) mod refresher;
pub mod volatile;
pub use aside::AsideImpl;
pub use document::DocumentImpl;
pub use indexed::IndexedImpl;
pub use volatile::VolatileImpl;
