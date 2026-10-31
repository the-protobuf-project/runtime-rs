//! Database-wide expiry, chosen when InfluxAdmin creates a database.
use std::time::Duration;

/// Database-wide expiry selected explicitly during database creation.
#[derive(Clone, Copy, Debug)]
pub enum Retention {
    /// Keep data indefinitely.
    Infinite,
    /// Retain this period. InfluxDB Core requires whole minutes of at least
    /// one hour and cannot change the period after database creation.
    For(Duration),
}
