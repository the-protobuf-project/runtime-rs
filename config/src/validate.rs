//! Checks a section runs when it is extracted.

/// A section that can refuse a configuration the program cannot run on, at
/// startup rather than at whichever call reads it first.
///
/// ```
/// struct Server { port: u16, shutdown_timeout_seconds: i64 }
///
/// impl config::Validate for Server {
///     fn validate(&self) -> Result<(), String> {
///         if self.shutdown_timeout_seconds < 0 {
///             return Err(format!(
///                 "server.shutdown_timeout_seconds={} (want 0 for the default, or more)",
///                 self.shutdown_timeout_seconds,
///             ));
///         }
///         Ok(())
///     }
/// }
/// ```
pub trait Validate {
    /// Ok, or what is wrong, naming the key: `server.mcp.path="mcp" (want an
    /// absolute path such as /mcp)`.
    fn validate(&self) -> Result<(), String>;
}
