//! What can go wrong loading or reading configuration.

use std::path::PathBuf;

/// A configuration that could not be loaded, read or accepted.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The profile variable holds something other than `dev`, `release` or
    /// nothing.
    #[error("{var}={value:?}: want dev or release")]
    Profile {
        /// The variable, e.g. `ACME_PROFILE`.
        var: String,
        /// What it holds.
        value: String,
    },
    /// A file's extension names no supported format.
    #[error("{0}: unsupported config file format (want .toml, .yaml, .yml or .json)")]
    UnsupportedFormat(PathBuf),
    /// A required file does not exist.
    #[error("config file {0} does not exist")]
    MissingFile(PathBuf),
    /// A layer does not parse, or a value does not fit the type it is read
    /// into. The message names the key and the layer it came from.
    #[error("config invalid: {0}")]
    Figment(Box<figment::Error>),
    /// A section's [`Validate`](crate::Validate) check refused it.
    #[error("config invalid: {0}")]
    Invalid(String),
    /// [`global::install`](crate::global::install) was called twice.
    #[error("the global configuration is already installed")]
    AlreadyInstalled,
}

impl From<figment::Error> for Error {
    fn from(err: figment::Error) -> Error {
        Error::Figment(Box::new(err))
    }
}
