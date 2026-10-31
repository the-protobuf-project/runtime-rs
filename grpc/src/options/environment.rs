//! The server's operating mode, and what it gates.

use std::fmt;
use std::str::FromStr;

/// The server's operating mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ServerEnvironment {
    /// Local development, often with relaxed security.
    #[default]
    Development,
    /// Verbose logging and other debugging aids.
    Debug,
    /// Mimics production for pre-deployment testing.
    Staging,
    /// Live deployment, with performance and security optimised.
    Production,
}

impl ServerEnvironment {
    /// The wire name, matching the Go constant's string value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Development => "development",
            Self::Debug => "debug",
            Self::Staging => "staging",
            Self::Production => "production",
        }
    }

    /// Whether server reflection should be registered.
    ///
    /// Reflection lets any client enumerate the services a process exposes, so it is on
    /// only where that is a convenience rather than a disclosure.
    pub fn exposes_reflection(&self) -> bool {
        matches!(self, Self::Debug | Self::Development)
    }
}

impl fmt::Display for ServerEnvironment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ServerEnvironment {
    type Err = crate::Error;

    /// Parses the wire names Go uses. Go's `IsValid` rejects anything else; a closed enum
    /// moves that rejection here, one step earlier.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "development" => Ok(Self::Development),
            "debug" => Ok(Self::Debug),
            "staging" => Ok(Self::Staging),
            "production" => Ok(Self::Production),
            other => Err(crate::Error::UnknownEnvironment(other.to_string())),
        }
    }
}
