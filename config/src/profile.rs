//! Which profile is layered over the release defaults.

use std::fmt;

use crate::Error;

/// The profile a load asks for, from `<PREFIX>PROFILE` or
/// [`Loader::profile`](crate::Loader::profile).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Profile {
    /// The development profile from the working directory when its file is
    /// there (a checkout, so edits apply without a rebuild), and none
    /// otherwise: the default.
    #[default]
    Auto,
    /// The development profile: its file when it is there, else the embedded
    /// copy.
    Dev,
    /// The release defaults alone, even in a checkout -- how a production tool
    /// run from one keeps to them.
    Release,
}

impl Profile {
    /// Parses a profile variable's value: empty is [`Profile::Auto`].
    pub fn parse(var: &str, value: &str) -> Result<Profile, Error> {
        match value {
            "" => Ok(Profile::Auto),
            "dev" => Ok(Profile::Dev),
            "release" => Ok(Profile::Release),
            other => Err(Error::Profile {
                var: var.to_owned(),
                value: other.to_owned(),
            }),
        }
    }
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Profile::Auto => "auto",
            Profile::Dev => "dev",
            Profile::Release => "release",
        })
    }
}
