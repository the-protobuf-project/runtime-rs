//! The process's configuration, loaded once at startup and read anywhere.
//!
//! ```
//! # fn main() -> Result<(), config::Error> {
//! let loaded = config::Loader::new("acme").release(config::Source::toml("name = \"acme\"")).load()?;
//! config::global::install(loaded)?;
//! // ... anywhere later:
//! let name: String = config::global::get().expect("installed at startup").section("name")?;
//! assert_eq!(name, "acme");
//! # Ok(()) }
//! ```

use std::sync::OnceLock;

use crate::{Config, Error};

static GLOBAL: OnceLock<Config> = OnceLock::new();

/// Makes config the process's configuration. It installs once: a second call
/// returns [`Error::AlreadyInstalled`] and leaves the first in place.
pub fn install(config: Config) -> Result<&'static Config, Error> {
    let mut installed = false;
    let out = GLOBAL.get_or_init(|| {
        installed = true;
        config
    });
    match installed {
        true => Ok(out),
        false => Err(Error::AlreadyInstalled),
    }
}

/// The installed configuration, `None` before [`install`].
pub fn get() -> Option<&'static Config> {
    GLOBAL.get()
}
