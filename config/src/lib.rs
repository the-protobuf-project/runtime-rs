//! Layered application configuration, the Rust twin of the configuration
//! runtime-go programs load: the application's own defaults, a development
//! profile, configuration files in TOML, YAML or JSON, and environment
//! variables, merged by [Figment](https://docs.rs/figment) and read into the
//! application's typed sections.
//!
//! Precedence, from lowest to highest -- each layer overrides the ones before
//! it, and the environment overrides everything:
//!
//! 1. the application's defaults, compiled into the binary
//!    ([`Loader::release`]);
//! 2. the development profile ([`Loader::dev_profile`], [`Loader::dev`]): for
//!    an application called `acme`, `acme.dev.toml` from the working
//!    directory when it is there, else the embedded profile when
//!    `ACME_PROFILE=dev` asks for it, and never with `ACME_PROFILE=release`;
//! 3. configuration files, in TOML, YAML or JSON by extension:
//!    [`Loader::file`], the first of [`Loader::discover`]'s paths that exists,
//!    and the file `ACME_CONFIG` names;
//! 4. any [`Provider`](figment::Provider) of the application's own
//!    ([`Loader::provider`]);
//! 5. environment variables, last so they win: `ACME_SERVER__GRPC__PORT=50051`
//!    sets `server.grpc.port`, double underscores between levels.
//!
//! ```
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct Server { host: String, port: u16 }
//!
//! #[derive(Deserialize)]
//! struct App { server: Server }
//!
//! let config = config::Loader::new("acme")
//!     .release(config::Source::toml(r#"
//!         [server]
//!         host = "0.0.0.0"
//!         port = 50051
//!     "#))
//!     .load()?;
//! let app: App = config.extract()?;
//! assert_eq!(app.server.port, 50051);
//! // A section the application declares on its own, read where it is used.
//! let server: Server = config.section("server")?;
//! assert_eq!(server.host, "0.0.0.0");
//! # Ok::<(), config::Error>(())
//! ```
//!
//! A section extracted with [`Config::extract_validated`] runs its
//! [`Validate`] check, so a configuration the program cannot run on is refused
//! at startup, naming the key at fault. Lists set from the environment are
//! comma-separated; [`de::comma_list`] reads both forms.
#![warn(missing_docs)]

mod config;
pub mod de;
mod env;
mod error;
pub mod global;
mod loader;
mod profile;
mod source;
mod validate;

pub use crate::config::Config;
pub use error::Error;
pub use loader::Loader;
pub use profile::Profile;
pub use source::{Format, Source};
pub use validate::Validate;

/// Figment, re-exported for an application that adds a provider of its own
/// ([`Loader::provider`]) or reaches the merged configuration directly
/// ([`Config::figment`]) without adding it to its own `Cargo.toml`.
pub use figment;
