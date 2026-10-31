//! The startup summary a server prints once every component is up: the service name as ASCII
//! art, a build line, a table of everything that came up, and any warnings under it.
//!
//! It is a faithful port of the Go runtime's banner — same font, same table, same rows — and
//! the tests hold it to output captured from that runtime rather than to expectations written
//! here. See `tests/banner_figure.rs` and `tests/banner_table.rs`.
//!
//! ```no_run
//! use grpc::banner::{self, Summary};
//! use grpc::options::Options;
//!
//! let opts = Options { service_name: "bookstore".into(), ..Default::default() };
//! banner::print(&opts, &Summary::default());
//! ```

mod figure;
mod font;
mod print;
mod style;
mod summary;
mod table;

pub use figure::render as render_figure;
pub use print::{print, render};
pub use style::{BUILD_REVISION, environment_label, pluralise, tls_label, user_service_names};
pub use summary::{Summary, rows};
pub use table::{HEADERS, Row, render as render_table};
