//! Time series captured to local Arrow IPC files for later analysis.
//!
//! Each database is a directory and each table a subdirectory of append-only
//! segments: `<root>/<database>/<table>/<created_ns>-<pid>-<seq>.arrows`. A
//! selection writes one segment per table and starts a new one after a restart
//! or a failed write, so an existing capture is never modified.
//!
//! Every write is one record batch, flushed before the call returns. A crash
//! loses at most the batch being written; [`load`] returns the rows before it
//! and reports the file as incomplete. Enable [`ArrowProvider::with_sync`] to
//! also survive power loss.
//!
//! Files use the IPC streaming format with non-null columns, UTC nanosecond
//! timestamps, and the table name and column roles stored as metadata, so they
//! open directly in other tools:
//!
//! ```text
//! pyarrow: t = pa.ipc.open_stream('….arrows').read_all()
//! duckdb:  duckdb.sql('SELECT object, avg(value) FROM t GROUP BY 1')
//! polars:  pl.read_ipc_stream('….arrows')
//! ```
//!
//! Segments rotate by size and age ([`Rotation`], default 1 GiB or 15 minutes).
//! Each cleanly closed segment gets a `.bounds` sidecar with its time range,
//! so range queries and aggregations skip files outside the window, then skip
//! batches outside it, and only turn rows inside it into records. The rows
//! that remain are filtered, sorted and reduced in memory, which suits a
//! capture reviewed later rather than a large shared archive.

mod aggregate;
mod bounds;
mod codec;
mod crud;
mod driver;
mod errors;
mod filter;
mod load;
mod provider;
mod rotation;
mod schema;
mod segment;
mod series;

pub use bounds::Bounds;
pub use driver::ArrowDriver;
pub use load::{Loaded, load, segments};
pub use provider::ArrowProvider;
pub use rotation::Rotation;
pub use segment::EXTENSION;
