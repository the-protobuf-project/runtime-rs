//! Shared telemetry bindings for runtime packages, compatible with telemetry-rs.
//!
//! Applications own provider initialization, exporters, configuration and flush/
//! shutdown. Runtime libraries receive explicit handles; default is no-op.
//! No observer installs global providers, subscribers or background exporters.
//!
//! ```
//! use telemetry::{Telemetry, Outcome, SpanKind};
//! let observer = Telemetry::default().observer("database");
//! let operation = observer.start("influxdb3", "write", SpanKind::Client);
//! operation.finish(Outcome::Ok);
//! ```
//!
//! Use with_meter/with_tracer/with_logger with any OTel 0.33 provider. The
//! optional tpp-sdk feature adapts the linked telemetry-rs SDK directly. Keep
//! that SDK owner alive until runtime workers have drained, then flush/close it.
//! Logs contain bounded classifications and explicit trace/span IDs; libraries
//! must never supply SQL, tenant names, tokens, record IDs or raw errors as
//! attributes. Complete stream lifetime and future cancellation use guards.
#![warn(missing_docs)]
mod config;
mod logging;
mod metrics;
mod observer;
mod operation;
mod outcome;
#[cfg(feature = "tpp-sdk")]
mod tpp;
pub use config::Telemetry;
pub use logging::{LogSink, Severity, TracingLogs};
pub use observer::Observer;
pub use opentelemetry::{
    self, Context, KeyValue,
    trace::{FutureExt, SpanKind},
};
pub use operation::Operation;
pub use outcome::Outcome;

/// Application SDK re-export; available only with the explicit tpp-sdk feature.
#[cfg(feature = "tpp-sdk")]
pub use telemetry_sdk as sdk;
