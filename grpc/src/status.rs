//! Constructing and reading gRPC statuses.
//!
//! Go wraps `status.New`, `status.Error`, `status.Convert` and friends so callers import one
//! package. tonic's [`tonic::Status`] already carries the constructors as inherent methods,
//! so what is left here is the handful Go adds on top: the null-safe readers, and the
//! formatted constructors.

/// The gRPC status object, re-exported so callers need not import tonic directly.
pub type Status = tonic::Status;

/// A new status with the given code and message.
pub fn new(code: tonic::Code, message: impl Into<String>) -> Status {
    Status::new(code, message)
}

/// A new status whose message is formatted, the analogue of Go's `Newf`.
///
/// Rust has `format!` at the call site, so this exists only to keep a Go reader's habits
/// working; `Status::new(code, format!(...))` says the same thing.
pub fn newf(code: tonic::Code, args: std::fmt::Arguments<'_>) -> Status {
    Status::new(code, args.to_string())
}

/// The code carried by a status, or [`tonic::Code::Ok`] when there is none.
///
/// Go takes a nilable `*Status`; the Rust equivalent of that nil is [`Option`], so this
/// reads one and gives the same answer for the empty case.
pub fn code(status: Option<&Status>) -> tonic::Code {
    status.map_or(tonic::Code::Ok, Status::code)
}

/// The message carried by a status, or the empty string when there is none.
pub fn message(status: Option<&Status>) -> &str {
    status.map_or("", Status::message)
}

/// Converts any error into a status, passing one through unchanged.
///
/// This is Go's `status.Convert`. An error that is not already a status becomes
/// [`tonic::Code::Unknown`], which is what the gRPC libraries in every language do with an
/// error from outside their own status space.
pub fn convert(err: &(dyn std::error::Error + 'static)) -> Status {
    from_error(err).unwrap_or_else(|| Status::new(tonic::Code::Unknown, err.to_string()))
}

/// Extracts a status from an error, or `None` when the error is not one.
///
/// Go returns `(*Status, bool)`; [`Option`] carries the same two answers in one value.
pub fn from_error(err: &(dyn std::error::Error + 'static)) -> Option<Status> {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(current) = source {
        if let Some(status) = current.downcast_ref::<Status>() {
            return Some(status.clone());
        }
        source = current.source();
    }
    None
}
