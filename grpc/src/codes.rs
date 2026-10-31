//! The gRPC status codes, namespaced so callers need not reach into tonic for them.

/// The standard gRPC status code type.
///
/// Go aliases `codes.Code`; this aliases tonic's equivalent for the same reason — so a
/// caller writes one import rather than two.
pub type StatusCodeType = tonic::Code;

/// The standard gRPC status codes, reachable without importing tonic directly.
///
/// Go exposes these as fields on a package-level struct value, which Rust has no need to
/// imitate: associated constants read the same at a call site and cost nothing at runtime.
///
/// ```
/// use grpc::StatusCode;
///
/// assert_eq!(StatusCode::NOT_FOUND, tonic::Code::NotFound);
/// ```
pub struct StatusCode;

impl StatusCode {
    /// The operation completed successfully.
    pub const OK: StatusCodeType = tonic::Code::Ok;
    /// An unknown error, used when an error came from a different address space.
    pub const UNKNOWN: StatusCodeType = tonic::Code::Unknown;
    /// The operation was cancelled, typically by the caller.
    pub const CANCELED: StatusCodeType = tonic::Code::Cancelled;
    /// The client specified an invalid argument. Differs from `FAILED_PRECONDITION`.
    pub const INVALID_ARGUMENT: StatusCodeType = tonic::Code::InvalidArgument;
    /// The deadline expired before the operation could complete.
    pub const DEADLINE_EXCEEDED: StatusCodeType = tonic::Code::DeadlineExceeded;
    /// A requested entity was not found.
    pub const NOT_FOUND: StatusCodeType = tonic::Code::NotFound;
    /// An attempt to create an entity failed because it already exists.
    pub const ALREADY_EXISTS: StatusCodeType = tonic::Code::AlreadyExists;
    /// The caller does not have permission to execute the operation.
    pub const PERMISSION_DENIED: StatusCodeType = tonic::Code::PermissionDenied;
    /// Some resource has been exhausted — a per-user quota, or disk space.
    pub const RESOURCE_EXHAUSTED: StatusCodeType = tonic::Code::ResourceExhausted;
    /// The system is not in a state required for the operation.
    pub const FAILED_PRECONDITION: StatusCodeType = tonic::Code::FailedPrecondition;
    /// The operation was aborted, often due to a concurrency issue.
    pub const ABORTED: StatusCodeType = tonic::Code::Aborted;
    /// The operation was attempted past the valid range.
    pub const OUT_OF_RANGE: StatusCodeType = tonic::Code::OutOfRange;
    /// The operation is not implemented or not supported.
    pub const UNIMPLEMENTED: StatusCodeType = tonic::Code::Unimplemented;
    /// An internal error: an invariant expected by the underlying system was broken.
    pub const INTERNAL: StatusCodeType = tonic::Code::Internal;
    /// The service is currently unavailable, most likely transiently.
    pub const UNAVAILABLE: StatusCodeType = tonic::Code::Unavailable;
    /// Unrecoverable data loss or corruption.
    pub const DATA_LOSS: StatusCodeType = tonic::Code::DataLoss;
    /// The request lacks valid authentication credentials.
    pub const UNAUTHENTICATED: StatusCodeType = tonic::Code::Unauthenticated;
}
