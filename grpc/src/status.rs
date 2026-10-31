//! Constructing and reading gRPC statuses.
//!
//! Go wraps `status.New`, `status.Error`, `status.Convert` and friends so callers import one
//! package. tonic's [`tonic::Status`] already carries the constructors as inherent methods,
//! so what is left here is the handful Go adds on top: the null-safe readers, and the
//! formatted constructors.
//!
//! Go's nilable `error` for an OK status becomes [`Option`] here, and the protobuf
//! `google.rpc.Status` is `tonic-types`' [`RpcStatus`], which is also how rich details
//! (`ErrorInfo`, `BadRequest`, ...) are attached and read.

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

/// The protobuf form of a status, `google.rpc.Status`, re-exported so callers can build and
/// read one without depending on `tonic-types` themselves.
pub type RpcStatus = tonic_types::Status;

/// A status as an error, or `None` when the code is [`tonic::Code::Ok`].
///
/// This is Go's `status.Error`, whose `nil` for OK becomes [`None`]: an OK status is not
/// an error, and `Option` says so in the type rather than in a comment.
pub fn error(code: tonic::Code, message: impl Into<String>) -> Option<Status> {
    (code != tonic::Code::Ok).then(|| Status::new(code, message))
}

/// [`error`] with a formatted message, the analogue of Go's `Errorf`.
pub fn errorf(code: tonic::Code, args: std::fmt::Arguments<'_>) -> Option<Status> {
    error(code, args.to_string())
}

/// A status built from its protobuf form, details and all. Go's `status.FromProto`.
///
/// The details travel as the encoded `google.rpc.Status`, which is the form
/// `grpc-status-details-bin` carries on the wire and the form `tonic-types` reads them
/// back from.
pub fn from_proto(proto: &RpcStatus) -> Status {
    let code = tonic::Code::from_i32(proto.code);
    if proto.details.is_empty() {
        return Status::new(code, proto.message.clone());
    }
    Status::with_details(
        code,
        proto.message.clone(),
        bytes::Bytes::from(prost::Message::encode_to_vec(proto)),
    )
}

/// [`from_proto`] as an error, or `None` when the code is OK. Go's `status.ErrorProto`.
pub fn error_proto(proto: &RpcStatus) -> Option<Status> {
    (proto.code != tonic::Code::Ok as i32).then(|| from_proto(proto))
}

/// The protobuf form of a status, with every detail it carries. Go's `(*Status).Proto`.
///
/// Details that do not decode are dropped rather than failing: a status is how a call
/// reports trouble, and the code and message are still worth having without them.
pub fn to_proto(status: &Status) -> RpcStatus {
    let details = if status.details().is_empty() {
        Vec::new()
    } else {
        <RpcStatus as prost::Message>::decode(status.details())
            .map(|proto| proto.details)
            .unwrap_or_default()
    };
    RpcStatus {
        code: status.code() as i32,
        message: status.message().to_string(),
        details,
    }
}

/// The status with `details` added after the ones it already carries — Go's
/// `(*Status).WithDetails`, keeping the code, message and every existing detail, including
/// ones `tonic-types` has no type for.
pub fn with_details(status: &Status, details: Vec<tonic_types::ErrorDetail>) -> Status {
    use tonic_types::StatusExt;

    let mut proto = to_proto(status);
    // tonic-types packs a detail into its `Any` privately; the encoded status it builds is
    // the public way to get at the packed form.
    let packed = Status::with_error_details_vec(tonic::Code::Unknown, "", details);
    if let Ok(extra) = <RpcStatus as prost::Message>::decode(packed.details()) {
        proto.details.extend(extra.details);
    }
    let mut out = from_proto(&proto);
    *out.metadata_mut() = status.metadata().clone();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic_types::{ErrorDetail, ErrorInfo, ResourceInfo, StatusExt};

    #[test]
    fn an_ok_code_is_no_error() {
        assert!(error(tonic::Code::Ok, "fine").is_none());
        assert!(errorf(tonic::Code::Ok, format_args!("{}", 1)).is_none());
        let err = errorf(tonic::Code::NotFound, format_args!("shelf {} is gone", "s")).unwrap();
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert_eq!(err.message(), "shelf s is gone");
    }

    #[test]
    fn the_proto_form_round_trips_with_its_details() {
        let status = Status::with_error_details_vec(
            tonic::Code::AlreadyExists,
            "taken",
            vec![ErrorDetail::ErrorInfo(ErrorInfo::new(
                "TAKEN",
                "shop",
                std::collections::HashMap::new(),
            ))],
        );
        let proto = to_proto(&status);
        assert_eq!(proto.code, tonic::Code::AlreadyExists as i32);
        assert_eq!(proto.details.len(), 1);

        let back = from_proto(&proto);
        assert_eq!(back.code(), tonic::Code::AlreadyExists);
        assert_eq!(back.message(), "taken");
        assert_eq!(back.get_details_error_info().unwrap().reason, "TAKEN");

        assert!(error_proto(&RpcStatus::default()).is_none());
        assert_eq!(
            error_proto(&proto).map(|s| s.code()),
            Some(tonic::Code::AlreadyExists)
        );
    }

    #[test]
    fn details_are_added_after_the_existing_ones() {
        let status = Status::with_error_details_vec(
            tonic::Code::NotFound,
            "gone",
            vec![ErrorDetail::ErrorInfo(ErrorInfo::new(
                "NOT_FOUND",
                "shop",
                std::collections::HashMap::new(),
            ))],
        );
        let more = with_details(
            &status,
            vec![ErrorDetail::ResourceInfo(ResourceInfo::new(
                "shop/Shelf",
                "shelves/s",
                "",
                "does not exist",
            ))],
        );
        assert_eq!(to_proto(&more).details.len(), 2);
        assert_eq!(more.get_details_error_info().unwrap().reason, "NOT_FOUND");
        assert_eq!(
            more.get_details_resource_info().unwrap().resource_name,
            "shelves/s"
        );
    }
}
