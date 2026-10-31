//! Classifications are bounded and do not format sensitive backend errors.
use crate::Error;
use telemetry::Outcome;

pub(crate) fn outcome(error: Option<&Error>) -> Outcome {
    match error {
        None => Outcome::Ok,
        Some(Error::NotFound) => Outcome::NotFound,
        Some(Error::AlreadyExists) => Outcome::Conflict,
        Some(Error::Unsupported { .. }) => Outcome::Unsupported,
        Some(Error::Invalid(_) | Error::Encoding(_)) => Outcome::Invalid,
        Some(Error::Timeout) => Outcome::Timeout,
        Some(Error::Closed) => Outcome::Closed,
        Some(Error::Ingestion(error)) => outcome(Some(error.as_ref())),
        Some(_) => Outcome::Error,
    }
}
