//! SDK error translation kept at the backend boundary.

use crate::store::Error;

pub(super) fn sdk(error: influxdb3_client::Error) -> Error {
    match error {
        influxdb3_client::Error::Timeout(_) => Error::Timeout,
        other => Error::Backend {
            backend: "influxdb3",
            source: Box::new(other),
        },
    }
}
