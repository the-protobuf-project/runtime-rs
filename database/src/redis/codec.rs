//! What the Redis driver keeps beside its commands: encoding, managed values, the order and
//! filter it reads from list options, and its errors. Go's helpers in `database/redis/driver.go`.

use prost::Message;
use prost_reflect::{DynamicMessage, ReflectMessage};

use super::BACKEND;
use crate::records::{Resource, columns_to_message, core, message_to_columns};
use crate::{Error, Result};

/// A record's stored bytes: proto wire format, which loses neither bytes nor wide integers.
pub(super) fn encode(msg: &DynamicMessage) -> Vec<u8> {
    msg.encode_to_vec()
}

/// Stored bytes as a message of the resource's type. Go's `decode`.
pub(super) fn decode(res: &Resource, body: &[u8]) -> Result<DynamicMessage> {
    DynamicMessage::decode(res.message.clone(), body)
        .map_err(|e| failed(format!("stored {} does not decode", res.name), e))
}

/// `msg` with the values the driver supplies: a generated key and audit times. Go's
/// `fillManaged`, which rebuilds the message from its columns; this copies only the managed
/// fields onto a clone, so a field no column maps survives. On update, `old` supplies the
/// create time, which is the record's, as an SQL backend leaves that column alone.
pub(super) fn fill_managed(
    res: &Resource,
    msg: &DynamicMessage,
    on_create: bool,
    old: Option<&DynamicMessage>,
) -> Result<DynamicMessage> {
    if !core::has_managed(res) {
        return Ok(msg.clone());
    }
    let mut cols = message_to_columns(res, msg)?;
    core::fill_managed(res, &mut cols, on_create);
    let filled = columns_to_message(res, &cols)?;
    let mut out = msg.clone();
    for col in res
        .columns
        .iter()
        .filter(|c| c.managed() && !c.field.is_empty())
    {
        let Some(fd) = out.descriptor().get_field_by_name(&col.field) else {
            continue;
        };
        let keeps_old = !on_create && col.auto_create && !col.auto_update;
        let from = match old {
            Some(old) if keeps_old => old,
            _ => &filled,
        };
        if from.has_field(&fd) {
            out.set_field(&fd, from.get_field(&fd).into_owned());
        } else {
            out.clear_field(&fd);
        }
    }
    Ok(out)
}

/// Whether an AIP-132 order asks for the primary key in reverse; any other column orders by
/// the key, as `ListOptions` permits. Go's `descending`.
pub(super) fn descending(order_by: &str, res: &Resource) -> bool {
    let lower = order_by.to_lowercase();
    let mut fields = lower.split_whitespace();
    match fields.next() {
        Some(first) if first.eq_ignore_ascii_case(&res.pk_column) => fields.next() == Some("desc"),
        _ => false,
    }
}

/// Refuses a filter rather than ignoring it: Redis has no query language, and returning the
/// unfiltered set is the one outcome the contract forbids. Go's `refuseFilter`.
pub(super) fn refuse_filter(filter: &str) -> Result<()> {
    if filter.trim().is_empty() {
        return Ok(());
    }
    Err(Error::Unimplemented {
        backend: BACKEND,
        capability: "apply an AIP-160 filter (comparisons, AND, OR, NOT, has): it has no \
                     query language, so list by key and narrow in the caller, or hold this \
                     resource in a backend that can query",
    })
}

/// A backend failure, with what was being done.
pub(super) fn failed(
    action: impl Into<String>,
    source: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    Error::Backend {
        backend: BACKEND,
        source: Box::new(Failure {
            action: action.into(),
            source: Box::new(source),
        }),
    }
}

/// A failure and the step it failed in, e.g. "cannot store books/dune: connection refused".
#[derive(Debug)]
struct Failure {
    action: String,
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.action, self.source)
    }
}

impl std::error::Error for Failure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// A failure with no underlying error, e.g. a record that kept changing under an update.
#[derive(Debug)]
pub(super) struct Gave(pub(super) String);

impl std::fmt::Display for Gave {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Gave {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{Column, Kind};

    #[test]
    fn only_the_key_orders_in_reverse_and_filters_are_refused() {
        let ts = prost_reflect::DescriptorPool::global()
            .get_message_by_name("google.protobuf.Timestamp")
            .unwrap();
        let res = Resource::new(ts, "t", "id", vec![Column::new("id", Kind::String)]);
        for (order, want) in [("id desc", true), ("ID DESC", true), ("id", false)] {
            assert_eq!(descending(order, &res), want, "{order}");
        }
        assert!(!descending("title desc", &res));
        assert!(!descending("", &res));
        refuse_filter("  ").unwrap();
        let err = refuse_filter("a = 1").unwrap_err();
        assert!(err.to_string().contains("AIP-160"), "{err}");
        assert!(matches!(
            err,
            Error::Unimplemented {
                backend: "redis",
                ..
            }
        ));
    }
}
