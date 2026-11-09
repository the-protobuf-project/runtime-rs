//! Headers and expiry in AMQP basic properties.
//!
//! These are pure functions, kept apart from the broker code so the mapping
//! between [`Headers`] and an AMQP field table can be tested without a broker.

use std::time::Duration;

use lapin::{
    BasicProperties,
    types::{AMQPValue, FieldTable, LongString, ShortString},
};

use crate::{Headers, Result, StreamError};

/// The header-name prefix RabbitMQ keeps for itself: `x-delivery-count`,
/// `x-death`, `x-first-death-*`, the plugins' `x-delay` and
/// `x-deduplication-header`, and AMQP 1.0's `x-opt-*` annotations.
pub(crate) const RESERVED_PREFIX: &str = "x-";

/// Header names RabbitMQ reads as routing: sender-selected distribution sends
/// a message to every routing key listed in `CC` and `BCC`, so setting one
/// would change where the message goes.
pub(crate) const RESERVED_NAMES: [&str; 2] = ["CC", "BCC"];

/// The longest expiry sent, in milliseconds (`2^32 - 1`, about 49.7 days):
/// RabbitMQ's documented range for a TTL. Older brokers close the channel on a
/// larger `expiration`; 4.4 takes one without complaint, so a client cannot
/// rely on either, and a larger expiry is refused here instead.
pub(crate) const MAX_EXPIRY_MS: u128 = u32::MAX as u128;

/// Refuses a header RabbitMQ would act on rather than carry, and a key too long
/// for an AMQP field name.
pub(crate) fn check(headers: &Headers) -> Result<()> {
    headers.reject_reserved("RabbitMQ", &[RESERVED_PREFIX])?;
    for (key, _) in headers.iter() {
        if RESERVED_NAMES.contains(&key) {
            return Err(StreamError::InvalidHeader(format!(
                "header key {key:?} is RabbitMQ's sender-selected distribution, which would \
                 route the message to the keys it lists"
            )));
        }
        if ShortString::try_new(key).is_err() {
            return Err(StreamError::InvalidHeader(format!(
                "header key {:?}… is {} bytes, and AMQP limits a field name to 255",
                key.chars().take(32).collect::<String>(),
                key.len()
            )));
        }
    }
    Ok(())
}

/// The field table carrying `headers`, every value an AMQP long string, or
/// `None` for no headers, so a header-less publish sends no table at all and
/// stays byte-identical to runtime-go's. The caller has run [`check`].
pub(crate) fn table(headers: &Headers) -> Option<FieldTable> {
    if headers.is_empty() {
        return None;
    }
    let mut table = FieldTable::default();
    for (key, value) in headers.iter() {
        table.insert(
            ShortString::from(key),
            AMQPValue::LongString(LongString::from(value)),
        );
    }
    Some(table)
}

/// The headers a delivery arrived with.
///
/// Only string values are headers in this contract, so a value of another
/// AMQP type (a number or table some other client set) is left out, as are
/// the names RabbitMQ reserves: what it adds on the way, `x-delivery-count`
/// on a quorum queue or `x-death` after dead-lettering, is the broker's
/// bookkeeping rather than something the publisher said.
pub(crate) fn received(properties: &BasicProperties) -> Headers {
    let Some(table) = properties.headers() else {
        return Headers::new();
    };
    table
        .inner()
        .iter()
        .filter(|(key, _)| {
            !key.as_str().starts_with(RESERVED_PREFIX) && !RESERVED_NAMES.contains(&key.as_str())
        })
        .filter_map(|(key, value)| {
            let value = match value {
                AMQPValue::LongString(s) => std::str::from_utf8(s.as_bytes()).ok()?.to_owned(),
                AMQPValue::ShortString(s) => s.as_str().to_owned(),
                _ => return None,
            };
            Some((key.as_str().to_owned(), value))
        })
        .collect()
}

/// The `expiration` property for `expires`: whole milliseconds as a decimal
/// string, rounded up so a sub-millisecond expiry is not read as `0`, which
/// RabbitMQ takes to mean "deliver now or drop".
pub(crate) fn expiration(expires: Duration) -> Result<ShortString> {
    let ms = expires.as_nanos().div_ceil(1_000_000);
    if ms > MAX_EXPIRY_MS {
        return Err(StreamError::Unsupported(format!(
            "an expiry of {expires:?} is beyond RabbitMQ's per-message limit of {MAX_EXPIRY_MS} ms"
        )));
    }
    Ok(ShortString::from(ms.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    /// A header set from literal pairs.
    fn headers(pairs: &[(&str, &str)]) -> Headers {
        pairs.iter().copied().collect()
    }

    #[test]
    fn headers_become_long_strings_and_come_back() {
        let sent = headers(&[("lang", "rust"), ("traceparent", "00-ab-cd-01")]);
        let table = table(&sent).unwrap();
        assert_eq!(
            table.inner().get("lang"),
            Some(&AMQPValue::LongString("rust".into()))
        );
        let properties = BasicProperties::default().with_headers(table);
        assert_eq!(received(&properties), sent);
    }

    #[test]
    fn no_headers_send_no_table() {
        assert!(table(&Headers::new()).is_none());
        assert!(received(&BasicProperties::default()).is_empty());
    }

    #[test]
    fn received_keeps_strings_and_drops_the_brokers_bookkeeping() {
        let table = FieldTable::from(BTreeMap::from([
            (
                ShortString::from("lang"),
                AMQPValue::LongString("go".into()),
            ),
            (
                ShortString::from("site"),
                AMQPValue::ShortString("p3".into()),
            ),
            (
                ShortString::from("x-delivery-count"),
                AMQPValue::LongLongInt(2),
            ),
            (
                ShortString::from("x-first-death-queue"),
                AMQPValue::LongString("q".into()),
            ),
            (ShortString::from("CC"), AMQPValue::LongString("k".into())),
            (ShortString::from("count"), AMQPValue::LongInt(3)),
            (
                ShortString::from("binary"),
                AMQPValue::LongString(vec![0xff, 0xfe].into()),
            ),
        ]));
        let got = received(&BasicProperties::default().with_headers(table));
        assert_eq!(got, headers(&[("lang", "go"), ("site", "p3")]));
    }

    #[test]
    fn reserved_names_are_refused() {
        for key in ["x-delivery-count", "x-death", "x-delay", "CC", "BCC"] {
            let error = check(&headers(&[(key, "v")])).unwrap_err();
            assert!(
                matches!(error, StreamError::InvalidHeader(_)),
                "{key}: {error}"
            );
        }
        // Only the exact names and the lower-case prefix mean anything to the broker.
        assert!(
            check(&headers(&[
                ("X-Request-Id", "1"),
                ("cc", "x"),
                ("CCX", "y")
            ]))
            .is_ok()
        );
        let long = headers(&[(&"k".repeat(256), "v")]);
        assert!(check(&long).unwrap_err().to_string().contains("255"));
        assert!(check(&headers(&[(&"k".repeat(255), "v")])).is_ok());
    }

    #[test]
    fn expiration_is_whole_milliseconds_rounded_up() {
        assert_eq!(expiration(Duration::from_secs(5)).unwrap().as_str(), "5000");
        assert_eq!(expiration(Duration::from_micros(1)).unwrap().as_str(), "1");
        assert_eq!(
            expiration(Duration::from_micros(1500)).unwrap().as_str(),
            "2"
        );
        let max = Duration::from_millis(u64::from(u32::MAX));
        assert_eq!(expiration(max).unwrap().as_str(), u32::MAX.to_string());
        let error = expiration(max + Duration::from_millis(1)).unwrap_err();
        assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
    }
}
