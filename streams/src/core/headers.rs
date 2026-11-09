//! Message headers: a plain string map that travels beside the payload.

use std::collections::{BTreeMap, btree_map};

use serde::{Deserialize, Serialize};

use crate::{Result, StreamError};

/// The longest header key accepted, in bytes.
pub const MAX_KEY: usize = 256;
/// The longest header value accepted, in bytes.
pub const MAX_VALUE: usize = 4096;
/// The most header bytes, keys and values together, one message may carry.
///
/// Small enough for every broker's own limit, MQTT's user properties and
/// NATS's header block among them, so a header that is accepted here is not
/// refused later by the broker.
pub const MAX_TOTAL: usize = 16 * 1024;

/// Headers carried with a message, outside its payload.
///
/// A plain string-to-string map with no schema and no keys this crate reserves:
/// whatever a deployment wants to say about a message travels here, the
/// language that produced it (`lang`), a trace (`traceparent`), a content
/// type, a site. Keys are case-sensitive and kept in order, so the same
/// headers always encode to the same bytes.
///
/// A [`Stream`](crate::Stream) carries default headers stamped on everything
/// published through it; [`Options`](crate::Options) layers a publish's own on
/// top, a key set there winning. Each provider sends them with its broker's
/// own mechanism (Kafka record headers, NATS headers, AMQP properties, MQTT 5
/// user properties), so other tools on the broker see them too.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct Headers(BTreeMap<String, String>);

impl Headers {
    /// No headers.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets `key` to `value`, returning the value it replaced.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) -> Option<String> {
        self.0.insert(key.into(), value.into())
    }

    /// `key`'s value, if it is set.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Removes `key`, returning its value.
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.0.remove(key)
    }

    /// Whether `key` is set.
    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    /// How many headers are set.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether no header is set.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The headers in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// These headers with `over`'s laid on top: a key in both takes `over`'s
    /// value. This is how a stream's defaults meet a publish's own.
    pub fn merged(&self, over: &Headers) -> Headers {
        if over.is_empty() {
            return self.clone();
        }
        let mut out = self.clone();
        out.0
            .extend(over.0.iter().map(|(k, v)| (k.clone(), v.clone())));
        out
    }

    /// Checks the headers fit every broker this crate speaks to.
    ///
    /// A key is non-empty, at most [`MAX_KEY`] bytes, and made of visible ASCII
    /// other than `:`, which the line-based formats (NATS) use as the
    /// separator. A value is at most [`MAX_VALUE`] bytes with no line breaks or
    /// other control characters. All of it together is at most [`MAX_TOTAL`].
    /// The error names the offending header, so it fails at the call that set
    /// it rather than at a broker later.
    pub fn validate(&self) -> Result<()> {
        let mut total = 0;
        for (key, value) in &self.0 {
            if key.is_empty() {
                return Err(invalid("a header key is empty".into()));
            }
            if key.len() > MAX_KEY {
                return Err(invalid(format!(
                    "header key {:?}… is {} bytes, beyond the {MAX_KEY}-byte limit",
                    key.chars().take(32).collect::<String>(),
                    key.len()
                )));
            }
            if let Some(bad) = key.chars().find(|c| !c.is_ascii_graphic() || *c == ':') {
                return Err(invalid(format!(
                    "header key {key:?} contains {bad:?}; keys are visible ASCII without ':'"
                )));
            }
            if value.len() > MAX_VALUE {
                return Err(invalid(format!(
                    "header {key:?} has a {}-byte value, beyond the {MAX_VALUE}-byte limit",
                    value.len()
                )));
            }
            if value.chars().any(char::is_control) {
                return Err(invalid(format!(
                    "header {key:?} contains a control character; values are one line of text"
                )));
            }
            total += key.len() + value.len();
        }
        if total > MAX_TOTAL {
            return Err(invalid(format!(
                "headers total {total} bytes, beyond the {MAX_TOTAL}-byte limit"
            )));
        }
        Ok(())
    }

    /// Refuses any key starting with one of `prefixes`, which a provider's
    /// broker reserves for itself (NATS's `Nats-`, for one). Setting one would
    /// silently change what the broker does with the message.
    pub fn reject_reserved(&self, backend: &str, prefixes: &[&str]) -> Result<()> {
        for key in self.0.keys() {
            if let Some(prefix) = prefixes.iter().find(|p| key.starts_with(*p)) {
                return Err(invalid(format!(
                    "header key {key:?} uses the {prefix:?} prefix {backend} reserves"
                )));
            }
        }
        Ok(())
    }
}

fn invalid(reason: String) -> StreamError {
    StreamError::InvalidHeader(reason)
}

impl<K: Into<String>, V: Into<String>> FromIterator<(K, V)> for Headers {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self(
            iter.into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

impl<K: Into<String>, V: Into<String>> Extend<(K, V)> for Headers {
    fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
        self.0
            .extend(iter.into_iter().map(|(k, v)| (k.into(), v.into())));
    }
}

impl<'a> IntoIterator for &'a Headers {
    type Item = (&'a String, &'a String);
    type IntoIter = btree_map::Iter<'a, String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl IntoIterator for Headers {
    type Item = (String, String);
    type IntoIter = btree_map::IntoIter<String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_publish_overrides_the_stream() {
        let stream: Headers = [("lang", "rust"), ("site", "plant-3")]
            .into_iter()
            .collect();
        let publish: Headers = [("site", "plant-4"), ("traceparent", "00-ab")]
            .into_iter()
            .collect();
        let merged = stream.merged(&publish);
        assert_eq!(merged.get("lang"), Some("rust"));
        assert_eq!(merged.get("site"), Some("plant-4"));
        assert_eq!(merged.get("traceparent"), Some("00-ab"));
        assert_eq!(
            stream.get("site"),
            Some("plant-3"),
            "the stream's own stay as they were"
        );
    }

    #[test]
    fn validation_names_what_is_wrong() {
        let ok: Headers = [("lang", "rust"), ("content-type", "application/arrow")]
            .into_iter()
            .collect();
        assert!(ok.validate().is_ok());
        for (key, value, says) in [
            ("", "x", "empty"),
            ("has space", "x", "visible ASCII"),
            ("has:colon", "x", "visible ASCII"),
            ("lang", "two\nlines", "control"),
        ] {
            let h: Headers = [(key, value)].into_iter().collect();
            let error = h.validate().unwrap_err().to_string();
            assert!(error.contains(says), "{key:?}: {error}");
        }
        let long: Headers = [("k".repeat(MAX_KEY + 1), "v".to_owned())]
            .into_iter()
            .collect();
        assert!(long.validate().is_err());
        let big: Headers = (0..5)
            .map(|i| (format!("k{i}"), "v".repeat(MAX_VALUE)))
            .collect();
        assert!(big.validate().unwrap_err().to_string().contains("total"));
    }

    #[test]
    fn reserved_prefixes_are_refused() {
        let h: Headers = [("Nats-Msg-Id", "x")].into_iter().collect();
        let error = h.reject_reserved("NATS", &["Nats-"]).unwrap_err();
        assert!(error.to_string().contains("Nats-"), "{error}");
    }

    #[test]
    fn serialises_as_a_plain_json_object() {
        let h: Headers = [("lang", "rust")].into_iter().collect();
        assert_eq!(serde_json::to_string(&h).unwrap(), r#"{"lang":"rust"}"#);
        let back: Headers = serde_json::from_str(r#"{"lang":"rust"}"#).unwrap();
        assert_eq!(back, h);
    }
}
