//! Deserializers for values the environment can only spell as one string.

use std::fmt;

use serde::Deserializer;
use serde::de::{SeqAccess, Visitor};

/// Reads a list of strings from a sequence or from one comma-separated
/// string, so `allowed_origins = ["https://a.example", "https://b.example"]`
/// in TOML and `ACME_SERVER__CORS__ALLOWED_ORIGINS=https://a.example,https://b.example`
/// from the environment read the same. Items are trimmed and empty ones
/// dropped.
///
/// ```
/// #[derive(serde::Deserialize)]
/// struct Cors {
///     #[serde(default, deserialize_with = "config::de::comma_list")]
///     allowed_origins: Vec<String>,
/// }
/// ```
pub fn comma_list<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<String>, D::Error> {
    deserializer.deserialize_any(CommaList)
}

struct CommaList;

impl<'de> Visitor<'de> for CommaList {
    type Value = Vec<String>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a list of strings, or one comma-separated string")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Vec<String>, E> {
        Ok(split(v))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<String>, A::Error> {
        let mut out = Vec::new();
        while let Some(item) = seq.next_element::<String>()? {
            out.extend(split(&item));
        }
        Ok(out)
    }
}

fn split(v: &str) -> Vec<String> {
    v.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}
