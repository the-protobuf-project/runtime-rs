//! Scalar types that tolerate engine-specific JSON encodings.
//!
//! Go's `graphql.go` also exposed six pointer-constructor functions (`String`, `Bool`,
//! `Int`, `Int32`, `Float64`, `JSON`, each `func X(v T) *T`) for building optional
//! argument values. They have no Rust counterpart: `Option<T>`/`Some(v)` already does
//! that job natively, so porting them would be pure noise.

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Carries an operation argument together with its exact GraphQL type (e.g. `"String!"`,
/// `"[UsersOrderByExp!]"`). Serializes transparently as the wrapped value.
#[derive(Debug, Clone, PartialEq)]
pub struct Variable {
    value: serde_json::Value,
    gql_type: String,
}

impl Variable {
    /// Wraps `value` with its declared GraphQL type.
    pub fn new(value: impl Serialize, gql_type: impl Into<String>) -> Self {
        Self {
            value: serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
            gql_type: gql_type.into(),
        }
    }

    /// Alias of [`Variable::new`], kept for call-site familiarity with Go's `Var`.
    pub fn var(value: impl Serialize, gql_type: impl Into<String>) -> Self {
        Self::new(value, gql_type)
    }

    /// Alias of [`Variable::new`], kept for call-site familiarity with Go's `VarPtr`.
    pub fn var_ptr(value: impl Serialize, gql_type: impl Into<String>) -> Self {
        Self::new(value, gql_type)
    }

    /// The declared GraphQL type.
    pub fn gql_type(&self) -> &str {
        &self.gql_type
    }
}

impl Serialize for Variable {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.value.serialize(serializer)
    }
}

/// A 64-bit integer GraphQL scalar. Engines commonly serialize 64-bit integers as JSON
/// strings to preserve precision, but may return computed values (e.g. aggregate counts)
/// as JSON numbers. `Int64` decodes from either form and encodes as a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Int64(pub i64);

impl Serialize for Int64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for Int64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match serde_json::Value::deserialize(deserializer)? {
            serde_json::Value::String(s) => {
                s.parse::<i64>().map(Int64).map_err(D::Error::custom)
            }
            serde_json::Value::Number(n) => n
                .as_i64()
                .map(Int64)
                .ok_or_else(|| D::Error::custom(format!("Int64: not an i64: {n}"))),
            other => Err(D::Error::custom(format!(
                "Int64: expected string or number, got {other}"
            ))),
        }
    }
}

/// An arbitrary-precision decimal scalar held as its textual form. Engines may return it
/// as a JSON string (to preserve precision) or, for computed aggregates, as a JSON
/// number; `Bigdecimal` decodes either and encodes as a string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bigdecimal(pub String);

impl Serialize for Bigdecimal {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Bigdecimal {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match serde_json::Value::deserialize(deserializer)? {
            serde_json::Value::String(s) => Ok(Bigdecimal(s)),
            serde_json::Value::Number(n) => Ok(Bigdecimal(n.to_string())),
            other => Err(D::Error::custom(format!(
                "Bigdecimal: expected string or number, got {other}"
            ))),
        }
    }
}
