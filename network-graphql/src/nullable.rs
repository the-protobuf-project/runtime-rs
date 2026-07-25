//! A three-state value for masked update inputs.

use serde::Serialize;

/// A three-state value for masked update inputs. A column field can be:
///
/// - unset — the default `Nullable`; [`ColumnPatch::set_columns`](crate::ColumnPatch::set_columns)
///   omits it, so the column is left unchanged.
/// - null — [`Nullable::null`]; the column is cleared to SQL NULL (Hasura `_set: {col: null}`).
/// - value — [`Nullable::value`]; the column is written to `v` (including `v`'s zero value).
///
/// A plain `T` field cannot express the difference between "leave unchanged" and "clear
/// to null" (both look like the default value), so a masked update could never clear an
/// optional column. `Nullable` removes that ambiguity.
#[derive(Debug, Clone)]
pub struct Nullable<T>(NullableState<T>);

#[derive(Debug, Clone, Default)]
enum NullableState<T> {
    #[default]
    Unset,
    Null,
    Value(T),
}

impl<T> Default for Nullable<T> {
    fn default() -> Self {
        Nullable(NullableState::Unset)
    }
}

impl<T> Nullable<T> {
    /// Returns a `Nullable` that sets the column to `v` (`v` may be its type's zero
    /// value, which a plain optional field could not express).
    pub fn value(v: T) -> Self {
        Self(NullableState::Value(v))
    }

    /// Returns a `Nullable` that clears the column to SQL NULL.
    pub fn null() -> Self {
        Self(NullableState::Null)
    }

    /// Returns the default, unset `Nullable`, which is omitted so the column is left
    /// unchanged. Exists for symmetry and readable intent.
    pub fn unset() -> Self {
        Self::default()
    }

    /// Reports whether the field carries an instruction (a value or an explicit null).
    pub fn is_set(&self) -> bool {
        !matches!(self.0, NullableState::Unset)
    }

    /// Reports whether the field clears the column to NULL.
    pub fn is_null(&self) -> bool {
        matches!(self.0, NullableState::Null)
    }

    /// Returns the held value, or `None` when unset or null.
    pub fn get(&self) -> Option<&T> {
        match &self.0 {
            NullableState::Value(v) => Some(v),
            _ => None,
        }
    }
}

impl<T: Serialize> Nullable<T> {
    /// Returns `Some({"set": value_or_null})` when set, `None` when unset — the
    /// building block hand-written [`ColumnPatch`](crate::ColumnPatch) impls call per
    /// field.
    pub fn to_set_entry(&self) -> Option<serde_json::Value> {
        match &self.0 {
            NullableState::Unset => None,
            NullableState::Null => Some(serde_json::json!({ "set": null })),
            NullableState::Value(v) => {
                Some(serde_json::json!({ "set": serde_json::to_value(v).ok()? }))
            }
        }
    }
}
