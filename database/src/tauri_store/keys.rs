//! Every key one driver writes into the store. The layout follows `database::redis`, so the
//! two read alike; the store is one flat map, so the selected database is a key segment.
//! Writing `{head}` for the database and its colon (empty for the default database):
//!
//! ```text
//! {head}{table}:rec:{key}          one record, a JSON object of its columns
//! {head}{table}:uniq:{col}:{value} a reservation on a unique column, holding the record's key
//! ```

use crate::records::Resource;

/// The key layout for one selected database.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Keys {
    head: String,
}

impl Keys {
    /// The layout of `database`; empty is the default database.
    pub(super) fn new(database: &str) -> Self {
        let head = if database.is_empty() {
            String::new()
        } else {
            format!("{database}:")
        };
        Self { head }
    }

    /// Where every key of this database starts.
    pub(super) fn database(&self) -> &str {
        &self.head
    }

    /// Where every key of `res` starts.
    pub(super) fn table(&self, res: &Resource) -> String {
        format!("{}{}:", self.head, res.table)
    }

    /// Where `res`'s records start: strip it to get a record's key.
    pub(super) fn records(&self, res: &Resource) -> String {
        format!("{}rec:", self.table(res))
    }

    /// The record of `res` under `key`.
    pub(super) fn record(&self, res: &Resource, key: &str) -> String {
        format!("{}{key}", self.records(res))
    }

    /// The reservation of `value` in `res`'s unique column `column`.
    pub(super) fn unique(&self, res: &Resource, column: &str, value: &str) -> String {
        format!("{}uniq:{column}:{value}", self.table(res))
    }
}

#[cfg(test)]
mod tests {
    use super::Keys;

    #[test]
    fn the_database_is_a_key_segment() {
        assert_eq!(Keys::new("").database(), "");
        assert_eq!(Keys::new("tenant_a").database(), "tenant_a:");
    }
}
