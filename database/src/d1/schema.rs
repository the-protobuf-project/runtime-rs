//! Tables from the descriptor. Go's `d1/schema.go`.

use super::driver::D1Driver;
use super::sql::{self, quote};
use super::wire::Statement;
use crate::records::{Migrator, Resource};
use crate::{Error, Result};

#[async_trait::async_trait]
impl Migrator for D1Driver {
    /// `CREATE TABLE IF NOT EXISTS` from the descriptor, with this driver's types (see
    /// [`sql::column_type`]). It creates and never alters.
    async fn ensure_schema(&self, res: &Resource) -> Result<()> {
        if res.columns.is_empty() {
            return Err(Error::Invalid(format!(
                "resource {:?} describes no columns",
                res.name
            )));
        }
        let defs: Vec<String> = res
            .columns
            .iter()
            .map(|c| {
                let mut def = format!("{} {}", quote(&c.name), sql::column_type(c));
                if c.primary_key {
                    def.push_str(" PRIMARY KEY");
                } else if c.unique {
                    def.push_str(" UNIQUE");
                }
                if c.not_null && !c.primary_key {
                    def.push_str(" NOT NULL");
                }
                def
            })
            .collect();
        let text = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            quote(&res.table),
            defs.join(", ")
        );
        self.run(Statement::new(text, Vec::new())).await.map(|_| ())
    }

    async fn drop_schema(&self, res: &Resource) -> Result<()> {
        let text = format!("DROP TABLE IF EXISTS {}", quote(&res.table));
        self.run(Statement::new(text, Vec::new())).await.map(|_| ())
    }

    async fn has_schema(&self, res: &Resource) -> Result<bool> {
        let text = "SELECT 1 AS hit FROM sqlite_master WHERE type = 'table' AND name = ?";
        let out = self
            .run(Statement::new(text, vec![res.table.clone()]))
            .await?;
        Ok(!out.rows.is_empty())
    }
}
