//! The provider that selects a D1 database by name. Go's `d1/provider.go`.

use std::sync::Arc;

use super::BACKEND;
use super::client::D1Client;
use super::driver::D1Driver;
use crate::records::{self, DB, Provider, check_database_name};
use crate::{Error, Result};

/// A D1 backend bound to an account. Go's `d1.Provider`.
#[derive(Clone)]
pub struct D1Provider {
    client: Arc<D1Client>,
}

impl D1Provider {
    /// A provider over `client`.
    pub fn new(client: Arc<D1Client>) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl Provider for D1Provider {
    /// Selects a D1 database and returns the driver over it. An empty name is
    /// [`D1Config::database_id`](super::D1Config); any other is looked up in the account, and
    /// one that matches none is an error rather than a database created on the spot. Either
    /// way the database is read once here, so a token without access surfaces on this line.
    async fn set_database(&self, name: &str) -> Result<DB> {
        check_database_name(name)?;
        let id = match name {
            "" => {
                let id = self.client.config.database_id.clone();
                if id.is_empty() {
                    return Err(Error::Invalid(
                        "d1 was asked for its configured database, and database_id is empty"
                            .to_owned(),
                    ));
                }
                self.client.ping(&id).await?;
                id
            }
            name => self.client.lookup(name).await?,
        };
        let driver = D1Driver {
            client: self.client.clone(),
            id,
        };
        Ok(records::observe::instrument_default(driver.into_db(name)))
    }

    /// `d1`.
    fn backend(&self) -> &'static str {
        BACKEND
    }
}
