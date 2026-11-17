//! [`MongoProvider`]: selecting a MongoDB database. Go's `mongodb/provider.go`.

use std::sync::Arc;

use super::client::ping;
use super::error::{backend, invalid};
use super::{BACKEND, MongoClient, MongoDriver};
use crate::Result;
use crate::records::{Capabilities, DB, Provider, check_database_name};

/// MongoDB bound to a client you own; it neither dials nor closes it. Go's
/// `mongodb.Provider`.
#[derive(Clone, Debug)]
pub struct MongoProvider {
    client: MongoClient,
}

impl MongoProvider {
    /// A provider over `client`. Go's `NewProvider`.
    pub fn new(client: MongoClient) -> Self {
        Self { client }
    }

    /// Removes database `name` and everything in it; an empty name is refused rather than
    /// taken to mean some default. Go's `Provider.DropDatabase`.
    pub async fn drop_database(&self, name: &str) -> Result<()> {
        check_database_name(name)?;
        if name.is_empty() {
            return Err(invalid("drop_database needs a name"));
        }
        self.client
            .unwrap()
            .database(name)
            .drop()
            .await
            .map_err(|e| backend(format!("cannot drop {name:?}"), e))
    }
}

#[async_trait::async_trait]
impl Provider for MongoProvider {
    /// The driver over MongoDB database `name`, with all four capabilities. The name is a
    /// real database, so isolation is the server's rather than a naming convention: dropping
    /// one leaves its neighbours alone, and no key reaches another tenant's collections.
    /// Empty keeps each resource in the database its descriptor names.
    ///
    /// The name is checked with [`check_database_name`], and the server pinged, so a bad
    /// address or credential fails here rather than three layers into a request. Nothing is
    /// derived, so [`DB::close`] is a no-op and closing the client stays yours.
    async fn set_database(&self, name: &str) -> Result<DB> {
        check_database_name(name)?;
        ping(self.client.unwrap())
            .await
            .map_err(|e| backend(format!("database {name:?} is not reachable"), e))?;
        let d = Arc::new(MongoDriver::over(
            self.client.unwrap().clone(),
            name.to_owned(),
        ));
        let caps = Capabilities {
            driver: d.clone(),
            tx: Some(d.clone()),
            schema: Some(d.clone()),
            batch: Some(d.clone()),
            watch: Some(d),
        };
        Ok(DB::build(BACKEND, name, caps, None))
    }

    fn backend(&self) -> &'static str {
        BACKEND
    }
}
