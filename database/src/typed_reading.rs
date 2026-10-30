//! Typed streaming and paged views over the portable reader capability.
use crate::{Collection, Range, Result, Scan};
use futures::{Stream, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use std::pin::Pin;

/// A stream of typed records. Dropping it cancels the underlying transport.
pub type TypedStream<T> = Pin<Box<dyn Stream<Item = Result<T>> + Send>>;

/// Typed page with the same opaque continuation token as dynamic reads.
#[derive(Debug)]
pub struct TypedPage<T> {
    /// Ordered models.
    pub items: Vec<T>,
    /// None at the end; tokens must be reused with the same query.
    pub next_page_token: Option<String>,
}

impl<T: Serialize + DeserializeOwned + Send + 'static> Collection<T> {
    /// Opens a typed stream; decoding happens as each record is polled.
    pub async fn stream(&self, range: &Scan) -> Result<TypedStream<T>> {
        let stream = self.db().reader()?.stream(self.resource(), range).await?;
        Ok(Box::pin(stream.map(|record| {
            Ok(serde_json::from_value(serde_json::to_value(record?)?)?)
        })))
    }

    /// Fetches one typed page; equal timestamps continue using tag identity.
    pub async fn page(&self, range: &Range, token: Option<&str>) -> Result<TypedPage<T>> {
        let page = self
            .db()
            .reader()?
            .page(self.resource(), range, token)
            .await?;
        let items = page
            .items
            .into_iter()
            .map(|record| Ok(serde_json::from_value(serde_json::to_value(record)?)?))
            .collect::<Result<_>>()?;
        Ok(TypedPage {
            items,
            next_page_token: page.next_page_token,
        })
    }
}
