//! Typed window streams, decoding each record as it is polled.
use super::{For, Result, Scan};
use futures::{Stream, StreamExt};
use std::pin::Pin;

/// A stream of typed records. Dropping it cancels the underlying transport.
pub type TypedStream<T> = Pin<Box<dyn Stream<Item = Result<T>> + Send>>;

impl<T: Send + 'static> For<T> {
    /// Streams the window as `T`, without a row cap unless `scan` sets one.
    pub async fn stream(&self, scan: &Scan) -> Result<TypedStream<T>> {
        let stream = self.db.streams.stream(&self.res, scan).await?;
        let decode = self.decode;
        Ok(Box::pin(stream.map(move |record| decode(record?))))
    }
}
