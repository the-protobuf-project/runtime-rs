//! Pin-safe lifetime wrapper; no worker polls ahead of a stream's consumer.
use super::outcome;
use crate::store::{Record, RecordStream, Result};
use futures::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use telemetry::{Operation, Outcome};

pub(super) struct ObservedStream {
    pub inner: RecordStream,
    pub operation: Option<Operation>,
    pub ended: bool,
}
impl Stream for ObservedStream {
    type Item = Result<Record>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }
        let context = self
            .operation
            .as_ref()
            .expect("active stream guard")
            .context();
        let _attached = context.attach(); // Synchronous poll only; never across await.
        let result = self.inner.as_mut().poll_next(cx);
        match &result {
            Poll::Ready(Some(Ok(_))) => self.operation.as_mut().unwrap().read_rows(1),
            Poll::Ready(Some(Err(error))) => {
                self.ended = true;
                self.inner = Box::pin(futures::stream::empty());
                self.operation.take().unwrap().finish(outcome(Some(error)));
            }
            Poll::Ready(None) => {
                self.ended = true;
                self.inner = Box::pin(futures::stream::empty());
                self.operation.take().unwrap().finish(Outcome::Ok);
            }
            Poll::Pending => (),
        }
        result
    }
}
// The retained Operation's Drop reports cancellation if EOF/error was not seen.
