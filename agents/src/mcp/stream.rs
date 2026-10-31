//! Calling a server-streaming RPC in-process.
//!
//! A Go streaming handler is handed a `grpc.ServerStream` to write into, so calling one
//! without a network hop means supplying that sink, and `InProcessServerStream` is it.
//! A tonic handler instead *returns* a stream, so the Rust shape has two directions:
//! [`InProcessServerStream::send`] for an implementation written against a sink, and
//! [`InProcessServerStream::pump`] for draining what a tonic method (or a tonic client over
//! the hybrid server's `Loopback`) returned. Either way the tool reads with
//! [`InProcessServerStream::recv`], and the stream is itself a [`futures::Stream`].

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// The buffer between producer and consumer: the 16 Go uses.
const CAPACITY: usize = 16;

/// A channel-backed server stream, connecting a streaming handler to the tool consuming it.
#[derive(Debug)]
pub struct InProcessServerStream<T> {
    tx: Option<mpsc::Sender<T>>,
    rx: mpsc::Receiver<T>,
    cancel: CancellationToken,
}

impl<T: Send + 'static> InProcessServerStream<T> {
    /// A stream whose [`InProcessServerStream::send`] gives up once `cancel` fires.
    pub fn new(cancel: CancellationToken) -> Self {
        let (tx, rx) = mpsc::channel(CAPACITY);
        Self {
            tx: Some(tx),
            rx,
            cancel,
        }
    }

    /// A handle a producer task sends through. The stream ends once every handle is gone and
    /// [`InProcessServerStream::close`] has been called.
    pub fn sender(&self) -> Option<mpsc::Sender<T>> {
        self.tx.clone()
    }

    /// Enqueues `message`, waiting for room.
    ///
    /// # Errors
    ///
    /// `CANCELLED` when the stream's token fires first, or when it was closed.
    pub async fn send(&self, message: T) -> Result<(), tonic::Status> {
        let Some(tx) = &self.tx else {
            return Err(tonic::Status::cancelled("stream closed"));
        };
        // Biased so a cancelled stream refuses deterministically, even with room to spare;
        // Go's unbiased select would sometimes still enqueue.
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => Err(tonic::Status::cancelled("stream cancelled")),
            sent = tx.send(message) => {
                sent.map_err(|_| tonic::Status::cancelled("stream closed"))
            }
        }
    }

    /// The next message; `None` once the stream is closed and drained.
    pub async fn recv(&mut self) -> Option<T> {
        self.rx.recv().await
    }

    /// Signals that nothing more will be sent from this handle. What is buffered can still be
    /// received.
    pub fn close(&mut self) {
        self.tx = None;
    }

    /// The token a handler watches: what Go's `stream.Context()` carries.
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancel
    }

    /// Drains what a tonic streaming method returned into a stream of this shape.
    ///
    /// The pump stops after the first error, which is delivered as the last item, and when
    /// `cancel` fires — so a tool that stops reading does not leave the producer running.
    pub fn pump<S, M>(stream: S, cancel: CancellationToken) -> Self
    where
        S: Stream<Item = Result<M, tonic::Status>> + Send + 'static,
        M: Send + 'static,
        T: From<Result<M, tonic::Status>>,
    {
        let mut pumped = Self::new(cancel.clone());
        let sender = pumped.sender();
        pumped.close();
        tokio::spawn(async move {
            let Some(sender) = sender else { return };
            let mut stream = Box::pin(stream);
            loop {
                let next = tokio::select! {
                    next = stream.next() => next,
                    () = cancel.cancelled() => None,
                };
                let Some(next) = next else { return };
                let failed = next.is_err();
                if sender.send(T::from(next)).await.is_err() || failed {
                    return;
                }
            }
        });
        pumped
    }
}

impl<T> Stream for InProcessServerStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.rx.poll_recv(cx)
    }
}
