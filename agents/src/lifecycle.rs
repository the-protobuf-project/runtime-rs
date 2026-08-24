//! Starting and draining: the two shapes a host wants, over the same runtime.

use tokio_util::sync::CancellationToken;

use crate::agents::SHUTDOWN_TIMEOUT;
use crate::error::{Error, Result};
use crate::runtime::Runtime;

impl Runtime {
    /// Starts the runtime and blocks until `cancel` fires, then drains whatever it opened.
    ///
    /// It is [`Runtime::start`], a wait, and [`Runtime::shutdown`] — the shape a process
    /// that does nothing else wants.
    pub async fn serve(&self, cancel: CancellationToken) -> Result<()> {
        self.start(cancel.clone()).await?;
        cancel.cancelled().await;
        // The drain outlives the token that triggered it, which is the whole reason it is
        // not driven by the same one: an in-flight request would otherwise be cut off by
        // the very signal that asked for a graceful stop.
        self.shutdown().await
    }

    /// Stops every service and drains the listeners this runtime opened. A router it was
    /// given is left alone — the host owns that server, and closing it here would take out
    /// whatever else is on it.
    ///
    /// Safe to call on a runtime that never started, and safe to call twice.
    pub async fn shutdown(&self) -> Result<()> {
        let (service_cancel, drain, listeners, tasks) = {
            let mut state = self.state.lock().expect("agents: state poisoned");
            (
                state.service_cancel.take(),
                state.drain.take(),
                std::mem::take(&mut state.listeners),
                std::mem::take(&mut state.tasks),
            )
        };

        if let Some(cancel) = service_cancel {
            cancel.cancel();
        }
        if let Some(drain) = drain {
            drain.cancel();
        }

        let mut errs: Vec<Error> = Vec::new();

        for listener in listeners {
            match tokio::time::timeout(SHUTDOWN_TIMEOUT, listener.handle).await {
                Ok(Ok(())) => {}
                Ok(Err(join)) if join.is_cancelled() => {}
                Ok(Err(join)) => errs.push(Error::Drain {
                    addr: listener.addr,
                    message: join.to_string(),
                }),
                Err(_) => errs.push(Error::Drain {
                    addr: listener.addr,
                    message: format!("did not drain within {SHUTDOWN_TIMEOUT:?}"),
                }),
            }
        }

        // A service that failed after it mounted is its own problem, not the shutdown's —
        // but it is the one failure nothing else would ever report, so it is logged here.
        for (protocol, handle) in tasks {
            match tokio::time::timeout(SHUTDOWN_TIMEOUT, handle).await {
                Ok(Ok(Err(err))) => {
                    tracing::error!(%protocol, error = %err, "agents: service stopped with an error");
                }
                Ok(Ok(Ok(()))) | Ok(Err(_)) | Err(_) => {}
            }
        }

        match errs.len() {
            0 => Ok(()),
            1 => Err(errs.pop().expect("checked len")),
            _ => Err(Error::Multiple(errs)),
        }
    }
}
