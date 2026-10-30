//! One worker owns all admitted records and drains them when senders disappear.
use super::IngestConfig;
use crate::{Error, Record, Resource, TimeSeries};
use std::sync::{Arc, Mutex};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, MissedTickBehavior, interval_at, timeout},
};

pub(super) type Outcome = std::result::Result<(), Arc<Error>>;
pub(super) type Failure = Arc<Mutex<Option<Arc<Error>>>>;
pub(super) enum Command {
    Append(Record),
    Flush(oneshot::Sender<Outcome>),
}

async fn flush(
    series: &dyn TimeSeries,
    resource: &Resource,
    pending: &mut Vec<Record>,
    config: &IngestConfig,
) -> Outcome {
    if pending.is_empty() {
        return Ok(());
    }
    let points = std::mem::take(pending);
    timeout(config.write_timeout, series.write(resource, &points))
        .await
        .map_err(|_| Arc::new(Error::Timeout))?
        .map_err(Arc::new)
}

async fn run(
    series: Arc<dyn TimeSeries>,
    resource: Resource,
    config: IngestConfig,
    receiver: &mut mpsc::Receiver<Command>,
) -> Outcome {
    let mut pending = Vec::with_capacity(config.batch_size);
    let mut timer = interval_at(
        Instant::now() + config.flush_interval,
        config.flush_interval,
    );
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            command = receiver.recv() => match command {
                Some(Command::Append(record)) => {
                    pending.push(record);
                    if pending.len() >= config.batch_size {
                        flush(series.as_ref(), &resource, &mut pending, &config).await?;
                    }
                }
                Some(Command::Flush(reply)) => {
                    let outcome = flush(series.as_ref(), &resource, &mut pending, &config).await;
                    let _ = reply.send(outcome.clone());
                    outcome?;
                }
                None => return flush(series.as_ref(), &resource, &mut pending, &config).await,
            },
            _ = timer.tick() => flush(series.as_ref(), &resource, &mut pending, &config).await?,
        }
    }
}

pub(super) async fn start(
    series: Arc<dyn TimeSeries>,
    resource: Resource,
    config: IngestConfig,
    mut receiver: mpsc::Receiver<Command>,
    failure: Failure,
) -> Outcome {
    let outcome = run(series, resource, config, &mut receiver).await;
    if let Err(error) = &outcome {
        *failure.lock().expect("ingestion failure lock") = Some(error.clone());
    }
    outcome
}
