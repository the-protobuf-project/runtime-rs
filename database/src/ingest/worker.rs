//! One worker owns all admitted records and drains them when senders disappear.
use super::IngestConfig;
use super::{
    batch::flush,
    queue::{Entry, Wiring},
};
use crate::{Error, Record, Resource, TimeSeries};
use std::sync::{Arc, Mutex};
use telemetry::Context;
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, MissedTickBehavior, interval_at},
};

pub(super) type Outcome = std::result::Result<(), Arc<Error>>;
pub(super) type Failure = Arc<Mutex<Option<Arc<Error>>>>;
pub(super) enum Command {
    Append(Entry),
    Flush(oneshot::Sender<Outcome>),
}

async fn run(
    series: Arc<dyn TimeSeries>,
    resource: Resource,
    config: IngestConfig,
    receiver: &mut mpsc::Receiver<Command>,
    wiring: &Wiring,
) -> Outcome {
    let mut pending: Vec<(Record, Context)> = Vec::with_capacity(config.batch_size);
    let mut timer = interval_at(
        Instant::now() + config.flush_interval,
        config.flush_interval,
    );
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            command = receiver.recv() => match command {
                Some(Command::Append(record)) => {
                    pending.push(record.dequeue());
                    if pending.len() >= config.batch_size {
                        flush(series.as_ref(), &resource, &mut pending, &config, wiring).await?;
                    }
                }
                Some(Command::Flush(reply)) => {
                    let outcome = flush(series.as_ref(), &resource, &mut pending, &config, wiring).await;
                    let _ = reply.send(outcome.clone());
                    outcome?;
                }
                None => return flush(series.as_ref(), &resource, &mut pending, &config, wiring).await,
            },
            _ = timer.tick() => flush(series.as_ref(), &resource, &mut pending, &config, wiring).await?,
        }
    }
}

pub(super) async fn start(
    series: Arc<dyn TimeSeries>,
    resource: Resource,
    config: IngestConfig,
    mut receiver: mpsc::Receiver<Command>,
    failure: Failure,
    wiring: Wiring,
) -> Outcome {
    let outcome = run(series, resource, config, &mut receiver, &wiring).await;
    if let Err(error) = &outcome {
        *failure.lock().expect("ingestion failure lock") = Some(error.clone());
    }
    outcome
}
