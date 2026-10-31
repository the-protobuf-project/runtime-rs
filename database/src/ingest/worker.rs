//! One worker owns all admitted records and drains them when senders disappear.
use super::IngestConfig;
use super::{
    batch::flush,
    queue::{Entry, Wiring},
};
use crate::store::{Batcher, Error, Record, Resource};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
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

/// Every in-flight batch, awaited to the end; the first failure wins.
async fn drain(inflight: &mut FuturesUnordered<impl Future<Output = Outcome>>) -> Outcome {
    let mut first = Ok(());
    while let Some(done) = inflight.next().await {
        if first.is_ok() {
            first = done;
        }
    }
    first
}

async fn run(
    batch: Arc<dyn Batcher>,
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
    let launch = |pending: &mut Vec<(Record, Context)>| {
        let entries = std::mem::take(pending);
        flush(
            batch.clone(),
            resource.clone(),
            entries,
            config.clone(),
            wiring.clone(),
        )
    };
    // While every slot is busy the queue is not read, so producers wait.
    let mut inflight = FuturesUnordered::new();
    loop {
        let room = inflight.len() < config.max_inflight;
        tokio::select! {
            Some(done) = inflight.next(), if !inflight.is_empty() => {
                let done: Outcome = done;
                if done.is_err() {
                    let _ = drain(&mut inflight).await;
                    return done;
                }
            }
            command = receiver.recv(), if room => match command {
                Some(Command::Append(record)) => {
                    pending.push(record.dequeue());
                    if pending.len() >= config.batch_size {
                        inflight.push(launch(&mut pending));
                    }
                }
                Some(Command::Flush(reply)) => {
                    inflight.push(launch(&mut pending));
                    let outcome = drain(&mut inflight).await;
                    let _ = reply.send(outcome.clone());
                    outcome?;
                }
                None => {
                    inflight.push(launch(&mut pending));
                    return drain(&mut inflight).await;
                }
            },
            _ = timer.tick(), if room => {
                if !pending.is_empty() {
                    inflight.push(launch(&mut pending));
                }
            }
        }
    }
}

pub(super) async fn start(
    batch: Arc<dyn Batcher>,
    resource: Resource,
    config: IngestConfig,
    mut receiver: mpsc::Receiver<Command>,
    failure: Failure,
    wiring: Wiring,
) -> Outcome {
    let outcome = run(batch, resource, config, &mut receiver, &wiring).await;
    if let Err(error) = &outcome {
        *failure.lock().expect("ingestion failure lock") = Some(error.clone());
    }
    outcome
}
