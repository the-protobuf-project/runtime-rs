//! Batch publishing helpers.

use crate::{Options, Payload, Publisher, Result, StreamError};

/// Publishes each payload in turn: the batch every backend without a native one
/// falls back to, and what a caller would otherwise write.
///
/// Ids are assigned before anything is sent, so they line up with `payloads`
/// even when some entries fail.
pub async fn publish_each(
    publisher: &dyn Publisher,
    subject: &str,
    payloads: Vec<Payload>,
    opts: &Options,
) -> Result<Vec<String>> {
    check_batch(opts)?;
    let ids: Vec<String> = payloads.iter().map(|_| super::new_id()).collect();
    let mut failures = Vec::new();
    for (index, (payload, id)) in payloads.into_iter().zip(&ids).enumerate() {
        let opts = opts.clone().with_id(id.clone());
        if let Err(error) = publisher.publish(subject, payload, &opts).await {
            failures.push((index, error));
        }
    }
    batch_result(subject, ids, failures)
}

/// Folds per-entry failures into [`StreamError::PartialBatch`], or returns the
/// ids when there were none.
pub fn batch_result(
    subject: &str,
    ids: Vec<String>,
    failures: Vec<(usize, StreamError)>,
) -> Result<Vec<String>> {
    if failures.is_empty() {
        return Ok(ids);
    }
    Err(StreamError::PartialBatch {
        subject: subject.to_owned(),
        total: ids.len(),
        ids,
        failures,
    })
}

/// Refuses options a batch cannot honor.
pub fn check_batch(opts: &Options) -> Result<()> {
    if opts.id.is_some() {
        return Err(StreamError::Unsupported(
            "one id cannot name several messages; drop the id option from a batch".into(),
        ));
    }
    Ok(())
}
