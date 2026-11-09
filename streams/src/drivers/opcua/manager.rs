//! The bound half of the OPC UA provider: writing and monitoring the nodes of
//! one declared stream.

use std::{collections::HashMap, sync::Arc};

use futures::StreamExt;
use opcua::types::{AttributeId, DataValue, NodeId, NumericRange, StatusCode, WriteValue};

use crate::{
    Batch, Manager, Options, Payload, Publisher, Result, Stream, StreamError, Subscriber,
    Subscription, provider,
};

use super::{
    Inner, OpcUaValue,
    hub::Item,
    node::{self, NodeRef},
};

/// Writes and monitors the nodes one stream declares.
pub(crate) struct OpcUaManager {
    inner: Arc<Inner>,
    stream: Stream,
    /// Each declared subject's node id, namespace URIs resolved.
    nodes: HashMap<String, NodeId>,
}

impl OpcUaManager {
    /// Resolves every declared subject to the node id the server knows it by.
    pub(crate) async fn bind(inner: Arc<Inner>, stream: Stream) -> Result<Self> {
        let mut nodes = HashMap::with_capacity(stream.subjects.len());
        for subject in &stream.subjects {
            let id = match node::parse(subject)? {
                NodeRef::Index(id) => id,
                NodeRef::Uri(uri, mut id) => {
                    id.namespace = inner.session.get_namespace_index(&uri).await.map_err(|e| {
                        StreamError::Internal(format!(
                            "opcua: cannot resolve the namespace of {subject:?}: {e}"
                        ))
                    })?;
                    id
                }
            };
            nodes.insert(subject.clone(), id);
        }
        Ok(Self {
            inner,
            stream,
            nodes,
        })
    }

    /// The node behind a declared subject.
    fn node(&self, subject: &str) -> Result<&NodeId> {
        self.nodes.get(subject).ok_or_else(|| {
            provider::unknown_subject(&self.stream.id, subject, &self.stream.subjects)
        })
    }

    /// Refuses a scheduled write.
    fn check_publish(opts: &Options) -> Result<()> {
        if opts.delay().is_some() {
            return Err(StreamError::Unsupported(
                "opcua: a Write applies at once; OPC UA cannot schedule one, so a TTL has no \
                 meaning here"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Writes `values` to `node` in Write requests of at most the configured
    /// size, returning one outcome per value.
    async fn write(&self, subject: &str, node: &NodeId, values: Vec<DataValue>) -> Vec<Result<()>> {
        let total = values.len();
        let mut outcomes = Vec::with_capacity(total);
        let chunk = self.inner.config.max_nodes_per_write.max(1);
        let mut values = values.into_iter().peekable();
        while values.peek().is_some() {
            let writes: Vec<WriteValue> = values
                .by_ref()
                .take(chunk)
                .map(|value| WriteValue {
                    node_id: node.clone(),
                    attribute_id: AttributeId::Value as u32,
                    index_range: NumericRange::None,
                    value,
                })
                .collect();
            let n = writes.len();
            match self.inner.session.write(&writes).await {
                Ok(results) if results.len() == n => {
                    outcomes.extend(results.into_iter().map(|status| check(subject, status)));
                }
                Ok(results) => {
                    let error = StreamError::Internal(format!(
                        "opcua: the server answered {} of {n} writes to {subject:?}",
                        results.len()
                    ));
                    outcomes.extend((0..n).map(|_| Err(error.clone())));
                }
                Err(e) => {
                    let error =
                        StreamError::Internal(format!("opcua: cannot write to {subject:?}: {e}"));
                    outcomes.extend((0..n).map(|_| Err(error.clone())));
                }
            }
        }
        outcomes
    }
}

/// Turns a Write result into the contract's error for it.
fn check(subject: &str, status: StatusCode) -> Result<()> {
    if !status.is_bad() {
        return Ok(());
    }
    let text = format!("opcua: the server refused the write to {subject:?}: {status}");
    Err(match status {
        StatusCode::BadTypeMismatch | StatusCode::BadOutOfRange => StreamError::Codec(text),
        StatusCode::BadNodeIdUnknown
        | StatusCode::BadNodeIdInvalid
        | StatusCode::BadNotWritable
        | StatusCode::BadUserAccessDenied
        | StatusCode::BadWriteNotSupported
        | StatusCode::BadAttributeIdInvalid => StreamError::Unsupported(text),
        _ => StreamError::Internal(text),
    })
}

#[async_trait::async_trait]
impl Publisher for OpcUaManager {
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        let node = self.node(subject)?;
        Self::check_publish(opts)?;
        let value = OpcUaValue::decode_payload(&payload)?;
        // OPC UA has no message id; this one is the caller's receipt only.
        let id = provider::publish_id(opts);
        self.write(subject, node, vec![value])
            .await
            .pop()
            .unwrap_or_else(|| Err(StreamError::Internal("opcua: nothing was written".into())))?;
        Ok(id)
    }
}

#[async_trait::async_trait]
impl Subscriber for OpcUaManager {
    async fn subscribe(&self, subject: &str, opts: &Options) -> Result<Subscription> {
        let node = self.node(subject)?.clone();
        if opts.group.is_some() {
            return Err(StreamError::Unsupported(
                "opcua: OPC UA has no consumer groups; every monitored item receives every \
                 change, so there is nothing to share"
                    .into(),
            ));
        }
        let config = &self.inner.config;
        let queue_size = opts
            .prefetch
            .map_or(config.queue_size, |n| u32::try_from(n).unwrap_or(u32::MAX))
            .max(1);
        let notifications = self
            .inner
            .hub
            .attach(Item {
                subject: Arc::from(subject),
                node,
                queue_size,
                buffer: config.buffer.max(queue_size as usize),
            })
            .await?;
        Ok(notifications.boxed())
    }
}

#[async_trait::async_trait]
impl Batch for OpcUaManager {
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>> {
        let node = self.node(subject)?;
        provider::check_batch(opts)?;
        Self::check_publish(opts)?;
        let ids: Vec<String> = payloads.iter().map(|_| provider::new_id()).collect();
        let mut failures = Vec::new();
        let mut values = Vec::with_capacity(payloads.len());
        let mut positions = Vec::with_capacity(payloads.len());
        for (index, payload) in payloads.iter().enumerate() {
            match OpcUaValue::decode_payload(payload) {
                Ok(value) => {
                    values.push(value);
                    positions.push(index);
                }
                Err(error) => failures.push((index, error)),
            }
        }
        let outcomes = self.write(subject, node, values).await;
        for (index, outcome) in positions.into_iter().zip(outcomes) {
            if let Err(error) = outcome {
                failures.push((index, error));
            }
        }
        failures.sort_by_key(|(index, _)| *index);
        provider::batch_result(subject, ids, failures)
    }
}

impl Manager for OpcUaManager {
    fn batch(&self) -> Option<&dyn Batch> {
        Some(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_statuses_map_to_settled_errors() {
        assert!(check("s", StatusCode::Good).is_ok());
        assert!(check("s", StatusCode::UncertainLastUsableValue).is_ok());
        let error = check("ns=2;s=x", StatusCode::BadTypeMismatch).unwrap_err();
        assert!(matches!(error, StreamError::Codec(_)));
        assert!(error.to_string().contains("BadTypeMismatch"), "{error}");
        assert!(error.to_string().contains("ns=2;s=x"), "{error}");
        for status in [StatusCode::BadNotWritable, StatusCode::BadUserAccessDenied] {
            assert!(check("s", status).unwrap_err().is_settled());
        }
        assert!(!check("s", StatusCode::BadTimeout).unwrap_err().is_settled());
    }

    #[test]
    fn a_ttl_is_refused() {
        let opts = Options::default().with_ttl(std::time::Duration::from_secs(1));
        assert!(matches!(
            OpcUaManager::check_publish(&opts),
            Err(StreamError::Unsupported(_))
        ));
        assert!(OpcUaManager::check_publish(&Options::default()).is_ok());
    }
}
