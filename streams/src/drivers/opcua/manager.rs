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

/// Why headers are refused, at `create` and at publish.
pub(crate) const NO_HEADERS: &str = "opcua: a Write sets a node's value, status and timestamps \
     and has nowhere to carry headers, so they would be dropped; OPC UA PubSub \
     (drivers::opcua::pubsub) carries them as MQTT 5 user properties";

/// Writes and monitors the nodes one stream declares.
pub(crate) struct OpcUaManager {
    inner: Arc<Inner>,
    stream: Stream,
    /// Each declared subject's node.
    nodes: HashMap<String, Target>,
}

/// A declared subject's node, and the namespace URI it names it by, if any.
struct Target {
    id: NodeId,
    uri: Option<Arc<str>>,
}

impl OpcUaManager {
    /// Resolves every declared subject to the node id the server knows it by,
    /// so a namespace URI the server does not have fails here.
    pub(crate) async fn bind(inner: Arc<Inner>, stream: Stream) -> Result<Self> {
        let mut nodes = HashMap::with_capacity(stream.subjects.len());
        for subject in &stream.subjects {
            let target = match node::parse(subject)? {
                NodeRef::Index(id) => Target { id, uri: None },
                NodeRef::Uri(uri, id) => Target {
                    id,
                    uri: Some(Arc::from(uri)),
                },
            };
            Self::resolve(&inner, subject, &target).await?;
            nodes.insert(subject.clone(), target);
        }
        Ok(Self {
            inner,
            stream,
            nodes,
        })
    }

    /// The node id `target` names on the server now. A namespace URI is
    /// looked up in the current session's namespace table, so a server that
    /// renumbered its namespaces when it restarted is written to by the
    /// index it gives now.
    async fn resolve(inner: &Inner, subject: &str, target: &Target) -> Result<NodeId> {
        let Some(uri) = &target.uri else {
            return Ok(target.id.clone());
        };
        let namespace = inner.hub.namespace(uri).await.map_err(|e| match e {
            StreamError::Unsupported(why) => StreamError::Unsupported(format!(
                "opcua: cannot resolve the namespace of {subject:?}: {why}"
            )),
            other => StreamError::Internal(format!(
                "opcua: cannot resolve the namespace of {subject:?}: {other}"
            )),
        })?;
        Ok(NodeId {
            namespace,
            ..target.id.clone()
        })
    }

    /// The node behind a declared subject, and the URI it was named by.
    async fn node(&self, subject: &str) -> Result<(NodeId, Option<Arc<str>>)> {
        let target = self.nodes.get(subject).ok_or_else(|| {
            provider::unknown_subject(&self.stream.id, subject, &self.stream.subjects)
        })?;
        Ok((
            Self::resolve(&self.inner, subject, target).await?,
            target.uri.clone(),
        ))
    }

    /// Refuses what a Write cannot do: schedule, expire, retain, or carry
    /// headers.
    fn check_publish(stream: &Stream, opts: &Options) -> Result<()> {
        if opts.delay().is_some() {
            return Err(StreamError::Unsupported(
                "opcua: a Write applies at once; OPC UA cannot schedule one, so a TTL has no \
                 meaning here"
                    .into(),
            ));
        }
        if opts.expiry().is_some() {
            return Err(StreamError::Unsupported(
                "opcua: a Write applies at once or fails, so nothing waits that could expire"
                    .into(),
            ));
        }
        if opts.retain {
            return Err(StreamError::Unsupported(
                "opcua: a node always holds its last value and has no retain flag to set; \
                 OpcUaConfig::initial_value hands it to a new subscription"
                    .into(),
            ));
        }
        if !provider::outgoing(stream, opts)?.is_empty() {
            return Err(StreamError::Unsupported(NO_HEADERS.into()));
        }
        Ok(())
    }

    /// Writes `value` to `node` and returns once the server has answered.
    async fn write(&self, subject: &str, node: &NodeId, value: DataValue) -> Result<()> {
        let write = WriteValue {
            node_id: node.clone(),
            attribute_id: AttributeId::Value as u32,
            index_range: NumericRange::None,
            value,
        };
        match self.inner.session.write(&[write]).await {
            Ok(results) => match results.first() {
                Some(status) => check(subject, *status),
                None => Err(StreamError::Internal(format!(
                    "opcua: the server answered no result for the write to {subject:?}"
                ))),
            },
            Err(e) => Err(StreamError::Internal(format!(
                "opcua: cannot write to {subject:?}: {e}"
            ))),
        }
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
    /// A Write of the decoded value to the subject's node; the id is a local receipt only.
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        let (node, _) = self.node(subject).await?;
        Self::check_publish(&self.stream, opts)?;
        let value = OpcUaValue::decode_payload(&payload)?;
        // OPC UA has no message id; this one is the caller's receipt only.
        let id = provider::publish_id(opts);
        self.write(subject, &node, value).await?;
        Ok(id)
    }
}

#[async_trait::async_trait]
impl Subscriber for OpcUaManager {
    /// A monitored item per call; [`Options::prefetch`] sets the server queue size.
    async fn subscribe(&self, subject: &str, opts: &Options) -> Result<Subscription> {
        let (node, uri) = self.node(subject).await?;
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
                uri,
                queue_size,
                buffer: config.buffer.max(queue_size as usize),
            })
            .await?;
        Ok(notifications.boxed())
    }
}

#[async_trait::async_trait]
impl Batch for OpcUaManager {
    /// One Write per value, each awaited, since one request may apply in any order.
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>> {
        let (node, _) = self.node(subject).await?;
        provider::check_batch(opts)?;
        Self::check_publish(&self.stream, opts)?;
        let ids: Vec<String> = payloads.iter().map(|_| provider::new_id()).collect();
        // One Write per value, each awaited before the next: every value is
        // for the same node, and a server applies the values of one Write
        // request in an order it chooses ("If a Client requires sequential
        // processing the Client needs separate Service calls", Part 4,
        // 5.11.4.1), so one request could leave the node holding any of them.
        let mut failures = Vec::new();
        for (index, payload) in payloads.iter().enumerate() {
            let written = match OpcUaValue::decode_payload(payload) {
                Ok(value) => self.write(subject, &node, value).await,
                Err(error) => Err(error),
            };
            if let Err(error) = written {
                failures.push((index, error));
            }
        }
        provider::batch_result(subject, ids, failures)
    }
}

impl Manager for OpcUaManager {
    /// Batches only; the server keeps no consumer positions, so there is no durable side.
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
    fn what_a_write_cannot_do_is_refused() {
        let stream = Stream::default();
        let second = std::time::Duration::from_secs(1);
        for (opts, says) in [
            (Options::default().with_ttl(second), "schedule"),
            (Options::default().with_expiry(second), "expire"),
            (Options::default().with_retain(), "retain"),
            (Options::default().header("lang", "rust"), "headers"),
        ] {
            match OpcUaManager::check_publish(&stream, &opts) {
                Err(StreamError::Unsupported(why)) => assert!(why.contains(says), "{why}"),
                other => panic!("{says}: {other:?}"),
            }
        }
        assert!(OpcUaManager::check_publish(&stream, &Options::default()).is_ok());
        // A zero expiry is none, as a zero TTL is.
        let zero = Options::default().with_expiry(std::time::Duration::ZERO);
        assert!(OpcUaManager::check_publish(&stream, &zero).is_ok());
    }
}
