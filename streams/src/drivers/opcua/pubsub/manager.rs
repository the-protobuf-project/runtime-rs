//! The bound half of the PubSub provider: one WriterGroup's DataSetWriters.

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use futures::StreamExt;
use opcua::types::{DataValue, DateTime};
use rumqttc::{
    Outgoing,
    v5::{
        AsyncClient, Event, EventLoop, Incoming,
        mqttbytes::v5::{Filter, Publish, SubscribeReasonCode},
    },
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    Batch, Manager, Message, Options, Payload, Publisher, Result, Stream, StreamError, Subscriber,
    Subscription,
    drivers::{
        mqtt::{Session, dial},
        opcua::{CODEC_NAME, OpcUaValue, codec},
    },
    provider,
};

use super::{DataSet, DataSetMessage, Group, Inner, NetworkMessage, link::LINGER, writer_id};

/// A subject's DataSetWriter, and its field when it names one.
pub(crate) fn split(subject: &str) -> (&str, Option<&str>) {
    match subject.split_once('/') {
        Some((writer, field)) => (writer, Some(field)),
        None => (subject, None),
    }
}

/// Refuses publishing as a publisher this provider is not.
pub(crate) fn check_own(stream: &str, group: &Group) -> Result<()> {
    if group.own {
        return Ok(());
    }
    Err(StreamError::Unsupported(format!(
        "opcua pubsub: stream {stream:?} is publisher {:?}'s WriterGroup, and publishing on it \
         would send as that publisher; subscribe to it, or declare a stream of your own",
        group.publisher
    )))
}

/// Publishes and subscribes the DataSetWriters one stream declares.
pub(crate) struct PubSubManager {
    inner: Arc<Inner>,
    stream: Stream,
    group: Group,
}

impl PubSubManager {
    pub(crate) fn new(inner: Arc<Inner>, stream: Stream) -> Self {
        let group = inner.group_of(&stream.id);
        Self {
            inner,
            stream,
            group,
        }
    }

    /// Splits a subject the stream declares: literally, or as a field of a
    /// writer it declares whole.
    fn check_subject<'a>(&self, subject: &'a str) -> Result<(&'a str, Option<&'a str>)> {
        let (writer, field) = split(subject);
        let declared = self.stream.subjects.iter().any(|s| s == subject)
            || (field.is_some() && self.stream.subjects.iter().any(|s| s == writer));
        if declared && !writer.is_empty() && field.is_none_or(|f| !f.is_empty()) {
            return Ok((writer, field));
        }
        Err(provider::unknown_subject(
            &self.stream.id,
            subject,
            &self.stream.subjects,
        ))
    }

    fn check_publish(&self, subject: &str, opts: &Options) -> Result<(String, Option<String>)> {
        let (writer, field) = self.check_subject(subject)?;
        check_own(&self.stream.id, &self.group)?;
        if opts.delay().is_some() {
            return Err(StreamError::Unsupported(
                "opcua pubsub: MQTT delivers on publish and cannot schedule a DataSetMessage"
                    .into(),
            ));
        }
        Ok((writer.to_owned(), field.map(str::to_owned)))
    }

    /// The fields a payload publishes: one field's value, or a whole DataSet.
    fn fields(field: Option<&str>, payload: &Payload) -> Result<Vec<(String, DataValue)>> {
        match field {
            Some(field) => Ok(vec![(
                field.to_owned(),
                OpcUaValue::decode_payload(payload)?,
            )]),
            None => Ok(DataSet::decode_payload(payload)?.fields),
        }
    }

    /// DataSetMessages for `datasets`, numbered from `first`.
    fn messages(
        &self,
        writer: &str,
        first: u32,
        datasets: Vec<Vec<(String, DataValue)>>,
    ) -> Vec<DataSetMessage> {
        let version = self.inner.version(&self.group.group, writer);
        let now = DateTime::now();
        datasets
            .into_iter()
            .enumerate()
            .map(|(n, payload)| DataSetMessage {
                data_set_writer_id: Some(writer_id(writer)),
                data_set_writer_name: Some(writer.to_owned()),
                sequence_number: Some(first.wrapping_add(n as u32)),
                meta_data_version: version.clone(),
                timestamp: Some(now),
                payload,
                ..DataSetMessage::default()
            })
            .collect()
    }

    fn network(&self, id: String, messages: Vec<DataSetMessage>) -> NetworkMessage {
        NetworkMessage {
            message_id: Some(id),
            publisher_id: Some(self.group.publisher.clone()),
            writer_group_name: Some(self.group.group.clone()),
            data_set_class_id: None,
            messages,
        }
    }

    /// Opens a clean connection and subscribes it to `filters`, returning
    /// once the broker has granted every one.
    async fn attach(
        &self,
        subject: &str,
        filters: &[String],
    ) -> Result<(AsyncClient, EventLoop, Vec<Publish>)> {
        let client_id = format!("runtime-rs-pubsub-sub-{}", provider::new_id());
        let (client, mut events, _) = dial(&self.inner.options, &client_id, Session::Clean).await?;
        let failed = |error: &dyn std::fmt::Display| {
            StreamError::Internal(format!(
                "opcua pubsub: cannot subscribe to {subject:?}: {error}"
            ))
        };
        let qos = self.inner.config.qos;
        client
            .subscribe_many(filters.iter().map(|f| Filter::new(f.clone(), qos)))
            .await
            .map_err(|e| failed(&e))?;
        let mut early = Vec::new();
        loop {
            match events.poll().await.map_err(|e| failed(&e))? {
                Event::Incoming(Incoming::SubAck(ack)) => {
                    if ack.return_codes.len() == filters.len()
                        && ack
                            .return_codes
                            .iter()
                            .all(|c| matches!(c, SubscribeReasonCode::Success(_)))
                    {
                        break;
                    }
                    let _ = client.try_disconnect();
                    return Err(failed(&format_args!(
                        "broker reasons {:?}",
                        ack.return_codes
                    )));
                }
                Event::Incoming(Incoming::Publish(publish)) => early.push(publish),
                _ => {}
            }
        }
        Ok((client, events, early))
    }
}

#[async_trait::async_trait]
impl Publisher for PubSubManager {
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        let (writer, field) = self.check_publish(subject, opts)?;
        let fields = Self::fields(field.as_deref(), &payload)?;
        let id = provider::publish_id(opts);
        let first = self.inner.reserve(&self.group.group, &writer, 1);
        let messages = self.messages(&writer, first, vec![fields]);
        let body = Bytes::from(self.network(id.clone(), messages).to_json()?);
        let topic = self.inner.topic("data", &self.group, Some(&writer));
        let len = body.len();
        if let Err(error) = self
            .inner
            .links
            .link()
            .publish(&topic, self.inner.config.qos, false, body)
            .await
        {
            tracing::error!(subject, id, %error, "opcua pubsub: could not publish");
            return Err(error);
        }
        tracing::debug!(
            subject,
            id,
            sequence = first,
            bytes = len,
            "opcua pubsub: published"
        );
        Ok(id)
    }
}

#[async_trait::async_trait]
impl Batch for PubSubManager {
    /// Publishes the payloads as DataSetMessages of one NetworkMessage, or of
    /// several when there are more than
    /// [`max_dataset_messages`](super::PubSubConfig::max_dataset_messages),
    /// all in flight at once and in order. A payload that does not decode
    /// fails alone; a NetworkMessage the broker refuses fails every entry in
    /// it.
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>> {
        provider::check_batch(opts)?;
        let (writer, field) = self.check_publish(subject, opts)?;
        let mut ids: Vec<String> = Vec::with_capacity(payloads.len());
        let mut failures = Vec::new();
        let mut good = Vec::with_capacity(payloads.len());
        for (index, payload) in payloads.iter().enumerate() {
            ids.push(String::new());
            match Self::fields(field.as_deref(), payload) {
                Ok(fields) => good.push((index, fields)),
                Err(error) => {
                    ids[index] = provider::new_id();
                    failures.push((index, error));
                }
            }
        }
        let first = self
            .inner
            .reserve(&self.group.group, &writer, good.len() as u32);
        let topic = self.inner.topic("data", &self.group, Some(&writer));
        let chunk = self.inner.config.max_dataset_messages.max(1);
        let mut networks = Vec::new();
        let mut members: Vec<Vec<usize>> = Vec::new();
        let mut sequence = first;
        for part in good.chunks_mut(chunk) {
            let id = provider::new_id();
            let single = part.len() == 1;
            let indexes: Vec<usize> = part.iter().map(|(index, _)| *index).collect();
            for (n, index) in indexes.iter().enumerate() {
                ids[*index] = if single {
                    id.clone()
                } else {
                    format!("{id}.{n}")
                };
            }
            let datasets = part.iter_mut().map(|(_, f)| std::mem::take(f)).collect();
            let messages = self.messages(&writer, sequence, datasets);
            sequence = sequence.wrapping_add(indexes.len() as u32);
            match self.network(id, messages).to_json() {
                Ok(body) => {
                    networks.push((topic.clone(), Bytes::from(body)));
                    members.push(indexes);
                }
                Err(error) => failures.extend(indexes.into_iter().map(|i| (i, error.clone()))),
            }
        }
        let outcomes = self
            .inner
            .links
            .link()
            .publish_all(self.inner.config.qos, networks)
            .await;
        for (indexes, outcome) in members.into_iter().zip(outcomes) {
            if let Err(error) = outcome {
                failures.extend(indexes.into_iter().map(|i| (i, error.clone())));
            }
        }
        failures.sort_by_key(|(index, _)| *index);
        if let Some((_, error)) = failures.first() {
            tracing::error!(subject, failed = failures.len(), total = ids.len(), %error,
                "opcua pubsub: could not publish every message of a batch");
        } else {
            tracing::debug!(
                subject,
                count = ids.len(),
                "opcua pubsub: published a batch"
            );
        }
        provider::batch_result(subject, ids, failures)
    }
}

#[async_trait::async_trait]
impl Subscriber for PubSubManager {
    async fn subscribe(&self, subject: &str, opts: &Options) -> Result<Subscription> {
        let (writer, field) = self.check_subject(subject)?;
        let topics = [
            self.inner.topic("data", &self.group, Some(writer)),
            self.inner.topic("data", &self.group, None),
        ];
        let filters: Vec<String> = topics
            .iter()
            .map(|topic| match &opts.group {
                Some(group) => format!("$share/{group}/{topic}"),
                None => topic.clone(),
            })
            .collect();
        let (client, events, early) = self.attach(subject, &filters).await?;
        let reader = Reader {
            inner: self.inner.clone(),
            writer: writer.to_owned(),
            writer_id: writer_id(writer),
            field: field.map(str::to_owned),
            group_topic: topics[1].clone(),
            track: opts.group.is_none(),
            last: HashMap::new(),
            subjects: HashMap::new(),
            codec: Arc::from(CODEC_NAME),
        };
        let (tx, rx) = mpsc::channel(provider::prefetch(opts).max(1));
        tokio::spawn(route(client, events, early, filters, reader, tx));
        tracing::debug!(subject, stream = %self.stream.id, "opcua pubsub: subscribed");
        Ok(ReceiverStream::new(rx).boxed())
    }
}

impl Manager for PubSubManager {
    fn batch(&self) -> Option<&dyn Batch> {
        Some(self)
    }
}

/// What a sequence number says about the one before it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Step {
    /// The first seen, or the next.
    InOrder,
    /// `n` messages were missed.
    Gap(u32),
    /// The last one again: a QoS 1 redelivery.
    Repeat,
    /// Behind the last: the publisher restarted and counts again.
    Restart,
}

/// Compares `sequence` with the `last` one seen, in UInt32 arithmetic that
/// wraps, as Part 14's sequence numbers do.
pub(crate) fn step(last: Option<u32>, sequence: u32) -> Step {
    let Some(last) = last else {
        return Step::InOrder;
    };
    match sequence.wrapping_sub(last) as i32 {
        1 => Step::InOrder,
        0 => Step::Repeat,
        d if d > 1 => Step::Gap((d - 1) as u32),
        _ => Step::Restart,
    }
}

/// Turns one subscription's NetworkMessages into messages.
struct Reader {
    inner: Arc<Inner>,
    writer: String,
    writer_id: u16,
    field: Option<String>,
    /// The writer-less topic, where a DataSetMessage names its writer.
    group_topic: String,
    track: bool,
    /// The last SequenceNumber per publisher.
    last: HashMap<String, u32>,
    /// `<writer>/<field>` per field, allocated once.
    subjects: HashMap<String, Arc<str>>,
    codec: Arc<str>,
}

impl Reader {
    /// Whether a DataSetMessage that arrived on `topic` is this writer's.
    fn ours(&self, message: &DataSetMessage, on_group_topic: bool) -> bool {
        match (&message.data_set_writer_name, message.data_set_writer_id) {
            (Some(name), _) => *name == self.writer,
            (None, Some(id)) if on_group_topic => id == self.writer_id,
            // The writer's own topic names it.
            (None, _) => !on_group_topic,
        }
    }

    fn convert(&mut self, publish: &Publish, out: &mut Vec<Message>) {
        if publish.retain {
            // Retained before this subscription: not published after it.
            return;
        }
        let network = match NetworkMessage::from_json(&publish.payload) {
            Ok(network) => network,
            Err(error) => {
                tracing::warn!(writer = %self.writer, %error, "opcua pubsub: dropping a malformed message");
                return;
            }
        };
        let on_group_topic = publish.topic.as_ref() == self.group_topic.as_bytes();
        let id = network.message_id.clone().unwrap_or_else(provider::new_id);
        let several = network.messages.len() > 1;
        for (index, message) in network.messages.iter().enumerate() {
            if message.is_keepalive() || !self.ours(message, on_group_topic) {
                continue;
            }
            if self.track
                && let Some(sequence) = message.sequence_number
            {
                let publisher = network.publisher_of(message).unwrap_or_default().to_owned();
                match step(self.last.get(&publisher).copied(), sequence) {
                    Step::InOrder => {}
                    Step::Repeat => {
                        tracing::debug!(writer = %self.writer, sequence, "opcua pubsub: dropping a repeated DataSetMessage");
                        continue;
                    }
                    Step::Gap(missed) => {
                        tracing::warn!(publisher, writer = %self.writer, sequence, missed,
                            "opcua pubsub: DataSetMessages are missing from the sequence");
                        self.inner
                            .record_gap(&publisher, &self.writer, u64::from(missed));
                    }
                    Step::Restart => {
                        tracing::info!(publisher, writer = %self.writer, sequence,
                            "opcua pubsub: the writer's sequence started again");
                    }
                }
                self.last.insert(publisher, sequence);
            }
            let message_id = if several {
                format!("{id}.{index}")
            } else {
                id.clone()
            };
            for (name, value) in &message.payload {
                if self.field.as_ref().is_some_and(|f| f != name) {
                    continue;
                }
                let subject = self
                    .subjects
                    .entry(name.clone())
                    .or_insert_with(|| Arc::from(format!("{}/{name}", self.writer)))
                    .clone();
                out.push(Message::new(
                    message_id.clone(),
                    subject,
                    self.codec.clone(),
                    codec::encode(value),
                ));
            }
        }
    }
}

/// Routes a subscription's publishes until its reader is dropped or the
/// connection is lost, then unsubscribes and disconnects. A lost connection
/// ends the stream rather than redialing: a clean session redialed has lost
/// what was published meanwhile, and an ended stream says so.
async fn route(
    client: AsyncClient,
    mut events: EventLoop,
    early: Vec<Publish>,
    filters: Vec<String>,
    mut reader: Reader,
    tx: mpsc::Sender<Message>,
) {
    let mut batch = Vec::new();
    'routing: {
        for publish in &early {
            reader.convert(publish, &mut batch);
            for message in batch.drain(..) {
                if tx.send(message).await.is_err() {
                    break 'routing;
                }
            }
        }
        loop {
            tokio::select! {
                _ = tx.closed() => break,
                event = events.poll() => match event {
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        reader.convert(&publish, &mut batch);
                        for message in batch.drain(..) {
                            if tx.send(message).await.is_err() {
                                break 'routing;
                            }
                        }
                    }
                    Ok(Event::Incoming(Incoming::Disconnect(disconnect))) => {
                        tracing::warn!(writer = %reader.writer, reason = ?disconnect.reason_code,
                            "opcua pubsub: broker ended the subscription");
                        return;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(writer = %reader.writer, %error,
                            "opcua pubsub: subscription connection lost; ending the stream");
                        return;
                    }
                }
            }
        }
    }
    for filter in filters {
        let _ = client.unsubscribe(filter).await;
    }
    let _ = client.disconnect().await;
    let _ = tokio::time::timeout(LINGER, async {
        loop {
            match events.poll().await {
                Ok(Event::Outgoing(Outgoing::Disconnect)) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_steps() {
        assert_eq!(step(None, 7), Step::InOrder);
        assert_eq!(step(Some(7), 8), Step::InOrder);
        assert_eq!(step(Some(7), 10), Step::Gap(2));
        assert_eq!(step(Some(7), 7), Step::Repeat);
        assert_eq!(step(Some(700), 1), Step::Restart);
        // UInt32 wraps.
        assert_eq!(step(Some(u32::MAX), 0), Step::InOrder);
        assert_eq!(step(Some(u32::MAX), 2), Step::Gap(2));
    }

    #[test]
    fn subjects_split_at_the_first_slash() {
        assert_eq!(split("boiler"), ("boiler", None));
        assert_eq!(split("boiler/Temp"), ("boiler", Some("Temp")));
        assert_eq!(split("boiler/a/b"), ("boiler", Some("a/b")));
    }
}
