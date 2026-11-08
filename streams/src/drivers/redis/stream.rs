//! Stream lifecycle: metadata stored as the single entry of a Redis stream key,
//! as runtime-go stores it.

use std::sync::Arc;

use redis::streams::StreamRangeReply;

use crate::{
    Manager, Result, Stream, StreamError, Streams,
    provider::{declares, new_stream_id, unknown_subject},
};

use super::{
    DurableManager, Inner, PubSubManager, RedisProvider, Replay, fail, keys::Kind,
    keys::METADATA_FIELD,
};

/// How many keys one SCAN step asks for.
const SCAN_COUNT: usize = 100;

impl Inner {
    /// Rejects a subject the stream does not declare.
    ///
    /// A Redis key is a literal name, so a declared `orders.*` is a subject
    /// called that and not a pattern. Publishing to an undeclared subject would
    /// write to a key nobody reads, and subscribing would wait forever.
    pub(super) fn declares(&self, stream: &Stream, subject: &str) -> Result<()> {
        if declares(&stream.subjects, subject) {
            return Ok(());
        }
        tracing::error!(subject, stream = %stream.id, declared = ?stream.subjects,
            "subject is not declared by this stream");
        Err(unknown_subject(&stream.id, subject, &stream.subjects))
    }

    /// Loads and decodes a stream's metadata entry.
    async fn read(&self, key: &str, id: &str) -> Result<Stream> {
        let reply: StreamRangeReply = self
            .query(
                redis::cmd("XRANGE")
                    .arg(key)
                    .arg("-")
                    .arg("+")
                    .arg("COUNT")
                    .arg(1),
                Replay::Idempotent,
            )
            .await
            .map_err(fail(format!("redis: cannot read stream {id}")))?;
        let Some(entry) = reply.ids.first() else {
            return Err(StreamError::NotFound(format!("stream {id}")));
        };
        let Some(raw) = entry.get::<Vec<u8>>(METADATA_FIELD) else {
            return Err(StreamError::NotFound(format!(
                "stream {id} carries no metadata"
            )));
        };
        let mut out: Stream = serde_json::from_slice(&raw).map_err(|error| {
            StreamError::Internal(format!("redis: stream {id} has invalid metadata: {error}"))
        })?;
        out.id = id.to_owned();
        Ok(out)
    }
}

/// The metadata a create or update writes. `active` is always set, as in Go.
fn normalized(id: &str, stream: Stream) -> Result<(Stream, Vec<u8>)> {
    let out = Stream {
        id: id.to_owned(),
        active: true,
        ..stream
    };
    let body = serde_json::to_vec(&out).map_err(|error| {
        StreamError::Internal(format!("redis: cannot encode stream {id}: {error}"))
    })?;
    Ok((out, body))
}

#[async_trait::async_trait]
impl Streams for RedisProvider {
    async fn create(&self, stream: Stream) -> Result<Stream> {
        let id = if stream.id.is_empty() {
            new_stream_id(&stream.name)
        } else {
            stream.id.clone()
        };
        let (out, body) = normalized(&id, stream)?;
        // Idempotent in effect: a repeat appends a second, identical metadata
        // entry, every reader takes the first, and an update trims to one.
        self.inner
            .query::<()>(
                redis::cmd("XADD")
                    .arg(self.inner.keys.stream(&id))
                    .arg("*")
                    .arg(METADATA_FIELD)
                    .arg(body),
                Replay::Idempotent,
            )
            .await
            .map_err(fail(format!("redis: cannot create stream {id}")))?;
        tracing::info!(id = %id, name = %out.name, "stream created");
        Ok(out)
    }

    async fn get(&self, id: &str) -> Result<Stream> {
        self.inner.read(&self.inner.keys.stream(id), id).await
    }

    /// Reads the stream here so an unknown id fails at bind rather than at the
    /// first publish, and so subjects validate without a round trip per call.
    ///
    /// The managers are separate types rather than one with a mode flag
    /// because the difference is visible through the contract: `as_durable`
    /// has to succeed on one and fail on the other.
    async fn bind(&self, id: &str) -> Result<Arc<dyn Manager>> {
        let stream = self.get(id).await?;
        let inner = Arc::clone(&self.inner);
        Ok(match inner.kind {
            Kind::Durable => Arc::new(DurableManager::new(inner, stream)),
            Kind::Stream | Kind::Notify => Arc::new(PubSubManager::new(inner, stream)),
        })
    }

    /// The replacement is written before the old metadata is trimmed, in one
    /// transaction, so a reader sees the old configuration or the new one and
    /// never an empty stream. Deleting first would destroy the stream whenever
    /// the rewrite failed.
    async fn update(&self, id: &str, stream: Stream) -> Result<Stream> {
        self.get(id).await?;
        let (out, body) = normalized(id, stream)?;
        let key = self.inner.keys.stream(id);
        // A repeat writes the same entry and trims to it again.
        self.inner
            .query_pipe::<()>(
                redis::pipe()
                    .atomic()
                    .cmd("XADD")
                    .arg(&key)
                    .arg("*")
                    .arg(METADATA_FIELD)
                    .arg(body)
                    .ignore()
                    .cmd("XTRIM")
                    .arg(&key)
                    .arg("MAXLEN")
                    .arg(1)
                    .ignore(),
                Replay::Idempotent,
            )
            .await
            .map_err(fail(format!("redis: cannot update stream {id}")))?;
        tracing::info!(id, "stream updated");
        Ok(out)
    }

    /// A stream that is not there is reported rather than treated as success:
    /// a silent `Ok` once made a failed delete look like a completed one.
    async fn delete(&self, id: &str) -> Result<()> {
        // The server ends up the same either way; only a reply lost after the
        // delete ran makes the repeat report NotFound for it.
        let removed: u64 = self
            .inner
            .query(
                redis::cmd("DEL").arg(self.inner.keys.stream(id)),
                Replay::Idempotent,
            )
            .await
            .map_err(fail(format!("redis: cannot delete stream {id}")))?;
        if removed == 0 {
            return Err(StreamError::NotFound(format!("stream {id}")));
        }
        tracing::info!(id, "stream deleted");
        Ok(())
    }

    /// Keys are found with SCAN rather than KEYS so a large keyspace does not
    /// block the server, and each candidate's type is checked before it is
    /// read, because something else may own a matching key.
    async fn list(&self) -> Result<Vec<Stream>> {
        let pattern = self.inner.keys.stream_pattern();
        let mut out = Vec::new();
        let mut cursor: u64 = 0;
        loop {
            let (next, keys): (u64, Vec<String>) = self
                .inner
                .query(
                    redis::cmd("SCAN")
                        .arg(cursor)
                        .arg("MATCH")
                        .arg(&pattern)
                        .arg("COUNT")
                        .arg(SCAN_COUNT),
                    Replay::Idempotent,
                )
                .await
                .map_err(fail("redis: cannot list streams"))?;
            for key in keys {
                let kind: String = self
                    .inner
                    .query(redis::cmd("TYPE").arg(&key), Replay::Idempotent)
                    .await
                    .map_err(fail(format!("redis: cannot type {key}")))?;
                if kind != "stream" {
                    continue;
                }
                let id = self.inner.keys.id_from_stream(&key);
                match self.inner.read(&key, id).await {
                    Ok(stream) => out.push(stream),
                    // A key without our metadata belongs to something else in
                    // this namespace; skip it rather than fail the listing.
                    Err(StreamError::NotFound(_)) => {}
                    Err(error) => return Err(error),
                }
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }
}
