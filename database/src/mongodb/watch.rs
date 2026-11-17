//! [`Watcher`] over MongoDB change streams. Go's `mongodb/watch.go`.

use std::sync::Arc;

use bson::{Bson, Document};
use futures::StreamExt;
use futures::stream::BoxStream;
use mongodb::change_stream::ChangeStream;
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType, ResumeToken};
use mongodb::options::FullDocumentType;

use super::MongoDriver;
use super::codec::{from_document, key_string};
use super::error::{backend, invalid};
use crate::Result;
use crate::records::{Change, ChangeKind, Resource, WatchOptions, Watcher};

#[async_trait::async_trait]
impl Watcher for MongoDriver {
    /// A change per insert, update, replace and delete until the stream is dropped, or until
    /// the first error, which it yields and then ends on. Updates carry the document as it
    /// now is (`updateLookup`), since [`Change::message`] is the record after the change.
    ///
    /// [`WatchOptions::resume`] continues just after a change already seen, while that point
    /// is still in the oplog; past it the server refuses the token and so does this, rather
    /// than restarting from now and leaving a gap nobody knows the size of. Change streams
    /// read the oplog, so this needs a replica set, and says so when it has none.
    async fn watch(
        &self,
        res: &Resource,
        opts: WatchOptions,
    ) -> Result<BoxStream<'static, Result<Change>>> {
        let coll = self.collection(res)?;
        let mut watch = coll.watch().full_document(FullDocumentType::UpdateLookup);
        if !opts.resume.is_empty() {
            watch = watch.resume_after(decode_resume(&opts.resume)?);
        }
        let stream = watch.await.map_err(|e| {
            backend(
                format!(
                    "cannot watch {} (change streams need a replica set)",
                    res.table
                ),
                e,
            )
        })?;
        Ok(changes(stream, Arc::new(res.clone())))
    }
}

type Events = ChangeStream<ChangeStreamEvent<Document>>;

/// The contract's changes out of a change stream, ending after its first error.
fn changes(stream: Events, res: Arc<Resource>) -> BoxStream<'static, Result<Change>> {
    futures::stream::unfold(Some(stream), move |stream| {
        let res = res.clone();
        async move {
            let mut stream = stream?;
            loop {
                match stream.next().await? {
                    Err(e) => {
                        let err = backend(format!("watching {}", res.table), e);
                        return Some((Err(err), None));
                    }
                    Ok(event) => {
                        if let Some(change) = decode_change(&res, event) {
                            return Some((Ok(change), Some(stream)));
                        }
                    }
                }
            }
        }
    })
    .boxed()
}

/// One event in the contract's shape, or `None` for one it has no vocabulary for: a drop, a
/// rename or an invalidate is real, but not a change to a record. Go's `decodeChange`.
fn decode_change(res: &Resource, event: ChangeStreamEvent<Document>) -> Option<Change> {
    let kind = match event.operation_type {
        OperationType::Insert => ChangeKind::Created,
        OperationType::Update | OperationType::Replace => ChangeKind::Updated,
        OperationType::Delete => ChangeKind::Deleted,
        _ => return None,
    };
    let key = event
        .document_key
        .as_ref()
        .and_then(|k| k.get("_id"))
        .map(key_string)
        .unwrap_or_default();
    let message = match event.full_document {
        Some(doc) if kind != ChangeKind::Deleted && !doc.is_empty() => from_document(res, doc).ok(),
        _ => None,
    };
    Some(Change {
        kind,
        key,
        message,
        resume: encode_resume(&event.id),
    })
}

/// A resume token a caller can store and hand back: canonical Extended JSON rather than raw
/// BSON, so it survives a config file or a text column. Go's `encodeResume`.
fn encode_resume(token: &ResumeToken) -> String {
    bson::serialize_to_bson(token)
        .map(|b| b.into_canonical_extjson().to_string())
        .unwrap_or_default()
}

/// Reverses [`encode_resume`]. Go's `decodeResume`.
fn decode_resume(s: &str) -> Result<ResumeToken> {
    let refused = |e: &dyn std::fmt::Display| {
        invalid(format!("resume token is not one this driver produced: {e}"))
    };
    let json: serde_json::Value = serde_json::from_str(s).map_err(|e| refused(&e))?;
    let bson = Bson::try_from(json).map_err(|e| refused(&e))?;
    bson::deserialize_from_bson(bson).map_err(|e| refused(&e))
}
