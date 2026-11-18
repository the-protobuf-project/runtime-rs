use super::feeds::{integer, message, timestamp};
use super::*;

#[derive(Clone, Copy)]
pub struct AdapterSpec {
    pub message: &'static str,
    pub collection: &'static str,
    pub parent: &'static str,
    pub fields: &'static [(&'static str, &'static str, bool)],
    pub states: &'static [(&'static str, i32, &'static str)],
}
impl AdapterSpec {
    fn field(&self, key: &str) -> Option<&'static str> {
        self.fields
            .iter()
            .find(|(k, _, _)| *k == key)
            .map(|(_, f, _)| *f)
    }
    fn state(&self, name: &str) -> i32 {
        self.states
            .iter()
            .find(|(k, _, _)| *k == name)
            .map(|(_, v, _)| *v)
            .unwrap_or_default()
    }
    fn literal(&self, name: &str) -> &str {
        self.states
            .iter()
            .find(|(k, _, _)| *k == name)
            .map(|(_, _, v)| *v)
            .unwrap_or("")
    }
    fn new_message(&self, o: &Options) -> Result<DynamicMessage, Status> {
        o.descriptors
            .get_message_by_name(self.message)
            .map(DynamicMessage::new)
            .ok_or_else(|| Status::internal(format!("missing descriptor {}", self.message)))
    }
    fn string(&self, m: &DynamicMessage, key: &str) -> String {
        self.field(key).map(|f| text(m, f)).unwrap_or_default()
    }
    fn int(&self, m: &DynamicMessage, key: &str) -> i32 {
        self.field(key).map(|f| integer(m, f)).unwrap_or_default()
    }
    fn time(&self, m: &DynamicMessage, key: &str) -> Option<prost_types::Timestamp> {
        self.field(key).and_then(|f| timestamp(m, f))
    }
    fn set(&self, m: &mut DynamicMessage, key: &str, value: Value) -> Result<(), Status> {
        if let Some(f) = self.field(key) {
            m.try_set_field_by_name(f, value)
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        Ok(())
    }
    fn set_time(
        &self,
        m: &mut DynamicMessage,
        key: &str,
        value: Option<prost_types::Timestamp>,
    ) -> Result<(), Status> {
        if let Some(f) = self.field(key) {
            if let Some(v) = value {
                let fd = m
                    .descriptor()
                    .get_field_by_name(f)
                    .ok_or_else(|| Status::internal("missing timestamp field"))?;
                let prost_reflect::Kind::Message(desc) = fd.kind() else {
                    return Err(Status::internal("timestamp field is not a message"));
                };
                let value = DynamicMessage::decode(desc, v.encode_to_vec().as_slice())
                    .map_err(|e| Status::internal(e.to_string()))?;
                m.set_field(&fd, Value::Message(value));
            } else {
                m.clear_field_by_name(f);
            }
        }
        Ok(())
    }
    async fn write<M: Message + Default + Send + Sync + 'static>(
        &self,
        r: &dyn Repository<M>,
        ctx: &Context,
        m: DynamicMessage,
        paths: &[String],
    ) -> Result<M, Status> {
        let mut mutable = Vec::new();
        let mut output = Vec::new();
        for key in paths {
            if let Some((_, f, owned)) = self.fields.iter().find(|(k, _, _)| *k == key) {
                if *owned {
                    output.push(f.to_string())
                } else {
                    mutable.push(f.to_string())
                }
            } else {
                return Err(Status::invalid_argument(format!(
                    "unsupported adapter update path {key}"
                )));
            }
        }
        r.update_with_output_only(ctx, typed(&m)?, mutable, output)
            .await
    }
}
#[derive(Default, Clone, Debug)]
pub struct Webhook {
    pub name: String,
    pub uri: String,
    pub event_types: Vec<String>,
    pub scope: String,
    pub paused: bool,
    pub secret: String,
    pub previous_secret: String,
    pub rotate_time: Option<prost_types::Timestamp>,
    pub create_time: Option<prost_types::Timestamp>,
}
#[derive(Default, Clone, Debug)]
pub struct Cursor {
    pub feed: String,
    pub position: Option<prost_types::Timestamp>,
    pub etag: String,
}
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryState {
    #[default]
    Pending,
    Succeeded,
    Failed,
}
#[derive(Default, Clone, Debug)]
pub struct Delivery {
    pub name: String,
    pub webhook: String,
    pub event_id: String,
    pub event_type: String,
    pub subject: String,
    pub payload: String,
    pub state: DeliveryState,
    pub attempt_count: i32,
    pub response_code: i32,
    pub error_message: String,
    pub next_attempt_time: Option<prost_types::Timestamp>,
    pub deliver_time: Option<prost_types::Timestamp>,
    pub create_time: Option<prost_types::Timestamp>,
    pub etag: String,
}
/// Typed adapters for a webhook dispatcher. Status codes are the store error
/// contract: NotFound, AlreadyExists and Aborted (optimistic conflict).
pub struct EventStore<W: Send + Sync + 'static, D: Send + Sync + 'static, C: Send + Sync + 'static>
{
    pub webhooks: Arc<dyn Repository<W>>,
    pub deliveries: Arc<dyn Repository<D>>,
    pub cursors: Arc<dyn Repository<C>>,
    pub options: Options,
    pub webhook_spec: AdapterSpec,
    pub delivery_spec: AdapterSpec,
    pub cursor_spec: AdapterSpec,
}
impl<
    W: Message + Default + Send + Sync + 'static,
    D: Message + Default + Send + Sync + 'static,
    C: Message + Default + Send + Sync + 'static,
> EventStore<W, D, C>
{
    fn webhook(&self, value: &W) -> Result<Webhook, Status> {
        let s = &self.webhook_spec;
        let m = message(&self.options, s.message, value)?;
        let paused = s
            .field("paused")
            .and_then(|f| m.get_field_by_name(f))
            .and_then(|v| v.as_bool())
            .unwrap_or_else(|| {
                s.states
                    .iter()
                    .any(|(k, n, _)| *k == "PAUSED" && *n == s.int(&m, "state"))
            });
        let event_types = s
            .field("event_types")
            .and_then(|f| m.get_field_by_name(f))
            .and_then(|v| {
                v.as_list().map(|xs| {
                    xs.iter()
                        .filter_map(|v| v.as_str().map(str::to_owned))
                        .collect()
                })
            })
            .unwrap_or_default();
        Ok(Webhook {
            name: text(&m, "name"),
            uri: s.string(&m, "uri"),
            event_types,
            scope: s.string(&m, "scope"),
            paused,
            secret: s.string(&m, "secret"),
            previous_secret: s.string(&m, "previous_secret"),
            rotate_time: s.time(&m, "rotate_time"),
            create_time: s.time(&m, "create_time"),
        })
    }
    pub async fn list_webhooks(
        &self,
        ctx: &Context,
        page_token: String,
    ) -> Result<(Vec<Webhook>, String), Status> {
        let (rows, next) = self
            .webhooks
            .list(
                ctx,
                "",
                ListInput {
                    page_size: 100,
                    page_token,
                    ..Default::default()
                },
            )
            .await?;
        Ok((
            rows.iter()
                .map(|r| self.webhook(r))
                .collect::<Result<_, _>>()?,
            next,
        ))
    }
    pub async fn get_webhook(&self, ctx: &Context, name: &str) -> Result<Webhook, Status> {
        let m = self.webhooks.get(ctx, name).await?;
        let d = message(&self.options, self.webhook_spec.message, &m)?;
        if self.webhook_spec.time(&d, "delete_time").is_some() {
            return Err(Status::not_found("webhook is deleted"));
        }
        self.webhook(&m)
    }
    fn cursor(&self, feed: &str, value: &C) -> Result<Cursor, Status> {
        let s = &self.cursor_spec;
        let m = message(&self.options, s.message, value)?;
        Ok(Cursor {
            feed: feed.into(),
            position: s.time(&m, "position"),
            etag: s.string(&m, "etag"),
        })
    }
    pub async fn get_cursor(&self, ctx: &Context, feed: &str) -> Result<Cursor, Status> {
        let value = self
            .cursors
            .get(ctx, &format!("{}/{feed}", self.cursor_spec.collection))
            .await?;
        self.cursor(feed, &value)
    }
    fn stored_cursor(&self, c: Cursor) -> Result<DynamicMessage, Status> {
        let s = &self.cursor_spec;
        let mut m = s.new_message(&self.options)?;
        m.set_field_by_name(
            "name",
            Value::String(format!("{}/{}", s.collection, c.feed)),
        );
        s.set_time(&mut m, "position", c.position)?;
        s.set(&mut m, "etag", Value::String(c.etag))?;
        Ok(m)
    }
    pub async fn create_cursor(&self, ctx: &Context, c: Cursor) -> Result<Cursor, Status> {
        let feed = c.feed.clone();
        let m = self.stored_cursor(c)?;
        let out = self.cursors.create(ctx, "", typed(&m)?).await?;
        self.cursor(&feed, &out)
    }
    pub async fn update_cursor(&self, ctx: &Context, c: Cursor) -> Result<(), Status> {
        let m = self.stored_cursor(c)?;
        self.cursor_spec
            .write(self.cursors.as_ref(), ctx, m, &["position".into()])
            .await?;
        Ok(())
    }
    fn delivery(&self, value: &D) -> Result<Delivery, Status> {
        let s = &self.delivery_spec;
        let m = message(&self.options, s.message, value)?;
        let name = text(&m, "name");
        let state = if s.int(&m, "state") == s.state("SUCCEEDED") {
            DeliveryState::Succeeded
        } else if s.int(&m, "state") == s.state("FAILED") {
            DeliveryState::Failed
        } else {
            DeliveryState::Pending
        };
        Ok(Delivery {
            webhook: parent_of(&name).into(),
            name,
            event_id: s.string(&m, "event_id"),
            event_type: s.string(&m, "event_type"),
            subject: s.string(&m, "subject"),
            payload: s.string(&m, "payload"),
            state,
            attempt_count: s.int(&m, "attempt_count"),
            response_code: s.int(&m, "response_code"),
            error_message: s.string(&m, "error_message"),
            next_attempt_time: s.time(&m, "next_attempt_time"),
            deliver_time: s.time(&m, "deliver_time"),
            create_time: s.time(&m, "create_time"),
            etag: s.string(&m, "etag"),
        })
    }
    fn stored_delivery(&self, d: Delivery) -> Result<DynamicMessage, Status> {
        let s = &self.delivery_spec;
        let mut m = s.new_message(&self.options)?;
        m.set_field_by_name("name", Value::String(d.name));
        for (k, v) in [
            ("event_id", d.event_id),
            ("event_type", d.event_type),
            ("subject", d.subject),
            ("payload", d.payload),
            ("error_message", d.error_message),
            ("etag", d.etag),
        ] {
            s.set(&mut m, k, Value::String(v))?;
        }
        s.set(
            &mut m,
            "state",
            Value::EnumNumber(s.state(match d.state {
                DeliveryState::Pending => "PENDING",
                DeliveryState::Succeeded => "SUCCEEDED",
                DeliveryState::Failed => "FAILED",
            })),
        )?;
        s.set(&mut m, "attempt_count", Value::I32(d.attempt_count))?;
        s.set(&mut m, "response_code", Value::I32(d.response_code))?;
        s.set_time(&mut m, "next_attempt_time", d.next_attempt_time)?;
        s.set_time(&mut m, "deliver_time", d.deliver_time)?;
        Ok(m)
    }
    pub async fn create_delivery(&self, ctx: &Context, mut d: Delivery) -> Result<(), Status> {
        let parent = if d.webhook.is_empty() {
            parent_of(&d.name).to_string()
        } else {
            d.webhook.clone()
        };
        d.etag.clear();
        let m = self.stored_delivery(d)?;
        self.deliveries.create(ctx, &parent, typed(&m)?).await?;
        Ok(())
    }
    pub async fn due_deliveries(
        &self,
        ctx: &Context,
        now: prost_types::Timestamp,
        limit: i32,
    ) -> Result<Vec<Delivery>, Status> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let s = &self.delivery_spec;
        let (rows, _) = self
            .deliveries
            .list(
                ctx,
                s.parent,
                ListInput {
                    page_size: limit,
                    order_by: s.field("next_attempt_time").unwrap().into(),
                    filter: format!(
                        "{} = {} AND {} <= {:?}",
                        s.field("state").unwrap(),
                        s.literal("PENDING"),
                        s.field("next_attempt_time").unwrap(),
                        now.to_string()
                    ),
                    ..Default::default()
                },
            )
            .await?;
        rows.iter().map(|r| self.delivery(r)).collect()
    }
    pub async fn update_delivery(
        &self,
        ctx: &Context,
        d: Delivery,
        paths: &[String],
    ) -> Result<Delivery, Status> {
        if paths.is_empty() {
            return Ok(d);
        }
        let m = self.stored_delivery(d)?;
        let out = self
            .delivery_spec
            .write(self.deliveries.as_ref(), ctx, m, paths)
            .await?;
        self.delivery(&out)
    }
    pub async fn prune_deliveries(
        &self,
        ctx: &Context,
        before: prost_types::Timestamp,
        limit: i32,
    ) -> Result<(), Status> {
        if limit <= 0 {
            return Ok(());
        }
        let s = &self.delivery_spec;
        let state = s.field("state").unwrap();
        let (rows, _) = self
            .deliveries
            .list(
                ctx,
                s.parent,
                ListInput {
                    page_size: limit,
                    filter: format!(
                        "({state} = {} OR {state} = {}) AND {} < {:?}",
                        s.literal("SUCCEEDED"),
                        s.literal("FAILED"),
                        s.field("update_time").unwrap(),
                        before.to_string()
                    ),
                    ..Default::default()
                },
            )
            .await?;
        for r in rows {
            let m = message(&self.options, s.message, &r)?;
            self.deliveries.purge(ctx, &text(&m, "name")).await?;
        }
        Ok(())
    }
}
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyState {
    #[default]
    InFlight,
    Done,
}
/// Response is a proto-JSON google.protobuf.Any, matching the Go adapter's storage.
#[derive(Default, Clone, Debug)]
pub struct IdempotencyKey {
    pub name: String,
    pub method: String,
    pub request_id: String,
    pub state: KeyState,
    pub response: String,
    pub create_time: Option<prost_types::Timestamp>,
}
pub struct IdempotencyStore<M: Send + Sync + 'static> {
    pub keys: Arc<dyn Repository<M>>,
    pub options: Options,
    pub spec: AdapterSpec,
}
impl<M: Message + Default + Send + Sync + 'static> IdempotencyStore<M> {
    pub async fn create(&self, ctx: &Context, k: IdempotencyKey) -> Result<(), Status> {
        let s = &self.spec;
        let mut m = s.new_message(&self.options)?;
        m.set_field_by_name("name", Value::String(k.name));
        s.set(&mut m, "method", Value::String(k.method))?;
        s.set(&mut m, "request_id", Value::String(k.request_id))?;
        s.set(&mut m, "state", Value::EnumNumber(s.state("IN_FLIGHT")))?;
        self.keys.create(ctx, "", typed(&m)?).await?;
        Ok(())
    }
    pub async fn get(&self, ctx: &Context, name: &str) -> Result<IdempotencyKey, Status> {
        let value = self.keys.get(ctx, name).await?;
        let s = &self.spec;
        let m = message(&self.options, s.message, &value)?;
        Ok(IdempotencyKey {
            name: text(&m, "name"),
            method: s.string(&m, "method"),
            request_id: s.string(&m, "request_id"),
            state: if s.int(&m, "state") == s.state("DONE") {
                KeyState::Done
            } else {
                KeyState::InFlight
            },
            response: s.string(&m, "response"),
            create_time: s.time(&m, "create_time"),
        })
    }
    pub async fn settle(&self, ctx: &Context, k: IdempotencyKey) -> Result<(), Status> {
        let s = &self.spec;
        let mut m = s.new_message(&self.options)?;
        m.set_field_by_name("name", Value::String(k.name));
        s.set(&mut m, "response", Value::String(k.response))?;
        s.set(
            &mut m,
            "state",
            Value::EnumNumber(s.state(if k.state == KeyState::Done {
                "DONE"
            } else {
                "IN_FLIGHT"
            })),
        )?;
        s.write(
            self.keys.as_ref(),
            ctx,
            m,
            &["state".into(), "response".into()],
        )
        .await?;
        Ok(())
    }
    pub async fn delete(&self, ctx: &Context, name: &str) -> Result<(), Status> {
        self.keys.purge(ctx, name).await
    }
}
