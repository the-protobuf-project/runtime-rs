//! Shared service behavior: the checks and hooks of the generated standard
//! methods around the handlers an application registers, one per RPC, and the
//! store adapters (feeds, events, idempotency) over its repositories.
//! Handlers and repositories that write must enforce the etag in the write.
//!
//! protoc-gen-service's Rust target generated this into every project as
//! `servicex`; the generated services now use it from here, so a fix reaches
//! every project with runtime-rs rather than with a regeneration. It needs
//! nothing of the hybrid server: a service depends on this crate with
//! `default-features = false` for it alone, and adds `servicex-store` for the
//! standard methods over runtime-rs repositories (`store=true`).
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, ReflectMessage, Value};
use std::sync::Arc;
use tonic::{Code, Status};
use tonic_types::{BadRequest, ErrorDetail, ErrorInfo, FieldViolation, StatusExt};

/// Response stream returned by generated server-streaming and bidirectional RPCs.
pub type ResponseStream<T> =
    std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<T, Status>> + Send + 'static>>;

mod adapters;
mod feeds;
#[cfg(feature = "servicex-store")]
mod store;

pub use adapters::*;
pub use feeds::*;
#[cfg(feature = "servicex-store")]
pub use store::*;

pub fn reason(code: Code, domain: &str, reason: &str, message: String) -> Status {
    Status::with_error_details_vec(
        code,
        message,
        vec![ErrorDetail::ErrorInfo(ErrorInfo::new(
            reason,
            domain,
            std::collections::HashMap::new(),
        ))],
    )
}
#[derive(Clone)]
pub struct Options {
    pub descriptors: DescriptorPool,
    pub domain: String,
}
impl Options {
    pub fn new(descriptors: DescriptorPool) -> Self {
        Self {
            descriptors,
            domain: String::new(),
        }
    }
    pub fn with_domain(&self, default: &str) -> Self {
        let mut o = self.clone();
        if o.domain.is_empty() {
            o.domain = default.into();
        }
        o
    }
    pub fn error(&self, code: Code, why: &str, message: impl Into<String>) -> Status {
        reason(code, &self.domain, why, message.into())
    }
    pub fn bad_field(&self, why: &str, field: &str, message: &str) -> Status {
        Status::with_error_details_vec(
            Code::InvalidArgument,
            message,
            vec![
                ErrorDetail::ErrorInfo(ErrorInfo::new(
                    why,
                    &self.domain,
                    std::collections::HashMap::new(),
                )),
                ErrorDetail::BadRequest(BadRequest::new(vec![FieldViolation::new(field, message)])),
            ],
        )
    }
    fn dynamic<M: Message>(&self, spec: &ResourceSpec, m: &M) -> Result<DynamicMessage, Status> {
        let desc = self
            .descriptors
            .get_message_by_name(spec.message)
            .ok_or_else(|| Status::internal(format!("missing descriptor {}", spec.message)))?;
        DynamicMessage::decode(desc, m.encode_to_vec().as_slice())
            .map_err(|e| Status::internal(e.to_string()))
    }
}
#[derive(Clone, Default)]
pub struct Context {
    pub metadata: tonic::metadata::MetadataMap,
    pub extensions: tonic::Extensions,
}
impl Context {
    pub fn from_request<T>(req: &tonic::Request<T>) -> Self {
        Self {
            metadata: req.metadata().clone(),
            extensions: req.extensions().clone(),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Create,
    Update,
    List,
}
#[derive(Default, Clone, Debug)]
pub struct ListInput {
    pub page_size: i32,
    pub page_token: String,
    pub filter: String,
    pub order_by: String,
    pub show_deleted: bool,
    pub read_mask: Vec<String>,
}
pub struct ResourceSpec {
    pub message: &'static str,
    pub resource_type: &'static str,
    pub collections: &'static [&'static str],
    pub required: &'static [&'static str],
    pub output_only: &'static [&'static str],
    pub immutable: &'static [&'static str],
    pub soft_delete: bool,
}
/// Implement this over your Rust persistence layer. Return tonic NotFound,
/// AlreadyExists, or Aborted for missing records, duplicate names, and etag conflicts.
/// A blank create name asks the repository to mint an id. Mutation checks and writes
/// must share a transaction; never implement an etag check as a separate read.
#[tonic::async_trait]
pub trait Repository<M>: Send + Sync
where
    M: Send + Sync + 'static,
{
    async fn get(&self, ctx: &Context, name: &str) -> Result<M, Status>;
    async fn list(
        &self,
        ctx: &Context,
        parent: &str,
        input: ListInput,
    ) -> Result<(Vec<M>, String), Status>;
    async fn create(&self, ctx: &Context, parent: &str, value: M) -> Result<M, Status>;
    async fn update(&self, ctx: &Context, value: M, paths: Vec<String>) -> Result<M, Status>;
    /// Write mutable and server-owned paths atomically, preserving all other fields.
    async fn update_with_output_only(
        &self,
        ctx: &Context,
        value: M,
        paths: Vec<String>,
        output_paths: Vec<String>,
    ) -> Result<M, Status> {
        if output_paths.is_empty() && !paths.is_empty() {
            self.update(ctx, value, paths).await
        } else {
            Err(Status::unimplemented(
                "repository must implement server-owned field updates",
            ))
        }
    }
    async fn delete(&self, ctx: &Context, name: &str, etag: &str) -> Result<Option<M>, Status>;
    async fn undelete(&self, ctx: &Context, name: &str, etag: &str) -> Result<M, Status>;
    async fn purge(&self, ctx: &Context, name: &str) -> Result<(), Status>;
}
#[tonic::async_trait]
pub trait Hooks<M>: Send + Sync
where
    M: Send + Sync + 'static,
{
    async fn parent_check(&self, _ctx: &Context, _op: Op, _parent: &str) -> Result<(), Status> {
        Ok(())
    }
    async fn validate(
        &self,
        _ctx: &Context,
        _op: Op,
        _value: &M,
        _paths: &[String],
    ) -> Result<(), Status> {
        Ok(())
    }
    async fn before_create(
        &self,
        _ctx: &Context,
        _parent: &str,
        _value: &mut M,
    ) -> Result<(), Status> {
        Ok(())
    }
    async fn after_create(&self, _ctx: &Context, _value: &M) -> Result<(), Status> {
        Ok(())
    }
    async fn before_update(
        &self,
        _ctx: &Context,
        _value: &mut M,
        _paths: &[String],
    ) -> Result<(), Status> {
        Ok(())
    }
    async fn after_update(&self, _ctx: &Context, _value: &M) -> Result<(), Status> {
        Ok(())
    }
    async fn before_delete(&self, _ctx: &Context, _name: &str, _force: bool) -> Result<(), Status> {
        Ok(())
    }
    async fn after_delete(&self, _ctx: &Context, _name: &str) -> Result<(), Status> {
        Ok(())
    }
    async fn before_undelete(&self, _ctx: &Context, _name: &str) -> Result<(), Status> {
        Ok(())
    }
}
pub struct NopHooks;
#[tonic::async_trait]
impl<M: Send + Sync + 'static> Hooks<M> for NopHooks {}
/// A boxed future, the shape every handler returns.
pub type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'static>>;

/// A registered hook for one RPC: the request message in, the response
/// message out. It is where the application's work happens -- a Diesel
/// query, a call to another service, any business logic. The generated server
/// runs its checks and hooks around it; a handler never sees a request those
/// checks refused.
pub struct Handler<Req, Resp> {
    f: Arc<HandlerFn<Req, Resp>>,
}
/// The function a Handler holds.
pub type HandlerFn<Req, Resp> =
    dyn Fn(Context, Req) -> BoxFuture<Result<Resp, Status>> + Send + Sync;
impl<Req, Resp> Clone for Handler<Req, Resp> {
    fn clone(&self) -> Self {
        Self { f: self.f.clone() }
    }
}
impl<Req: Send + 'static, Resp: Send + 'static> Handler<Req, Resp> {
    /// A handler from an async function or closure.
    pub fn new<F, Fut>(f: F) -> Self
    where
        F: Fn(Context, Req) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Resp, Status>> + Send + 'static,
    {
        Self {
            f: Arc::new(move |ctx, req| Box::pin(f(ctx, req))),
        }
    }
    /// Runs the handler.
    pub async fn call(&self, ctx: Context, req: Req) -> Result<Resp, Status> {
        (self.f)(ctx, req).await
    }
}
/// The handler of method, or INTERNAL when none was registered: a server is
/// only built with every handler (see MissingHandlers), so this is never
/// reached through one.
pub fn handler<'a, Req, Resp>(
    h: &'a Option<Handler<Req, Resp>>,
    method: &str,
) -> Result<&'a Handler<Req, Resp>, Status> {
    h.as_ref()
        .ok_or_else(|| Status::internal(format!("no handler is registered for {method}")))
}
/// The RPCs a server was built without a handler for: building it fails, so
/// no method is ever served UNIMPLEMENTED by omission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingHandlers {
    pub service: &'static str,
    pub methods: Vec<&'static str>,
}
impl std::fmt::Display for MissingHandlers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} has no handler for {}",
            self.service,
            self.methods.join(", ")
        )
    }
}
impl std::error::Error for MissingHandlers {}

fn typed<M: Message + Default>(m: &DynamicMessage) -> Result<M, Status> {
    m.transcode_to()
        .map_err(|e| Status::internal(e.to_string()))
}
fn text(m: &DynamicMessage, field: &str) -> String {
    m.get_field_by_name(field)
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}
fn has(m: &DynamicMessage, field: &str) -> bool {
    m.has_field_by_name(field)
}
pub fn touches(paths: &[String], field: &str) -> bool {
    paths.is_empty()
        || paths.iter().any(|p| {
            p == field || p.starts_with(&format!("{field}.")) || field.starts_with(&format!("{p}."))
        })
}
fn check_name(
    o: &Options,
    field: &str,
    name: &str,
    wild: bool,
    collections: &[&str],
) -> Result<(), Status> {
    if name.is_empty() {
        return Err(o.bad_field("REQUIRED_FIELD", field, "resource name is required"));
    }
    let parts: Vec<_> = name.split('/').collect();
    if parts.len() != collections.len() * 2
        || collections.iter().enumerate().any(|(i, c)| {
            parts[i * 2] != *c || parts[i * 2 + 1].is_empty() || (!wild && parts[i * 2 + 1] == "-")
        })
    {
        return Err(o.bad_field(
            "MALFORMED_NAME",
            field,
            "resource name has the wrong collection or parent",
        ));
    }
    Ok(())
}
fn check_id(o: &Options, field: &str, id: &str) -> Result<(), Status> {
    if id.is_empty() {
        return Ok(());
    }
    let b = id.as_bytes();
    if b.len() > 63
        || !b[0].is_ascii_lowercase()
        || !b[b.len() - 1].is_ascii_alphanumeric()
        || b.iter()
            .any(|c| !c.is_ascii_lowercase() && !c.is_ascii_digit() && *c != b'-')
    {
        return Err(o.bad_field("INVALID_ID",field,"id must start with a lowercase letter and contain only lowercase letters, digits and hyphens (at most 63 characters)"));
    }
    Ok(())
}
fn check_etag(o: &Options, want: &str, have: &str) -> Result<(), Status> {
    if !want.is_empty() && want != have {
        Err(o.error(
            Code::Aborted,
            "CONCURRENT_MODIFICATION",
            "etag is no longer current",
        ))
    } else {
        Ok(())
    }
}
fn id_of(name: &str) -> &str {
    name.rsplit('/').next().unwrap_or("")
}
fn parent_of(name: &str) -> &str {
    name.rsplit_once('/')
        .and_then(|(p, _)| p.rsplit_once('/'))
        .map(|(p, _)| p)
        .unwrap_or("")
}
fn required(o: &Options, s: &ResourceSpec, m: &DynamicMessage) -> Result<(), Status> {
    let absent: Vec<_> = s.required.iter().filter(|f| !has(m, f)).copied().collect();
    if absent.is_empty() {
        Ok(())
    } else {
        Err(o.bad_field(
            "REQUIRED_FIELD",
            absent[0],
            &format!("required fields: {}", absent.join(", ")),
        ))
    }
}
fn paths(
    o: &Options,
    s: &ResourceSpec,
    m: &DynamicMessage,
    mask: Option<Vec<String>>,
) -> Result<Vec<String>, Status> {
    let Some(mask) = mask else {
        let mut p: Vec<_> = m
            .fields()
            .map(|(f, _)| f.name().to_owned())
            .filter(|f| f != "name" && !s.output_only.contains(&f.as_str()))
            .collect();
        p.sort();
        if p.is_empty() {
            return Err(o.bad_field(
                "NOTHING_TO_UPDATE",
                "update_mask",
                "no mutable field was populated",
            ));
        }
        return Ok(p);
    };
    if mask == ["*"] {
        return Ok(m
            .descriptor()
            .fields()
            .map(|f| f.name().to_owned())
            .filter(|f| f != "name" && !s.output_only.contains(&f.as_str()))
            .collect());
    }
    let mut out = Vec::new();
    for p in mask {
        let mut desc = m.descriptor();
        let mut canonical = Vec::new();
        let parts: Vec<_> = p.split('.').collect();
        for (i, part) in parts.iter().enumerate() {
            let f = desc
                .get_field_by_name(part)
                .or_else(|| desc.get_field_by_json_name(part))
                .ok_or_else(|| {
                    o.bad_field(
                        "UNKNOWN_MASK_PATH",
                        "update_mask",
                        &format!("unknown path {p}"),
                    )
                })?;
            canonical.push(f.name().to_owned());
            if i + 1 < parts.len() {
                match f.kind() {
                    prost_reflect::Kind::Message(next) if !f.is_list() && !f.is_map() => {
                        desc = next
                    }
                    _ => {
                        return Err(o.bad_field(
                            "UNKNOWN_MASK_PATH",
                            "update_mask",
                            &format!("invalid path {p}"),
                        ));
                    }
                }
            }
        }
        if canonical[0] == "name" || s.output_only.contains(&canonical[0].as_str()) {
            continue;
        }
        out.push(canonical.join("."));
    }
    out.sort();
    out.dedup();
    if out.is_empty() {
        return Err(o.bad_field(
            "NOTHING_TO_UPDATE",
            "update_mask",
            "no mutable fields selected",
        ));
    }
    Ok(out)
}
fn merge(dst: &mut DynamicMessage, src: &DynamicMessage, paths: &[String]) {
    for path in paths {
        let (head, tail) = path
            .split_once('.')
            .map(|(h, t)| (h, Some(t)))
            .unwrap_or((path, None));
        let Some(fd) = src.descriptor().get_field_by_name(head) else {
            continue;
        };
        if let Some(tail) = tail {
            let sv = src.get_field(&fd).into_owned();
            let dv = dst.get_field(&fd).into_owned();
            if let (Value::Message(mut d), Value::Message(s)) = (dv, sv) {
                merge(&mut d, &s, &[tail.to_owned()]);
                dst.set_field(&fd, Value::Message(d));
            }
        } else if src.has_field(&fd) {
            dst.set_field(&fd, src.get_field(&fd).into_owned());
        } else {
            dst.clear_field(&fd);
        }
    }
}
fn mapped(o: &Options, e: Status) -> Status {
    if !e.details().is_empty() {
        return e;
    }
    let why = match e.code() {
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::Aborted => "CONCURRENT_MODIFICATION",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::FailedPrecondition => "INVALID_STATE",
        _ => return e,
    };
    o.error(e.code(), why, e.message())
}
/// What an Update hands its handler: a create, when allow_missing found
/// nothing, or the record to write and the paths it writes.
pub enum Update<M> {
    /// allow_missing on a missing record: the prepared resource to create.
    Create(M),
    /// The record to write -- the stored one with the request's paths merged
    /// in, when the service has a Get to read it with -- carrying the
    /// request's etag for the handler to guard the write; the canonical
    /// paths; and the preview a validate_only update returns, which keeps the
    /// stored etag.
    Write {
        value: M,
        paths: Vec<String>,
        preview: M,
    },
}
/// What a Delete does next.
pub enum Delete<M> {
    /// allow_missing on a missing record: nothing to delete.
    Skip,
    /// Call the handler; the stored record, when the service could read it,
    /// is what a validate_only delete returns.
    Proceed(Option<M>),
}
/// The checks and hooks of one standard method around its handler, over one
/// resource. Where a check needs the stored record (Update, Delete,
/// Undelete), the generated server reads it with the service's own Get
/// handler and passes it in; without a Get it is None, and those checks are
/// the handler's.
pub struct Call<'a, M: Send + Sync + 'static> {
    hooks: &'a dyn Hooks<M>,
    options: &'a Options,
    spec: &'a ResourceSpec,
    context: &'a Context,
}

impl<'a, M: Message + Default + Send + Sync + 'static> Call<'a, M> {
    pub fn new(
        hooks: &'a dyn Hooks<M>,
        options: &'a Options,
        spec: &'a ResourceSpec,
        context: &'a Context,
    ) -> Self {
        Self {
            hooks,
            options,
            spec,
            context,
        }
    }
    /// A handler's error with its standard reason attached.
    pub fn mapped(&self, e: Status) -> Status {
        mapped(self.options, e)
    }
    /// Get and Purge: the name's shape.
    pub fn name(&self, name: &str) -> Result<(), Status> {
        check_name(self.options, "name", name, false, self.spec.collections)
    }
    /// List: the parent's shape (AIP-159 wildcards allowed), its ParentCheck
    /// hook, and the page size.
    pub async fn list(&self, parent: &str, page_size: i32) -> Result<(), Status> {
        let (o, s) = (self.options, self.spec);
        if s.collections.len() > 1 {
            check_name(
                o,
                "parent",
                parent,
                true,
                &s.collections[..s.collections.len() - 1],
            )?;
            self.hooks
                .parent_check(self.context, Op::List, parent)
                .await?;
        }
        if page_size < 0 {
            return Err(o.bad_field(
                "INVALID_ARGUMENT",
                "page_size",
                "page_size must not be negative",
            ));
        }
        Ok(())
    }
    /// Create: the id (AIP-122) and parent, then the resource prepared for
    /// its handler -- named, its OUTPUT_ONLY fields cleared, its REQUIRED
    /// ones present -- through ParentCheck, Validate and BeforeCreate.
    pub async fn create(&self, parent: &str, id: &str, value: M) -> Result<M, Status> {
        let (o, s) = (self.options, self.spec);
        check_id(o, "resource_id", id)?;
        if s.collections.len() > 1 {
            check_name(
                o,
                "parent",
                parent,
                false,
                &s.collections[..s.collections.len() - 1],
            )?;
        }
        let name = if id.is_empty() {
            String::new()
        } else {
            format!(
                "{}{}/{}",
                if parent.is_empty() {
                    String::new()
                } else {
                    format!("{parent}/")
                },
                s.collections.last().unwrap(),
                id
            )
        };
        self.create_named(parent, &name, value).await
    }
    async fn create_named(&self, parent: &str, name: &str, value: M) -> Result<M, Status> {
        let (o, s, c) = (self.options, self.spec, self.context);
        let mut d = o.dynamic(s, &value)?;
        for f in s.output_only {
            d.clear_field_by_name(f);
        }
        d.set_field_by_name("name", Value::String(name.into()));
        required(o, s, &d)?;
        let mut value = typed(&d)?;
        if s.collections.len() > 1 {
            self.hooks.parent_check(c, Op::Create, parent).await?;
        }
        self.hooks.validate(c, Op::Create, &value, &[]).await?;
        self.hooks.before_create(c, parent, &mut value).await?;
        Ok(value)
    }
    /// AfterCreate, over what the handler wrote.
    pub async fn after_create(&self, out: &M) -> Result<(), Status> {
        self.hooks.after_create(self.context, out).await
    }
    /// Update: the name and update mask, then, with the stored record, its
    /// etag, the merge, IMMUTABLE and REQUIRED fields; Validate and
    /// BeforeUpdate over what is to be written. Without the stored record
    /// (no Get handler), REQUIRED is checked on the fields the mask writes.
    pub async fn update(
        &self,
        value: M,
        mask: Option<Vec<String>>,
        allow_missing: bool,
        existing: Option<Result<M, Status>>,
    ) -> Result<Update<M>, Status> {
        let (o, s, c) = (self.options, self.spec, self.context);
        let incoming = o.dynamic(s, &value)?;
        let name = text(&incoming, "name");
        check_name(o, "name", &name, false, s.collections)?;
        let paths = paths(o, s, &incoming, mask)?;
        let existing = match existing {
            None => {
                let selected: Vec<&str> = s
                    .required
                    .iter()
                    .copied()
                    .filter(|f| touches(&paths, f))
                    .collect();
                let absent: Vec<_> = selected.iter().filter(|f| !has(&incoming, f)).collect();
                if let Some(first) = absent.first() {
                    return Err(o.bad_field(
                        "REQUIRED_FIELD",
                        first,
                        "a required field the mask writes is empty",
                    ));
                }
                let mut value = value;
                self.hooks.validate(c, Op::Update, &value, &paths).await?;
                self.hooks.before_update(c, &mut value, &paths).await?;
                let preview = typed(&o.dynamic(s, &value)?)?;
                return Ok(Update::Write {
                    value,
                    paths,
                    preview,
                });
            }
            Some(Ok(v)) => v,
            Some(Err(e)) if e.code() == Code::NotFound && allow_missing => {
                // AIP-134 allow_missing creates under the id the name
                // chooses, held to Create's rule.
                check_id(o, "name", id_of(&name))?;
                return Ok(Update::Create(
                    self.create_named(parent_of(&name), &name, value).await?,
                ));
            }
            Some(Err(e)) => return Err(mapped(o, e)),
        };
        let mut merged = o.dynamic(s, &existing)?;
        check_etag(o, &text(&incoming, "etag"), &text(&merged, "etag"))?;
        merge(&mut merged, &incoming, &paths);
        let old = o.dynamic(s, &existing)?;
        for f in s.immutable {
            if touches(&paths, f) && old.get_field_by_name(f) != merged.get_field_by_name(f) {
                return Err(o.bad_field("IMMUTABLE_FIELD", f, "field cannot change once set"));
            }
        }
        required(o, s, &merged)?;
        let mut preview = typed(&merged)?;
        self.hooks.validate(c, Op::Update, &preview, &paths).await?;
        self.hooks.before_update(c, &mut preview, &paths).await?;
        // Carry the caller's etag: the handler guards its write with it.
        let mut write = o.dynamic(s, &preview)?;
        if write.descriptor().get_field_by_name("etag").is_some() {
            write.set_field_by_name("etag", Value::String(text(&incoming, "etag")));
        }
        Ok(Update::Write {
            value: typed(&write)?,
            paths,
            preview,
        })
    }
    /// AfterUpdate, over what the handler wrote.
    pub async fn after_update(&self, out: &M) -> Result<(), Status> {
        self.hooks.after_update(self.context, out).await
    }
    /// Delete: the name, then, with the stored record, its etag and whether
    /// it is already deleted; BeforeDelete.
    pub async fn delete(
        &self,
        name: &str,
        etag: &str,
        allow_missing: bool,
        force: bool,
        existing: Option<Result<M, Status>>,
    ) -> Result<Delete<M>, Status> {
        let (o, s, c) = (self.options, self.spec, self.context);
        check_name(o, "name", name, false, s.collections)?;
        let existing = match existing {
            None => None,
            Some(Ok(v)) => Some(v),
            Some(Err(e)) if e.code() == Code::NotFound && allow_missing => return Ok(Delete::Skip),
            Some(Err(e)) => return Err(mapped(o, e)),
        };
        if let Some(v) = &existing {
            let d = o.dynamic(s, v)?;
            check_etag(o, etag, &text(&d, "etag"))?;
            if s.soft_delete && has(&d, "delete_time") {
                if allow_missing {
                    return Ok(Delete::Skip);
                }
                return Err(o.error(Code::NotFound, "NOT_FOUND", "resource is already deleted"));
            }
        }
        self.hooks.before_delete(c, name, force).await?;
        Ok(Delete::Proceed(existing))
    }
    /// AfterDelete.
    pub async fn after_delete(&self, name: &str) -> Result<(), Status> {
        self.hooks.after_delete(self.context, name).await
    }
    /// Undelete: the name, then, with the stored record, its etag and that it
    /// is deleted; BeforeUndelete. Returns the restored record a
    /// validate_only undelete previews, when the service could read it.
    pub async fn undelete(
        &self,
        name: &str,
        etag: &str,
        existing: Option<Result<M, Status>>,
    ) -> Result<Option<M>, Status> {
        let (o, s, c) = (self.options, self.spec, self.context);
        check_name(o, "name", name, false, s.collections)?;
        let preview = match existing {
            None => None,
            Some(Err(e)) => return Err(mapped(o, e)),
            Some(Ok(v)) => {
                let mut d = o.dynamic(s, &v)?;
                check_etag(o, etag, &text(&d, "etag"))?;
                if !has(&d, "delete_time") {
                    return Err(o.error(
                        Code::AlreadyExists,
                        "ALREADY_EXISTS",
                        "resource is not deleted",
                    ));
                }
                d.clear_field_by_name("delete_time");
                Some(typed(&d)?)
            }
        };
        self.hooks.before_undelete(c, name).await?;
        Ok(preview)
    }
}

/// Optional structured details attached to a generated error reason.
#[derive(Default)]
pub struct ReasonDetail<'a> {
    pub fix: &'a str,
    pub field: &'a str,
    pub resource_type: &'a str,
    pub name: &'a str,
}

pub fn detailed_reason(
    code: Code,
    domain: &str,
    why: &str,
    what: String,
    detail: ReasonDetail<'_>,
) -> Status {
    let ReasonDetail {
        fix,
        field,
        resource_type,
        name,
    } = detail;
    use tonic_types::{PreconditionFailure, PreconditionViolation, ResourceInfo};
    let message = if fix.is_empty() {
        what
    } else {
        format!("{}. {}", what.trim_end_matches('.'), fix)
    };
    let mut details = vec![ErrorDetail::ErrorInfo(ErrorInfo::new(
        why,
        domain,
        std::collections::HashMap::new(),
    ))];
    if !field.is_empty() {
        details.push(ErrorDetail::BadRequest(BadRequest::new(vec![
            FieldViolation::new(field, &message),
        ])));
    } else if !name.is_empty() && code == Code::FailedPrecondition {
        details.push(ErrorDetail::PreconditionFailure(PreconditionFailure::new(
            vec![PreconditionViolation::new(why, name, &message)],
        )));
    } else if !name.is_empty() {
        details.push(ErrorDetail::ResourceInfo(ResourceInfo::new(
            resource_type,
            name,
            "",
            fix,
        )));
    }
    Status::with_error_details_vec(code, message, details)
}
