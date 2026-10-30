//! Turns an API's generic errors into ones a developer can act on, after the handler has
//! run.
//!
//! A generated repository reports a missing, duplicate or concurrently changed record with
//! a bare sentinel — "not found", "already exists", "conflict" — because it cannot know
//! what the caller was doing. The request can: it names the resource. So a bare error is
//! rewritten to say which resource, of what kind, and what to do next — the List call that
//! shows what does exist, the Get that reads what is in the way — with the gRPC method and
//! its REST path, both read from the protos' `google.api.resource` and `google.api.http`
//! annotations, so the hint is right on whichever surface the caller used.
//!
//! An `INTERNAL` or `UNKNOWN` error is a fault on the server's side, and its text — a SQL
//! error, a stack of wrapping — helps nobody calling the API and says more about the
//! database than it should. It is replaced by an apology that carries the trace id, which a
//! developer can report and an operator can look up; the original is in that trace and the
//! log.
//!
//! An error a service described itself, with an `ErrorInfo` reason not among
//! [`Options::generic_reasons`], is left as it is.
//!
//! # Differences from Go
//!
//! - Go reads the protos from its global registry. Rust has no registry of the server's
//!   descriptors, so the [`Hinter`] reads the pool the calls are decoded against — the sets
//!   registered with [`crate::HybridServer::with_reflection`] — unless
//!   [`Options::pool`] names one. The package helpers [`type_of`] and [`call`] read
//!   [`DescriptorPool::global`], the nearest thing to Go's registry.
//! - Go takes the trace id from the server's OpenTelemetry span. This crate does not depend
//!   on OpenTelemetry, so the trace id is read from the request's W3C `traceparent` header,
//!   which is how the caller's tracer propagated the same id.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use prost_reflect::{DescriptorPool, DynamicMessage, Kind, MessageDescriptor, ReflectMessage};
use tonic::{Code, Status};
use tonic_types::{ErrorDetail, ResourceInfo, StatusExt};

use crate::annotations;
use crate::intercept::{Interceptor, Next, UnaryCall, UnaryReply};

/// The reasons that mean nobody described the error further: the default reasons a
/// store's bare sentinels map to.
pub const DEFAULT_GENERIC_REASONS: [&str; 3] =
    ["NOT_FOUND", "ALREADY_EXISTS", "CONCURRENT_MODIFICATION"];

/// Configures a [`Hinter`]. The default of each field is Go's.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Names the service in the apology for an internal error: `"<product> failed
    /// unexpectedly"`. Empty means `"The service"`.
    pub product: String,
    /// The `ErrorInfo` reasons a rewrite may replace; an error with no `ErrorInfo`, or an
    /// empty reason, is always generic. `None` means [`DEFAULT_GENERIC_REASONS`].
    pub generic_reasons: Option<Vec<String>>,
    /// The protos read for resources and methods. `None` means the pool of the first call
    /// the hinter sees, or [`DescriptorPool::global`] when asked before any.
    pub pool: Option<DescriptorPool>,
}

/// Rewrites generic errors. It reads the protos once, on first use.
#[derive(Debug)]
pub struct Hinter {
    product: String,
    generic: Vec<String>,
    pool: Option<DescriptorPool>,
    loaded: OnceLock<Loaded>,
}

/// What the protos say: every resource, and every method's REST binding.
#[derive(Debug, Default)]
struct Loaded {
    kinds: Vec<ResourceKind>,
    calls: HashMap<String, Call>,
}

/// What the protos say about one resource.
#[derive(Debug, Clone, Default)]
struct ResourceKind {
    /// `shop.example.com/Shelf`.
    kind: String,
    /// `shelf`.
    singular: String,
    /// The first pattern, split on `/`.
    pattern: Vec<String>,
    /// `List<Plural>`.
    list: Option<Call>,
    /// `Get<Singular>`.
    get: Option<Call>,
}

/// An RPC and its REST binding.
#[derive(Debug, Clone, Default)]
struct Call {
    /// `ListShelves`.
    method: String,
    /// `GET`, or empty when the method has no binding.
    verb: String,
    /// `/v1/{parent=shelves/*}/books`.
    path: String,
}

impl Hinter {
    /// A hinter with `opts`.
    #[must_use]
    pub fn new(opts: Options) -> Self {
        Self {
            product: if opts.product.is_empty() {
                "The service".to_string()
            } else {
                opts.product
            },
            generic: opts.generic_reasons.unwrap_or_else(|| {
                DEFAULT_GENERIC_REASONS
                    .iter()
                    .map(ToString::to_string)
                    .collect()
            }),
            pool: opts.pool,
            loaded: OnceLock::new(),
        }
    }

    /// The message-level interceptor that rewrites the errors handlers return. Go's
    /// `Hinter.Interceptor`.
    #[must_use]
    pub fn interceptor(self) -> HintsInterceptor {
        HintsInterceptor(Arc::new(self))
    }

    /// `status` with a hint when it is one of the generic errors and the request says
    /// enough to give one, and `status` unchanged otherwise.
    ///
    /// `full_method` is `/package.Service/Method`; `request` is the decoded request, when
    /// there is one; `trace_id` is what the apology for an internal error names.
    #[must_use]
    pub fn rewrite(
        &self,
        full_method: &str,
        request: Option<&DynamicMessage>,
        trace_id: Option<&str>,
        status: Status,
    ) -> Status {
        if matches!(status.code(), Code::Internal | Code::Unknown) {
            return self.internal(full_method, trace_id);
        }
        let Some(request) = request else {
            return status;
        };
        if !self.generic(&status) {
            return status;
        }
        let loaded = self.load(Some(request.descriptor().parent_pool()));
        let method = method_name(full_method);
        match status.code() {
            // Only a bare "not found": a message that names something was written by
            // someone who knew what was missing, which need not be the resource the
            // request names.
            Code::NotFound if bare(status.message()) => match named(request) {
                Some(name) => not_found(loaded, &status, &name),
                None => status,
            },
            Code::AlreadyExists => {
                if method.starts_with("Undelete")
                    && let Some(name) = named(request)
                {
                    return not_deleted(loaded, &status, &name);
                }
                match created(request) {
                    Some(name) => already_exists(loaded, &status, &name),
                    None => status,
                }
            }
            Code::Aborted => match updated(request) {
                Some(name) => stale(loaded, &status, &name),
                None => status,
            },
            _ => status,
        }
    }

    /// The resource type a name has, as its proto declares it (`shop.example.com/Shelf`),
    /// or `None` when none matches.
    #[must_use]
    pub fn type_of(&self, name: &str) -> Option<String> {
        let kind = lookup(self.load(None), name);
        (!kind.kind.is_empty()).then_some(kind.kind)
    }

    /// Renders an RPC: its name and REST binding, with the path's variables filled from
    /// `vars` — `call("UndeleteShelf", {"name": "shelves/s"})` is `UndeleteShelf (POST
    /// /v1/shelves/s:undelete)`. An unknown method renders as its name alone, so a hint
    /// never breaks over a rename.
    #[must_use]
    pub fn call(&self, method: &str, vars: &HashMap<&str, &str>) -> String {
        match self.load(None).calls.get(method) {
            Some(call) => call.render(vars),
            None => method.to_string(),
        }
    }

    /// Whether the status's `ErrorInfo`, if any, carries a generic reason.
    fn generic(&self, status: &Status) -> bool {
        status
            .get_details_error_info()
            .is_none_or(|info| info.reason.is_empty() || self.generic.contains(&info.reason))
    }

    fn internal(&self, full_method: &str, trace_id: Option<&str>) -> Status {
        let mut msg = format!("{} failed unexpectedly", self.product);
        let method = method_name(full_method);
        if !method.is_empty() {
            msg.push_str(" while handling ");
            msg.push_str(method);
        }
        msg.push_str(
            ". This is a fault on our side, not in your request: retry it, and if it keeps \
             failing, report",
        );
        match trace_id {
            Some(id) => {
                msg.push_str(" trace ");
                msg.push_str(id);
            }
            None => msg.push_str(" the time and the call"),
        }
        msg.push('.');
        Status::internal(msg)
    }

    /// Reads every resource and its List and Get methods, once.
    fn load(&self, hint: Option<&DescriptorPool>) -> &Loaded {
        self.loaded.get_or_init(|| {
            let pool = self
                .pool
                .clone()
                .or_else(|| hint.cloned())
                .unwrap_or_else(DescriptorPool::global);
            let loaded = Loaded::read(&pool);
            tracing::debug!(
                resources = loaded.kinds.len(),
                methods = loaded.calls.len(),
                "hints: read the protos"
            );
            loaded
        })
    }
}

impl Loaded {
    fn read(pool: &DescriptorPool) -> Self {
        let mut calls = HashMap::new();
        for service in pool.services() {
            for method in service.methods() {
                let (verb, path) = annotations::http_rule(&method)
                    .map(|(verb, path)| (verb.to_string(), path))
                    .unwrap_or_default();
                calls.insert(
                    method.name().to_string(),
                    Call {
                        method: method.name().to_string(),
                        verb,
                        path,
                    },
                );
            }
        }
        let kinds = pool
            .all_messages()
            .filter_map(|message| annotations::resource(&message))
            .map(|res| ResourceKind {
                list: (!res.plural.is_empty())
                    .then(|| {
                        calls
                            .get(&format!("List{}", upper_first(&res.plural)))
                            .cloned()
                    })
                    .flatten(),
                get: (!res.singular.is_empty())
                    .then(|| {
                        calls
                            .get(&format!("Get{}", upper_first(&res.singular)))
                            .cloned()
                    })
                    .flatten(),
                pattern: res.patterns[0].split('/').map(str::to_string).collect(),
                kind: res.kind,
                singular: res.singular,
            })
            .collect();
        Self { kinds, calls }
    }
}

impl ResourceKind {
    /// The kind in words: `Organizational unit`; `resource` for no known kind.
    fn label(&self) -> String {
        if self.singular.is_empty() {
            return "resource".to_string();
        }
        let mut words = String::new();
        for (i, c) in self.singular.chars().enumerate() {
            if c.is_uppercase() && i > 0 {
                words.push(' ');
            }
            words.extend(c.to_lowercase());
        }
        upper_first(&words)
    }

    fn list_call(&self, parent: &str) -> Option<String> {
        self.list
            .as_ref()
            .map(|call| call.render(&HashMap::from([("parent", parent)])))
    }

    fn get_call(&self, name: &str) -> Option<String> {
        self.get
            .as_ref()
            .map(|call| call.render(&HashMap::from([("name", name)])))
    }
}

impl Call {
    /// `ListBooks (GET /v1/shelves/x/books)`, filling the path's variables from `vars`.
    fn render(&self, vars: &HashMap<&str, &str>) -> String {
        if self.path.is_empty() {
            return self.method.clone();
        }
        format!(
            "{} ({} {})",
            self.method,
            self.verb,
            fill_path(&self.path, vars)
        )
    }
}

/// Replaces each `{var}` or `{var=pattern}` in a path template — Go's
/// `\{([a-z_.]+)(=[^}]*)?\}` — with its value in `vars`, leaving the ones with no value as
/// written.
fn fill_path(path: &str, vars: &HashMap<&str, &str>) -> String {
    let mut out = String::with_capacity(path.len());
    let mut rest = path;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open..];
        let Some(close) = after.find('}') else {
            break;
        };
        let inner = &after[1..close];
        let var = inner.split_once('=').map_or(inner, |(var, _)| var);
        let valid = !var.is_empty()
            && var
                .chars()
                .all(|c| c.is_ascii_lowercase() || c == '_' || c == '.');
        match vars.get(var).filter(|v| valid && !v.is_empty()) {
            Some(value) => out.push_str(value),
            None => out.push_str(&after[..=close]),
        }
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    out
}

/// The kind whose pattern a name has, or the empty kind.
fn lookup(loaded: &Loaded, name: &str) -> ResourceKind {
    let segments: Vec<&str> = name.split('/').collect();
    loaded
        .kinds
        .iter()
        .find(|kind| {
            kind.pattern.len() == segments.len()
                && kind
                    .pattern
                    .iter()
                    .zip(&segments)
                    .all(|(p, s)| p.starts_with('{') || p == s)
        })
        .cloned()
        .unwrap_or_default()
}

/// Whether a message is a bare sentinel with nothing said about what.
fn bare(message: &str) -> bool {
    let lower = message.trim().to_lowercase();
    matches!(
        lower.strip_suffix('.').unwrap_or(&lower),
        "not found" | "already exists" | "conflict"
    )
}

fn not_deleted(loaded: &Loaded, status: &Status, name: &str) -> Status {
    let kind = lookup(loaded, name);
    let mut msg = format!(
        "{} {name:?} is not deleted, so there is nothing to restore",
        kind.label()
    );
    if let Some(get) = kind.get_call(name) {
        msg.push_str(&format!("; {get} reads it as it is"));
    }
    with(status, msg + ".", &kind.kind, name, "not deleted")
}

fn not_found(loaded: &Loaded, status: &Status, name: &str) -> Status {
    // "shelves/x:archive" sent as GET reaches the Get of shelves/x, verb and all: the
    // caller meant the custom method, which is a POST.
    if let Some((base, verb)) = name.split_once(':')
        && !verb.is_empty()
        && !verb.contains('/')
    {
        let kind = lookup(loaded, base);
        return with(
            status,
            format!(
                "{name:?} is not a {} name: :{verb} is a custom method, and custom methods are \
                 called with POST, not GET.",
                kind.label().to_lowercase()
            ),
            &kind.kind,
            base,
            "custom method called with GET",
        );
    }
    let kind = lookup(loaded, name);
    let mut msg = format!("{} {name:?} does not exist. Check the name", kind.label());
    if let Some(list) = kind.list_call(&parent_of(name)) {
        msg.push_str(&format!(", or list what does exist with {list}"));
    }
    with(status, msg + ".", &kind.kind, name, "does not exist")
}

fn already_exists(loaded: &Loaded, status: &Status, name: &str) -> Status {
    let kind = lookup(loaded, name);
    let mut msg = format!(
        "{} {name:?} already exists. Choose another id, or leave the id empty to have one \
         assigned",
        kind.label()
    );
    if let Some(get) = kind.get_call(name) {
        msg.push_str(&format!("; {get} reads the existing one"));
    }
    with(status, msg + ".", &kind.kind, name, "already exists")
}

fn stale(loaded: &Loaded, status: &Status, name: &str) -> Status {
    let kind = lookup(loaded, name);
    let mut msg = format!(
        "{} {name:?} changed after you read it: the etag you sent is no longer current",
        kind.label()
    );
    match kind.get_call(name) {
        Some(get) => msg.push_str(&format!(
            ". Read it again with {get}, reapply your change and retry with the new etag"
        )),
        None => msg.push_str(". Read it again, reapply your change and retry with the new etag"),
    }
    with(status, msg + ".", &kind.kind, name, "etag is stale")
}

/// Replaces the status's message, keeping its code and details and adding a
/// `ResourceInfo`.
fn with(status: &Status, message: String, kind: &str, name: &str, description: &str) -> Status {
    let mut proto = crate::status::to_proto(status);
    proto.message = message;
    crate::status::with_details(
        &crate::status::from_proto(&proto),
        vec![ErrorDetail::ResourceInfo(ResourceInfo::new(
            kind,
            name,
            "",
            description,
        ))],
    )
}

/// The resource a request is about: its name, or the name of the resource it carries (an
/// update sends the resource itself).
fn named(request: &DynamicMessage) -> Option<String> {
    string_field(request, "name").or_else(|| updated(request))
}

/// The name of the resource message a request carries.
fn updated(request: &DynamicMessage) -> Option<String> {
    if let Some(name) = string_field(request, "name") {
        return Some(name);
    }
    request.fields().find_map(|(fd, value)| {
        let Kind::Message(message) = fd.kind() else {
            return None;
        };
        if fd.is_list() || fd.is_map() || !is_resource(&message) {
            return None;
        }
        value.as_message().and_then(|sub| string_field(sub, "name"))
    })
}

/// The name a create asked for: the resource's own name if it set one, else parent plus
/// the caller-chosen id (AIP-133's `<resource>_id`).
fn created(request: &DynamicMessage) -> Option<String> {
    if let Some(name) = updated(request) {
        return Some(name);
    }
    let parent = string_field(request, "parent");
    for fd in request.descriptor().fields() {
        let Kind::Message(message) = fd.kind() else {
            continue;
        };
        let Some(res) = annotations::resource(&message) else {
            continue;
        };
        let id = string_field(request, &format!("{}_id", fd.name()))?;
        let segments: Vec<&str> = res.patterns[0].split('/').collect();
        if segments.len() < 2 {
            return None;
        }
        let name = format!("{}/{id}", segments[segments.len() - 2]);
        return Some(match &parent {
            Some(parent) => format!("{parent}/{name}"),
            None => name,
        });
    }
    None
}

/// A non-empty singular string field.
fn string_field(message: &DynamicMessage, name: &str) -> Option<String> {
    let fd = message.descriptor().get_field_by_name(name)?;
    if fd.kind() != Kind::String || fd.is_list() {
        return None;
    }
    message
        .get_field(&fd)
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn is_resource(message: &MessageDescriptor) -> bool {
    annotations::resource(message).is_some()
}

/// The parent of a resource name, empty for a top-level one.
fn parent_of(name: &str) -> String {
    let segments: Vec<&str> = name.split('/').collect();
    if segments.len() < 4 {
        return String::new();
    }
    segments[..segments.len() - 2].join("/")
}

fn method_name(full_method: &str) -> &str {
    full_method
        .rsplit_once('/')
        .map_or("", |(_, method)| method)
}

fn upper_first(s: &str) -> String {
    let mut chars = s.chars();
    chars
        .next()
        .map(|c| c.to_uppercase().chain(chars).collect())
        .unwrap_or_default()
}

/// The trace id of a W3C `traceparent` header — `00-<trace id>-<span id>-<flags>` — or
/// `None` when it is absent, malformed, or the all-zero invalid id.
fn trace_id(metadata: &tonic::metadata::MetadataMap) -> Option<String> {
    let header = metadata.get("traceparent")?.to_str().ok()?;
    let id = header.split('-').nth(1)?;
    let valid =
        id.len() == 32 && id.chars().all(|c| c.is_ascii_hexdigit()) && id.chars().any(|c| c != '0');
    valid.then(|| id.to_ascii_lowercase())
}

/// A shared hinter over [`DescriptorPool::global`], for the package helpers.
fn global() -> &'static Hinter {
    static GLOBAL: OnceLock<Hinter> = OnceLock::new();
    GLOBAL.get_or_init(|| {
        Hinter::new(Options {
            pool: Some(DescriptorPool::global()),
            ..Options::default()
        })
    })
}

/// [`Hinter::type_of`] over [`DescriptorPool::global`].
#[must_use]
pub fn type_of(name: &str) -> Option<String> {
    global().type_of(name)
}

/// [`Hinter::call`] over [`DescriptorPool::global`].
#[must_use]
pub fn call(method: &str, vars: &HashMap<&str, &str>) -> String {
    global().call(method, vars)
}

/// The interceptor [`Hinter::interceptor`] builds.
///
/// Register it with [`crate::HybridServer::with_interceptor`]. When
/// [`crate::problem::ProblemInterceptor`] runs too, register this one first — outer to it —
/// so it rewrites the statuses problem translation produced; inner, it would turn an
/// untranslated sentinel (`UNKNOWN`) into the internal-error apology before the translator
/// saw it.
#[derive(Debug, Clone)]
pub struct HintsInterceptor(Arc<Hinter>);

#[async_trait::async_trait]
impl Interceptor for HintsInterceptor {
    async fn intercept(&self, call: UnaryCall, next: Next<'_>) -> Result<UnaryReply, Status> {
        // Kept for the error path only: the bytes are reference-counted, so a successful
        // call never pays for a decode.
        let payload = call.payload().clone();
        let input = call
            .descriptor()
            .map(prost_reflect::MethodDescriptor::input);
        let full_method = call.full_method().to_string();
        let trace = trace_id(&call.metadata);

        next.run(call).await.map_err(|status| {
            let request = input.and_then(|input| DynamicMessage::decode(input, payload).ok());
            self.0
                .rewrite(&full_method, request.as_ref(), trace.as_deref(), status)
        })
    }
}

#[cfg(test)]
mod tests;
