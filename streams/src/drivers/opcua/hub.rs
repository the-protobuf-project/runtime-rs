//! One client subscription per provider, and the routing of its notifications
//! to the streams that asked for them.
//!
//! OPC UA servers limit subscriptions per session and charge per subscription
//! for every publishing cycle, so the provider keeps one and adds a monitored
//! item to it per [`Subscriber::subscribe`](crate::Subscriber::subscribe). It
//! is created with the first item and deleted with the last.
//!
//! # Identity across reconnects
//!
//! When the server loses a session, async-opcua recreates its subscriptions
//! and monitored items on the new one, and the server hands out new ids for
//! both. What survives is the client handle, the number the client chose for
//! each item and the one every notification names. So routes are keyed by
//! client handle, and a subscription id is trusted only while the server
//! session that issued it lasts: after that the provider's subscription is
//! the one holding its items, looked up afresh by handle. An old id may by
//! then belong to another subscription on the same session, the caller's or
//! another provider's, which must be neither reused nor deleted.
//!
//! A namespace index is the same kind of number: a restarted server may
//! renumber its namespaces, so an item named by namespace URI is checked
//! against the new session's namespace table, and moved when its index
//! changed.
//!
//! # A nudge after a reconnect
//!
//! async-opcua 0.19 recreates the subscription during the reconnect, and the
//! publish trigger that recreation fires is spent before the new session's
//! publishing loop starts, so that loop waits for a trigger that never comes
//! and the subscription is silent though the server holds it. The hub hears
//! every message the subscription sends, keep-alives included, so a
//! watchdog calls [`Session::trigger_publish_now`] when the subscription has
//! been silent past twice its keep-alive period, which a healthy one never is.

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::{FutureExt, Stream as FuturesStream};
use opcua::{
    client::{MonitoredItemMap, OnSubscriptionNotificationCore, Session},
    types::{
        DataChangeFilter, DataChangeNotification, DataChangeTrigger, DataValue, ExtensionObject,
        MonitoredItemCreateRequest, MonitoringMode, MonitoringParameters, NodeId,
        NotificationMessage, ReadValueId, StatusCode, TimestampsToReturn,
    },
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{Message, Result, StreamError, provider};

use super::{CODEC_NAME, OpcUaConfig, codec};

/// Client handles this crate hands out, from a range far above async-opcua's
/// own allocator (which starts at 1000), so a monitored item of ours is not
/// mistaken for one the caller created on the same session.
static NEXT_HANDLE: AtomicU32 = AtomicU32::new(0x7000_0000);

/// Where one monitored item's notifications go.
struct Route {
    tx: mpsc::Sender<Message>,
    subject: Arc<str>,
    codec: Arc<str>,
    /// The node monitored, and the namespace URI its subject named it by.
    node: NodeId,
    uri: Option<Arc<str>>,
    /// The queue size asked of the server.
    queue_size: u32,
    initial: Initial,
    /// Notifications dropped because the reader fell behind.
    dropped: u64,
}

/// What becomes of an item's first notification.
///
/// The server reports a new item's current value at once. That value was not
/// published after the subscription, so by default it is skipped; but an item
/// whose queue is full before the first publishing cycle has discarded it for
/// a later write, which must be delivered. So the node is read before the
/// item is made, and the first notification is skipped only when it is that
/// value: the same value, status and source timestamp, which OPC UA changes
/// only with the value or status (Part 4, 7.11).
enum Initial {
    Deliver,
    /// Skip the first notification if it is this.
    Skip(DataValue),
    /// Skip the first notification: the node could not be read first.
    SkipAny,
}

type Routes = Arc<Mutex<HashMap<u32, Route>>>;

/// The provider's subscription, and the server session that issued its id.
#[derive(Clone)]
struct Owned {
    id: u32,
    session: NodeId,
}

/// Where the server has one monitored item.
struct Found {
    subscription: u32,
    item: u32,
    node: NodeId,
}

/// The provider's client subscription and its routing table.
pub(crate) struct Hub {
    session: Arc<Session>,
    config: OpcUaConfig,
    routes: Routes,
    /// The subscription when last seen. Held across every create and delete,
    /// so one subscribe and one drop never race over whether the
    /// subscription exists.
    slot: Arc<tokio::sync::Mutex<Option<Owned>>>,
    /// The server session whose namespace table the session's cache holds.
    namespaces: Mutex<Option<NodeId>>,
    /// The server session the items named by namespace URI were last
    /// checked on.
    checked: Mutex<Option<NodeId>>,
    /// When the subscription last sent anything, in milliseconds since
    /// `epoch`.
    heard: Arc<AtomicU64>,
    epoch: Instant,
    watchdog: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Hub {
    /// Stops the watchdog; the subscription itself goes with the session.
    fn drop(&mut self) {
        if let Some(task) = lock(&self.watchdog).take() {
            task.abort();
        }
    }
}

/// What a subscribe asks the server for.
pub(crate) struct Item {
    pub(crate) subject: Arc<str>,
    pub(crate) node: NodeId,
    /// The namespace URI the subject named the node by, if it did.
    pub(crate) uri: Option<Arc<str>>,
    pub(crate) queue_size: u32,
    pub(crate) buffer: usize,
}

impl Hub {
    /// An empty hub: no subscription exists until the first subscribe asks for one.
    pub(crate) fn new(session: Arc<Session>, config: OpcUaConfig) -> Arc<Self> {
        Arc::new(Self {
            session,
            config,
            routes: Arc::default(),
            slot: Arc::new(tokio::sync::Mutex::new(None)),
            namespaces: Mutex::new(None),
            checked: Mutex::new(None),
            heard: Arc::default(),
            epoch: Instant::now(),
            watchdog: Mutex::new(None),
        })
    }

    /// Milliseconds since `epoch`, the clock `heard` is kept in.
    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// The server's index for namespace `uri` now.
    ///
    /// async-opcua reads the server's namespace table once and keeps it
    /// across reconnects, but a restarted server may have renumbered its
    /// namespaces, so the table is read again once per server session.
    pub(crate) async fn namespace(&self, uri: &str) -> std::result::Result<u16, StreamError> {
        let session = self.session.server_session_id();
        if lock(&self.namespaces).as_ref() != Some(&session) {
            self.session.read_namespace_array().await.map_err(|e| {
                StreamError::Internal(format!("opcua: cannot read the namespace table: {e}"))
            })?;
            *lock(&self.namespaces) = Some(session);
        }
        self.session
            .get_namespace_index_from_cache(uri)
            .ok_or_else(|| {
                StreamError::Unsupported(format!("opcua: the server has no namespace {uri:?}"))
            })
    }

    /// How long a healthy subscription goes without sending anything: one
    /// keep-alive period.
    fn keep_alive_period(&self) -> Duration {
        self.config
            .publishing_interval
            .max(Duration::from_millis(1))
            .saturating_mul(self.config.keep_alive_count.max(1))
    }

    /// Starts the watchdog once, on the runtime the first subscribe runs on.
    /// Each period it nudges a silent subscription, and on a new server
    /// session checks the namespaces of items named by URI.
    fn watch(self: &Arc<Self>) {
        let mut slot = lock(&self.watchdog);
        if slot.is_some() {
            return;
        }
        let hub: Weak<Self> = Arc::downgrade(self);
        let period = self.keep_alive_period();
        *slot = Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(period);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(hub) = hub.upgrade() else { return };
                let named_by_uri = {
                    let routes = lock(&hub.routes);
                    if routes.is_empty() {
                        continue;
                    }
                    routes.values().any(|r| r.uri.is_some())
                };
                let silent = hub
                    .now_ms()
                    .saturating_sub(hub.heard.load(Ordering::Relaxed));
                if silent > 2 * period.as_millis() as u64 {
                    tracing::debug!(
                        silent_ms = silent,
                        "opcua: subscription is silent; nudging publish"
                    );
                    hub.session.trigger_publish_now();
                }
                if named_by_uri
                    && lock(&hub.checked).as_ref() != Some(&hub.session.server_session_id())
                {
                    hub.rebind().await;
                }
            }
        }));
    }

    /// Adds a monitored item for `item` and returns the stream of its
    /// notifications, live once this returns.
    pub(crate) async fn attach(self: &Arc<Self>, item: Item) -> Result<Notifications> {
        let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
        let initial = if self.config.initial_value {
            Initial::Deliver
        } else {
            self.before(&item.node).await
        };
        let (tx, rx) = mpsc::channel(item.buffer.max(1));
        let slot = self.slot.clone().lock_owned().await;
        // The server is asked on a task of its own, holding the slot, so a
        // subscribe cancelled from here on cannot cut a request off halfway
        // and leave an item or a subscription the client never learns of.
        // The guard detaches whatever the task made once it is done.
        let guard = Detach::new(self.clone(), handle);
        let hub = self.clone();
        let work = tokio::spawn(async move {
            let mut slot = slot;
            // Routed before the server is asked, because the server may
            // report the item before it answers the request.
            lock(&hub.routes).insert(
                handle,
                Route {
                    tx,
                    subject: item.subject.clone(),
                    codec: Arc::from(CODEC_NAME),
                    node: item.node.clone(),
                    uri: item.uri.clone(),
                    queue_size: item.queue_size,
                    initial,
                    dropped: 0,
                },
            );
            hub.watch();
            let created = hub.create_item(&mut slot, handle, &item).await;
            if created.is_err() {
                lock(&hub.routes).remove(&handle);
                hub.release_if_empty(&mut slot).await;
            }
            created
        });
        let created = work
            .await
            .unwrap_or_else(|e| Err(StreamError::Internal(format!("opcua: subscribe: {e}"))));
        if let Err(error) = created {
            guard.disarm();
            return Err(error);
        }
        Ok(Notifications { rx, _guard: guard })
    }

    /// What `node` holds now, for recognising the server's first report.
    async fn before(&self, node: &NodeId) -> Initial {
        match self
            .session
            .read(
                &[ReadValueId::from(node.clone())],
                TimestampsToReturn::Source,
                0.0,
            )
            .await
        {
            Ok(mut values) if values.len() == 1 => Initial::Skip(values.remove(0)),
            _ => Initial::SkipAny,
        }
    }

    /// Adds one monitored item, creating the subscription first if `slot` has none.
    /// A refused node is [`StreamError::Unsupported`]; a revised-down queue only warns.
    async fn create_item(&self, slot: &mut Option<Owned>, handle: u32, item: &Item) -> Result<()> {
        let subject = &item.subject;
        let failed = |error: &dyn std::fmt::Display| {
            StreamError::Internal(format!("opcua: cannot monitor {subject:?}: {error}"))
        };
        let subscription = match self.resolve(slot) {
            Some(id) => id,
            None => {
                let session = self.session.server_session_id();
                let id = self.create_subscription().await.map_err(|e| failed(&e))?;
                *slot = Some(Owned { id, session });
                id
            }
        };
        let request = MonitoredItemCreateRequest::new(
            item.node.clone().into(),
            MonitoringMode::Reporting,
            MonitoringParameters {
                client_handle: handle,
                sampling_interval: self.config.sampling_interval.as_secs_f64() * 1000.0,
                // Every write is a message, even one repeating the last value:
                // its source timestamp differs, and this trigger reports that.
                filter: ExtensionObject::from_message(DataChangeFilter {
                    trigger: DataChangeTrigger::StatusValueTimestamp,
                    deadband_type: 0,
                    deadband_value: 0.0,
                }),
                queue_size: item.queue_size,
                discard_oldest: true,
            },
        );
        let created = self
            .session
            .create_monitored_items(subscription, TimestampsToReturn::Both, vec![request])
            .await
            .map_err(|e| failed(&e))?;
        let result = created
            .first()
            .ok_or_else(|| failed(&"the server returned no result"))?;
        let status = result.result.status_code;
        if status.is_bad() {
            return Err(refusal(subject, status));
        }
        if result.result.revised_queue_size < item.queue_size {
            tracing::warn!(
                subject = %subject,
                requested = item.queue_size,
                revised = result.result.revised_queue_size,
                "opcua: the server holds fewer changes per publishing cycle than asked; a burst \
                 larger than that between cycles keeps only its newest values"
            );
        }
        tracing::debug!(
            subject = %subject,
            subscription,
            item = result.result.monitored_item_id,
            queue = result.result.revised_queue_size,
            sampling_ms = result.result.revised_sampling_interval,
            "opcua: monitoring"
        );
        Ok(())
    }

    /// Creates the shared subscription with [`Router`] as its callback, resetting `heard`.
    async fn create_subscription(&self) -> std::result::Result<u32, opcua::types::Error> {
        let config = &self.config;
        self.heard.store(self.now_ms(), Ordering::Relaxed);
        let router = Router {
            routes: self.routes.clone(),
            heard: self.heard.clone(),
            epoch: self.epoch,
        };
        self.session
            .create_subscription(
                config.publishing_interval,
                config.lifetime_count,
                config.keep_alive_count,
                config.max_notifications_per_publish,
                0,
                true,
                router,
            )
            .await
    }

    /// Brings `slot` up to date and returns the subscription's id now: the
    /// one last seen while the server session that issued it lasts, or else
    /// the one holding an item of ours, which is where a reconnect put them.
    /// Call it while the routes still name the items being looked for.
    fn resolve(&self, slot: &mut Option<Owned>) -> Option<u32> {
        let session = self.session.server_session_id();
        let handles: Vec<u32> = lock(&self.routes).keys().copied().collect();
        let id = {
            let state = self.session.subscription_state.lock();
            match slot {
                Some(last) if last.session == session && state.subscription_exists(last.id) => {
                    Some(last.id)
                }
                _ => state.subscription_ids().and_then(|ids| {
                    ids.into_iter().find(|id| {
                        state.get(*id).is_some_and(|s| {
                            s.monitored_items()
                                .any(|m| handles.contains(&m.client_handle()))
                        })
                    })
                }),
            }
        };
        *slot = id.map(|id| Owned { id, session });
        id
    }

    /// Finds the server's ids for the item under `handle`.
    fn locate(&self, handle: u32) -> Option<Found> {
        let state = self.session.subscription_state.lock();
        state.subscription_ids()?.into_iter().find_map(|id| {
            state.get(id)?.monitored_items().find_map(|m| {
                (m.client_handle() == handle).then(|| Found {
                    subscription: id,
                    item: m.id(),
                    node: m.item_to_monitor().node_id.clone(),
                })
            })
        })
    }

    /// The items in `subscription`, which is ours, that no stream routes
    /// to: ones being detached, and ones whose delete failed before.
    fn strays(&self, subscription: u32) -> Vec<u32> {
        let routes = lock(&self.routes);
        let state = self.session.subscription_state.lock();
        state.get(subscription).map_or_else(Vec::new, |s| {
            s.monitored_items()
                .filter(|m| !routes.contains_key(&m.client_handle()))
                .map(|m| m.id())
                .collect()
        })
    }

    /// Deletes the monitored item under `handle`, and the subscription when it
    /// was the last.
    async fn detach(&self, handle: u32) {
        let mut slot = self.slot.lock().await;
        let subscription = self.resolve(&mut slot);
        let removed = lock(&self.routes).remove(&handle);
        if let Some(route) = &removed
            && route.dropped > 0
        {
            tracing::warn!(subject = %route.subject, dropped = route.dropped,
                "opcua: notifications were dropped because the reader fell behind");
        }
        if lock(&self.routes).is_empty() {
            // Deleting the subscription deletes its items.
            self.release_if_empty(&mut slot).await;
            return;
        }
        let Some(subscription) = subscription else {
            return;
        };
        let items = self.strays(subscription);
        if !items.is_empty() {
            self.delete_items(subscription, &items).await;
        }
    }

    /// Deletes `items`. One whose delete fails stays in the session's state,
    /// so the next detach tries it again; a bad status for one means the
    /// server has no such item (Part 4, 5.12.6), so there is nothing left to
    /// delete.
    async fn delete_items(&self, subscription: u32, items: &[u32]) {
        match self
            .session
            .delete_monitored_items(subscription, items)
            .await
        {
            Ok(statuses) => {
                for (item, status) in items.iter().zip(statuses) {
                    if status.is_good() || status == StatusCode::BadMonitoredItemIdInvalid {
                        tracing::debug!(subscription, item, %status, "opcua: monitored item deleted");
                    } else {
                        tracing::warn!(subscription, item, %status,
                            "opcua: the server refused to delete a monitored item");
                    }
                }
            }
            Err(error) => {
                tracing::warn!(subscription, ?items, %error, "opcua: cannot delete monitored items")
            }
        }
    }

    /// Deletes the subscription when no stream needs it any more. `slot`
    /// must have been resolved while the routes still named its items.
    async fn release_if_empty(&self, slot: &mut Option<Owned>) {
        if !lock(&self.routes).is_empty() {
            return;
        }
        let Some(owned) = slot.take() else { return };
        if owned.session != self.session.server_session_id()
            || !self
                .session
                .subscription_state
                .lock()
                .subscription_exists(owned.id)
        {
            return;
        }
        let subscription = owned.id;
        match self.session.delete_subscription(subscription).await {
            Ok(status) if status.is_bad() => {
                tracing::debug!(subscription, %status, "opcua: the subscription was already gone")
            }
            Ok(_) => tracing::debug!(subscription, "opcua: subscription deleted"),
            Err(error) => {
                tracing::warn!(subscription, %error, "opcua: cannot delete the subscription")
            }
        }
    }

    /// Moves every item named by namespace URI whose index the server's
    /// namespace table no longer gives it. async-opcua recreates items after
    /// a reconnect with the node ids they had, which on a renumbered server
    /// name another node, or none, and then the item is not recreated at all.
    async fn rebind(&self) {
        // The session reports itself connected only once the reconnect has
        // recreated what it will, so what is missing then stays missing.
        if self.session.wait_for_connection().now_or_never() != Some(true) {
            return;
        }
        let session = self.session.server_session_id();
        let mut slot = self.slot.lock().await;
        let named: Vec<(u32, Item)> = lock(&self.routes)
            .iter()
            .filter_map(|(handle, r)| {
                let item = Item {
                    subject: r.subject.clone(),
                    node: r.node.clone(),
                    uri: Some(r.uri.clone()?),
                    queue_size: r.queue_size,
                    buffer: 0,
                };
                Some((*handle, item))
            })
            .collect();
        let mut done = true;
        for (handle, mut item) in named {
            let (Some(uri), subject) = (item.uri.clone(), item.subject.clone()) else {
                continue;
            };
            let index = match self.namespace(&uri).await {
                Ok(index) => index,
                Err(error) => {
                    // A namespace the server no longer has will not come
                    // back by asking again on this session.
                    done &= matches!(error, StreamError::Unsupported(_));
                    tracing::warn!(subject = %subject, %error, "opcua: cannot check a monitored node");
                    continue;
                }
            };
            let found = self.locate(handle);
            let monitored = found
                .as_ref()
                .map_or(item.node.namespace, |f| f.node.namespace);
            if monitored == index {
                continue;
            }
            tracing::info!(subject = %subject, from = monitored, to = index,
                "opcua: the server renumbered a namespace; moving the monitored item");
            if let Some(found) = found
                && let Err(error) = self
                    .session
                    .delete_monitored_items(found.subscription, &[found.item])
                    .await
            {
                tracing::warn!(subject = %subject, %error, "opcua: cannot move a monitored item");
                done = false;
                continue;
            }
            item.node.namespace = index;
            if let Some(route) = lock(&self.routes).get_mut(&handle) {
                route.node = item.node.clone();
                // What the renumbered node holds is news to this stream.
                route.initial = Initial::Deliver;
            }
            if let Err(error) = self.create_item(&mut slot, handle, &item).await {
                // Ending the stream says the node is gone better than a
                // silence would.
                tracing::warn!(subject = %subject, %error,
                    "opcua: cannot move a monitored item; ending its stream");
                lock(&self.routes).remove(&handle);
            }
        }
        if done {
            *lock(&self.checked) = Some(session);
        }
    }

    /// Ends every stream and deletes the subscription.
    pub(crate) async fn close(&self) {
        let mut slot = self.slot.lock().await;
        self.resolve(&mut slot);
        lock(&self.routes).clear();
        self.release_if_empty(&mut slot).await;
    }

    /// How many monitored items this provider has on the server.
    pub(crate) fn items(&self) -> usize {
        lock(&self.routes).len()
    }
}

/// Receives every message the subscription sends.
struct Router {
    routes: Routes,
    heard: Arc<AtomicU64>,
    epoch: Instant,
}

impl OnSubscriptionNotificationCore for Router {
    /// Runs on the session's event loop: marks the subscription heard, then routes changes.
    fn on_subscription_notification(
        &mut self,
        notification: NotificationMessage,
        _items: MonitoredItemMap<'_>,
    ) {
        self.heard
            .store(self.epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
        let Some(data) = notification.notification_data else {
            // A keep-alive.
            return;
        };
        let mut routes = lock(&self.routes);
        for object in data {
            let Some(change) = object.into_inner_as::<DataChangeNotification>() else {
                continue;
            };
            for item in change.monitored_items.into_iter().flatten() {
                route(&mut routes, item.client_handle, item.value);
            }
        }
    }
}

/// Hands one notification to its stream. It runs on the session's event
/// loop, so it never waits: a stream whose reader has fallen `buffer` behind
/// loses the notification, as the server's own queue would.
fn route(routes: &mut HashMap<u32, Route>, handle: u32, value: DataValue) {
    let Some(route) = routes.get_mut(&handle) else {
        return;
    };
    match std::mem::replace(&mut route.initial, Initial::Deliver) {
        Initial::Deliver => {}
        Initial::SkipAny => return,
        Initial::Skip(before) if same(&before, &value) => return,
        Initial::Skip(_) => {}
    }
    let message = Message::shared(
        provider::new_id(),
        route.subject.clone(),
        route.codec.clone(),
        codec::encode(&value),
    );
    if let Err(mpsc::error::TrySendError::Full(_)) = route.tx.try_send(message) {
        route.dropped += 1;
        if route.dropped.is_power_of_two() {
            tracing::warn!(subject = %route.subject, dropped = route.dropped,
                "opcua: reader is behind; dropping notifications");
        }
    }
}

/// Whether a notification reports the value a read returned: same value,
/// status, and source timestamp.
fn same(read: &DataValue, reported: &DataValue) -> bool {
    read.value == reported.value
        && read.status() == reported.status()
        && read.source_timestamp == reported.source_timestamp
        && read.source_picoseconds.unwrap_or(0) == reported.source_picoseconds.unwrap_or(0)
}

/// The notifications of one monitored item. Dropping it deletes the item.
pub(crate) struct Notifications {
    rx: mpsc::Receiver<Message>,
    _guard: Detach,
}

impl FuturesStream for Notifications {
    type Item = Message;

    /// Ends once the hub drops this item's route, as closing the provider does.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Message>> {
        self.rx.poll_recv(cx)
    }
}

/// Deletes a monitored item once it is dropped.
///
/// The server is told on a task, because a drop cannot wait for a round
/// trip; until then the closed channel discards what arrives. The task runs
/// on the runtime the subscribe ran on, kept here, so a stream dropped on
/// another thread or outside any runtime is still detached.
struct Detach {
    hub: Option<Arc<Hub>>,
    handle: u32,
    runtime: tokio::runtime::Handle,
}

impl Detach {
    /// A guard for `handle`; it must be made on a Tokio runtime.
    fn new(hub: Arc<Hub>, handle: u32) -> Self {
        Self {
            hub: Some(hub),
            handle,
            runtime: tokio::runtime::Handle::current(),
        }
    }

    /// Drops the guard without detaching: the item was never made.
    fn disarm(mut self) {
        self.hub = None;
    }
}

impl Drop for Detach {
    /// Detaches on a spawned task, since a drop cannot wait for the server.
    fn drop(&mut self) {
        let Some(hub) = self.hub.take() else { return };
        let handle = self.handle;
        // A runtime that has shut down drops the task, and its sessions with
        // it, so there is no server left to tell.
        self.runtime.spawn(async move { hub.detach(handle).await });
    }
}

/// The error for a monitored item the server refused.
fn refusal(subject: &str, status: opcua::types::StatusCode) -> StreamError {
    let text = format!("opcua: the server refused to monitor {subject:?}: {status}");
    match status {
        opcua::types::StatusCode::BadNodeIdUnknown
        | opcua::types::StatusCode::BadNodeIdInvalid
        | opcua::types::StatusCode::BadAttributeIdInvalid
        | opcua::types::StatusCode::BadUserAccessDenied
        | opcua::types::StatusCode::BadNotReadable => StreamError::Unsupported(text),
        _ => StreamError::Internal(text),
    }
}

/// Locks `mutex`, recovering from poison: every guarded value is replaced whole.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use opcua::types::{DateTime, Variant};

    use super::*;

    #[test]
    fn the_first_report_is_skipped_only_when_it_is_the_value_read() {
        let at = DateTime::now();
        let read = DataValue {
            value: Some(Variant::from(1i32)),
            source_timestamp: Some(at),
            ..DataValue::default()
        };
        let (tx, mut rx) = mpsc::channel(4);
        let mut routes = HashMap::from([(
            1,
            Route {
                tx,
                subject: Arc::from("s"),
                codec: Arc::from(CODEC_NAME),
                node: NodeId::null(),
                uri: None,
                queue_size: 1,
                initial: Initial::Skip(read.clone()),
                dropped: 0,
            },
        )]);
        // A later write that replaced the initial value in a full queue.
        let later = DataValue {
            value: Some(Variant::from(2i32)),
            source_timestamp: Some(DateTime::now()),
            server_timestamp: Some(DateTime::now()),
            ..DataValue::default()
        };
        route(&mut routes, 1, later);
        assert!(rx.try_recv().is_ok(), "a later write is delivered");

        // The value read, reported with a server timestamp of its own.
        routes.get_mut(&1).unwrap().initial = Initial::Skip(read.clone());
        let reported = DataValue {
            server_timestamp: Some(DateTime::now()),
            ..read.clone()
        };
        route(&mut routes, 1, reported.clone());
        assert!(rx.try_recv().is_err(), "the initial value is skipped");
        route(&mut routes, 1, reported);
        assert!(rx.try_recv().is_ok(), "only once");
    }
}
