//! GraphQL subscription support over the graphql-transport-ws protocol. See
//! [`GraphQLClient::subscribe_fields`].

use std::marker::PhantomData;

use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_util::sync::CancellationToken;

use super::GraphQLClient;
use super::named::{FieldArg, build_field_tag, build_variable_declarations, sorted_args};
use super::subscription_task::{await_connection_ack, run_subscription, send_json};
use super::ws_protocol::{ClientMessage, SUBPROTOCOL, SubscribePayload};
use crate::error::{Error, Result};
use crate::url::{build_full_url, websocket_url};
use serde::de::DeserializeOwned;

/// A live GraphQL subscription opened by [`GraphQLClient::subscribe_fields`]. Each server message
/// is decoded into `T` and delivered on [`Subscription::updates`]. Dropping the handle does not
/// stop the subscription (the background task keeps running); call [`Subscription::stop`] to end
/// it explicitly, or cancel the token passed to `subscribe_fields`.
pub struct Subscription<T> {
    updates: mpsc::Receiver<Result<T>>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    _marker: PhantomData<T>,
}

impl<T> Subscription<T> {
    /// Returns the channel of subscription results. It closes when the subscription stops (via
    /// [`Subscription::stop`], a fatal error, or server completion).
    pub fn updates(&mut self) -> &mut mpsc::Receiver<Result<T>> {
        &mut self.updates
    }

    /// Ends the subscription and releases the underlying WebSocket connection.
    pub async fn stop(self) -> Result<()> {
        self.cancel.cancel();
        let _ = self.task.await;
        Ok(())
    }
}

impl GraphQLClient {
    /// Opens a subscription selecting `selection` under `field` with the given arguments,
    /// declaring only the arguments present. Each server message is decoded into `T` and
    /// delivered on the returned [`Subscription`]'s updates channel. Headers are sent on the
    /// WebSocket handshake. Cancelling `cancel` stops the subscription (equivalent to calling
    /// [`Subscription::stop`]).
    pub async fn subscribe_fields<T: DeserializeOwned + Send + 'static>(
        &self,
        field: &str,
        args: &[FieldArg],
        selection: &str,
        cancel: Option<CancellationToken>,
    ) -> Result<Subscription<T>> {
        let host = self.options.url.host.clone();
        let ws_url_opts = websocket_url(&self.options.url);
        let full_url = build_full_url(&ws_url_opts, 0).map_err(|e| Error::BuildUrl(Box::new(e)))?;

        let mut request = full_url.as_str().into_client_request().map_err(|e| {
            Error::SubscriptionStart(Box::new(Error::WsConnect {
                host: host.clone(),
                source: Box::new(e),
            }))
        })?;
        request.headers_mut().insert(
            http::header::SEC_WEBSOCKET_PROTOCOL,
            http::HeaderValue::from_static(SUBPROTOCOL),
        );
        for (k, v) in &self.options.headers {
            let name = http::HeaderName::from_bytes(k.as_bytes())
                .map_err(|_| Error::InvalidHeader(k.clone()))?;
            let value =
                http::HeaderValue::from_str(v).map_err(|_| Error::InvalidHeader(v.clone()))?;
            request.headers_mut().insert(name, value);
        }

        let dial = tokio_tungstenite::connect_async(request);
        let (ws_stream, _resp) = tokio::time::timeout(self.options.timeout, dial)
            .await
            .map_err(|_| Error::SubscriptionStart(Box::new(Error::Timeout)))?
            .map_err(|e| {
                Error::SubscriptionStart(Box::new(Error::WsConnect {
                    host: host.clone(),
                    source: Box::new(e),
                }))
            })?;

        let (mut sink, mut stream) = futures_util::StreamExt::split(ws_stream);

        send_json(&mut sink, &ClientMessage::ConnectionInit { payload: None })
            .await
            .map_err(|e| Error::SubscriptionStart(Box::new(e)))?;
        await_connection_ack(&mut stream, self.options.timeout)
            .await
            .map_err(|e| Error::SubscriptionStart(Box::new(e)))?;

        let sorted = sorted_args(args);
        let head = build_field_tag(field, &sorted);
        let var_decls = build_variable_declarations(&sorted);
        let query = format!("subscription{var_decls} {{ {head} {selection} }}");
        let mut variables = serde_json::Map::new();
        for a in &sorted {
            variables.insert(a.name.clone(), a.value.clone());
        }
        let variables = if variables.is_empty() {
            None
        } else {
            Some(serde_json::Value::Object(variables))
        };

        let sub_id = "1".to_string();
        send_json(
            &mut sink,
            &ClientMessage::Subscribe {
                id: sub_id.clone(),
                payload: SubscribePayload {
                    query: &query,
                    variables,
                },
            },
        )
        .await
        .map_err(|e| Error::SubscriptionStart(Box::new(e)))?;

        let (tx, rx) = mpsc::channel(16);
        let cancel = cancel.unwrap_or_default();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(run_subscription::<T>(sink, stream, sub_id, tx, task_cancel));

        Ok(Subscription {
            updates: rx,
            cancel,
            task,
            _marker: PhantomData,
        })
    }
}
