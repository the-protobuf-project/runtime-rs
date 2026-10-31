//! The gRPC OpenTelemetry metric names. Go's `AvailableOpenTelemetryMetricTypes`.
//!
//! Go keeps them as fields of a package-level struct value so they can be handed to gRPC's
//! own metrics option. Modules of constants read the same at a call site and cost nothing.
//! The names are gRFC A66's and its successors'. This server emits the [`server`] ones; the
//! rest belong to a gRPC client, a load balancer or an xDS client, and are here so the code
//! that does run those has the same vocabulary.

/// Metrics a gRPC client emits during request attempts and the call lifecycle.
pub mod client {
    /// The number of client call attempts started.
    pub const ATTEMPT_STARTED: &str = "grpc.client.attempt.started";
    /// The duration of a single client call attempt.
    pub const ATTEMPT_DURATION: &str = "grpc.client.attempt.duration";
    /// The total compressed size of messages a client sent in one attempt.
    pub const ATTEMPT_SENT_TOTAL_COMPRESSED_MESSAGE_SIZE: &str =
        "grpc.client.attempt.sent_total_compressed_message_size";
    /// The total compressed size of messages a client received in one attempt.
    pub const ATTEMPT_RCVD_TOTAL_COMPRESSED_MESSAGE_SIZE: &str =
        "grpc.client.attempt.rcvd_total_compressed_message_size";
    /// The duration of a whole client call, retries included.
    pub const CALL_DURATION: &str = "grpc.client.call.duration";
}

/// Metrics a gRPC server emits while handling calls.
pub mod server {
    /// The number of calls the server started.
    pub const CALL_STARTED: &str = "grpc.server.call.started";
    /// The duration of a call the server handled.
    pub const CALL_DURATION: &str = "grpc.server.call.duration";
    /// The total compressed size of messages the server sent during a call.
    pub const CALL_SENT_TOTAL_COMPRESSED_MESSAGE_SIZE: &str =
        "grpc.server.call.sent_total_compressed_message_size";
    /// The total compressed size of messages the server received during a call.
    pub const CALL_RCVD_TOTAL_COMPRESSED_MESSAGE_SIZE: &str =
        "grpc.server.call.rcvd_total_compressed_message_size";
}

/// Metrics the `pick_first` load balancer policy emits.
pub mod load_balancer {
    /// Successful connection attempts.
    pub const PICK_FIRST_CONNECTION_ATTEMPTS_SUCCEEDED: &str =
        "grpc.lb.pick_first.connection_attempts_succeeded";
    /// Failed connection attempts.
    pub const PICK_FIRST_CONNECTION_ATTEMPTS_FAILED: &str =
        "grpc.lb.pick_first.connection_attempts_failed";
    /// Disconnections observed.
    pub const PICK_FIRST_DISCONNECTIONS: &str = "grpc.lb.pick_first.disconnections";
}

/// Metrics the weighted round-robin load balancer emits.
pub mod wrr_load_balancer {
    /// Times the scheduler fell back to plain round robin.
    pub const FALLBACK: &str = "grpc.lb.wrr.rr_fallback";
    /// Endpoints whose weights are not yet usable.
    pub const ENDPOINT_WEIGHT_NOT_YET_USABLE: &str = "grpc.lb.wrr.endpoint_weight_not_yet_usable";
    /// Endpoints whose weights have gone stale.
    pub const ENDPOINT_WEIGHT_STALE: &str = "grpc.lb.wrr.endpoint_weight_stale";
    /// The weights assigned to endpoints.
    pub const ENDPOINT_WEIGHTS: &str = "grpc.lb.wrr.endpoint_weights";
}

/// Metrics an xDS client emits.
pub mod xds_client {
    /// Whether the client is connected.
    pub const CONNECTED: &str = "grpc.xds_client.connected";
    /// Failures talking to the xDS server.
    pub const SERVER_FAILURE: &str = "grpc.xds_client.server_failure";
    /// Valid resource updates received.
    pub const RESOURCE_UPDATES_VALID: &str = "grpc.xds_client.resource_updates_valid";
    /// Invalid resource updates received.
    pub const RESOURCE_UPDATES_INVALID: &str = "grpc.xds_client.resource_updates_invalid";
    /// Active resources the client manages.
    pub const RESOURCES: &str = "grpc.xds_client.resources";
}
