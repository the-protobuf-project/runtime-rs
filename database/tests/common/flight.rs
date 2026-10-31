//! In-process Arrow Flight service exercising the SDK query transport.
use arrow_array::{ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer},
    *,
};
use futures::{Stream, StreamExt};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

pub type Tickets = Arc<Mutex<Vec<serde_json::Value>>>;
type Output<T> = Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send>>;
type Reply<T> = std::result::Result<Response<T>, Status>;

#[derive(Clone)]
struct Service {
    tickets: Tickets,
    mode: &'static str,
}

#[tonic::async_trait]
impl FlightService for Service {
    type HandshakeStream = Output<HandshakeResponse>;
    type ListFlightsStream = Output<FlightInfo>;
    type DoGetStream = Output<FlightData>;
    type DoPutStream = Output<PutResult>;
    type DoExchangeStream = Output<FlightData>;
    type DoActionStream = Output<arrow_flight::Result>;
    type ListActionsStream = Output<ActionType>;

    async fn do_get(&self, request: Request<Ticket>) -> Reply<Self::DoGetStream> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer test-token"
        );
        let ticket: serde_json::Value =
            serde_json::from_slice(&request.into_inner().ticket).unwrap();
        self.tickets.lock().unwrap().push(ticket.clone());
        if self.mode == "denied" {
            return Err(Status::permission_denied("no query permission"));
        }
        let query = ticket["sql_query"].as_str().unwrap();
        let batch = if query.contains("information_schema") {
            RecordBatch::try_from_iter(vec![(
                "table_name",
                Arc::new(StringArray::from(vec!["readings"])) as ArrayRef,
            )])
            .unwrap()
        } else if query.contains("date_bin") {
            RecordBatch::try_from_iter(vec![
                (
                    "observed_at",
                    Arc::new(TimestampNanosecondArray::from(vec![0])) as ArrayRef,
                ),
                ("mean", Arc::new(Float64Array::from(vec![22.5])) as ArrayRef),
            ])
            .unwrap()
        } else {
            RecordBatch::try_from_iter(vec![
                (
                    "observed_at",
                    Arc::new(TimestampNanosecondArray::from(vec![1234567890])) as ArrayRef,
                ),
                (
                    "device",
                    Arc::new(StringArray::from(vec!["boiler"])) as ArrayRef,
                ),
                (
                    "temperature",
                    Arc::new(Float64Array::from(vec![22.5])) as ArrayRef,
                ),
            ])
            .unwrap()
        };
        let stream = arrow_flight::encode::FlightDataEncoderBuilder::new()
            .build(futures::stream::iter(vec![Ok(batch)]))
            .map(|result| result.map_err(|e| Status::internal(e.to_string())));
        let stream: Output<FlightData> = if self.mode == "stall" {
            Box::pin(stream.chain(futures::stream::pending()))
        } else if self.mode == "broken" {
            Box::pin(stream.chain(futures::stream::once(async {
                Err(Status::internal("broken stream"))
            })))
        } else {
            Box::pin(stream)
        };
        Ok(Response::new(stream))
    }

    async fn handshake(
        &self,
        _: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Reply<Self::HandshakeStream> {
        Err(Status::unimplemented("handshake"))
    }
    async fn list_flights(&self, _: Request<Criteria>) -> Reply<Self::ListFlightsStream> {
        Err(Status::unimplemented("list"))
    }
    async fn get_flight_info(&self, _: Request<FlightDescriptor>) -> Reply<FlightInfo> {
        Err(Status::unimplemented("info"))
    }
    async fn poll_flight_info(&self, _: Request<FlightDescriptor>) -> Reply<PollInfo> {
        Err(Status::unimplemented("poll"))
    }
    async fn get_schema(&self, _: Request<FlightDescriptor>) -> Reply<SchemaResult> {
        Err(Status::unimplemented("schema"))
    }
    async fn do_put(&self, _: Request<tonic::Streaming<FlightData>>) -> Reply<Self::DoPutStream> {
        Err(Status::unimplemented("put"))
    }
    async fn do_exchange(
        &self,
        _: Request<tonic::Streaming<FlightData>>,
    ) -> Reply<Self::DoExchangeStream> {
        Err(Status::unimplemented("exchange"))
    }
    async fn do_action(&self, _: Request<Action>) -> Reply<Self::DoActionStream> {
        Err(Status::unimplemented("action"))
    }
    async fn list_actions(&self, _: Request<Empty>) -> Reply<Self::ListActionsStream> {
        Err(Status::unimplemented("actions"))
    }
}

pub struct Fixture {
    pub host: String,
    pub tickets: Tickets,
    task: tokio::task::JoinHandle<()>,
}
impl Fixture {
    pub async fn start(mode: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = format!("http://{}", listener.local_addr().unwrap());
        let tickets = Arc::new(Mutex::new(Vec::new()));
        let service = Service {
            tickets: tickets.clone(),
            mode,
        };
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        Self {
            host,
            tickets,
            task,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
