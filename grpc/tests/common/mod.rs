//! The shop API the error/validation tests serve: generated from `tests/proto`, with a
//! service whose behaviour each request picks.

#![allow(dead_code)]

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use grpc::HybridServer;
use grpc::options::{Endpoint, Options};
use tokio::sync::Notify;
use tonic::{Request, Response, Status};

#[allow(clippy::all, clippy::pedantic, missing_docs)]
pub mod pb {
    include!("../fixtures/shop.v1.rs");
}

pub use pb::shop_service_client::ShopServiceClient;
use pb::shop_service_server::{ShopService, ShopServiceServer};
use pb::*;

/// The shop's encoded descriptor set, with its imports.
pub const SHOP_DESCRIPTOR_SET: &[u8] = include_bytes!("../fixtures/shop.binpb");

/// The sentinel the shop's sold-out problem names.
pub const SOLD_OUT: &str = "sold out";

/// The shop. What a call does is chosen by the request:
///
/// - `CreateBook` with `book_id` `sold-out` returns a [`grpc::problem::Problem`],
///   `plain` a plain error wrapping the `not found` sentinel, `fail` a
///   `FAILED_PRECONDITION`, and `wait` blocks until [`Shop::release`]; anything else
///   succeeds with the run number in the title.
/// - `GetShelf` of `shelves/missing` is a bare `NOT_FOUND`, `shelves/boom` an `INTERNAL`
///   leaking a SQL error; anything else is found.
#[derive(Clone, Default)]
pub struct Shop {
    runs: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    gate: Arc<Notify>,
}

impl Shop {
    /// How many times `CreateBook` ran.
    pub fn runs(&self) -> usize {
        self.runs.load(Ordering::SeqCst)
    }

    /// Waits until a `wait` call is inside the handler.
    pub async fn entered(&self) {
        self.entered.notified().await;
    }

    /// Lets a `wait` call finish.
    pub fn release(&self) {
        self.gate.notify_one();
    }
}

#[tonic::async_trait]
impl ShopService for Shop {
    async fn get_shelf(
        &self,
        request: Request<GetShelfRequest>,
    ) -> Result<Response<Shelf>, Status> {
        let name = request.into_inner().name;
        match name.as_str() {
            "shelves/missing" => Err(Status::not_found("not found")),
            "shelves/boom" => Err(Status::internal(
                "ERROR: relation \"shelves\" does not exist",
            )),
            _ => Ok(Response::new(Shelf {
                name,
                theme: String::new(),
            })),
        }
    }

    async fn list_shelves(
        &self,
        _: Request<ListShelvesRequest>,
    ) -> Result<Response<ListShelvesResponse>, Status> {
        Ok(Response::new(ListShelvesResponse::default()))
    }

    async fn get_book(&self, _: Request<GetBookRequest>) -> Result<Response<Book>, Status> {
        Err(Status::unimplemented("not in this test"))
    }

    async fn list_books(
        &self,
        _: Request<ListBooksRequest>,
    ) -> Result<Response<ListBooksResponse>, Status> {
        Ok(Response::new(ListBooksResponse::default()))
    }

    async fn create_book(
        &self,
        request: Request<CreateBookRequest>,
    ) -> Result<Response<Book>, Status> {
        let request = request.into_inner();
        let run = self.runs.fetch_add(1, Ordering::SeqCst) + 1;
        match request.book_id.as_str() {
            "sold-out" => {
                return Err(grpc::problem::Problem::new(SOLD_OUT)
                    .reason("SHELF_EMPTY")
                    .resource("shop.example.com/Shelf", request.parent.clone())
                    .what(format!("shelf {} is empty", request.parent))
                    .fix("Restock it first")
                    .into());
            }
            "plain" => {
                return Err(Status::from_error(Box::new(std::io::Error::other(
                    "not found: shelf shelves/x has no room",
                ))));
            }
            "fail" => return Err(Status::failed_precondition("no")),
            "wait" => {
                self.entered.notify_one();
                self.gate.notified().await;
            }
            _ => {}
        }
        Ok(Response::new(Book {
            name: format!("{}/books/{}", request.parent, request.book_id),
            title: format!("run-{run}"),
        }))
    }

    async fn update_shelf(
        &self,
        _: Request<UpdateShelfRequest>,
    ) -> Result<Response<Shelf>, Status> {
        Err(Status::unimplemented("not in this test"))
    }

    async fn undelete_shelf(
        &self,
        _: Request<UndeleteShelfRequest>,
    ) -> Result<Response<Shelf>, Status> {
        Err(Status::unimplemented("not in this test"))
    }
}

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .expect("a free port")
}

/// Starts a server for `shop` with the shop's descriptors registered, configured further by
/// `configure`, and a client dialled to it.
pub async fn serve(
    shop: Shop,
    configure: impl FnOnce(HybridServer) -> HybridServer,
) -> (HybridServer, ShopServiceClient<tonic::transport::Channel>) {
    let port = free_port();
    let server = HybridServer::new(Options {
        service_name: "shop-test".into(),
        grpc: Endpoint::new("127.0.0.1", port),
        ..Default::default()
    })
    .with_reflection(SHOP_DESCRIPTOR_SET)
    .with_service(ShopServiceServer::new(shop));
    let mut server = configure(server);
    server.start().await.expect("the server starts");
    (server, client(port).await)
}

/// A shop client dialled to `port`.
pub async fn client(port: u16) -> ShopServiceClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .expect("a valid endpoint")
        .connect()
        .await
        .expect("connects");
    ShopServiceClient::new(channel)
}

/// A `CreateBook` request.
pub fn create(parent: &str, book_id: &str, request_id: &str) -> CreateBookRequest {
    CreateBookRequest {
        parent: parent.into(),
        book_id: book_id.into(),
        book: None,
        request_id: request_id.into(),
        validate_only: false,
    }
}

pub fn shop_server(shop: Shop) -> ShopServiceServer<Shop> {
    ShopServiceServer::new(shop)
}
