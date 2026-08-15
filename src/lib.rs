//! # MailBaby 鈥?Rust client
//!
//! A fully-featured asynchronous Rust client for the
//! [MailBaby](https://github.com/mailbabys/mailbaby) email delivery microservice.
//! MailBaby is a high-throughput, message-queue-driven SMTP sending service; this
//! crate lets your Rust application dispatch emails through any of its three
//! ingestion channels:
//!
//! | Channel | Feature | Description |
//! |---|---|---|
//! | **REST** | `rest` (default) | `POST /v1/email/send` and `POST /v1/email/batch` over HTTP(S) |
//! | **gRPC** | `grpc` | `mailbaby.v1.MailService` 鈥?`Send`, `SendBatch`, `Ping`, `HealthCheck` |
//! | **Message queues** | `mq-rabbitmq` / `mq-redis` / `mq-kafka` | Publish email jobs straight into RabbitMQ, Redis or Kafka, bypassing the HTTP/gRPC endpoints entirely |
//!
//! All channels share the same [`Email`] model, so the exact same payload can be
//! routed over any channel 鈥?mirroring the server-side `sender.Email` wire format
//! (snake_case JSON, base64 attachment data).
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use mailbaby::{Email, rest::MailBabyClient};
//!
//! # async fn example() -> Result<(), mailbaby::Error> {
//! let client = MailBabyClient::new("http://localhost:8080", Some("your_secret_key"))?;
//!
//! let email = Email::builder("Order Confirmation #10024")
//!     .account("default")
//!     .from_with_name("noreply@example.com", "MailBaby System")
//!     .to(["alice@example.com"])
//!     .html_body("<h2>Order Confirmed</h2><p>Tracking: <b>987654</b></p>")
//!     .build()?;
//!
//! let response = client.send(&email).await?;
//! println!("sent: {} ({})", response.id, response.status);
//! # Ok(())
//! # }
//! ```
//!
//! ## Feature flags
//!
//! - `rest` (default) 鈥?HTTP REST client via [`rest::MailBabyClient`].
//! - `grpc` 鈥?gRPC client via [`grpc::GrpcClient`]; compiles
//!   `proto/mailbaby.proto` at build time (pure Rust, no `protoc` required).
//! - `mq` 鈥?enables all three MQ producer drivers below.
//! - `mq-rabbitmq` 鈥?AMQP 0-9-1 producer backed by [`lapin`](https://docs.rs/lapin).
//! - `mq-redis` 鈥?Redis Stream/List/PubSub producer backed by
//!   [`redis`](https://docs.rs/redis).
//! - `mq-kafka` 鈥?Kafka producer backed by [`rskafka`](https://docs.rs/rskafka)
//!   (pure Rust, no `librdkafka` dependency).
//!
//! Enable the ones you need, e.g.:
//!
//! ```toml
//! [dependencies]
//! mailbaby = { version = "0.1", features = ["grpc", "mq"] }
//! ```
//!
//! ## Authentication
//!
//! When the server runs with `auth.enabled: true`, pass the secret key as an
//! [`Auth`] to the REST/gRPC clients. The server accepts the key via a custom
//! header (`X-API-Key` by default), `Authorization: Bearer <key>`, or a query
//! parameter 鈥?all three are supported here.
//!
//! ## Feature reference (docs.rs)
//!
//! On docs.rs this crate is built with `--all-features`, so all modules are
//! visible; in a local build only the enabled features' modules are compiled.
//! Code blocks below that require a specific feature are tagged with
//! `#[cfg(feature = "...")]` in their examples.

#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![doc(html_root_url = "https://docs.rs/mailbaby")]

pub mod error;
pub mod model;

#[cfg(any(feature = "rest", feature = "grpc"))]
mod auth;
#[cfg(feature = "rest")]
pub mod rest;

#[cfg(feature = "grpc")]
pub mod grpc;

#[cfg(any(feature = "mq-rabbitmq", feature = "mq-redis", feature = "mq-kafka"))]
pub mod mq;

pub use error::Error;
pub use model::{
    ApiErrorBody, Attachment, Base64, BatchResponse, Email, EmailBuilder, SendResponse,
};

#[cfg(any(feature = "rest", feature = "grpc"))]
pub use auth::{Auth, AuthScheme};

#[cfg(feature = "mq-kafka")]
pub use mq::KafkaProducer;
#[cfg(feature = "mq-rabbitmq")]
pub use mq::RabbitMqProducer;
#[cfg(any(feature = "mq-rabbitmq", feature = "mq-redis", feature = "mq-kafka"))]
pub use mq::{MqMessage, MqProducer, PublishOptions};
#[cfg(feature = "mq-redis")]
pub use mq::{RedisMode, RedisProducer};
