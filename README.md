<div align="center">

# 🦀 MailBaby Rust Client

**Official Rust client for the [MailBaby](https://github.com/mailbabys/mailbaby) email delivery microservice**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![crates.io](https://img.shields.io/crates/v/mailbaby.svg)](https://crates.io/crates/mailbaby)
[![docs.rs](https://img.shields.io/docsrs/mailbaby.svg)](https://docs.rs/mailbaby)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](Cargo.toml)

</div>

`mailbaby` is a fully-featured asynchronous Rust client for the MailBaby
email delivery service. MailBaby is a high-throughput, message-queue-driven
SMTP sending service; this crate lets your Rust application dispatch emails
through any of its three ingestion channels:

- **HTTP REST API** — `POST /v1/email/send`, `POST /v1/email/batch`, health probes
- **gRPC `MailService`** — `Send`, `SendBatch`, `Ping`, `HealthCheck` (proto
  compiled at build time, pure Rust, no `protoc` required)
- **Message-queue publishing** — RabbitMQ, Redis, Kafka with payloads in the
  exact wire format the MailBaby server consumes

All channels share the same [`Email`] model, so the same payload can be
routed over any channel.

## 📦 Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
mailbaby = "0.1"
```

By default only the REST feature is enabled. Enable the ones you need:

```toml
[dependencies]
mailbaby = { version = "0.1", features = ["grpc", "mq"] }
```

Available features:

| Feature | Description |
|---|---|
| `rest` *(default)* | HTTP REST client via [`reqwest`] |
| `grpc` | gRPC client via [`tonic`] — compiles `proto/mailbaby.proto` at build time |
| `mq` | Enables all three MQ producer drivers |
| `mq-rabbitmq` | AMQP 0-9-1 producer via [`lapin`] |
| `mq-redis` | Redis Stream / List / PubSub producer via [`redis`] |
| `mq-kafka` | Kafka producer via [`rskafka`] (pure Rust, no `librdkafka`) |

## 🚀 Quick start

### REST (default feature)

```rust,no_run
use mailbaby::{Email, rest::MailBabyClient};

#[tokio::main]
async fn main() -> Result<(), mailbaby::Error> {
    let client = MailBabyClient::new("http://localhost:8080", Some("your_secret_key"))?;

    let email = Email::builder("Order Confirmation #10024")
        .account("default")
        .from_with_name("noreply@example.com", "MailBaby System")
        .to(["alice@example.com"])
        .html_body("<h2>Order Confirmed</h2><p>Tracking: <b>987654</b></p>")
        .build()?;

    let response = client.send(&email).await?;
    println!("sent: {} ({})", response.id, response.status);

    let batch = client.send_batch(&[email.clone(), email]).await?;
    println!("batch: {}/{} ok", batch.succeeded, batch.total);
    Ok(())
}
```

### gRPC

```rust,no_run
# #[cfg(feature = "grpc")]
use mailbaby::{Email, grpc::GrpcClient};

#[tokio::main]
# #[cfg(feature = "grpc")]
async fn main() -> Result<(), mailbaby::Error> {
    let client = GrpcClient::connect("http://localhost:8081", Some("your_secret_key")).await?;
    let pong = client.ping("hello").await?;
    println!("ping: {}", pong.status);
    Ok(())
}
```

### Message queues

```rust,no_run
# #[cfg(feature = "mq-rabbitmq")]
use mailbaby::{Email, mq::RabbitMqProducer};

# #[cfg(feature = "mq-rabbitmq")]
#[tokio::main]
async fn main() -> Result<(), mailbaby::Error> {
    let producer = RabbitMqProducer::new("amqp://guest:guest@localhost:5672").await?;
    let email = Email::builder("Hi").to(["a@example.com"]).build()?;
    producer.publish(&email).await?;
    Ok(())
}
```

See the [`examples/`](examples) directory for runnable programs covering all
three channels (`send.rs`, `grpc_send.rs`, `mq_publish.rs`).

## 🔐 Authentication

When the server runs with `auth.enabled: true`, pass the secret key as an
`Auth` to the REST/gRPC clients. The server accepts the key via a custom
header (`X-API-Key` by default), `Authorization: Bearer <key>`, or a query
parameter — all three are supported here.

## 🩺 Health probes

```rust,no_run
use mailbaby::rest::MailBabyClient;

# #[tokio::main]
# async fn main() -> Result<(), mailbaby::Error> {
let client = MailBabyClient::new("http://localhost:8080", None)?;
let live = client.livez().await?;
let ready = client.readyz().await?;
println!("live: {} | ready: {}", live.status, ready.status);
# Ok(())
# }
```

## 🧱 Examples & docs

- [Full crate documentation on docs.rs](https://docs.rs/mailbaby) — built with
  `--all-features` so every module is visible.
- [`examples/send.rs`](examples/send.rs) — REST end-to-end
- [`examples/grpc_send.rs`](examples/grpc_send.rs) — gRPC send + ping
- [`examples/mq_publish.rs`](examples/mq_publish.rs) — direct MQ publishing

## 📄 License

Licensed under the [Apache License, Version 2.0](LICENSE).