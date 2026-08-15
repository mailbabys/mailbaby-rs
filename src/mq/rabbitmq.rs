//! RabbitMQ / AMQP 0-9-1 producer (`mq-rabbitmq` feature).
//!
//! Publishes email jobs to the queue the MailBaby server consumes, with the
//! same AMQP envelope the Go server's `rabbitProducer` produces:
//!
//! - **exchange** — from [`RabbitMqProducer::connect`] (empty string = the
//!   default exchange);
//! - **routing key** — `PublishOptions::key` → `PublishOptions::topic` →
//!   `MqMessage::topic` → the configured default queue name;
//! - **properties** — `MessageId` = message id, `DeliveryMode = Persistent`
//!   (2), `Timestamp` = Unix seconds, `Content-Type` from the `Content-Type`
//!   header (default `application/octet-stream`), plus all message/publish
//!   headers as an AMQP table;
//! - **publisher confirms** are enabled and awaited, so `publish` only
//!   returns `Ok` after the broker acknowledges the message.
//!
//! # Example
//!
//! ```rust,no_run
//! use mailbaby::Email;
//! use mailbaby::mq::{MqMessage, MqProducer, PublishOptions, RabbitMqProducer};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), mailbaby::Error> {
//!     let producer = RabbitMqProducer::connect(
//!         "amqp://guest:guest@localhost:5672/%2f",
//!         "mailbaby",   // exchange ("" = default exchange)
//!         "mailqueue",  // default routing key / queue name
//!     )
//!     .await?;
//!
//!     let email = Email::builder("Order update")
//!         .to(["alice@example.com"])
//!         .build()?;
//!     let message = MqMessage::from_email(&email)?;
//!
//!     producer
//!         .publish(&message, &PublishOptions::default())
//!         .await?;
//!     println!("published {}", message.id);
//!
//!     Ok(())
//! }
//! ```
//!
//! # Errors
//!
//! - [`Error::MqConnect`] — the connection or channel could not be established;
//! - [`Error::Mq`] — the broker rejected the publish (`Nack`), the exchange is
//!   missing, or the connection broke mid-publish.
//!
//! TLS is supported out of the box: pass an `amqps://` URI (lapin's `rustls`
//! backend with the platform trust store).

use std::sync::Arc;

use lapin::options::{BasicPublishOptions, ConfirmSelectOptions};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{BasicProperties, Channel, Confirmation, Connection, ConnectionProperties};

use super::{MqMessage, MqProducer, PublishOptions};
use crate::error::Error;

/// RabbitMQ producer.
///
/// Cheap to clone (the underlying [`Channel`] is shared) and safe to use from
/// multiple tasks concurrently; AMQP multiplexes everything over one
/// connection.
///
/// See the [module documentation](self) for the wire-format details and an
/// example.
#[derive(Clone, Debug)]
pub struct RabbitMqProducer {
    _connection: Arc<Connection>,
    channel: Channel,
    exchange: String,
    default_routing_key: String,
}

impl RabbitMqProducer {
    /// Connects to the broker and opens a channel with publisher confirms.
    ///
    /// # Arguments
    ///
    /// - `addr` — AMQP URI, e.g. `amqp://guest:guest@localhost:5672/%2f`
    ///   (`%2f` = vhost `/`); `amqps://` for TLS;
    /// - `exchange` — exchange to publish to; `""` selects the default
    ///   (direct) exchange, which routes directly to queues;
    /// - `routing_key` — default routing key (queue name); per-message
    ///   destinations override it.
    ///
    /// The exchange must exist on the broker (the server declares its own on
    /// startup); this client is producer-only and never declares topology.
    ///
    /// # Errors
    ///
    /// Returns [`Error::MqConnect`] when the URI is invalid or the broker is
    /// unreachable, and [`Error::Mq`] when the channel setup fails.
    pub async fn connect(addr: &str, exchange: &str, routing_key: &str) -> Result<Self, Error> {
        let properties = ConnectionProperties::default().enable_auto_recover();
        let connection = Connection::connect(addr, properties)
            .await
            .map_err(|e| Error::MqConnect(format!("rabbitmq: connect failed: {e}")))?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| Error::Mq(format!("rabbitmq: create channel failed: {e}")))?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|e| Error::Mq(format!("rabbitmq: confirm_select failed: {e}")))?;

        Ok(RabbitMqProducer {
            _connection: Arc::new(connection),
            channel,
            exchange: exchange.to_string(),
            default_routing_key: routing_key.to_string(),
        })
    }

    /// Resolves the routing key with the Go producer's precedence:
    /// `options.key` > `options.topic` > `message.topic` > default.
    fn routing_key(&self, message: &MqMessage, options: &PublishOptions) -> String {
        if let Some(key) = options.key.as_ref().filter(|k| !k.is_empty()) {
            return key.clone();
        }
        if let Some(topic) = options.topic.as_ref().filter(|t| !t.is_empty()) {
            return topic.clone();
        }
        if !message.topic.is_empty() {
            return message.topic.clone();
        }
        self.default_routing_key.clone()
    }
}

impl MqProducer for RabbitMqProducer {
    async fn publish(&self, message: &MqMessage, options: &PublishOptions) -> Result<(), Error> {
        let routing_key = self.routing_key(message, options);

        let mut headers = message.headers.clone();
        for (k, v) in &options.headers {
            headers.insert(k.clone(), v.clone());
        }

        let mut table = FieldTable::default();
        for (k, v) in &headers {
            table.insert(
                ShortString::from(k.as_str()),
                AMQPValue::LongString(v.clone().into()),
            );
        }

        let content_type = headers
            .get("Content-Type")
            .cloned()
            .unwrap_or_else(|| "application/octet-stream".to_string());

        // delivery_mode = 2 corresponds to AMQP's DeliveryMode::Persistent,
        // mirroring the Go producer's amqp.Persistent.
        let properties = BasicProperties::default()
            .with_delivery_mode(2)
            .with_message_id(ShortString::from(message.id.as_str()))
            .with_timestamp(message.timestamp.timestamp() as u64)
            .with_content_type(ShortString::from(content_type.as_str()))
            .with_headers(table);

        let confirm = self
            .channel
            .basic_publish(
                self.exchange.clone().into(),
                routing_key.into(),
                BasicPublishOptions::default(),
                &message.payload,
                properties,
            )
            .await
            .map_err(|e| Error::Mq(format!("rabbitmq: publish failed: {e}")))?;

        match confirm
            .await
            .map_err(|e| Error::Mq(format!("rabbitmq: confirm failed: {e}")))?
        {
            Confirmation::Ack(_) => Ok(()),
            Confirmation::Nack(_) => Err(Error::Mq(
                "rabbitmq: broker negatively acknowledged the message".to_string(),
            )),
            other => Err(Error::Mq(format!(
                "rabbitmq: unexpected confirmation: {other:?}"
            ))),
        }
    }
}
