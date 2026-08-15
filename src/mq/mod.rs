//! Message-queue producers (`mq-rabbitmq`, `mq-redis`, `mq-kafka` features).
//!
//! MailBaby's architecture is message-queue-driven: the HTTP/gRPC endpoints
//! are just thin producers that enqueue email jobs. This module gives you the
//! same capability directly 鈥?publish emails into the very same queue the
//! server consumes, **bypassing the server's HTTP/gRPC endpoints entirely**
//! (useful for scale-out or for running your own consumers).
//!
//! All three drivers publish the **same payload** the server's `sendAsync`
//! path produces: the [`Email`] serialized with [`Email::to_json`] (snake_case,
//! base64 attachments),
//! wrapped in driver-specific envelopes (AMQP properties, Redis stream fields,
//! Kafka headers) 鈥?byte-for-byte compatible with the Go server's own
//! producers, so the server's consumer engine processes them seamlessly.
//!
//! # Drivers
//!
//! | Driver | Feature | Producer |
//! |---|---|---|
//! | RabbitMQ | `mq-rabbitmq` | [`RabbitMqProducer`] |
//! | Redis (Stream / List / PubSub) | `mq-redis` | [`RedisProducer`] |
//! | Kafka | `mq-kafka` | [`KafkaProducer`] |
//!
//! # Quick start
//!
//! ```rust,no_run
//! use mailbaby::Email;
//! use mailbaby::mq::{MqMessage, MqProducer, PublishOptions, RabbitMqProducer};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), mailbaby::Error> {
//!     let producer = RabbitMqProducer::connect(
//!         "amqp://guest:guest@localhost:5672/%2f",
//!         "mailbaby",
//!         "mailqueue",
//!     )
//!     .await?;
//!
//!     let email = Email::builder("Welcome!")
//!         .to(["user@example.com"])
//!         .text_body("Welcome aboard!")
//!         .build()?;
//!
//!     let message = MqMessage::from_email(&email)?;
//!     producer
//!         .publish(&message, &PublishOptions::default())
//!         .await?;
//!     println!("published {}", message.id);
//!
//!     Ok(())
//! }
//! ```
//!
//! # Channel semantics
//!
//! - **Destination resolution** (all drivers): the destination of each
//!   publish is `PublishOptions::topic` 鈫?`MqMessage::topic` 鈫?the producer's
//!   configured default (`queue name`, `redis key`, `kafka topic`), matching
//!   the server's `queue.PublishOptions` precedence.
//! - **Headers**: `PublishOptions::headers` override
//!   [`MqMessage::headers`](MqMessage::headers), which override the driver
//!   defaults 鈥?same merge order as the Go drivers.
//! - **Delay**: [`PublishOptions::delay`] delays the publish by spawning a
//!   background task (like the Go server's delayed-publish goroutine).
//!
//! # Error semantics
//!
//! Transport/driver failures surface as [`Error::Mq`]; failures while
//! establishing the connection as [`Error::MqConnect`]. Serialization errors
//! (e.g. from [`MqMessage::from_email`]) surface as [`Error::Json`].

#[cfg(feature = "mq-kafka")]
pub mod kafka;
#[cfg(feature = "mq-rabbitmq")]
pub mod rabbitmq;
#[cfg(feature = "mq-redis")]
pub mod redis;

#[cfg(feature = "mq-kafka")]
pub use kafka::KafkaProducer;
#[cfg(feature = "mq-rabbitmq")]
pub use rabbitmq::RabbitMqProducer;
#[cfg(feature = "mq-redis")]
pub use redis::{RedisMode, RedisProducer};

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
#[cfg(feature = "mq-redis")]
use serde::Serialize;

use crate::error::Error;
#[cfg(feature = "mq-redis")]
use crate::model::Base64;
use crate::model::{Email, generate_id};

/// A message ready to be published to a queue.
///
/// Mirrors the Go server's `queue.Message` (the generic message type used by
/// every driver) one-to-one. The payload is the email JSON; [`MqMessage::from_email`]
/// fills everything from an [`Email`].
///
/// ```json
/// {
///   "id": "e8a93bf84c379a20",
///   "topic": "mailqueue",
///   "payload": "{...email json...}",
///   "headers": {"Content-Type": "application/json"},
///   "key": "optional-partition-key",
///   "delay": 0,
///   "timestamp": "2026-08-16T12:34:56.789012345+08:00",
///   "attempts": 1
/// }
/// ```
#[derive(Clone, Debug)]
pub struct MqMessage {
    /// Unique message id; the server's consumer uses it for logging, metrics
    /// and de-duplication.
    pub id: String,
    /// Destination topic. Empty falls back to the producer's default
    /// destination (queue name / redis key / kafka topic).
    pub topic: String,
    /// Raw payload 鈥?the email JSON for [`MqMessage::from_email`].
    pub payload: Vec<u8>,
    /// Metadata headers, merged into driver-specific headers on publish.
    pub headers: HashMap<String, String>,
    /// Partition / sharding key (used by Kafka, RabbitMQ routing key).
    pub key: String,
    /// Duration to wait before publishing (also honored per-driver).
    pub delay: Duration,
    /// Message generation timestamp (RFC3339 in the Redis envelope).
    pub timestamp: DateTime<Utc>,
    /// Delivery attempt counter; 1 for freshly created messages.
    pub attempts: u32,
}

impl MqMessage {
    /// Builds a queue message from an [`Email`].
    ///
    /// This is the exact payload the server's `sendAsync` handler enqueues:
    ///
    /// - `payload` = `email.to_json()` (the shared wire format);
    /// - `id` = `email.id`, or a freshly generated 32-hex-char id when empty;
    /// - `headers` = `Content-Type: application/json` plus the email's custom
    ///   headers;
    /// - `topic` = empty (falls back to the producer's default destination);
    /// - `attempts` = 1.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Json`] if the email cannot be serialized (cannot
    /// happen for `Email`, but the call can still fail for future types).
    ///
    /// # Example
    ///
    /// ```rust
    /// use mailbaby::Email;
    /// use mailbaby::mq::MqMessage;
    ///
    /// let email = Email::builder("Hi").to(["a@example.com"]).build().unwrap();
    /// let message = MqMessage::from_email(&email).unwrap();
    /// assert_eq!(message.headers["Content-Type"], "application/json");
    /// assert_eq!(message.attempts, 1);
    /// assert_eq!(message.id.len(), 32); // generated when email.id is empty
    /// ```
    pub fn from_email(email: &Email) -> Result<Self, Error> {
        let payload = email.to_json()?;
        let id = if email.id.is_empty() {
            generate_id()
        } else {
            email.id.clone()
        };
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        for (k, v) in &email.headers {
            headers.insert(k.clone(), v.clone());
        }
        Ok(MqMessage {
            id,
            topic: String::new(),
            payload,
            headers,
            key: String::new(),
            delay: Duration::ZERO,
            timestamp: Utc::now(),
            attempts: 1,
        })
    }

    /// Overrides or adds a metadata header.
    pub fn set_header(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.headers.insert(key.into(), value.into());
    }

    /// Decodes the payload as an [`Email`], e.g. when re-publishing a message
    /// fetched from a queue.
    pub fn to_email(&self) -> Result<Email, Error> {
        Ok(serde_json::from_slice(&self.payload)?)
    }
}

/// Per-publish overrides for [`MqProducer::publish`].
///
/// Mirrors the Go server's `queue.PublishOptions`:
///
/// | Field | Driver usage |
/// |---|---|
/// | `topic` | RabbitMQ routing key, Redis key/stream, Kafka topic |
/// | `headers` | AMQP table entries, Redis envelope `headers`, Kafka headers |
/// | `key` | RabbitMQ routing key (overrides topic), Kafka record key |
/// | `delay` | delays the publish by spawning a background task |
///
/// # Example
///
/// ```rust
/// use mailbaby::mq::PublishOptions;
/// use std::collections::HashMap;
///
/// let options = PublishOptions {
///     topic: Some("mailqueue_high".to_string()),
///     headers: HashMap::from([("X-Priority".to_string(), "1".to_string())]),
///     ..Default::default()
/// };
/// ```
#[derive(Clone, Debug, Default)]
pub struct PublishOptions {
    /// Destination override (routing key / redis key / kafka topic).
    pub topic: Option<String>,
    /// Extra headers merged on top of the message's own headers.
    pub headers: HashMap<String, String>,
    /// Partition / routing key override.
    pub key: Option<String>,
    /// Publish after this delay (background task, fire-and-forget).
    pub delay: Option<Duration>,
}

/// Common contract of all MQ producers.
///
/// Implemented by [`RabbitMqProducer`], [`RedisProducer`] and
/// [`KafkaProducer`], so publish code can be written generically:
///
/// ```rust,no_run
/// use mailbaby::Email;
/// use mailbaby::mq::{MqMessage, MqProducer, PublishOptions};
///
/// # async fn example<P: MqProducer>(producer: &P) -> Result<(), mailbaby::Error> {
/// let email = Email::builder("Alert").to(["oncall@example.com"]).build()?;
/// producer
///     .publish(&MqMessage::from_email(&email)?, &PublishOptions::default())
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// Implementors must be `Send + Sync`, allowing producers to be shared
/// between tasks (e.g. behind `Arc`).
pub trait MqProducer: Send + Sync {
    /// Publishes a message with optional per-publish overrides.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Mq`] on transport/publish failures.
    fn publish(
        &self,
        message: &MqMessage,
        options: &PublishOptions,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

/// Redis envelope serialized as the `data` field of a stream entry, list
/// element or pubsub message 鈥?the exact structure the Go server's
/// `redisMessageEnvelope` produces and its consumers parse.
///
/// `payload` is the email JSON, base64-encoded like Go's `[]byte` JSON
/// marshaling; `timestamp` is RFC3339 with nanoseconds.
#[cfg(feature = "mq-redis")]
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct RedisEnvelope {
    pub id: String,
    pub topic: String,
    pub payload: Base64,
    pub headers: HashMap<String, String>,
    pub timestamp: String,
    pub attempts: u32,
}

#[cfg(feature = "mq-redis")]
impl RedisEnvelope {
    /// Builds the envelope for a publish, resolving destination, headers and
    /// delay exactly like the Go producer (options > message > defaults).
    pub(crate) fn from_publish(
        message: &MqMessage,
        options: &PublishOptions,
        default_topic: &str,
    ) -> Self {
        let topic = options
            .topic
            .clone()
            .or_else(|| (!message.topic.is_empty()).then(|| message.topic.clone()))
            .unwrap_or_else(|| default_topic.to_string());

        let mut headers = message.headers.clone();
        for (k, v) in &options.headers {
            headers.insert(k.clone(), v.clone());
        }

        RedisEnvelope {
            id: message.id.clone(),
            topic,
            payload: Base64(message.payload.clone()),
            headers,
            timestamp: message
                .timestamp
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, false),
            attempts: message.attempts,
        }
    }
}

/// Resolves the effective delay for a publish (options win, like Go).
#[cfg(feature = "mq-redis")]
pub(crate) fn effective_delay(message: &MqMessage, options: &PublishOptions) -> Duration {
    options.delay.unwrap_or(message.delay)
}
