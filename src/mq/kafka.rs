//! Kafka producer (`mq-kafka` feature, backed by [`rskafka`](https://docs.rs/rskafka)).
//!
//! Publishes email jobs to the same Kafka topic the MailBaby server consumes,
//! with the same record shape the Go server's `kafkaProducer` produces:
//!
//! - **topic** — `PublishOptions::topic` → `MqMessage::topic` → the producer's
//!   configured default topic;
//! - **key** — `PublishOptions::key` → `MqMessage::key` → `None`;
//! - **headers** — message headers merged with publish headers, plus an
//!   `X-Message-ID` header carrying the message id (when set), exactly like
//!   the Go producer;
//! - **timestamp** — the message timestamp as a Kafka record timestamp;
//! - **partition** — the producer publishes to a single fixed partition
//!   (default 0), which the server's seller/consumer group still reads.
//!
//! rskafka is a pure-Rust Kafka client — no `librdkafka` system dependency.
//!
//! # Example
//!
//! ```rust,no_run
//! use mailbaby::Email;
//! use mailbaby::mq::{KafkaProducer, MqMessage, MqProducer, PublishOptions};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), mailbaby::Error> {
//!     let producer = KafkaProducer::connect(
//!         "localhost:9092", // broker(s), comma-separated
//!         "mailqueue",      // default topic
//!         0,                // partition
//!     )
//!     .await?;
//!
//!     let email = Email::builder("Newsletter #7").to(["carol@example.com"]).build()?;
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
//! - [`Error::MqConnect`] — the broker list could not be reached or the topic
//!   is unknown (and the broker rejects auto-retry);
//! - [`Error::Mq`] — the produce request failed.
//!
//! # SASL / TLS
//!
//! Plaintext is the default. rskafka supports SASL and TLS through its
//! `ClientConfig` (`rskafka` re-exports it at the crate root) — for setups
//! that need them, construct the `rskafka::Client` yourself and use
//! [`KafkaProducer::with_client`].

use std::collections::BTreeMap;
use std::sync::Arc;

use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;

use super::{MqMessage, MqProducer, PublishOptions};
use crate::error::Error;

/// Kafka producer.
///
/// Cheap to clone (the underlying [`Client`] is shared) and safe to use from
/// multiple tasks concurrently.
///
/// See the [module documentation](self) for wire-format details.
#[derive(Clone, Debug)]
pub struct KafkaProducer {
    client: Arc<Client>,
    topic: String,
    partition: i32,
}

impl KafkaProducer {
    /// Connects to `brokers` for the given default `topic` and `partition`.
    ///
    /// # Arguments
    ///
    /// - `brokers` — comma-separated `host:port` list of Kafka brokers;
    /// - `topic` — default topic; per-message destinations override it;
    /// - `partition` — partition to publish to (0..partitions-1).
    ///
    /// # Errors
    ///
    /// Returns [`Error::MqConnect`] when the broker cannot be reached.
    pub async fn connect(brokers: &str, topic: &str, partition: i32) -> Result<Self, Error> {
        let brokers = brokers.split(',').map(str::to_string).collect::<Vec<_>>();
        let client = ClientBuilder::new(brokers)
            .build()
            .await
            .map_err(|e| Error::MqConnect(format!("kafka: client build failed: {e}")))?;
        Ok(KafkaProducer {
            client: Arc::new(client),
            topic: topic.to_string(),
            partition,
        })
    }

    /// Wraps a pre-configured rskafka [`Client`] (e.g. with SASL/TLS custom
    /// settings).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use mailbaby::mq::KafkaProducer;
    ///
    /// # async fn example() -> Result<(), mailbaby::Error> {
    /// let client = rskafka::client::ClientBuilder::new(vec!["host:9092".to_string()])
    ///     .build()
    ///     .await
    ///     .map_err(|e| mailbaby::Error::MqConnect(format!("kafka: {e}")))?;
    ///
    /// let producer = KafkaProducer::with_client(client, "mailqueue", 0);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_client(client: Client, topic: &str, partition: i32) -> Self {
        KafkaProducer {
            client: Arc::new(client),
            topic: topic.to_string(),
            partition,
        }
    }
}

impl MqProducer for KafkaProducer {
    async fn publish(&self, message: &MqMessage, options: &PublishOptions) -> Result<(), Error> {
        let topic = options
            .topic
            .as_deref()
            .filter(|t| !t.is_empty())
            .or_else(|| (!message.topic.is_empty()).then_some(message.topic.as_str()))
            .unwrap_or(&self.topic);

        let key = options
            .key
            .as_deref()
            .filter(|k| !k.is_empty())
            .or_else(|| (!message.key.is_empty()).then_some(message.key.as_str()));

        let mut headers = message.headers.clone();
        for (k, v) in &options.headers {
            headers.insert(k.clone(), v.clone());
        }
        if !message.id.is_empty() {
            headers.insert("X-Message-ID".to_string(), message.id.clone());
        }

        let record = Record {
            key: key.map(|k| k.as_bytes().to_vec()),
            value: Some(message.payload.clone()),
            headers: headers
                .into_iter()
                .map(|(k, v)| (k, v.into_bytes()))
                .collect::<BTreeMap<_, _>>(),
            timestamp: message.timestamp,
        };

        let partition = self
            .client
            .partition_client(
                topic.to_string(),
                self.partition,
                UnknownTopicHandling::Retry,
            )
            .await
            .map_err(|e| Error::Mq(format!("kafka: partition client failed: {e}")))?;

        partition
            .produce(
                vec![record],
                rskafka::client::partition::Compression::default(),
            )
            .await
            .map_err(|e| Error::Mq(format!("kafka: produce failed: {e}")))?;
        Ok(())
    }
}
