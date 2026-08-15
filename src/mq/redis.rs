//! Redis producer (`mq-redis` feature) — Stream, List and PubSub modes.
//!
//! Publishes email jobs into Redis in one of three modes, matching the Go
//! server's `redisProducer` exactly:
//!
//! | Mode | Command | Payload |
//! |---|---|---|
//! | [`RedisMode::Stream`] | `XADD <key> * id <id> payload <b64> data <json>` | envelope JSON in the `data` field, base64 payload in `payload` |
//! | [`RedisMode::List`] | `RPUSH <key> <json>` | raw envelope JSON |
//! | [`RedisMode::PubSub`] | `PUBLISH <key> <json>` | raw envelope JSON |
//!
//! The envelope is the server's `redisMessageEnvelope` serialized to JSON:
//!
//! ```json
//! {
//!   "id": "e8a93bf84c379a20",
//!   "topic": "mailqueue",
//!   "payload": "<base64 of email JSON>",
//!   "headers": {"Content-Type": "application/json"},
//!   "timestamp": "2026-08-16T12:34:56.789012345+08:00",
//!   "attempts": 1
//! }
//! ```
//!
//! The destination key resolves as `PublishOptions::topic` → `MqMessage::topic`
//! → the producer's configured key (same precedence as the Go producer).
//!
//! # Example
//!
//! ```rust,no_run
//! use mailbaby::Email;
//! use mailbaby::mq::{MqMessage, MqProducer, PublishOptions, RedisProducer, RedisMode};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), mailbaby::Error> {
//!     let producer = RedisProducer::connect(
//!         "redis://127.0.0.1:6379",
//!         "mailqueue",
//!         RedisMode::Stream,
//!     )
//!     .await?;
//!
//!     let email = Email::builder("Hi again").to(["bob@example.com"]).build()?;
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
//! - [`Error::MqConnect`] — the Redis server is unreachable or the URL is invalid;
//! - [`Error::Mq`] — the Redis command failed.
//!
//! TLS and redis:// URL forms are supported via the `redis` crate's `RedisURL`
//! parsing (`rediss://` works when the server exposes TLS).

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, RedisResult};

use super::{MqMessage, MqProducer, PublishOptions, RedisEnvelope, effective_delay};
use crate::error::Error;

/// How the producer writes into Redis.
///
/// Mirrors the server's `queue.driver.redis.mode` configuration
/// (`stream` / `list` / `pubsub`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RedisMode {
    /// Redis Streams — `XADD <key> * id ... payload ... data ...`.
    #[default]
    Stream,
    /// Plain list — `RPUSH <key> <envelope>`.
    List,
    /// Pub/Sub — `PUBLISH <key> <envelope>`.
    PubSub,
}

impl RedisMode {
    /// The string form used in the server's configuration file.
    pub fn as_str(self) -> &'static str {
        match self {
            RedisMode::Stream => "stream",
            RedisMode::List => "list",
            RedisMode::PubSub => "pubsub",
        }
    }
}

impl TryFrom<&str> for RedisMode {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Error> {
        match value {
            "stream" => Ok(RedisMode::Stream),
            "list" => Ok(RedisMode::List),
            "pubsub" => Ok(RedisMode::PubSub),
            other => Err(Error::Mq(format!(
                "redis: unsupported mode {other:?} (expected stream, list or pubsub)"
            ))),
        }
    }
}

/// Redis producer.
///
/// Cheap to clone and safe to share between tasks; each clone manages its own
/// connection to the server via the `redis` crate's connection manager.
///
/// See the [module documentation](self) for wire-format details.
#[derive(Clone, Debug)]
pub struct RedisProducer {
    conn: ConnectionManager,
    key: String,
    mode: RedisMode,
}

impl RedisProducer {
    /// Connects to Redis for the given key and mode.
    ///
    /// # Arguments
    ///
    /// - `url` — Redis URL, e.g. `redis://127.0.0.1:6379/0` (`/0` = DB 0);
    /// - `key` — default stream/list/channel key;
    /// - `mode` — [`RedisMode::Stream`], [`RedisMode::List`] or
    ///   [`RedisMode::PubSub`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::MqConnect`] when the URL is invalid or the server is
    /// unreachable.
    pub async fn connect(url: &str, key: &str, mode: RedisMode) -> Result<Self, Error> {
        let client = redis::Client::open(url)
            .map_err(|e| Error::MqConnect(format!("redis: invalid URL: {e}")))?;
        let conn = client
            .get_connection_manager()
            .await
            .map_err(|e| Error::MqConnect(format!("redis: connect failed: {e}")))?;
        Ok(RedisProducer {
            conn,
            key: key.to_string(),
            mode,
        })
    }

    /// Runs the mode-specific write command with the pre-serialized payload.
    async fn publish_once(
        conn: &mut ConnectionManager,
        mode: RedisMode,
        key: &str,
        id: &str,
        payload_b64: &str,
        raw: Vec<u8>,
    ) -> RedisResult<()> {
        match mode {
            RedisMode::Stream => {
                // XADD key * id <id> payload <b64> data <json>
                redis::cmd("XADD")
                    .arg(key)
                    .arg("*")
                    .arg("id")
                    .arg(id)
                    .arg("payload")
                    .arg(payload_b64)
                    .arg("data")
                    .arg(raw)
                    .query_async::<String>(conn)
                    .await
                    .map(|_| ())
            }
            RedisMode::List => conn.rpush(key, raw).await.map(|_: i64| ()),
            RedisMode::PubSub => conn.publish(key, raw).await.map(|_: i64| ()),
        }
    }
}

impl MqProducer for RedisProducer {
    async fn publish(&self, message: &MqMessage, options: &PublishOptions) -> Result<(), Error> {
        let envelope = RedisEnvelope::from_publish(message, options, &self.key);
        let raw = serde_json::to_vec(&envelope)?;
        let payload_b64 = STANDARD.encode(&envelope.payload.0);
        let delay = effective_delay(message, options);

        if delay > std::time::Duration::ZERO {
            // Fire-and-forget delayed publish, like the Go producer's
            // goroutine: the caller gets Ok immediately.
            let mut conn = self.conn.clone();
            let mode = self.mode;
            let key = self.key.clone();
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = Self::publish_once(&mut conn, mode, &key, &envelope.id, &payload_b64, raw)
                    .await;
            });
            return Ok(());
        }

        let mut conn = self.conn.clone();
        Self::publish_once(
            &mut conn,
            self.mode,
            &self.key,
            &envelope.id,
            &payload_b64,
            raw,
        )
        .await
        .map_err(|e| Error::Mq(format!("redis: publish failed: {e}")))?;
        Ok(())
    }
}
