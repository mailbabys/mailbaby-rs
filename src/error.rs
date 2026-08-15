//! Error types for the MailBaby client.
//!
//! Every fallible operation in this crate returns [`Error`], a single error
//! enum that distinguishes between:
//!
//! - local validation failures ([`Error::Validation`]) — the email itself is malformed,
//! - JSON serialization failures ([`Error::Json`]),
//! - transport-level failures ([`Error::Transport`], `rest` feature),
//! - server-reported errors ([`Error::Api`], `rest` feature),
//! - gRPC status errors ([`Error::Grpc`], `grpc` feature),
//! - queue operation failures ([`Error::Mq`] / [`Error::MqConnect`], MQ features).
//!
//! [`Error`] implements [`std::error::Error`], so it composes with `anyhow`,
//! `thiserror`, `Box<dyn std::error::Error>` and the usual async error handling
//! patterns:
//!
//! ```rust,no_run
//! # use mailbaby::Error;
//! # async fn example() -> Result<(), anyhow::Error> {
//! # let client = mailbaby::rest::MailBabyClient::new("http://localhost:8080", None)?;
//! # let email = mailbaby::Email::builder("hi").to(["a@example.com"]).build()?;
//! let result = client.send(&email).await;
//! match result {
//!     Ok(resp) => println!("ok: {}", resp.id),
//!     Err(Error::Api { code, message, details }) => {
//!         eprintln!("server rejected request: {code} {message} {details:?}");
//!     }
//!     Err(err) => eprintln!("other failure: {err}"),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Note: the doc example above calls `client.send` inside `anyhow::Error`;
//! `Error` converts into it via `From<Error> for anyhow::Error`, which is
//! provided by the `anyhow` crate itself.

/// Errors returned by the MailBaby client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The email payload failed local validation before it could be sent.
    ///
    /// Mirrors the server-side `sender.Email.Validate()` rules:
    /// at least one recipient (`to`/`cc`/`bcc`) is required and all present
    /// addresses must be well-formed. Check [`Email::validate`](crate::Email::validate)
    /// for the exact rules.
    ///
    /// # Example
    ///
    /// ```rust
    /// use mailbaby::{Email, Error};
    ///
    /// let err = Email::builder("no recipients").build().unwrap_err();
    /// assert!(matches!(err, Error::Validation(_)));
    /// ```
    #[error("invalid email: {0}")]
    Validation(String),

    /// A JSON serialization or deserialization failure.
    ///
    /// Occurs when an email cannot be serialized to its JSON wire format
    /// (e.g. via [`Email::to_json`](crate::Email::to_json)) or when the server's
    /// response body cannot be parsed into the expected DTO.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// A network-level failure while talking to the MailBaby server.
    ///
    /// Wraps [`reqwest::Error`]: connection refused, DNS failure, TLS errors,
    /// timeouts, request/response body errors, etc. Retryable in most cases.
    ///
    /// Only present with the `rest` feature (the default).
    #[cfg(feature = "rest")]
    #[error("http transport error: {0}")]
    Transport(#[from] reqwest::Error),

    /// The MailBaby server responded with a non-success status code.
    ///
    /// Carries the HTTP status code, the machine-readable error code
    /// (e.g. `invalid_json`, `validation_error`, `delivery_failed`,
    /// `unauthorized`) and optional human-readable details.
    ///
    /// # Example
    ///
    /// ```rust
    /// use mailbaby::Error;
    ///
    /// fn report(err: &Error) {
    ///     if let Error::Api { code, message, details } = err {
    ///         println!("HTTP {code}: {message} {details:?}");
    ///     }
    /// }
    /// ```
    ///
    /// Only present with the `rest` feature (the default).
    #[cfg(feature = "rest")]
    #[error("api error {code}: {message}{}", Error::details_suffix(details))]
    Api {
        /// HTTP status code (e.g. 400, 401, 500).
        code: u16,
        /// Machine-readable error code returned by the server.
        message: String,
        /// Optional human-readable details (field `details`, or `message` for auth errors).
        details: Option<String>,
    },

    /// A gRPC invocation failed.
    ///
    /// Carries the gRPC status code name (e.g. `InvalidArgument`,
    /// `Unauthenticated`, `Internal`, `Unavailable`) and the status message.
    /// Only present with the `grpc` feature.
    #[cfg(feature = "grpc")]
    #[error("grpc error {code}: {message}")]
    Grpc {
        /// gRPC status code name.
        code: String,
        /// gRPC status message.
        message: String,
    },

    /// A gRPC transport-level failure (connection refused, TLS errors, ...).
    ///
    /// Only present with the `grpc` feature.
    #[cfg(feature = "grpc")]
    #[error("grpc transport error: {0}")]
    GrpcTransport(String),

    /// An MQ producer operation failed.
    ///
    /// Covers publish failures (broker rejections, serialization issues,
    /// unsupported modes, ...). Only present with any MQ feature
    /// (`mq-rabbitmq`, `mq-redis` or `mq-kafka`).
    #[cfg(any(feature = "mq-rabbitmq", feature = "mq-redis", feature = "mq-kafka"))]
    #[error("mq error: {0}")]
    Mq(String),

    /// A message-queue connection could not be established.
    ///
    /// Raised by the `connect` constructors of the MQ producers when the
    /// broker is unreachable, authentication fails, or the topology cannot be
    /// resolved. Only present with any MQ feature.
    #[cfg(any(feature = "mq-rabbitmq", feature = "mq-redis", feature = "mq-kafka"))]
    #[error("mq connection error: {0}")]
    MqConnect(String),
}

#[cfg(feature = "rest")]
impl Error {
    fn details_suffix(details: &Option<String>) -> String {
        details
            .as_deref()
            .filter(|d| !d.is_empty())
            .map(|d| format!(": {d}"))
            .unwrap_or_default()
    }
}

#[cfg(feature = "grpc")]
impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        Error::Grpc {
            code: status.code().to_string(),
            message: status.message().to_string(),
        }
    }
}

#[cfg(feature = "grpc")]
impl From<tonic::transport::Error> for Error {
    fn from(err: tonic::transport::Error) -> Self {
        Error::GrpcTransport(err.to_string())
    }
}
