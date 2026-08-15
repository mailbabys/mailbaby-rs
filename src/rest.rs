//! HTTP REST API client (`rest` feature, enabled by default).
//!
//! Talks to the MailBaby unified HTTP server (`server.port`, default 8080):
//!
//! | Endpoint | Method | Client method |
//! |---|---|---|
//! | `/v1/email/send` | `POST` | [`MailBabyClient::send`], [`MailBabyClient::send_async`] |
//! | `/v1/email/batch` | `POST` | [`MailBabyClient::send_batch`] |
//! | `/livez` | `GET` | [`MailBabyClient::live`] |
//! | `/readyz` | `GET` | [`MailBabyClient::ready`] |
//!
//! # Example
//!
//! ```rust,no_run
//! use mailbaby::Email;
//! use mailbaby::rest::MailBabyClient;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), mailbaby::Error> {
//!     let client = MailBabyClient::new("http://localhost:8080", Some("your_secret_key"))?;
//!
//!     let email = Email::builder("Welcome aboard!")
//!         .to(["user@example.com"])
//!         .html_body("<h1>Welcome!</h1>")
//!         .build()?;
//!
//!     // Synchronous delivery (blocks until the server SMTP round-trip finishes)
//!     let resp = client.send(&email).await?;
//!     println!("delivered: id={} status={}", resp.id, resp.status);
//!
//!     // Asynchronous dispatch (202 Accepted, queued immediately)
//!     let resp = client.send_async(&email).await?;
//!     println!("queued: id={} status={}", resp.id, resp.status);
//!
//!     // Batch of emails, delivered in parallel by the server
//!     let emails = vec![email];
//!     let batch = client.send_batch(&emails, false).await?;
//!     println!("batch: {}/{} succeeded", batch.succeeded, batch.total);
//!
//!     Ok(())
//! }
//! ```
//!
//! # Errors
//!
//! - [`Error::Transport`] — network-level failures (connection refused, timeouts, TLS);
//! - [`Error::Api`] — the server answered with a non-2xx status
//!   (`validation_error`, `unauthorized`, `delivery_failed`, ...);
//! - [`Error::Json`] — malformed response body.

use std::time::Duration;

use reqwest::RequestBuilder;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::auth::{Auth, AuthScheme};
use crate::error::Error;
use crate::model::{ApiErrorBody, BatchResponse, Email, SendResponse};

/// Request body of `POST /v1/email/batch` (internal wire format).
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct BatchEmailRequest {
    emails: Vec<Email>,
    #[serde(rename = "async")]
    async_flag: bool,
}

/// Builder for [`MailBabyClient`].
///
/// Lets you customize the underlying [`reqwest::Client`] (connection pooling,
/// proxies, TLS, retries) and the authentication scheme before building:
///
/// ```rust,no_run
/// use mailbaby::rest::MailBabyClient;
///
/// # fn main() -> Result<(), mailbaby::Error> {
/// let client = MailBabyClient::builder("http://localhost:8080")
///     .api_key("s3cret")
///     .timeout(std::time::Duration::from_secs(30))
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct MailBabyClientBuilder {
    base_url: String,
    auth: Option<Auth>,
    http: Option<reqwest::Client>,
    timeout: Option<Duration>,
}

impl MailBabyClientBuilder {
    fn new(base_url: impl Into<String>) -> Self {
        MailBabyClientBuilder {
            base_url: base_url.into(),
            auth: None,
            http: None,
            timeout: None,
        }
    }

    /// Sets the API key authentication (default scheme: `X-API-Key` header).
    ///
    /// See [`Auth`] for the available schemes.
    pub fn auth(mut self, auth: Auth) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Convenience for [`Self::auth`] with [`Auth::api_key`] (header scheme).
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.auth = Some(Auth::api_key(key));
        self
    }

    /// Uses a caller-provided [`reqwest::Client`].
    ///
    /// This is the escape hatch for advanced HTTP settings: proxies, custom
    /// TLS roots, redirect policies, retry middleware, etc. Note that any
    /// timeout configured via [`Self::timeout`] is ignored when a client is
    /// supplied (the supplied client's own settings win).
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.http = Some(client);
        self
    }

    /// Sets the per-request timeout (default: 30 seconds).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Builds the client.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] when the base URL cannot be parsed as an
    /// HTTP(S) URL.
    pub fn build(self) -> Result<MailBabyClient, Error> {
        let base = normalize_base_url(&self.base_url);
        let http = match self.http {
            Some(client) => client,
            None => {
                let mut builder = reqwest::Client::builder();
                if let Some(timeout) = self.timeout {
                    builder = builder.timeout(timeout);
                } else {
                    builder = builder.timeout(Duration::from_secs(30));
                }
                builder.build()?
            }
        };
        Ok(MailBabyClient {
            http,
            base,
            auth: self.auth,
        })
    }
}

/// HTTP REST client for the MailBaby server.
///
/// Cheap to clone (the underlying [`reqwest::Client`] is shared) and safe to
/// use from multiple tasks concurrently. Connection pooling is handled by
/// `reqwest`; each client instance keeps one pooled connection per host.
///
/// See the [module documentation](self) for a usage example and the endpoint
/// table.
#[derive(Clone, Debug)]
pub struct MailBabyClient {
    http: reqwest::Client,
    base: String,
    auth: Option<Auth>,
}

impl MailBabyClient {
    /// Creates a client for the given base URL with optional API key auth.
    ///
    /// Defaults: 30 s request timeout, `X-API-Key` header scheme. For
    /// anything more elaborate use [`MailBabyClient::builder`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mailbaby::rest::MailBabyClient;
    ///
    /// // No auth (server auth.enabled = false)
    /// let client = MailBabyClient::new("http://localhost:8080", None)?;
    ///
    /// // Secret key via X-API-Key header
    /// let client = MailBabyClient::new("http://localhost:8080", Some("s3cret"))?;
    /// # Ok::<(), mailbaby::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] when the base URL is not a valid HTTP(S)
    /// URL, or [`Error::Transport`] when the default HTTP client cannot be
    /// constructed (rare).
    pub fn new(base_url: impl Into<String>, api_key: Option<&str>) -> Result<Self, Error> {
        let mut builder = Self::builder(base_url);
        if let Some(key) = api_key {
            builder = builder.api_key(key);
        }
        builder.build()
    }

    /// Starts a builder for full customization (auth scheme, timeout, HTTP client).
    pub fn builder(base_url: impl Into<String>) -> MailBabyClientBuilder {
        MailBabyClientBuilder::new(base_url)
    }

    /// Sends a single email synchronously.
    ///
    /// The server blocks until the SMTP delivery round-trip completes and
    /// answers `200 OK` with a [`SendResponse`] carrying status `sent`.
    /// Any SMTP failure surfaces as [`Error::Api`] with code 500 and the
    /// machine-readable code `delivery_failed`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mailbaby::{Email, rest::MailBabyClient};
    ///
    /// # async fn example() -> Result<(), mailbaby::Error> {
    /// let client = MailBabyClient::new("http://localhost:8080", None)?;
    /// let email = Email::builder("Test").to(["a@example.com"]).build()?;
    /// let resp = client.send(&email).await?;
    /// assert_eq!(resp.status, "sent");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(&self, email: &Email) -> Result<SendResponse, Error> {
        let rb = self
            .http
            .post(format!("{}/v1/email/send", self.base))
            .json(email);
        self.execute(rb).await
    }

    /// Queues a single email asynchronously.
    ///
    /// Equivalent to `send` with `?async=true`: the server appends the job to
    /// its message queue and answers `202 Accepted` immediately with status
    /// `queued`. Delivery then happens in the background on the server side.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mailbaby::{Email, rest::MailBabyClient};
    ///
    /// # async fn example() -> Result<(), mailbaby::Error> {
    /// let client = MailBabyClient::new("http://localhost:8080", None)?;
    /// let email = Email::builder("Test").to(["a@example.com"]).build()?;
    /// let resp = client.send_async(&email).await?;
    /// assert_eq!(resp.status, "queued");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_async(&self, email: &Email) -> Result<SendResponse, Error> {
        let rb = self
            .http
            .post(format!("{}/v1/email/send", self.base))
            .query(&[("async", "true")])
            .json(email);
        self.execute(rb).await
    }

    /// Sends a batch of emails.
    ///
    /// The server executes the batch with parallel workers and always answers
    /// `200 OK`; per-item results are reported in
    /// [`BatchResponse::results`] (same order as the input). Inspect
    /// `results[i].status` and `results[i].message` to find failures.
    ///
    /// Set `async_flag` to `true` to enqueue the whole batch instead.
    ///
    /// # Errors
    ///
    /// A request-level error (e.g. `empty_batch` with an empty slice, auth
    /// failure) surfaces as [`Error::Api`]. Per-email failures do **not**
    /// produce an error — check [`BatchResponse::failed`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mailbaby::{Email, rest::MailBabyClient};
    ///
    /// # async fn example() -> Result<(), mailbaby::Error> {
    /// let client = MailBabyClient::new("http://localhost:8080", None)?;
    /// let emails = vec![
    ///     Email::builder("Statement #1").to(["u1@example.com"]).build()?,
    ///     Email::builder("Statement #2").to(["u2@example.com"]).build()?,
    /// ];
    /// let batch = client.send_batch(&emails, false).await?;
    /// println!("{} succeeded, {} failed", batch.succeeded, batch.failed);
    /// for result in &batch.results {
    ///     println!("  {} -> {}", result.id, result.status);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_batch(
        &self,
        emails: &[Email],
        async_flag: bool,
    ) -> Result<BatchResponse, Error> {
        let body = BatchEmailRequest {
            emails: emails.to_vec(),
            async_flag,
        };
        let rb = self
            .http
            .post(format!("{}/v1/email/batch", self.base))
            .json(&body);
        self.execute(rb).await
    }

    /// Checks the server liveness probe (`GET /livez`).
    ///
    /// Returns `Ok(())` when the process is responsive, [`Error::Api`]
    /// otherwise.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mailbaby::rest::MailBabyClient;
    ///
    /// # async fn example() -> Result<(), mailbaby::Error> {
    /// let client = MailBabyClient::new("http://localhost:8080", None)?;
    /// client.live().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn live(&self) -> Result<(), Error> {
        let rb = self.http.get(format!("{}/livez", self.base));
        self.execute::<()>(rb).await
    }

    /// Checks the server readiness probe (`GET /readyz`).
    ///
    /// The server answers `200 OK` only when the consumer engine, the
    /// configured broker and the SMTP pools are all healthy. Returns
    /// [`Error::Api`] (code 503) while the server is not ready.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mailbaby::rest::MailBabyClient;
    ///
    /// # async fn example() -> Result<(), mailbaby::Error> {
    /// let client = MailBabyClient::new("http://localhost:8080", None)?;
    /// client.ready().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ready(&self) -> Result<(), Error> {
        let rb = self.http.get(format!("{}/readyz", self.base));
        self.execute::<()>(rb).await
    }

    /// Executes the request, applying auth and decoding the response.
    async fn execute<T: DeserializeOwned>(&self, rb: RequestBuilder) -> Result<T, Error> {
        let rb = self.apply_auth(rb);
        let resp = rb.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body: Option<ApiErrorBody> = resp.json().await.ok();
            return Err(match body {
                Some(body) => Error::Api {
                    code: status.as_u16(),
                    message: body.error,
                    details: body.details,
                },
                None => Error::Api {
                    code: status.as_u16(),
                    message: format!("http error {status}"),
                    details: None,
                },
            });
        }
        if std::mem::size_of::<T>() == 0 {
            // Health probes: no body to parse.
            return Ok(serde_json::from_str("null").unwrap_or_else(|_| unreachable!()));
        }
        Ok(resp.json().await?)
    }

    fn apply_auth(&self, rb: RequestBuilder) -> RequestBuilder {
        match &self.auth {
            None => rb,
            Some(auth) => match auth.scheme() {
                AuthScheme::Header => rb.header(auth.header_name(), auth.key()),
                AuthScheme::Bearer => rb.bearer_auth(auth.key()),
                AuthScheme::Query => rb.query(&[("api_key", auth.key())]),
            },
        }
    }
}

/// Strips a trailing slash so path joins never produce `//`.
fn normalize_base_url(base_url: &str) -> String {
    base_url.trim_end_matches('/').to_string()
}
