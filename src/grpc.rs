//! gRPC API client (`grpc` feature).
//!
//! Talks to the MailBaby gRPC server (`grpc.port`, default 50051) which serves
//! the `mailbaby.v1.MailService` service:
//!
//! | RPC | Client method |
//! |---|---|
//! | `Send` | [`GrpcClient::send`] |
//! | `SendBatch` | [`GrpcClient::send_batch`] |
//! | `Ping` | [`GrpcClient::ping`] |
//! | `HealthCheck` | [`GrpcClient::health_check`] |
//!
//! # Example
//!
//! ```rust,no_run
//! use mailbaby::Email;
//! use mailbaby::grpc::GrpcClient;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), mailbaby::Error> {
//!     let client = GrpcClient::connect("http://localhost:50051", Some("s3cret")).await?;
//!
//!     let email = Email::builder("Hello via gRPC")
//!         .to(["user@example.com"])
//!         .text_body("gRPC is fast")
//!         .build()?;
//!
//!     let resp = client.send(&email).await?;
//!     println!("delivered: id={} status={}", resp.id, resp.status);
//!
//!     let status = client.health_check().await?;
//!     println!("server: {status}");
//!
//!     Ok(())
//! }
//! ```
//!
//! # Errors
//!
//! - [`Error::Grpc`] — the server returned a non-`OK` gRPC status
//!   (`InvalidArgument`, `Unauthenticated`, `Internal`, ...);
//! - [`Error::Transport`] — the gRPC channel is unreachable.
//!
//! The generated protobuf types are re-exported under
//! [`mailbaby::v1`] for advanced use cases.
//!
//! > Only the client half is generated (`build_server(false)`); there is no
//! > server-side implementation in this crate.

use std::time::Duration;

use tonic::codegen::InterceptedService;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use crate::auth::{Auth, AuthScheme};
use crate::error::Error;
use crate::model::{BatchResponse, Email, SendResponse};

/// Generated protobuf types (`mailbaby.v1`), available when the `grpc` feature is enabled.
///
/// ```
/// pub mod mailbaby {
///     pub mod v1 {
///         include!(concat!(env!("OUT_DIR"), "/mailbaby.v1.rs"));
///     }
/// }
/// ```
///
/// These mirror the Go server's `proto/mailbaby.proto` one-to-one and are used
/// by [`GrpcClient`] internally; you generally do not need to touch them.
pub mod mailbaby {
    /// Generated protobuf types for `mailbaby.v1` (prost). Field docs are not
    /// generated; see the `.proto` sources for semantics.
    #[allow(missing_docs)]
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/mailbaby.v1.rs"));
    }
}

/// Injects the API key into every gRPC call's metadata.
///
/// The server reads the key from either the `authorization: Bearer <key>`
/// header or the `x-api-key` header — both are forwarded when a custom
/// header name is configured.
#[derive(Clone, Debug)]
struct AuthInterceptor {
    bearer: Option<String>,
    api_key: Option<String>,
}

impl AuthInterceptor {
    fn new(auth: Option<&Auth>) -> Self {
        let mut interceptor = AuthInterceptor {
            bearer: None,
            api_key: None,
        };
        if let Some(auth) = auth {
            match auth.scheme() {
                // The server's gRPC handler checks `authorization: Bearer` first.
                AuthScheme::Bearer | AuthScheme::Header => {
                    interceptor.bearer = Some(auth.key().to_string());
                }
                AuthScheme::Query => {}
            }
            // Always mirror the key into the server's `x-api-key` fallback.
            if !matches!(auth.scheme(), AuthScheme::Query) {
                interceptor.api_key = Some(auth.key().to_string());
            }
        }
        interceptor
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let metadata = request.metadata_mut();
        if let Some(bearer) = &self.bearer {
            let value: AsciiMetadataValue = format!("Bearer {bearer}").parse().map_err(|_| {
                Status::invalid_argument("api key contains invalid metadata characters")
            })?;
            metadata.insert("authorization", value);
        }
        if let Some(key) = &self.api_key {
            let value: AsciiMetadataValue = key.parse().map_err(|_| {
                Status::invalid_argument("api key contains invalid metadata characters")
            })?;
            metadata.insert("x-api-key", value);
        }
        Ok(request)
    }
}

/// Builder for [`GrpcClient`].
///
/// ```rust,no_run
/// use mailbaby::grpc::GrpcClient;
///
/// # async fn example() -> Result<(), mailbaby::Error> {
/// let client = GrpcClient::builder("http://localhost:50051")
///     .api_key("s3cret")
///     .connect_timeout(std::time::Duration::from_secs(5))
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct GrpcClientBuilder {
    addr: String,
    auth: Option<Auth>,
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    tls: bool,
}

impl GrpcClientBuilder {
    fn new(addr: impl Into<String>) -> Self {
        GrpcClientBuilder {
            addr: addr.into(),
            auth: None,
            timeout: None,
            connect_timeout: None,
            tls: false,
        }
    }

    /// Sets the API key authentication.
    ///
    /// Only [`AuthScheme::Header`] and [`AuthScheme::Bearer`] are meaningful
    /// over gRPC; [`Auth::query`] is ignored (documented server behavior).
    pub fn auth(mut self, auth: Auth) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Convenience for [`Self::auth`] with [`Auth::api_key`].
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.auth = Some(Auth::api_key(key));
        self
    }

    /// Sets the per-call timeout (default: 30 seconds).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the initial channel connection timeout (default: 10 seconds).
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Enables TLS (server must expose a TLS-enabled gRPC listener).
    ///
    /// When enabled, the address is rewritten to `https://` so the tonic
    /// transport uses the native TLS stack. Custom CA roots are not supported
    /// by this builder; use [`GrpcClient::connect_tls_custom`] for that.
    pub fn tls(mut self) -> Self {
        self.tls = true;
        self
    }

    /// Builds and connects the client channel.
    ///
    /// The channel is lazy — the first request establishes the actual
    /// connection — but a failed address scheme (e.g. `grpc://` without TLS
    /// enabled) surfaces here as [`Error::GrpcTransport`].
    pub async fn build(self) -> Result<GrpcClient, Error> {
        let channel =
            build_channel(&self.addr, self.tls, self.timeout, self.connect_timeout).await?;
        let inner = mailbaby::v1::mail_service_client::MailServiceClient::with_interceptor(
            channel,
            AuthInterceptor::new(self.auth.as_ref()),
        );
        Ok(GrpcClient {
            inner,
            timeout: self.timeout.unwrap_or(Duration::from_secs(30)),
        })
    }
}

/// gRPC client for the MailBaby `MailService`.
///
/// Cheap to clone (the channel is shared) and safe to use from multiple tasks
/// concurrently; tonic multiplexes all requests over the underlying
/// connection.
///
/// See the [module documentation](self) for a usage example.
#[derive(Clone, Debug)]
pub struct GrpcClient {
    inner: mailbaby::v1::mail_service_client::MailServiceClient<
        InterceptedService<Channel, AuthInterceptor>,
    >,
    timeout: Duration,
}

impl GrpcClient {
    /// Connects to the server at `addr` with optional API key auth.
    ///
    /// Defaults: plaintext channel, 30 s per-call timeout, 10 s connect
    /// timeout, `X-API-Key`/`authorization` auth injection.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mailbaby::grpc::GrpcClient;
    ///
    /// # async fn example() -> Result<(), mailbaby::Error> {
    /// // No auth (server auth.enabled = false)
    /// let client = GrpcClient::connect("http://localhost:50051", None).await?;
    ///
    /// // With secret key
    /// let client = GrpcClient::connect("http://localhost:50051", Some("s3cret")).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] when the address is not a valid URI, and
    /// [`Error::Transport`] when a TLS channel cannot be built.
    pub async fn connect(addr: impl Into<String>, api_key: Option<&str>) -> Result<Self, Error> {
        let mut builder = Self::builder(addr);
        if let Some(key) = api_key {
            builder = builder.api_key(key);
        }
        builder.build().await
    }

    /// Connects over TLS with a custom CA root (DER/PEM), for private deployments.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn example() -> Result<(), mailbaby::Error> {
    /// use mailbaby::grpc::GrpcClient;
    ///
    /// let ca = std::fs::read("ca.pem").map_err(|e| {
    ///     mailbaby::Error::GrpcTransport(format!("failed to read CA root: {e}"))
    /// })?;
    /// let client = GrpcClient::connect_tls_custom(
    ///     "mail.example.com:50051",
    ///     None,
    ///     ca,
    ///     "mail.example.com",
    /// )
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::Transport`] when the TLS root cannot be parsed or the client
    /// builder fails.
    pub async fn connect_tls_custom(
        addr: impl Into<String>,
        api_key: Option<&str>,
        ca_root: Vec<u8>,
        server_domain: impl Into<String>,
    ) -> Result<Self, Error> {
        let addr = addr.into();
        let server_domain = server_domain.into();
        let mut endpoint = Endpoint::from_shared(addr.clone())?;
        let mut tls = tonic::transport::ClientTlsConfig::new();
        tls = tls
            .ca_certificate(tonic::transport::Certificate::from_pem(&ca_root))
            .domain_name(&server_domain);
        endpoint = endpoint.tls_config(tls)?;
        let channel = endpoint.connect().await?;
        let auth = api_key.map(Auth::api_key);
        let inner = mailbaby::v1::mail_service_client::MailServiceClient::with_interceptor(
            channel,
            AuthInterceptor::new(auth.as_ref()),
        );
        Ok(GrpcClient {
            inner,
            timeout: Duration::from_secs(30),
        })
    }

    /// Starts a builder for full customization (auth, TLS, timeouts).
    pub fn builder(addr: impl Into<String>) -> GrpcClientBuilder {
        GrpcClientBuilder::new(addr)
    }

    /// Sends a single email synchronously.
    ///
    /// Maps to the `Send` RPC with `async = false`: the server blocks until
    /// the SMTP delivery round-trip completes. SMTP failures surface as
    /// [`Error::Grpc`] with code `Internal`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mailbaby::{Email, grpc::GrpcClient};
    ///
    /// # async fn example() -> Result<(), mailbaby::Error> {
    /// let client = GrpcClient::connect("http://localhost:50051", None).await?;
    /// let email = Email::builder("Test").to(["a@example.com"]).build()?;
    /// let resp = client.send(&email).await?;
    /// assert_eq!(resp.status, "sent");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(&self, email: &Email) -> Result<SendResponse, Error> {
        let mut request = Request::new(email_to_pb(email, false));
        self.apply_timeout(&mut request);
        let resp = self.inner.clone().send(request).await?;
        Ok(SendResponse {
            id: resp.get_ref().id.clone(),
            status: resp.get_ref().status.clone(),
            message: resp.get_ref().message.clone(),
            sent_at: resp.get_ref().sent_at,
        })
    }

    /// Queues a single email asynchronously (`Send` RPC with `async = true`).
    ///
    /// The server enqueues the job and returns immediately with status
    /// `queued`; delivery happens in the background.
    pub async fn send_async(&self, email: &Email) -> Result<SendResponse, Error> {
        let mut request = Request::new(email_to_pb(email, true));
        self.apply_timeout(&mut request);
        let resp = self.inner.clone().send(request).await?;
        Ok(SendResponse {
            id: resp.get_ref().id.clone(),
            status: resp.get_ref().status.clone(),
            message: resp.get_ref().message.clone(),
            sent_at: resp.get_ref().sent_at,
        })
    }

    /// Sends a batch of emails via the `SendBatch` RPC.
    ///
    /// The server executes the batch with parallel workers and reports
    /// per-item results; a failed email does **not** fail the call. Check
    /// [`BatchResponse::failed`] and `results[i].message` for failures.
    ///
    /// # Errors
    ///
    /// Request-level failures (empty batch, auth) surface as
    /// [`Error::Grpc`]; per-email failures are reported in the response.
    pub async fn send_batch(
        &self,
        emails: &[Email],
        async_flag: bool,
    ) -> Result<BatchResponse, Error> {
        let mut request = Request::new(mailbaby::v1::BatchSendMailRequest {
            emails: emails.iter().map(|e| email_to_pb(e, async_flag)).collect(),
            r#async: async_flag,
        });
        self.apply_timeout(&mut request);
        let resp = self.inner.clone().send_batch(request).await?;
        let pb = resp.get_ref();
        Ok(BatchResponse {
            total: pb.total,
            succeeded: pb.succeeded,
            failed: pb.failed,
            results: pb
                .results
                .iter()
                .map(|r| SendResponse {
                    id: r.id.clone(),
                    status: r.status.clone(),
                    message: r.message.clone(),
                    sent_at: r.sent_at,
                })
                .collect(),
        })
    }

    /// Checks service liveness via the `Ping` RPC.
    ///
    /// Returns the server version string on success. Fails as
    /// [`Error::Grpc`] when the server process is unhealthy.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mailbaby::grpc::GrpcClient;
    ///
    /// # async fn example() -> Result<(), mailbaby::Error> {
    /// let client = GrpcClient::connect("http://localhost:50051", None).await?;
    /// let version = client.ping().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ping(&self) -> Result<String, Error> {
        let mut request = Request::new(mailbaby::v1::PingRequest {
            message: String::new(),
        });
        self.apply_timeout(&mut request);
        let resp = self.inner.clone().ping(request).await?;
        Ok(resp.get_ref().version.clone())
    }

    /// Checks readiness via the `HealthCheck` RPC.
    ///
    /// Returns a human-readable string like `"SERVING"` or
    /// `"NOT_SERVING"`; the method fails as [`Error::Grpc`] when the server
    /// reports `NOT_SERVING` or `SERVICE_UNKNOWN`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mailbaby::grpc::GrpcClient;
    ///
    /// # async fn example() -> Result<(), mailbaby::Error> {
    /// let client = GrpcClient::connect("http://localhost:50051", None).await?;
    /// let status = client.health_check().await?;
    /// println!("status: {status}");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health_check(&self) -> Result<String, Error> {
        use mailbaby::v1::health_check_response::ServingStatus;
        let mut request = Request::new(mailbaby::v1::HealthCheckRequest {
            service: String::new(),
        });
        self.apply_timeout(&mut request);
        let resp = self.inner.clone().health_check(request).await?;
        let pb = resp.get_ref();
        let status = ServingStatus::try_from(pb.status).unwrap_or(ServingStatus::Unknown);
        match status {
            ServingStatus::Serving => Ok("SERVING".to_string()),
            ServingStatus::NotServing => Err(Error::Grpc {
                code: "Unavailable".to_string(),
                message: "server reports NOT_SERVING".to_string(),
            }),
            _ => Err(Error::Grpc {
                code: "Unavailable".to_string(),
                message: "server reports SERVICE_UNKNOWN".to_string(),
            }),
        }
    }

    fn apply_timeout<T>(&self, request: &mut Request<T>) {
        request.set_timeout(self.timeout);
    }
}

/// Converts the shared [`Email`] model into the protobuf wire type.
fn email_to_pb(email: &Email, async_flag: bool) -> mailbaby::v1::SendMailRequest {
    mailbaby::v1::SendMailRequest {
        id: email.id.clone(),
        account: email.account.clone(),
        from: email.from.clone(),
        from_name: email.from_name.clone(),
        reply_to: email.reply_to.clone(),
        to: email.to.clone(),
        cc: email.cc.clone(),
        bcc: email.bcc.clone(),
        subject: email.subject.clone(),
        text_body: email.text_body.clone(),
        html_body: email.html_body.clone(),
        headers: email.headers.clone(),
        attachments: email
            .attachments
            .iter()
            .map(|a| mailbaby::v1::Attachment {
                filename: a.filename.clone(),
                content_type: a.content_type.clone(),
                data: a.data.0.clone(),
                inline: a.inline,
                content_id: a.content_id.clone(),
            })
            .collect(),
        tags: email.tags.clone(),
        metadata: email.metadata.clone(),
        r#async: async_flag,
    }
}

async fn build_channel(
    addr: &str,
    tls: bool,
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
) -> Result<Channel, Error> {
    let scheme = if tls { "https" } else { "http" };
    let addr = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("{scheme}://{addr}")
    };
    let mut endpoint = Endpoint::from_shared(addr)?;
    endpoint = endpoint
        .timeout(timeout.unwrap_or(Duration::from_secs(30)))
        .connect_timeout(connect_timeout.unwrap_or(Duration::from_secs(10)));
    let channel = endpoint.connect().await?;
    Ok(channel)
}
