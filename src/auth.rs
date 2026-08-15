//! API-key authentication for MailBaby requests (`rest` feature).
//!
//! MailBaby can require a secret key on its HTTP and gRPC endpoints
//! (`auth.enabled: true` in the server config). The server accepts the key in
//! several places, all of which this module supports:
//!
//! 1. a custom header (default `X-API-Key`, configurable via `auth.header_name`),
//! 2. `Authorization: Bearer <key>`,
//! 3. query parameters `?api_key=<key>` (fallback).
//!
//! Construct an [`Auth`] and hand it to the client builder:
//!
//! ```rust,no_run
//! use mailbaby::Auth;
//! use mailbaby::rest::MailBabyClient;
//!
//! # async fn example() -> Result<(), mailbaby::Error> {
//! // Default: X-API-Key header
//! let client = MailBabyClient::new("http://localhost:8080", Some("s3cret"))?;
//!
//! // Custom header name (must match server auth.header_name)
//! let auth = Auth::api_key("s3cret").with_header("X-My-Key");
//! let client = MailBabyClient::builder("http://localhost:8080").auth(auth).build()?;
//!
//! // Bearer token style
//! let auth = Auth::api_key("s3cret").bearer();
//! let client = MailBabyClient::builder("http://localhost:8080").auth(auth).build()?;
//! # Ok(())
//! # }
//! ```

/// How the API key is presented to the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthScheme {
    /// Custom header — `X-API-Key` (or the name from [`Auth::with_header`]).
    Header,
    /// `Authorization: Bearer <key>`.
    Bearer,
    /// Query string parameter `?api_key=<key>`.
    Query,
}

/// Secret-key authentication for MailBaby REST requests.
///
/// The server-side counterpart is the `auth` section of the MailBaby
/// configuration (`enabled`, `secret_key`, `header_name`). Authentication is
/// skipped entirely when the server runs with `auth.enabled: false`.
///
/// # Security
///
/// Prefer a header-based scheme over [`Auth::query`]: query strings end up in
/// access logs and proxy logs. The header schemes are honored by the server
/// in constant time (`crypto/subtle`), matching this crate's expectations.
#[derive(Clone, Debug)]
pub struct Auth {
    key: String,
    header_name: String,
    scheme: AuthScheme,
}

impl Auth {
    /// Creates an API key auth that uses the `X-API-Key` header.
    ///
    /// This is the most common setup and matches the server default
    /// (`auth.header_name: "X-API-Key"`).
    ///
    /// # Example
    ///
    /// ```rust
    /// use mailbaby::Auth;
    /// let auth = Auth::api_key("your_secret_key");
    /// ```
    pub fn api_key(key: impl Into<String>) -> Self {
        Auth {
            key: key.into(),
            header_name: "X-API-Key".to_string(),
            scheme: AuthScheme::Header,
        }
    }

    /// Uses a custom header name instead of `X-API-Key`.
    ///
    /// Must match the server's `auth.header_name` setting. Switches the scheme
    /// to [`AuthScheme::Header`].
    ///
    /// # Example
    ///
    /// ```rust
    /// use mailbaby::Auth;
    /// let auth = Auth::api_key("s3cret").with_header("X-Api-Token");
    /// ```
    pub fn with_header(mut self, header_name: impl Into<String>) -> Self {
        self.header_name = header_name.into();
        self.scheme = AuthScheme::Header;
        self
    }

    /// Uses `Authorization: Bearer <key>` instead of the custom header.
    ///
    /// The server's gRPC endpoint only checks this header plus `x-api-key`,
    /// so this scheme is also the natural choice for gRPC clients.
    ///
    /// # Example
    ///
    /// ```rust
    /// use mailbaby::Auth;
    /// let auth = Auth::api_key("s3cret").bearer();
    /// ```
    pub fn bearer(mut self) -> Self {
        self.scheme = AuthScheme::Bearer;
        self
    }

    /// Sends the key as a query string parameter (`?api_key=<key>`).
    ///
    /// The server accepts `api_key` and `token` query parameters as a
    /// fallback. **Prefer a header scheme**; see the type-level docs.
    ///
    /// # Example
    ///
    /// ```rust
    /// use mailbaby::Auth;
    /// let auth = Auth::api_key("s3cret").query();
    /// ```
    pub fn query(mut self) -> Self {
        self.scheme = AuthScheme::Query;
        self
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    #[cfg(feature = "rest")]
    pub(crate) fn header_name(&self) -> &str {
        &self.header_name
    }

    pub(crate) fn scheme(&self) -> AuthScheme {
        self.scheme
    }
}
