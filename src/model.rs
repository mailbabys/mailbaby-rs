//! Data models shared by all dispatch channels (REST, gRPC and MQ).
//!
//! The types in this module mirror the Go server's `sender.Email` struct
//! exactly, so the same payload works over every ingestion channel:
//!
//! - **REST**: serialized as the JSON body of `POST /v1/email/send` and
//!   `POST /v1/email/batch`;
//! - **gRPC**: converted into `mailbaby.v1.SendMailRequest`;
//! - **MQ**: serialized as the queue message payload consumed by the worker.
//!
//! Field names are `snake_case` on the wire (matching Go's `json` tags) and
//! attachment data is base64-encoded, as Go encodes `[]byte`.
//!
//! # Building an email
//!
//! Use [`Email::builder`] for a fluent, validation-checked construction:
//!
//! ```rust
//! use mailbaby::Email;
//!
//! let email = Email::builder("Welcome aboard!")
//!     .account("default")
//!     .from_with_name("noreply@example.com", "MailBaby")
//!     .reply_to("support@example.com")
//!     .to(["alice@example.com", "bob@example.com"])
//!     .cc(["manager@example.com"])
//!     .text_body("Welcome to our platform!")
//!     .html_body("<h1>Welcome!</h1><p>Thanks for joining.</p>")
//!     .header("X-Environment", "production")
//!     .attachment("logo.png", vec![0x89, 0x50, 0x4e, 0x47], None::<&str>)
//!     .inline_attachment("chart.svg", "chart_img", b"<svg/>".to_vec(), None::<&str>)
//!     .tag("onboarding")
//!     .metadata("user_id", "42")
//!     .build()
//!     .expect("email is valid");
//! ```
//!
//! [`Email::validate`] is also available directly for existing instances.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use base64::Engine;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Error;

/// Bytes that are encoded as standard base64 in JSON, matching Go's `[]byte` encoding.
///
/// Go's `encoding/json` marshals `[]byte` as a standard (padded) base64 string;
/// this wrapper makes that transparent in serde. It wraps the raw attachment
/// bytes and serializes to/from the base64 representation automatically.
///
/// # Example
///
/// ```rust
/// use mailbaby::Base64;
///
/// let data = Base64(vec![0x00, 0xff, 0x10]);
/// let json = serde_json::to_string(&data).unwrap();
/// assert_eq!(json, "\"AP8Q\"");
///
/// let back: Base64 = serde_json::from_str(&json).unwrap();
/// assert_eq!(back.0, vec![0x00, 0xff, 0x10]);
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Base64(pub Vec<u8>);

impl Serialize for Base64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let encoded = base64::engine::general_purpose::STANDARD.encode(&self.0);
        serializer.serialize_str(&encoded)
    }
}

impl<'de> Deserialize<'de> for Base64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .map_err(serde::de::Error::custom)?;
        Ok(Base64(bytes))
    }
}

/// A file attached to an email, either as a regular attachment or an inline CID resource.
///
/// Inline attachments (e.g. images) can be referenced from the HTML body via
/// `<img src="cid:<content_id>">`.
///
/// Wire format (`snake_case`, matching the Go server):
///
/// ```json
/// {
///   "filename": "metrics.png",
///   "content_type": "image/png",
///   "data": "<base64>",
///   "inline": true,
///   "content_id": "chart_img"
/// }
/// ```
///
/// Prefer the builder helpers [`EmailBuilder::attachment`] and
/// [`EmailBuilder::inline_attachment`], which fill in the content type from
/// the file extension when it is not given.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Attachment {
    /// File name as it appears in the recipient's mail client.
    pub filename: String,
    /// MIME type (e.g. `image/png`, `application/pdf`).
    pub content_type: String,
    /// File contents, base64-encoded on the wire.
    pub data: Base64,
    /// `true` for inline CID resources embedded in the HTML body.
    pub inline: bool,
    /// Content-ID used by `<img src="cid:...">` references (inline attachments only).
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub content_id: String,
}

/// An email message to be dispatched.
///
/// Field names and semantics mirror the Go server's `sender.Email` type, so
/// the same model can be sent over REST, gRPC or a message queue.
///
/// # Construction
///
/// [`Email::builder`] is the recommended entry point; it validates the
/// payload at build time (see [`Email::validate`]). For programmatic
/// construction, the struct itself implements `Serialize`/`Deserialize` and
/// `Default`, so it can also be deserialized from the server's JSON payload
/// format 鈥?useful when consuming queued messages.
///
/// # Wire format
///
/// ```json
/// {
///   "id": "optional-custom-id",
///   "account": "default",
///   "from": "noreply@example.com",
///   "from_name": "MailBaby System",
///   "reply_to": "support@example.com",
///   "to": ["alice@example.com"],
///   "cc": [],
///   "bcc": [],
///   "subject": "Order Confirmation #10024",
///   "text_body": "plain text fallback",
///   "html_body": "<h2>Order Confirmed</h2>",
///   "headers": {"X-Priority": "1"},
///   "attachments": [{"filename": "...", "content_type": "...", "data": "<base64>", "inline": false}],
///   "tags": ["order"],
///   "metadata": {"order_id": "10024"}
/// }
/// ```
///
/// Empty optional fields are omitted (`skip_serializing_if`), exactly like the
/// Go server's `omitempty` tags.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Email {
    /// Caller-provided message id; the server generates one when empty.
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub id: String,
    /// Target SMTP account (empty selects the server-side `default` account).
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub account: String,
    /// Envelope sender address; overrides the account's default `from`.
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub from: String,
    /// Sender display name (e.g. `"MailBaby System"`).
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub from_name: String,
    /// Reply-To address (optional).
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub reply_to: String,
    /// Primary recipients. At least one of `to`/`cc`/`bcc` is required.
    pub to: Vec<String>,
    /// Carbon-copy recipients.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub cc: Vec<String>,
    /// Blind carbon-copy recipients.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub bcc: Vec<String>,
    /// Email subject line (required).
    pub subject: String,
    /// Plain-text body; recommended as a fallback for HTML emails.
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub text_body: String,
    /// HTML body.
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub html_body: String,
    /// Custom MIME headers (e.g. `X-Priority`).
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub headers: HashMap<String, String>,
    /// Attachments and inline CID resources.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub attachments: Vec<Attachment>,
    /// Free-form tags (e.g. for metrics and filtering).
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub tags: Vec<String>,
    /// Arbitrary key-value metadata attached to the message.
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub metadata: HashMap<String, String>,
}

impl Email {
    /// Returns a builder for a new email with the given subject.
    ///
    /// The subject is the only required textual field besides at least one
    /// recipient. The builder validates the payload in
    /// [`EmailBuilder::build`].
    ///
    /// # Example
    ///
    /// ```rust
    /// use mailbaby::Email;
    /// let email = Email::builder("Hello").to(["a@example.com"]).build().unwrap();
    /// assert_eq!(email.subject, "Hello");
    /// ```
    pub fn builder(subject: impl Into<String>) -> EmailBuilder {
        EmailBuilder::new(subject)
    }

    /// Validates essential fields, mirroring the server-side `Validate()` rules:
    ///
    /// - at least one recipient overall (`to`, `cc` or `bcc` non-empty);
    /// - `from`, `reply_to` and every recipient must be well-formed addresses;
    ///   both bare (`user@example.com`) and display-name (`Name <user@example.com>`)
    ///   forms are accepted.
    ///
    /// Returns [`Error::Validation`] on the first violation.
    ///
    /// # Example
    ///
    /// ```rust
    /// use mailbaby::{Email, Error};
    ///
    /// let ok = Email::builder("hi").to(["alice@example.com"]).build().unwrap();
    /// assert!(ok.validate().is_ok());
    ///
    /// let err = Email::builder("hi").build().unwrap_err();
    /// assert!(matches!(err, Error::Validation(_)));
    /// ```
    pub fn validate(&self) -> Result<(), Error> {
        if self.to.is_empty() && self.cc.is_empty() && self.bcc.is_empty() {
            return Err(Error::Validation(
                "at least one recipient (to/cc/bcc) is required".into(),
            ));
        }
        if !self.from.is_empty() {
            validate_address(&self.from, "from")?;
        }
        if !self.reply_to.is_empty() {
            validate_address(&self.reply_to, "reply_to")?;
        }
        for (list, field) in [(&self.to, "to"), (&self.cc, "cc"), (&self.bcc, "bcc")] {
            for addr in list {
                validate_address(addr, field)?;
            }
        }
        Ok(())
    }

    /// Serializes the email to the JSON payload consumed by the server.
    ///
    /// The same wire format is used for the REST request body, the gRPC
    /// message payload and MQ ingestion 鈥?this is exactly what the Go server
    /// produces with `sender.Email.ToJSON()`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use mailbaby::Email;
    ///
    /// let email = Email::builder("hi").to(["a@example.com"]).build().unwrap();
    /// let payload = email.to_json().unwrap();
    /// assert!(String::from_utf8(payload).unwrap().contains("\"subject\":\"hi\""));
    /// ```
    pub fn to_json(&self) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_vec(self)?)
    }
}

/// Builder for [`Email`], mirroring the Go client's fluent API.
///
/// All methods are chainable and consume `self`, so build the email in one
/// expression and finish with [`build`](EmailBuilder::build), which validates
/// the payload:
///
/// ```rust
/// use mailbaby::Email;
///
/// let email = Email::builder("Alert")
///     .account("alert")
///     .from("alerts@example.com")
///     .to(["oncall@example.com"])
///     .text_body("High CPU load detected.")
///     .header("X-Priority", "1")
///     .build()
///     .unwrap();
/// ```
#[derive(Clone, Debug, Default)]
pub struct EmailBuilder {
    id: String,
    account: String,
    from: String,
    from_name: String,
    reply_to: String,
    to: Vec<String>,
    cc: Vec<String>,
    bcc: Vec<String>,
    subject: String,
    text_body: String,
    html_body: String,
    headers: HashMap<String, String>,
    attachments: Vec<Attachment>,
    tags: Vec<String>,
    metadata: HashMap<String, String>,
}

impl EmailBuilder {
    fn new(subject: impl Into<String>) -> Self {
        EmailBuilder {
            subject: subject.into(),
            ..Default::default()
        }
    }

    /// Sets a caller-provided message id.
    ///
    /// The server (and the MQ helpers) generate an id when this is empty, so
    /// this is only needed for idempotency or correlation with your own
    /// systems.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// Sets the target SMTP account name.
    ///
    /// The account must be declared in the server's `smtp` configuration;
    /// when empty, the server uses its `default` account.
    pub fn account(mut self, account: impl Into<String>) -> Self {
        self.account = account.into();
        self
    }

    /// Sets the envelope sender address.
    ///
    /// Overrides the account's default `from` address when given.
    pub fn from(mut self, from: impl Into<String>) -> Self {
        self.from = from.into();
        self
    }

    /// Sets the sender address and display name.
    ///
    /// # Example
    ///
    /// ```rust
    /// use mailbaby::Email;
    /// let email = Email::builder("x")
    ///     .from_with_name("noreply@example.com", "MailBaby System")
    ///     .to(["a@example.com"])
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(email.from_name, "MailBaby System");
    /// ```
    pub fn from_with_name(mut self, from: impl Into<String>, name: impl Into<String>) -> Self {
        self.from = from.into();
        self.from_name = name.into();
        self
    }

    /// Sets the Reply-To address.
    pub fn reply_to(mut self, reply_to: impl Into<String>) -> Self {
        self.reply_to = reply_to.into();
        self
    }

    /// Adds primary recipients.
    ///
    /// Accepts any iterable of `Into<String>`:
    ///
    /// ```rust
    /// use mailbaby::Email;
    /// let email = Email::builder("x")
    ///     .to(["a@example.com", "b@example.com"])
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(email.to.len(), 2);
    /// ```
    pub fn to<I, S>(mut self, addresses: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.to.extend(addresses.into_iter().map(Into::into));
        self
    }

    /// Adds carbon-copy recipients.
    pub fn cc<I, S>(mut self, addresses: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.cc.extend(addresses.into_iter().map(Into::into));
        self
    }

    /// Adds blind carbon-copy recipients.
    pub fn bcc<I, S>(mut self, addresses: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.bcc.extend(addresses.into_iter().map(Into::into));
        self
    }

    /// Sets the plain-text body.
    ///
    /// Recommended as a fallback for HTML emails; mail clients that cannot
    /// render HTML will show this instead.
    pub fn text_body(mut self, body: impl Into<String>) -> Self {
        self.text_body = body.into();
        self
    }

    /// Sets the HTML body.
    pub fn html_body(mut self, body: impl Into<String>) -> Self {
        self.html_body = body.into();
        self
    }

    /// Sets a custom MIME header.
    ///
    /// # Example
    ///
    /// ```rust
    /// use mailbaby::Email;
    /// let email = Email::builder("x")
    ///     .to(["a@example.com"])
    ///     .header("X-Priority", "1")
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(email.headers.get("X-Priority").unwrap(), "1");
    /// ```
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Attaches a file.
    ///
    /// The content type is inferred from the file extension when `content_type`
    /// is `None` (falling back to `application/octet-stream`), mirroring the
    /// Go server's `detectContentType`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use mailbaby::Email;
    /// let email = Email::builder("x")
    ///     .to(["a@example.com"])
    ///     .attachment("metrics.pdf", b"%PDF-1.4".to_vec(), None::<&str>)
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(email.attachments[0].content_type, "application/pdf");
    /// ```
    pub fn attachment(
        mut self,
        filename: impl Into<String>,
        data: Vec<u8>,
        content_type: Option<impl Into<String>>,
    ) -> Self {
        let filename = filename.into();
        let content_type = detect_content_type(&filename, content_type.map(Into::into));
        self.attachments.push(Attachment {
            filename,
            content_type,
            data: Base64(data),
            inline: false,
            content_id: String::new(),
        });
        self
    }

    /// Attaches an inline resource (e.g. an image referenced via `<img src="cid:...">`).
    ///
    /// The `content_id` is trimmed of surrounding `<>` characters if given;
    /// use it in the HTML body as `cid:<content_id>`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use mailbaby::Email;
    /// let email = Email::builder("x")
    ///     .to(["a@example.com"])
    ///     .html_body("<img src=\"cid:chart_img\">")
    ///     .inline_attachment("chart.png", "chart_img", vec![1, 2, 3], None::<&str>)
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(email.attachments[0].content_id, "chart_img");
    /// assert!(email.attachments[0].inline);
    /// ```
    pub fn inline_attachment(
        mut self,
        filename: impl Into<String>,
        content_id: impl Into<String>,
        data: Vec<u8>,
        content_type: Option<impl Into<String>>,
    ) -> Self {
        let filename = filename.into();
        let content_type = detect_content_type(&filename, content_type.map(Into::into));
        self.attachments.push(Attachment {
            filename,
            content_type,
            data: Base64(data),
            inline: true,
            content_id: content_id
                .into()
                .trim_matches(|c| c == '<' || c == '>')
                .to_string(),
        });
        self
    }

    /// Adds a tag.
    ///
    /// Tags are free-form strings surfaced in server metrics
    /// (`mailbaby_emails_sent_total{...}`) and useful for filtering.
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Adds a metadata entry.
    ///
    /// Metadata is carried with the message and available to consumers; it is
    /// not part of the MIME message itself.
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Builds the email, validating required fields.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] when there are no recipients or when any
    /// present address (`from`, `reply_to`, `to`, `cc`, `bcc`) is malformed.
    pub fn build(self) -> Result<Email, Error> {
        let email = Email {
            id: self.id,
            account: self.account,
            from: self.from,
            from_name: self.from_name,
            reply_to: self.reply_to,
            to: self.to,
            cc: self.cc,
            bcc: self.bcc,
            subject: self.subject,
            text_body: self.text_body,
            html_body: self.html_body,
            headers: self.headers,
            attachments: self.attachments,
            tags: self.tags,
            metadata: self.metadata,
        };
        email.validate()?;
        Ok(email)
    }
}

fn validate_address(addr: &str, field: &str) -> Result<(), Error> {
    if !is_valid_address(addr) {
        return Err(Error::Validation(format!(
            "invalid email address in {field}: {addr:?}"
        )));
    }
    Ok(())
}

/// Structural address check covering both bare (`user@example.com`) and
/// display-name (`Name <user@example.com>`) forms.
fn is_valid_address(addr: &str) -> bool {
    let addr = addr.trim();
    if addr.is_empty() {
        return false;
    }
    if addr.contains('<') || addr.contains('>') {
        let Some(open) = addr.rfind('<') else {
            return false;
        };
        if !addr.ends_with('>') || open == 0 {
            return false;
        }
        return is_valid_address(&addr[open + 1..addr.len() - 1]);
    }
    if addr.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return false;
    }
    let mut parts = addr.split('@');
    let local = parts.next().unwrap_or_default();
    let domain = parts.next().unwrap_or_default();
    !local.is_empty() && !domain.is_empty() && parts.next().is_none()
}

/// Infers a MIME type from the file extension, mirroring the Go server's
/// `detectContentType`; falls back to `application/octet-stream`.
fn detect_content_type(filename: &str, custom: Option<String>) -> String {
    if let Some(ct) = custom
        && !ct.trim().is_empty()
    {
        return ct;
    }
    let ext = Path::new(filename)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    let mime = match ext.as_deref() {
        Some("txt") => "text/plain",
        Some("html") | Some("htm") => "text/html",
        Some("css") => "text/css",
        Some("csv") => "text/csv",
        Some("xml") => "text/xml",
        Some("json") => "application/json",
        Some("pdf") => "application/pdf",
        Some("zip") => "application/zip",
        Some("gz") => "application/gzip",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("svg") => "image/svg+xml",
        Some("webp") => "image/webp",
        Some("bmp") => "image/bmp",
        Some("ico") => "image/x-icon",
        Some("js") => "text/javascript",
        Some("md") => "text/markdown",
        Some("rtf") => "application/rtf",
        Some("doc") => "application/msword",
        Some("docx") => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        Some("xls") => "application/vnd.ms-excel",
        Some("xlsx") => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        Some("ppt") => "application/vnd.ms-powerpoint",
        Some("pptx") => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        Some("mp3") => "audio/mpeg",
        Some("mp4") => "video/mp4",
        Some("wav") => "audio/wav",
        Some("eml") => "message/rfc822",
        _ => "application/octet-stream",
    };
    mime.to_string()
}

/// Generates a 32-char hex message id (used as MQ message id when the email has none).
///
/// Unique within this process (nanosecond timestamp + monotonic counter);
/// guaranteed to be exactly 32 lowercase hex characters, like the server's
/// `generateUUID()`.
#[cfg_attr(
    not(any(feature = "mq-rabbitmq", feature = "mq-redis", feature = "mq-kafka")),
    allow(dead_code)
)]
pub(crate) fn generate_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0) as u64;
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{counter:016x}{nanos:016x}")
}

/// Response of a single email delivery operation.
///
/// Returned by both `POST /v1/email/send` (REST) and the gRPC `Send` call.
/// `status` is one of:
///
/// - `sent` 鈥?delivered synchronously (HTTP 200);
/// - `queued` 鈥?accepted into the message queue (HTTP 202);
/// - `failed` 鈥?the batch endpoint reports per-item failures here.
///
/// `sent_at` is a Unix timestamp in **nanoseconds**.
///
/// ```json
/// {
///   "id": "e8a93bf84c379a20",
///   "status": "sent",
///   "message": "email sent successfully",
///   "sent_at": 1771142400000000000
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SendResponse {
    /// Message id (server-generated when the request carried none).
    pub id: String,
    /// Delivery status: `sent`, `queued` or `failed`.
    pub status: String,
    /// Human-readable status message.
    pub message: String,
    /// Unix timestamp in nanoseconds.
    pub sent_at: i64,
}

/// Response of a batch email delivery operation.
///
/// Returned by `POST /v1/email/batch` (REST) and the gRPC `SendBatch` call.
/// Each email in the request has a corresponding entry in `results` at the
/// same index; per-item failures are reported inline instead of failing the
/// whole request.
///
/// ```json
/// {
///   "total": 2,
///   "succeeded": 2,
///   "failed": 0,
///   "results": [
///     {"id": "4f9b2...", "status": "sent", "message": "email sent successfully", "sent_at": 1771142400000000000},
///     {"id": "6a8c1...", "status": "sent", "message": "email sent successfully", "sent_at": 1771142400000000000}
///   ]
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct BatchResponse {
    /// Number of emails in the request.
    pub total: i32,
    /// Number of successfully dispatched emails.
    pub succeeded: i32,
    /// Number of failed emails.
    pub failed: i32,
    /// One response per requested email, in request order.
    pub results: Vec<SendResponse>,
}

/// Error body returned by the REST API on non-2xx responses.
///
/// Two shapes exist on the server:
///
/// - handler errors: `{"code": 400, "error": "validation_error", "details": "..."}`
/// - auth errors: `{"code": 401, "error": "unauthorized", "message": "..."}`
///
/// This type reads both: `details` maps to either `details` or `message`.
///
/// # Example
///
/// ```rust
/// use mailbaby::ApiErrorBody;
///
/// let auth: ApiErrorBody = serde_json::from_str(
///     r#"{"code":401,"error":"unauthorized","message":"invalid token"}"#,
/// )
/// .unwrap();
/// assert_eq!(auth.code, 401);
/// assert_eq!(auth.details.as_deref(), Some("invalid token"));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ApiErrorBody {
    /// HTTP status code.
    pub code: i32,
    /// Machine-readable error code.
    pub error: String,
    /// `details` on validation/delivery errors, `message` on auth errors.
    #[serde(alias = "message", default)]
    pub details: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_builds_valid_email() {
        let email = Email::builder("Order Confirmation #10024")
            .id("e8a93bf84c379a20")
            .account("default")
            .from_with_name("noreply@example.com", "MailBaby System")
            .reply_to("support@example.com")
            .to(["alice@example.com", "bob@example.com"])
            .cc(["manager@example.com"])
            .text_body("Thank you for your order!")
            .html_body("<h2>Order Confirmed</h2>")
            .header("X-Priority", "1")
            .attachment("metrics.png", b"fake-png".to_vec(), None::<&str>)
            .inline_attachment("logo.svg", "logo_img", b"<svg/>".to_vec(), None::<&str>)
            .tag("order")
            .metadata("order_id", "10024")
            .build()
            .unwrap();

        assert_eq!(email.id, "e8a93bf84c379a20");
        assert_eq!(email.to.len(), 2);
        assert_eq!(email.attachments.len(), 2);
        assert_eq!(email.attachments[0].content_type, "image/png");
        assert!(email.attachments[1].inline);
        assert_eq!(email.attachments[1].content_id, "logo_img");
        assert_eq!(email.attachments[0].data, Base64(b"fake-png".to_vec()));
        assert!(email.validate().is_ok());
    }

    #[test]
    fn builder_rejects_missing_recipients() {
        let err = Email::builder("no recipients").build().unwrap_err();
        assert!(matches!(err, Error::Validation(_)));
    }

    #[test]
    fn builder_rejects_invalid_address() {
        let err = Email::builder("bad address")
            .to(["not an email"])
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::Validation(_)));

        let err = Email::builder("bad from")
            .to(["a@example.com"])
            .from("Name without angle <nope")
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::Validation(_)));
    }

    #[test]
    fn accepts_display_name_addresses() {
        let email = Email::builder("ok")
            .to(["Alice <alice@example.com>"])
            .from("MailBaby <noreply@example.com>")
            .build()
            .unwrap();
        assert!(email.validate().is_ok());
    }

    #[test]
    fn serializes_with_snake_case_and_base64() {
        let email = Email::builder("Test")
            .id("abc123")
            .account("default")
            .from_with_name("noreply@example.com", "MailBaby")
            .reply_to("support@example.com")
            .to(["alice@example.com"])
            .cc(["cc@example.com"])
            .bcc(["bcc@example.com"])
            .text_body("plain")
            .html_body("<b>rich</b>")
            .header("X-Priority", "1")
            .attachment("notes.txt", b"hello".to_vec(), None::<&str>)
            .tag("one")
            .tag("two")
            .metadata("k", "v")
            .build()
            .unwrap();

        let value: serde_json::Value = serde_json::to_value(&email).unwrap();
        assert_eq!(value["to"], serde_json::json!(["alice@example.com"]));
        assert_eq!(value["from_name"], "MailBaby");
        assert_eq!(value["reply_to"], "support@example.com");
        assert_eq!(value["html_body"], "<b>rich</b>");
        assert_eq!(value["headers"]["X-Priority"], "1");
        assert_eq!(value["attachments"][0]["filename"], "notes.txt");
        assert_eq!(value["attachments"][0]["content_type"], "text/plain");
        assert_eq!(value["attachments"][0]["data"], "aGVsbG8=");
        assert_eq!(value["attachments"][0]["inline"], false);
        assert_eq!(value["tags"], serde_json::json!(["one", "two"]));
        assert_eq!(value["metadata"]["k"], "v");
        assert!(value.get("async").is_none());
    }

    #[test]
    fn empty_optional_fields_are_omitted() {
        let email = Email::builder("bare")
            .to(["alice@example.com"])
            .build()
            .unwrap();
        let json = serde_json::to_string(&email).unwrap();
        assert!(json.contains("\"to\":[\"alice@example.com\"]"));
        assert!(json.contains("\"subject\":\"bare\""));
        assert!(!json.contains("cc"));
        assert!(!json.contains("html_body"));
        assert!(!json.contains("id"));
    }

    #[test]
    fn base64_round_trip() {
        let encoded = serde_json::to_string(&Base64(vec![0x00, 0xff, 0x10])).unwrap();
        assert_eq!(encoded, "\"AP8Q\"");
        let decoded: Base64 = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.0, vec![0x00, 0xff, 0x10]);
    }

    #[test]
    fn decodes_send_response() {
        let resp: SendResponse = serde_json::from_str(
            r#"{"id":"e8a93bf84c379a20","status":"sent","message":"email sent successfully","sent_at":1771142400000000000}"#,
        )
        .unwrap();
        assert_eq!(resp.status, "sent");
        assert_eq!(resp.sent_at, 1771142400000000000);
    }

    #[test]
    fn decodes_batch_response() {
        let resp: BatchResponse = serde_json::from_str(
            r#"{"total":2,"succeeded":1,"failed":1,"results":[{"id":"a","status":"sent","message":"ok","sent_at":1},{"id":"b","status":"failed","message":"delivery failed: boom","sent_at":2}]}"#,
        )
        .unwrap();
        assert_eq!(resp.total, 2);
        assert_eq!(resp.succeeded, 1);
        assert_eq!(resp.failed, 1);
        assert_eq!(resp.results[1].message, "delivery failed: boom");
    }

    #[test]
    fn decodes_error_body_with_details_and_message_alias() {
        let with_details: ApiErrorBody =
            serde_json::from_str(r#"{"code":400,"error":"validation_error","details":"bad to"}"#)
                .unwrap();
        assert_eq!(with_details.details.as_deref(), Some("bad to"));

        let auth: ApiErrorBody = serde_json::from_str(
            r#"{"code":401,"error":"unauthorized","message":"invalid or missing authentication token / secret key"}"#,
        )
        .unwrap();
        assert_eq!(auth.code, 401);
        assert_eq!(
            auth.details.as_deref(),
            Some("invalid or missing authentication token / secret key")
        );
    }

    #[test]
    fn generate_id_is_32_hex_chars() {
        let a = generate_id();
        let b = generate_id();
        assert_eq!(a.len(), 32);
        assert_ne!(a, b);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
