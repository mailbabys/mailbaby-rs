//! Sends an email through the MailBaby REST API (both sync and async modes).
//!
//! Usage:
//! ```text
//! cargo run --example send -- http://localhost:8080 [api-key]
//! ```

use mailbaby::Email;
use mailbaby::rest::MailBabyClient;

#[tokio::main]
async fn main() -> Result<(), mailbaby::Error> {
    let base_url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://localhost:8080".to_string());
    let api_key = std::env::args().nth(2);

    let client = MailBabyClient::new(&base_url, api_key.as_deref())?;

    let email = Email::builder("Welcome to MailBaby (Rust client)!")
        .account("default")
        .from_with_name("noreply@example.com", "MailBaby Demo")
        .reply_to("support@example.com")
        .to(["alice@example.com", "bob@example.com"])
        .text_body("Hello! This email was sent through the mailbaby Rust client.")
        .html_body("<h2>Hello!</h2><p>Sent via the <b>mailbaby-rs</b> REST client.</p>")
        .header("X-Proto", "rest")
        .tag("welcome")
        .metadata("example", "send")
        .build()?;

    println!("sending sync via REST: subject={:?}", email.subject);
    let resp = client.send(&email).await?;
    println!(
        "  -> id={} status={} message={}",
        resp.id, resp.status, resp.message
    );

    println!("queuing async via REST (202 Accepted expected)...");
    let resp = client.send_async(&email).await?;
    println!(
        "  -> id={} status={} message={}",
        resp.id, resp.status, resp.message
    );

    match client.ready().await {
        Ok(()) => println!("server /readyz: healthy"),
        Err(err) => println!("server /readyz: {}", err),
    }

    Ok(())
}
