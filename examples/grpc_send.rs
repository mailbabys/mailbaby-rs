//! Sends an email through the MailBaby gRPC API (`grpc` feature).
//!
//! Usage:
//! ```text
//! cargo run --features grpc --example grpc_send -- http://localhost:50051 [api-key]
//! ```

use mailbaby::Email;
use mailbaby::grpc::GrpcClient;

#[tokio::main]
async fn main() -> Result<(), mailbaby::Error> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://localhost:50051".to_string());
    let api_key = std::env::args().nth(2);

    let client = GrpcClient::connect(&addr, api_key.as_deref()).await?;

    let email = Email::builder("Welcome via gRPC")
        .account("default")
        .from_with_name("noreply@example.com", "MailBaby Demo")
        .to(["alice@example.com"])
        .text_body("Hello! This email was sent through the mailbaby Rust client over gRPC.")
        .header("X-Proto", "grpc")
        .build()?;

    println!("calling Send (sync)...");
    let resp = client.send(&email).await?;
    println!(
        "  -> id={} status={} message={}",
        resp.id, resp.status, resp.message
    );

    println!("calling Send (async)...");
    let resp = client.send_async(&email).await?;
    println!(
        "  -> id={} status={} message={}",
        resp.id, resp.status, resp.message
    );

    let version = client.ping().await?;
    println!("server version via Ping: {version}");

    match client.health_check().await {
        Ok(status) => println!("HealthCheck: {status}"),
        Err(err) => println!("HealthCheck: {err}"),
    }

    Ok(())
}
