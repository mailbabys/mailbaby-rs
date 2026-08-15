//! Publishes an email job directly into a message queue (bypassing the
//! server's HTTP/gRPC endpoints), one of: RabbitMQ, Redis or Kafka.
//!
//! Usage:
//! ```text
//! cargo run --features mq-rabbitmq --example mq_publish -- rabbitmq amqp://guest:guest@localhost:5672/%2f mailqueue
//! cargo run --features mq-redis    --example mq_publish -- redis redis://127.0.0.1:6379 mailqueue
//! cargo run --features mq-kafka    --example mq_publish -- kafka localhost:9092 mailqueue
//! ```

use mailbaby::Email;
use mailbaby::mq::{
    KafkaProducer, MqMessage, MqProducer, PublishOptions, RabbitMqProducer, RedisMode,
    RedisProducer,
};

#[tokio::main]
async fn main() -> Result<(), mailbaby::Error> {
    let driver = std::env::args()
        .nth(1)
        .expect("driver: rabbitmq|redis|kafka");
    let addr = std::env::args().nth(2).expect("broker address");
    let destination = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "mailqueue".to_string());

    let email = Email::builder("MQ delivery test")
        .account("default")
        .from_with_name("noreply@example.com", "MailBaby Demo")
        .to(["alice@example.com"])
        .text_body("Hello! This email was enqueued directly by the mailbaby Rust MQ producer.")
        .header("X-Proto", "mq")
        .build()?;

    let message = MqMessage::from_email(&email)?;
    let options = PublishOptions::default();

    match driver.as_str() {
        "rabbitmq" => {
            let producer = RabbitMqProducer::connect(&addr, "", &destination).await?;
            producer.publish(&message, &options).await?;
        }
        "redis" => {
            let producer = RedisProducer::connect(&addr, &destination, RedisMode::Stream).await?;
            producer.publish(&message, &options).await?;
        }
        "kafka" => {
            let producer = KafkaProducer::connect(&addr, &destination, 0).await?;
            producer.publish(&message, &options).await?;
        }
        other => {
            eprintln!("unknown driver: {other} (expected rabbitmq, redis or kafka)");
            std::process::exit(2);
        }
    }

    println!(
        "published message id={} topic={destination} driver={driver}, attempts={}",
        message.id, message.attempts
    );
    Ok(())
}
