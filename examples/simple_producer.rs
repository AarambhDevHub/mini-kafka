use mini_kafka::Producer;
use mini_kafka::network::protocol::{CreateTopicRequest, Request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

async fn create_topic_if_needed(broker_address: &str, topic: &str) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(broker_address).await?;

    let request = Request::CreateTopic(CreateTopicRequest {
        name: topic.to_string(),
        partitions: Some(3),
    });

    let serialized = bincode::serialize(&request)?;
    stream.write_u32(serialized.len() as u32).await?;
    stream.write_all(&serialized).await?;

    let response_len = stream.read_u32().await?;
    let mut buffer = vec![0; response_len as usize];
    stream.read_exact(&mut buffer).await?;

    println!("✅ Topic '{}' created/ensured", topic);
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let broker_address = "127.0.0.1:9092";
    let topic = "test-topic";

    println!("🚀 Starting enhanced producer with client tracking...");

    // Wait a moment for broker to be ready
    sleep(Duration::from_millis(1000)).await;

    // Create topic first
    if let Err(e) = create_topic_if_needed(broker_address, topic).await {
        println!(
            "⚠️  Could not create topic (broker might auto-create): {}",
            e
        );
    }

    let producer = Producer::new(
        broker_address.to_string(),
        "enhanced-producer-v1".to_string(),
    );

    // Display producer info
    let (client_id, broker_addr) = producer.client_info();
    println!(
        "📊 Producer Info: ID='{}', Broker='{}'",
        client_id, broker_addr
    );

    println!("\n📤 Sending messages with varied keys and payloads...");

    // Send different types of messages
    let message_types = vec![
        ("user-action", "User clicked button"),
        ("system-event", "System startup completed"),
        ("error-log", "Connection timeout occurred"),
        ("metrics", "CPU usage: 75%"),
        ("notification", "New user registered"),
        ("audit", "Admin user logged in"),
        ("heartbeat", "Service health check"),
        ("data-sync", "Database sync completed"),
        ("security", "Failed login attempt detected"),
        ("performance", "Response time: 120ms"),
    ];

    for (i, (key, message_text)) in message_types.iter().enumerate() {
        let enhanced_message = format!(
            "{{\"id\":{},\"type\":\"{}\",\"message\":\"{}\",\"timestamp\":\"{}\"}}",
            i,
            key,
            message_text,
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
        );

        match producer
            .send(
                topic.to_string(),
                Some(key.to_string()),
                enhanced_message.into_bytes(),
            )
            .await
        {
            Ok(offset) => println!(
                "  ✓ Message {}: '{}' → offset {} (key: {})",
                i + 1,
                message_text,
                offset,
                key
            ),
            Err(e) => eprintln!("  ❌ Failed to send message {}: {}", i + 1, e),
        }

        sleep(Duration::from_millis(300)).await;
    }

    // Send a batch of messages
    println!("\n📦 Sending batch messages...");
    let batch_messages = vec![
        (
            "batch-1".to_string(),
            Some("batch-key-1".to_string()),
            "Batch message 1".as_bytes().to_vec(),
        ),
        (
            "batch-2".to_string(),
            Some("batch-key-2".to_string()),
            "Batch message 2".as_bytes().to_vec(),
        ),
        (
            "batch-3".to_string(),
            Some("batch-key-3".to_string()),
            "Batch message 3".as_bytes().to_vec(),
        ),
    ];

    match producer.send_batch(batch_messages).await {
        Ok(offsets) => {
            println!("✅ Batch sent successfully! Offsets: {:?}", offsets);
        }
        Err(e) => {
            eprintln!("❌ Batch send failed: {}", e);
        }
    }

    println!("\n🎉 Enhanced producer finished successfully!");
    println!(
        "   - Sent {} individual messages with different keys",
        message_types.len()
    );
    println!("   - Sent 1 batch with 3 messages");
    println!("   - All messages distributed across partitions based on keys");

    Ok(())
}
