use mini_kafka::Consumer;
use tokio::time::{Duration, sleep, timeout};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("🔔 Starting enhanced consumer with offset management...");

    sleep(Duration::from_secs(1)).await;

    let mut consumer = Consumer::new(
        "127.0.0.1:9092".to_string(),
        "enhanced-consumer-group".to_string(),
        "enhanced-consumer-v1".to_string(),
    )
    .with_auto_commit(true) // Enable auto-commit
    .with_commit_interval(Duration::from_secs(2)); // Commit every 2 seconds

    // Display consumer info
    let (client_id, group_id, broker_addr) = consumer.client_info();
    println!(
        "📊 Consumer Info: ID='{}', Group='{}', Broker='{}'",
        client_id, group_id, broker_addr
    );

    // Subscribe will now fetch stored offsets
    match consumer
        .subscribe("test-topic".to_string(), vec![0, 1, 2])
        .await
    {
        Ok(_) => println!("✅ Subscribed to 'test-topic' and resumed from stored offsets"),
        Err(e) => {
            eprintln!("❌ Failed to subscribe: {}", e);
            return Err(e.into());
        }
    }

    println!("👂 Listening for messages on 'test-topic'...");
    println!("   - Will auto-commit offsets every 2 seconds");
    println!("   - Will process up to 25 messages");
    println!("   - Timeout after 90 seconds\n");

    let mut message_count = 0;
    let mut messages_by_partition = std::collections::HashMap::new();
    let mut unique_keys = std::collections::HashSet::new();

    let result = timeout(
        Duration::from_secs(90),
        consumer.start_consuming(|message| {
            message_count += 1;

            // Track partition distribution
            *messages_by_partition.entry(message.partition).or_insert(0) += 1;

            // Track unique keys
            if let Some(ref key) = message.key {
                unique_keys.insert(key.clone());
            }

            // Try to parse as JSON for better display
            let payload_str = String::from_utf8_lossy(&message.payload);
            let display_message = if payload_str.starts_with('{') {
                // Pretty print JSON if possible
                match serde_json::from_str::<serde_json::Value>(&payload_str) {
                    Ok(json) => format!("{}", json),
                    Err(_) => payload_str.to_string(),
                }
            } else {
                payload_str.to_string()
            };

            println!(
                "📨 Message #{}: {}\n   └─ Partition: {}, Key: {:?}, Time: {}",
                message_count,
                display_message,
                message.partition,
                message.key,
                message.timestamp.format("%H:%M:%S")
            );

            // Show statistics every 10 messages
            if message_count % 10 == 0 {
                println!("\n📊 Statistics after {} messages:", message_count);
                println!("   - Partition distribution: {:?}", messages_by_partition);
                println!("   - Unique keys seen: {}", unique_keys.len());
                println!();
            }

            message_count < 25 // Process 25 messages then stop
        }),
    )
    .await;

    match result {
        Ok(_) => {
            println!("\n🎉 Consumer finished successfully!");
            println!("   - Processed {} messages", message_count);
            println!("   - Partition distribution: {:?}", messages_by_partition);
            println!("   - Unique keys processed: {}", unique_keys.len());
            println!("   - All offsets committed automatically");
        }
        Err(_) => {
            println!("\n⏰ Consumer timed out after {} messages", message_count);
            println!("   - Partial statistics: {:?}", messages_by_partition);
        }
    }

    Ok(())
}
