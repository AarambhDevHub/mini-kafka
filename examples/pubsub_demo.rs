use mini_kafka::{Consumer, Producer};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("🚀 Starting Advanced Pub/Sub Demo with Offset Management");
    println!("========================================================");

    let notify = Arc::new(Notify::new());
    let notify_clone = notify.clone();

    // Start consumer
    let consumer_task = tokio::spawn(async move {
        let mut consumer = Consumer::new(
            "127.0.0.1:9092".to_string(),
            "advanced-pubsub-group".to_string(),
            "advanced-pubsub-consumer".to_string(),
        )
        .with_auto_commit(true)
        .with_commit_interval(Duration::from_secs(1)); // More frequent commits

        // Subscribe with async method
        match consumer
            .subscribe("notifications".to_string(), vec![0, 1, 2])
            .await
        {
            Ok(_) => {
                println!("🔔 Consumer subscribed and resumed from stored offsets");
                let (client_id, group_id, _) = consumer.client_info();
                println!("   └─ Consumer ID: '{}', Group: '{}'", client_id, group_id);
            }
            Err(e) => {
                eprintln!("❌ Failed to subscribe: {}", e);
                return;
            }
        }

        println!("👂 Consumer waiting for notifications...\n");

        let mut count = 0;
        let mut categories = std::collections::HashMap::new();

        let result = consumer
            .start_consuming(|message| {
                count += 1;

                // Extract category from key if available
                let category = message.key.as_deref().unwrap_or("unknown");
                *categories.entry(category.to_string()).or_insert(0) += 1;

                let payload = String::from_utf8_lossy(&message.payload);
                println!(
                    "📨 Notification #{} [{}]: {}\n   └─ Partition: {}, Time: {}",
                    count,
                    category,
                    payload,
                    message.partition,
                    message.timestamp.format("%H:%M:%S")
                );

                // Show category stats every few messages
                if count % 3 == 0 {
                    println!("   📊 Categories so far: {:?}\n", categories);
                }

                count < 12 // Process 12 messages then stop
            })
            .await;

        match result {
            Ok(_) => {
                println!("✅ Consumer finished processing {} notifications", count);
                println!("   📊 Final category breakdown: {:?}", categories);
            }
            Err(e) => eprintln!("❌ Consumer error: {}", e),
        }

        notify_clone.notify_one();
    });

    // Wait a bit for consumer to start and subscribe
    sleep(Duration::from_secs(2)).await;

    // Start producer
    println!("📤 Starting advanced producer...\n");
    let producer = Producer::new(
        "127.0.0.1:9092".to_string(),
        "advanced-pubsub-producer".to_string(),
    );

    let notifications = vec![
        ("system", "🎉 Welcome to Advanced Mini Kafka Pub/Sub!"),
        ("alert", "⚠️ High memory usage detected on server-01"),
        ("info", "✅ Database backup completed successfully"),
        (
            "feature",
            "🚀 New feature: Advanced offset management deployed!",
        ),
        ("report", "📊 Daily analytics report is ready for review"),
        ("social", "👥 5 new user registrations today"),
        ("security", "🔒 Security scan completed - no issues found"),
        ("system", "🔄 System maintenance scheduled for 2 AM"),
        ("alert", "📈 CPU usage spike detected - investigating"),
        ("info", "✨ Performance improvements deployed"),
        ("feature", "🎯 A/B test results are now available"),
        ("report", "📋 Weekly summary report generated"),
    ];

    for (i, (category, notification)) in notifications.iter().enumerate() {
        let enhanced_notification = format!(
            "{{\"id\":{},\"category\":\"{}\",\"message\":\"{}\",\"timestamp\":\"{}\"}}",
            i + 1,
            category,
            notification,
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        );

        println!("📤 Publishing [{}]: {}", category, notification);

        match producer
            .send(
                "notifications".to_string(),
                Some(category.to_string()),
                enhanced_notification.into_bytes(),
            )
            .await
        {
            Ok(offset) => println!(
                "   ✓ Published at offset {} (category: {})",
                offset, category
            ),
            Err(e) => eprintln!("   ❌ Failed to publish: {}", e),
        }

        sleep(Duration::from_millis(600)).await;
    }

    // Wait for consumer to finish
    println!("\n⏳ Waiting for consumer to finish processing...");
    notify.notified().await;
    consumer_task.await?;

    println!("\n🎉 Advanced Pub/Sub demo completed successfully!");
    println!("========================================================");
    println!("Summary:");
    println!(
        "   - {} notifications published across {} categories",
        notifications.len(),
        notifications
            .iter()
            .map(|(cat, _)| cat)
            .collect::<std::collections::HashSet<_>>()
            .len()
    );
    println!("   - Messages were categorized and distributed by key");
    println!("   - Consumer automatically resumed from stored offsets");
    println!("   - Offsets were auto-committed every 1 second");
    println!("   - JSON payloads included metadata and timestamps");
    println!("========================================================");

    Ok(())
}
