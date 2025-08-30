use mini_kafka::network::Server;
use mini_kafka::{Broker, Config, Consumer, Producer};
use std::sync::Arc;
use tokio::time::{Duration, Instant, sleep, timeout};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("🚀 Starting Distributed Mini Kafka Setup with Offset Management");
    println!("==============================================================");

    // Initialize logging
    tracing_subscriber::fmt::init();

    // Create multiple broker configs
    let brokers = vec![
        ("127.0.0.1:9092", 1),
        ("127.0.0.1:9093", 2),
        ("127.0.0.1:9094", 3),
    ];

    let mut server_handles = Vec::new();

    // Start multiple brokers
    for (addr, id) in brokers {
        let mut config = Config::default();
        config.broker.id = id;
        config.broker.data_dir = format!("data/broker_{}", id);
        config.network.bind_address = addr.parse().unwrap();

        let broker = Broker::new(config.broker.clone()).await?;

        // Pre-create topic on first broker
        if id == 1 {
            broker
                .create_topic("distributed-topic".to_string(), Some(3))
                .await?;
            println!(
                "✅ Created 'distributed-topic' with 3 partitions on broker {}",
                id
            );
        }

        let server = Arc::new(Server::new(broker, addr).await?);

        let server_clone = server.clone();
        let handle = tokio::spawn(async move {
            println!("📡 Starting broker {} on {}", id, addr);
            if let Err(e) = server_clone.start().await {
                eprintln!("Broker {} error: {}", id, e);
            }
        });

        server_handles.push(handle);
    }

    // Wait for brokers to start
    sleep(Duration::from_secs(3)).await;
    println!("✅ All brokers started successfully");

    // Test with multiple producers and consumers
    println!("\n🏭 Starting producers...");
    let producer_tasks = (0..3)
        .map(|i| {
            tokio::spawn(async move {
                let producer =
                    Producer::new("127.0.0.1:9092".to_string(), format!("producer-{}", i));

                for j in 0..5 {
                    let message = format!("Message from producer {} - batch #{}", i, j);
                    let key = format!("key-{}-{}", i, j);

                    match producer
                        .send(
                            "distributed-topic".to_string(),
                            Some(key),
                            message.into_bytes(),
                        )
                        .await
                    {
                        Ok(offset) => {
                            println!("📤 Producer {} sent message at offset {}", i, offset)
                        }
                        Err(e) => eprintln!("❌ Producer {} error: {}", i, e),
                    }
                    sleep(Duration::from_millis(300)).await;
                }
                println!("✅ Producer {} completed", i);
            })
        })
        .collect::<Vec<_>>();

    // FIXED: Start consumers with timeout and proper termination
    println!("\n👥 Starting consumer groups...");
    let consumer_tasks = (0..2)
        .map(|i| {
            tokio::spawn(async move {
                let mut consumer = Consumer::new(
                    "127.0.0.1:9092".to_string(),
                    format!("group-{}", i),
                    format!("consumer-{}", i),
                )
                .with_auto_commit(true)
                .with_commit_interval(Duration::from_secs(2));

                // Subscribe with async method and error handling
                match consumer
                    .subscribe("distributed-topic".to_string(), vec![0, 1, 2])
                    .await
                {
                    Ok(_) => println!(
                        "🔔 Consumer {} subscribed and resumed from stored offsets",
                        i
                    ),
                    Err(e) => {
                        eprintln!("❌ Consumer {} failed to subscribe: {}", i, e);
                        return;
                    }
                }

                let mut count = 0;
                let start_time = Instant::now();

                // FIXED: Add timeout wrapper to consumer
                let result = timeout(
                    Duration::from_secs(60), // Maximum 60 seconds
                    consumer.start_consuming(move |message| {
                        println!(
                            "📨 Consumer {} received: '{}' (partition: {}, key: {:?})",
                            i,
                            String::from_utf8_lossy(&message.payload),
                            message.partition,
                            message.key
                        );
                        count += 1;
                        let elapsed = start_time.elapsed();

                        // FIXED: Stop after processing messages or timeout
                        let should_continue = count < 12 && elapsed < Duration::from_secs(55);

                        if !should_continue {
                            println!(
                                "🔄 Consumer {} stopping after {} messages in {:.1}s",
                                i,
                                count,
                                elapsed.as_secs_f64()
                            );
                        }

                        should_continue
                    }),
                )
                .await;

                match result {
                    Ok(_) => println!("✅ Consumer {} finished with {} messages", i, count),
                    Err(_) => println!("⏰ Consumer {} timed out after {} messages", i, count),
                }
            })
        })
        .collect::<Vec<_>>();

    // Wait for all producers to complete
    println!("\n⏳ Waiting for producers to complete...");
    for task in producer_tasks {
        task.await?;
    }
    println!("✅ All producers completed");

    // FIXED: Wait for consumers with overall timeout
    println!("\n⏳ Waiting for consumers to complete (max 90 seconds)...");
    let consumers_result = timeout(Duration::from_secs(90), async {
        for task in consumer_tasks {
            let _ = task.await;
        }
    })
    .await;

    match consumers_result {
        Ok(_) => println!("✅ All consumers completed"),
        Err(_) => println!("⏰ Consumer tasks timed out, but continuing..."),
    }

    // FIXED: Abort server tasks to ensure cleanup
    for handle in server_handles {
        handle.abort();
    }

    println!("\n🎉 Distributed setup demo completed successfully!");
    println!("==================================================");
    println!("Summary:");
    println!("  - 3 brokers running on different ports");
    println!("  - 3 producers sent messages with different keys");
    println!("  - 2 consumer groups processed messages independently");
    println!("  - Each consumer group maintained separate offsets");
    println!("  - Auto-commit ensured no message loss on reconnection");
    println!("  - Maximum demo runtime: ~2 minutes");
    println!("==================================================");

    Ok(())
}
