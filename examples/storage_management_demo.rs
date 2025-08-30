use mini_kafka::network::Server;
use mini_kafka::{Broker, Config, Consumer, Producer};
use std::sync::{Arc, Mutex};
use tokio::time::{Duration, Instant, sleep, timeout};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("🗄️ Starting Storage Management & Performance Demo");
    println!("=================================================");

    // Initialize logging
    tracing_subscriber::fmt::init();

    // Create broker with custom config for storage demo
    let mut config = Config::default();
    config.broker.data_dir = "data/storage_demo".to_string();
    config.broker.retention_ms = 60 * 1000; // 1 minute retention for demo
    config.network.bind_address = "127.0.0.1:9095".parse().unwrap();

    println!("🚀 Starting broker with storage focus...");
    let broker = Broker::new(config.broker.clone()).await?;

    // Start server
    let server = Arc::new(Server::new(broker, "127.0.0.1:9095").await?);
    let server_clone = server.clone();
    let server_task = tokio::spawn(async move {
        if let Err(e) = server_clone.start().await {
            eprintln!("Server error: {}", e);
        }
    });

    // Wait for server to start
    sleep(Duration::from_secs(2)).await;
    println!("✅ Broker started on 127.0.0.1:9095\n");

    // Create topics for storage demo
    let topics = vec![
        ("storage-heavy", 2),
        ("storage-light", 1),
        ("storage-mixed", 3),
    ];

    let producer = Producer::new(
        "127.0.0.1:9095".to_string(),
        "storage-demo-producer".to_string(),
    );

    for (topic_name, partition_count) in &topics {
        // Create topic by sending a message (auto-create)
        let init_message = format!(
            "Initializing topic '{}' with {} partitions",
            topic_name, partition_count
        );
        producer
            .send(
                topic_name.to_string(),
                Some("init".to_string()),
                init_message.into_bytes(),
            )
            .await?;
        println!("📁 Initialized topic '{}'", topic_name);
    }

    println!("\n📊 Generating storage data...");

    // Generate different types of storage load
    let heavy_data_task = {
        let producer = producer.clone();
        tokio::spawn(async move {
            for i in 0..50 {
                let large_message = "X".repeat(1000); // 1KB message
                let payload = format!(
                    "{{\"id\":{},\"type\":\"heavy\",\"data\":\"{}\",\"size\":{}}}",
                    i,
                    large_message,
                    large_message.len()
                );

                if let Ok(_) = producer
                    .send(
                        "storage-heavy".to_string(),
                        Some(format!("heavy-{}", i % 2)),
                        payload.into_bytes(),
                    )
                    .await
                {
                    if i % 10 == 0 {
                        println!("  📦 Heavy storage: {} messages written", i + 1);
                    }
                }
                sleep(Duration::from_millis(50)).await;
            }
            println!("✅ Heavy storage generation completed (50 x 1KB messages)");
        })
    };

    let light_data_task = {
        let producer = producer.clone();
        tokio::spawn(async move {
            for i in 0..200 {
                let small_message = format!(
                    "{{\"id\":{},\"type\":\"light\",\"data\":\"small-{}\"}}",
                    i, i
                );

                if let Ok(_) = producer
                    .send(
                        "storage-light".to_string(),
                        Some("light".to_string()),
                        small_message.into_bytes(),
                    )
                    .await
                {
                    if i % 50 == 0 {
                        println!("  📄 Light storage: {} messages written", i + 1);
                    }
                }
                sleep(Duration::from_millis(10)).await;
            }
            println!("✅ Light storage generation completed (200 x ~50B messages)");
        })
    };

    let mixed_data_task = {
        let producer = producer.clone();
        tokio::spawn(async move {
            for i in 0..30 {
                let message_size = if i % 3 == 0 { 500 } else { 100 };
                let data = "M".repeat(message_size);
                let payload = format!(
                    "{{\"id\":{},\"type\":\"mixed\",\"data\":\"{}\",\"size\":{}}}",
                    i, data, message_size
                );

                if let Ok(_) = producer
                    .send(
                        "storage-mixed".to_string(),
                        Some(format!("mixed-{}", i % 3)),
                        payload.into_bytes(),
                    )
                    .await
                {
                    if i % 10 == 0 {
                        println!("  📊 Mixed storage: {} messages written", i + 1);
                    }
                }
                sleep(Duration::from_millis(30)).await;
            }
            println!("✅ Mixed storage generation completed (30 x variable size messages)");
        })
    };

    // Wait for all data generation to complete
    println!("⏳ Waiting for data generation...");
    tokio::try_join!(heavy_data_task, light_data_task, mixed_data_task)?;

    // Give some time for messages to be processed
    sleep(Duration::from_secs(2)).await;

    println!("\n🔍 Starting storage analysis with consumers...");

    // Start consumers to analyze the data with FIXED TERMINATION CONDITIONS
    let storage_analyzer_tasks = topics
        .iter()
        .enumerate()
        .map(|(i, (topic_name, _))| {
            let topic = topic_name.to_string();
            tokio::spawn(async move {
                let mut consumer = Consumer::new(
                    "127.0.0.1:9095".to_string(),
                    format!("storage-analyzer-{}", i),
                    format!("storage-analyzer-consumer-{}", i),
                )
                .with_auto_commit(true)
                .with_commit_interval(Duration::from_millis(500));

                match consumer.subscribe(topic.clone(), vec![0, 1, 2]).await {
                    Ok(_) => println!("🔍 Analyzer {} subscribed to '{}'", i, topic),
                    Err(e) => {
                        eprintln!(
                            "❌ Analyzer {} failed to subscribe to '{}': {}",
                            i, topic, e
                        );
                        return;
                    }
                }

                // FIXED: Use Arc<Mutex<T>> for shared mutable state
                let message_count = Arc::new(Mutex::new(0));
                let total_bytes = Arc::new(Mutex::new(0));
                let message_types =
                    Arc::new(Mutex::new(std::collections::HashMap::<String, i32>::new()));
                let start_time = Instant::now();

                // Clone Arc handles for closure
                let message_count_closure = Arc::clone(&message_count);
                let total_bytes_closure = Arc::clone(&total_bytes);
                let message_types_closure = Arc::clone(&message_types);

                // FIXED: Add proper timeout wrapper
                let consumer_result = timeout(
                    Duration::from_secs(30), // 30 second maximum timeout
                    consumer.start_consuming(move |message| {
                        // Update message count
                        {
                            let mut count = message_count_closure.lock().unwrap();
                            *count += 1;
                        }

                        // Update total bytes
                        {
                            let mut bytes = total_bytes_closure.lock().unwrap();
                            *bytes += message.payload.len();
                        }

                        // Update message types
                        if let Ok(json) =
                            serde_json::from_slice::<serde_json::Value>(&message.payload)
                        {
                            if let Some(msg_type) = json.get("type").and_then(|t| t.as_str()) {
                                let mut mt = message_types_closure.lock().unwrap();
                                *mt.entry(msg_type.to_string()).or_insert(0) += 1;
                            }
                        }

                        // FIXED: Stop after reasonable amount or timeout
                        let count = *message_count_closure.lock().unwrap();
                        let elapsed = start_time.elapsed();
                        let should_continue = count < 300 && elapsed < Duration::from_secs(25);

                        if !should_continue {
                            println!(
                                "🔄 Analyzer {} stopping: processed {} messages in {:.1}s",
                                i,
                                count,
                                elapsed.as_secs_f64()
                            );
                        }

                        should_continue
                    }),
                )
                .await;

                let elapsed = start_time.elapsed();

                // Extract final values from Arc<Mutex<T>>
                let final_count = *message_count.lock().unwrap();
                let final_bytes = *total_bytes.lock().unwrap();
                let final_types = message_types.lock().unwrap().clone();

                // Handle timeout or completion
                match consumer_result {
                    Ok(_) => {
                        println!("\n📊 Storage Analysis for '{}':", topic);
                        println!("   - Messages processed: {}", final_count);
                        println!("   - Total bytes consumed: {} bytes", final_bytes);
                        println!(
                            "   - Average message size: {:.1} bytes",
                            if final_count > 0 {
                                final_bytes as f64 / final_count as f64
                            } else {
                                0.0
                            }
                        );
                        println!("   - Processing time: {:.2} seconds", elapsed.as_secs_f64());
                        if elapsed.as_secs_f64() > 0.0 {
                            println!(
                                "   - Throughput: {:.1} messages/sec",
                                final_count as f64 / elapsed.as_secs_f64()
                            );
                        }
                        println!("   - Message types: {:?}", final_types);
                    }
                    Err(_) => {
                        println!(
                            "\n⏰ Analyzer {} timed out after {:.1}s",
                            i,
                            elapsed.as_secs_f64()
                        );
                        println!("   - Messages processed before timeout: {}", final_count);
                        if final_count > 0 {
                            println!("   - Total bytes: {} bytes", final_bytes);
                            println!("   - Message types: {:?}", final_types);
                        }
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    // Wait for analysis to complete with overall timeout
    println!("⏳ Running analysis (max 45 seconds)...");
    let analysis_timeout = timeout(Duration::from_secs(45), async {
        for task in storage_analyzer_tasks {
            let _ = task.await;
        }
    })
    .await;

    match analysis_timeout {
        Ok(_) => println!("✅ All analyzers completed"),
        Err(_) => println!("⏰ Analysis timed out, but continuing..."),
    }

    // Storage file analysis
    println!("\n🗂️ Storage File Analysis:");
    println!("   📁 Storage location: {}", config.broker.data_dir);

    // Check if storage directory exists and show some stats
    let storage_path = std::path::Path::new(&config.broker.data_dir);
    if storage_path.exists() {
        if let Ok(entries) = std::fs::read_dir(storage_path) {
            let mut total_files = 0;
            let mut total_size = 0u64;

            for entry in entries.flatten() {
                if entry.path().is_dir() {
                    if let Ok(topic_entries) = std::fs::read_dir(entry.path()) {
                        for topic_entry in topic_entries.flatten() {
                            if topic_entry.path().extension() == Some(std::ffi::OsStr::new("log")) {
                                total_files += 1;
                                if let Ok(metadata) = topic_entry.metadata() {
                                    total_size += metadata.len();
                                }
                            }
                        }
                    }
                }
            }

            println!("   - Total partition files: {}", total_files);
            println!(
                "   - Total storage size: {} bytes ({:.1} KB)",
                total_size,
                total_size as f64 / 1024.0
            );
        }
    }

    println!("\n🎉 Storage Management Demo completed!");
    println!("=====================================");
    println!("Summary:");
    println!("  - Generated different storage load patterns");
    println!("  - Heavy storage: 50 x 1KB messages");
    println!("  - Light storage: 200 x ~50B messages");
    println!("  - Mixed storage: 30 x variable size messages");
    println!("  - Analyzed storage performance with timeouts");
    println!("  - Demonstrated partition-based storage organization");
    println!("  - Maximum demo runtime: ~2 minutes");
    println!("=====================================");

    // Cleanup - ensure server stops
    server_task.abort();

    Ok(())
}
