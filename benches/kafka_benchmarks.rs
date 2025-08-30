use criterion::{
    BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use mini_kafka::*;
use std::sync::{Arc, LazyLock};
use tempfile::TempDir;
use tokio::time::Duration;

// CI-optimized broker setup
static CI_BROKER: LazyLock<Arc<Broker>> = LazyLock::new(|| {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let config = BrokerConfig {
        id: 1,
        data_dir: temp_dir.path().to_string_lossy().to_string(),
        default_partitions: 2, // Reduced for CI
        retention_ms: 3600000, // 1 hour for CI
        max_message_size: 1024 * 1024,
    };

    let broker = rt.block_on(async { Broker::new(config).await.expect("Failed to create broker") });

    std::mem::forget(temp_dir);
    Arc::new(broker)
});

// Configure Criterion for CI environment
fn configure_criterion() -> Criterion {
    let is_ci = std::env::var("CI").is_ok();

    if is_ci {
        // CI configuration: faster but less precise
        Criterion::default()
            .sample_size(15)
            .measurement_time(Duration::from_secs(8))
            .warm_up_time(Duration::from_secs(2))
    } else {
        // Local configuration: more precise
        Criterion::default()
            .sample_size(50)
            .measurement_time(Duration::from_secs(15))
            .warm_up_time(Duration::from_secs(3))
    }
}

fn benchmark_produce_optimized(c: &mut Criterion) {
    let broker = CI_BROKER.clone();
    let mut group = c.benchmark_group("produce");

    group.sampling_mode(SamplingMode::Flat);

    // Pre-create messages
    let small_messages: Vec<Message> = (0..100)
        .map(|i| Message::new(format!("topic_{}", i % 5), 0, vec![42u8; 64]))
        .collect();

    let large_messages: Vec<Message> = (0..100)
        .map(|i| Message::new(format!("topic_{}", i % 5), 0, vec![42u8; 4096]))
        .collect();

    group.throughput(Throughput::Bytes(64));
    group.bench_function("small_messages", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut counter = 0;

        b.iter(|| {
            let message = small_messages[counter % 100].clone();
            counter += 1;
            rt.block_on(async { broker.produce(message).await.unwrap() })
        })
    });

    group.throughput(Throughput::Bytes(4096));
    group.bench_function("large_messages", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut counter = 0;

        b.iter(|| {
            let message = large_messages[counter % 100].clone();
            counter += 1;
            rt.block_on(async { broker.produce(message).await.unwrap() })
        })
    });

    group.finish();
}

fn benchmark_consume_optimized(c: &mut Criterion) {
    let broker = CI_BROKER.clone();
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Pre-populate with test messages
    rt.block_on(async {
        for i in 0..50 {
            let message = Message::new(
                "consume_test".to_string(),
                0,
                format!("Test message {}", i).into_bytes(),
            );
            broker.produce(message).await.unwrap();
        }
    });

    let mut group = c.benchmark_group("consume");
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("sequential_reads", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut offset = 0u64;

        b.iter(|| {
            let result = rt.block_on(async {
                broker
                    .consume("consume_test", 0, offset % 50)
                    .await
                    .unwrap()
            });
            offset += 1;
            result
        })
    });

    group.finish();
}

fn benchmark_memory_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory");
    group.sampling_mode(SamplingMode::Auto);

    group.bench_function("message_creation", |b| {
        b.iter(|| {
            Message::new("bench".to_string(), 0, vec![42u8; 1024])
                .with_key("key".to_string())
                .with_header("type".to_string(), "test".to_string())
        })
    });

    group.bench_function("serialization", |b| {
        let message = Message::new("bench".to_string(), 0, vec![42u8; 1024]);
        b.iter(|| {
            let serialized = bincode::serialize(&message).unwrap();
            let _: Message = bincode::deserialize(&serialized).unwrap();
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = configure_criterion();
    targets = benchmark_produce_optimized, benchmark_consume_optimized, benchmark_memory_operations
}

criterion_main!(benches);
