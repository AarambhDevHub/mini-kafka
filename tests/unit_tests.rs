use mini_kafka::*;
use tempfile::TempDir;
use tokio::time::Duration;

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[tokio::test]
    async fn test_message_creation() {
        let message = Message::new("test_topic".to_string(), 0, b"Hello, Mini Kafka!".to_vec());

        assert!(!message.id.to_string().is_empty());
        assert_eq!(message.topic, "test_topic");
        assert_eq!(message.partition, 0);
        assert_eq!(message.payload, b"Hello, Mini Kafka!".to_vec());
        assert!(message.key.is_none());
    }

    #[tokio::test]
    async fn test_message_with_key_and_headers() {
        let message = Message::new("test_topic".to_string(), 1, b"payload".to_vec())
            .with_key("test_key".to_string())
            .with_header("content-type".to_string(), "application/json".to_string());

        assert_eq!(message.key, Some("test_key".to_string()));
        assert_eq!(
            message.headers.get("content-type"),
            Some(&"application/json".to_string())
        );
    }

    #[tokio::test]
    async fn test_broker_creation() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = BrokerConfig {
            id: 1,
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            default_partitions: 3,
            retention_ms: 86400000,        // 1 day
            max_message_size: 1024 * 1024, // 1MB
        };

        let broker = Broker::new(config).await;
        assert!(broker.is_ok());

        let broker = broker.unwrap();
        let topics = broker.list_topics();
        assert_eq!(topics.len(), 0); // Should start empty
    }

    #[tokio::test]
    async fn test_broker_topic_creation() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = BrokerConfig {
            id: 1,
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            default_partitions: 3,
            retention_ms: 86400000,
            max_message_size: 1024 * 1024,
        };

        let broker = Broker::new(config).await.unwrap();

        // Create topic
        let result = broker.create_topic("test_topic".to_string(), Some(2)).await;
        assert!(result.is_ok());

        // List topics should now include our topic
        let topics = broker.list_topics();
        assert_eq!(topics.len(), 1);
        assert!(topics.contains(&"test_topic".to_string()));
    }

    #[tokio::test]
    async fn test_broker_produce_and_consume() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = BrokerConfig {
            id: 1,
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            default_partitions: 3,
            retention_ms: 86400000,
            max_message_size: 1024 * 1024,
        };

        let broker = Broker::new(config).await.unwrap();

        // Create and produce message
        let message = Message::new(
            "test_topic".to_string(),
            0,
            b"Test message payload".to_vec(),
        );

        let offset = broker.produce(message.clone()).await.unwrap();
        assert_eq!(offset, 0);

        // Consume the message
        let consumed_message = broker.consume("test_topic", 0, 0).await.unwrap();

        assert!(consumed_message.is_some());
        let consumed = consumed_message.unwrap();
        assert_eq!(consumed.payload, b"Test message payload".to_vec());
        assert_eq!(consumed.topic, "test_topic");
        assert_eq!(consumed.partition, 0);
    }

    #[tokio::test]
    async fn test_multiple_messages() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = BrokerConfig {
            id: 1,
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            default_partitions: 3,
            retention_ms: 86400000,
            max_message_size: 1024 * 1024,
        };

        let broker = Broker::new(config).await.unwrap();

        // Produce multiple messages
        for i in 0..5 {
            let message = Message::new(
                "multi_test".to_string(),
                0,
                format!("Message {}", i).into_bytes(),
            );
            let offset = broker.produce(message).await.unwrap();
            assert_eq!(offset, i as u64);
        }

        // Consume all messages
        for i in 0..5 {
            let consumed = broker.consume("multi_test", 0, i).await.unwrap();

            assert!(consumed.is_some());
            let message = consumed.unwrap();
            assert_eq!(message.payload, format!("Message {}", i).into_bytes());
        }

        // Try to consume beyond available messages
        let beyond = broker.consume("multi_test", 0, 5).await.unwrap();
        assert!(beyond.is_none());
    }

    #[tokio::test]
    async fn test_consumer_offset_management() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = BrokerConfig {
            id: 1,
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            default_partitions: 3,
            retention_ms: 86400000,
            max_message_size: 1024 * 1024,
        };

        let broker = Broker::new(config).await.unwrap();

        let group_id = "test_group";
        let topic = "offset_test";
        let partition = 0;

        // Initially, offset should be 0
        let initial_offset = broker
            .fetch_offset(group_id, topic, partition)
            .await
            .unwrap();
        assert_eq!(initial_offset, 0);

        // Commit an offset
        let test_offset = 42;
        broker
            .commit_offset(group_id, topic, partition, test_offset)
            .await
            .unwrap();

        // Fetch the committed offset
        let fetched_offset = broker
            .fetch_offset(group_id, topic, partition)
            .await
            .unwrap();
        assert_eq!(fetched_offset, test_offset);
    }

    #[tokio::test]
    async fn test_producer_client_info() {
        let producer = Producer::new("127.0.0.1:9092".to_string(), "test_producer".to_string());

        let (client_id, broker_address) = producer.client_info();
        assert_eq!(client_id, "test_producer");
        assert_eq!(broker_address, "127.0.0.1:9092");
    }

    #[tokio::test]
    async fn test_consumer_client_info() {
        let consumer = Consumer::new(
            "127.0.0.1:9092".to_string(),
            "test_group".to_string(),
            "test_consumer".to_string(),
        );

        let (client_id, group_id, broker_address) = consumer.client_info();
        assert_eq!(client_id, "test_consumer");
        assert_eq!(group_id, "test_group");
        assert_eq!(broker_address, "127.0.0.1:9092");
    }

    #[tokio::test]
    async fn test_consumer_configuration() {
        let consumer = Consumer::new(
            "127.0.0.1:9092".to_string(),
            "test_group".to_string(),
            "test_consumer".to_string(),
        )
        .with_auto_commit(false)
        .with_commit_interval(Duration::from_secs(10));

        let (client_id, group_id, _) = consumer.client_info();
        assert_eq!(client_id, "test_consumer");
        assert_eq!(group_id, "test_group");
    }
}
