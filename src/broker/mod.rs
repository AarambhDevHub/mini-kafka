pub mod partition;
pub mod storage;
pub mod topic;

use crate::{KafkaError, KafkaResult, Message};
use dashmap::DashMap;
use std::sync::Arc;
use storage::Storage;
use tokio::sync::RwLock;
use topic::Topic;
use tracing::{debug, info, warn};

pub use crate::config::BrokerConfig;

#[derive(Clone)]
pub struct Broker {
    config: BrokerConfig,
    topics: Arc<DashMap<String, Arc<RwLock<Topic>>>>,
    storage: Arc<Storage>,
    // Consumer group offset management
    consumer_offsets: Arc<DashMap<String, u64>>, // key: "group_id:topic:partition", value: offset
}

impl Broker {
    pub async fn new(config: BrokerConfig) -> KafkaResult<Self> {
        let storage = Arc::new(Storage::new(&config.data_dir).await?);

        let broker = Self {
            config,
            topics: Arc::new(DashMap::new()),
            storage: storage.clone(),
            consumer_offsets: Arc::new(DashMap::new()),
        };

        // Load existing consumer offsets from storage
        broker.load_consumer_offsets().await?;

        Ok(broker)
    }

    async fn load_consumer_offsets(&self) -> KafkaResult<()> {
        match self.storage.load_consumer_offsets().await {
            Ok(offsets) => {
                for (key, offset) in offsets {
                    self.consumer_offsets.insert(key, offset);
                }
                info!(
                    "Loaded {} consumer offsets from storage",
                    self.consumer_offsets.len()
                );
            }
            Err(e) => {
                warn!("Could not load consumer offsets: {}", e);
            }
        }
        Ok(())
    }

    // Auto-create topic if it doesn't exist
    async fn ensure_topic_exists(&self, topic_name: &str) -> KafkaResult<()> {
        if !self.topics.contains_key(topic_name) {
            info!("Auto-creating topic '{}'", topic_name);
            self.create_topic(topic_name.to_string(), None).await?;
        }
        Ok(())
    }

    pub async fn create_topic(&self, name: String, partitions: Option<u32>) -> KafkaResult<()> {
        let partition_count = partitions.unwrap_or(self.config.default_partitions);

        if self.topics.contains_key(&name) {
            return Ok(()); // Topic already exists, return success
        }

        let topic = Topic::new(name.clone(), partition_count, self.storage.clone()).await?;
        self.topics
            .insert(name.clone(), Arc::new(RwLock::new(topic)));

        info!(
            "Created topic '{}' with {} partitions",
            name, partition_count
        );
        Ok(())
    }

    pub async fn produce(&self, message: Message) -> KafkaResult<u64> {
        self.ensure_topic_exists(&message.topic).await?;

        let topic = self
            .topics
            .get(&message.topic)
            .ok_or_else(|| KafkaError::TopicNotFound(message.topic.clone()))?;

        let mut topic = topic.write().await;
        topic.append_message(message).await
    }

    pub async fn consume(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> KafkaResult<Option<Message>> {
        self.ensure_topic_exists(topic).await?;

        let topic_ref = self
            .topics
            .get(topic)
            .ok_or_else(|| KafkaError::TopicNotFound(topic.to_string()))?;

        let topic = topic_ref.read().await;
        topic.read_message(partition, offset).await
    }

    pub async fn get_latest_offset(&self, topic: &str, partition: u32) -> KafkaResult<u64> {
        self.ensure_topic_exists(topic).await?;

        let topic_ref = self
            .topics
            .get(topic)
            .ok_or_else(|| KafkaError::TopicNotFound(topic.to_string()))?;

        let topic = topic_ref.read().await;
        topic.get_latest_offset(partition).await
    }

    // New offset management methods
    pub async fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> KafkaResult<()> {
        let key = format!("{}:{}:{}", group_id, topic, partition);
        self.consumer_offsets.insert(key.clone(), offset);

        // Persist to storage
        self.storage.save_consumer_offset(&key, offset).await?;

        debug!(
            "Committed offset {} for group '{}' topic '{}' partition {}",
            offset, group_id, topic, partition
        );
        Ok(())
    }

    pub async fn fetch_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
    ) -> KafkaResult<u64> {
        let key = format!("{}:{}:{}", group_id, topic, partition);

        let offset = self
            .consumer_offsets
            .get(&key)
            .map(|entry| *entry.value())
            .unwrap_or(0); // Default to 0 if no offset stored

        debug!(
            "Fetched offset {} for group '{}' topic '{}' partition {}",
            offset, group_id, topic, partition
        );
        Ok(offset)
    }

    pub fn list_topics(&self) -> Vec<String> {
        self.topics
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }
}
