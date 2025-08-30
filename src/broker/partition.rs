use super::storage::Storage;
use crate::{KafkaResult, Message};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

pub struct Partition {
    topic: String,
    id: u32,
    messages: Arc<Mutex<Vec<Message>>>,
    storage: Arc<Storage>,
    next_offset: Arc<Mutex<u64>>,
}

impl Partition {
    pub async fn new(topic: String, id: u32, storage: Arc<Storage>) -> KafkaResult<Self> {
        debug!("Creating partition {} for topic '{}'", id, topic);

        // Load existing messages from storage
        let messages = storage.load_partition(&topic, id).await?;
        let next_offset = messages.len() as u64;

        info!(
            "Partition {} for topic '{}' loaded with {} existing messages",
            id,
            topic,
            messages.len()
        );

        Ok(Self {
            topic,
            id,
            messages: Arc::new(Mutex::new(messages)),
            storage,
            next_offset: Arc::new(Mutex::new(next_offset)),
        })
    }

    pub async fn append(&self, message: Message) -> KafkaResult<u64> {
        let mut messages = self.messages.lock().await;
        let mut offset = self.next_offset.lock().await;

        messages.push(message.clone());
        let current_offset = *offset;
        *offset += 1;

        // Persist to storage
        self.storage
            .append_message(&self.topic, self.id, &message, current_offset)
            .await?;

        debug!(
            "Partition {} appended message at offset {}",
            self.id, current_offset
        );
        Ok(current_offset)
    }

    pub async fn read(&self, offset: u64) -> KafkaResult<Option<Message>> {
        let messages = self.messages.lock().await;

        if offset >= messages.len() as u64 {
            debug!(
                "Partition {} offset {} beyond available messages ({})",
                self.id,
                offset,
                messages.len()
            );
            return Ok(None);
        }

        debug!("Partition {} reading message at offset {}", self.id, offset);
        Ok(Some(messages[offset as usize].clone()))
    }

    pub async fn latest_offset(&self) -> u64 {
        *self.next_offset.lock().await
    }

    // Add this public getter method (synchronous, no async needed)
    pub fn id(&self) -> u32 {
        self.id
    }

    // Add method to get partition info
    pub fn info(&self) -> (&str, u32) {
        (&self.topic, self.id)
    }

    // Add method to get topic name
    pub fn topic(&self) -> &str {
        &self.topic
    }
}
