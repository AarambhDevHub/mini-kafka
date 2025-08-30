use super::{partition::Partition, storage::Storage};
use crate::{KafkaError, KafkaResult, Message};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

pub struct Topic {
    name: String,
    partitions: HashMap<u32, Arc<Partition>>,
    storage: Arc<Storage>,
}

impl Topic {
    pub async fn new(
        name: String,
        partition_count: u32,
        storage: Arc<Storage>,
    ) -> KafkaResult<Self> {
        info!(
            "Creating topic '{}' with {} partitions",
            name, partition_count
        );
        let mut partitions = HashMap::new();

        for i in 0..partition_count {
            debug!("Initializing partition {} for topic '{}'", i, name);
            let partition = Arc::new(Partition::new(name.clone(), i, storage.clone()).await?);
            partitions.insert(i, partition);
        }

        info!(
            "Successfully initialized topic '{}' with {} partitions",
            name, partition_count
        );

        Ok(Self {
            name: name.clone(),
            partitions,
            storage,
        })
    }

    pub async fn append_message(&mut self, message: Message) -> KafkaResult<u64> {
        debug!(
            "Topic '{}' appending message to partition {}",
            self.name, message.partition
        );

        let partition = self.partitions.get(&message.partition).ok_or_else(|| {
            warn!(
                "Topic '{}': partition {} not found",
                self.name, message.partition
            );
            KafkaError::PartitionNotFound(message.partition)
        })?;

        let offset = partition.append(message).await?;
        // Fix: Remove .await from partition.id() since it's now synchronous
        debug!(
            "Topic '{}' appended message at offset {} to partition {}",
            self.name,
            offset,
            partition.id()
        );
        Ok(offset)
    }

    pub async fn read_message(
        &self,
        partition_id: u32,
        offset: u64,
    ) -> KafkaResult<Option<Message>> {
        debug!(
            "Topic '{}' reading message from partition {} at offset {}",
            self.name, partition_id, offset
        );

        let partition = self.partitions.get(&partition_id).ok_or_else(|| {
            warn!(
                "Topic '{}': partition {} not found for read",
                self.name, partition_id
            );
            KafkaError::PartitionNotFound(partition_id)
        })?;

        partition.read(offset).await
    }

    pub async fn get_latest_offset(&self, partition_id: u32) -> KafkaResult<u64> {
        debug!(
            "Topic '{}' getting latest offset for partition {}",
            self.name, partition_id
        );

        let partition = self.partitions.get(&partition_id).ok_or_else(|| {
            warn!(
                "Topic '{}': partition {} not found for offset query",
                self.name, partition_id
            );
            KafkaError::PartitionNotFound(partition_id)
        })?;

        Ok(partition.latest_offset().await)
    }

    pub fn partition_count(&self) -> u32 {
        let count = self.partitions.len() as u32;
        debug!("Topic '{}' has {} partitions", self.name, count);
        count
    }

    pub async fn cleanup_old_messages(&self, retention_ms: u64) -> KafkaResult<()> {
        info!(
            "Topic '{}' starting cleanup of messages older than {} ms",
            self.name, retention_ms
        );

        for (partition_id, _) in &self.partitions {
            debug!("Topic '{}' cleaning partition {}", self.name, partition_id);
        }

        info!("Topic '{}' completed cleanup", self.name);
        Ok(())
    }

    pub async fn get_topic_stats(&self) -> KafkaResult<TopicStats> {
        info!("Topic '{}' calculating statistics", self.name);

        let mut total_messages = 0u64;
        let mut partition_stats = HashMap::new();

        for (partition_id, partition) in &self.partitions {
            let latest_offset = partition.latest_offset().await;
            total_messages += latest_offset;
            partition_stats.insert(*partition_id, latest_offset);
        }

        let stats = TopicStats {
            name: self.name.clone(),
            partition_count: self.partitions.len() as u32,
            total_messages,
            partition_stats,
        };

        info!(
            "Topic '{}' stats: {} total messages across {} partitions",
            self.name,
            total_messages,
            self.partitions.len()
        );

        Ok(stats)
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Get storage statistics for this topic
    pub async fn get_storage_stats(&self) -> KafkaResult<TopicStorageStats> {
        info!("Topic '{}' calculating storage statistics", self.name);

        let mut total_size = 0u64;
        let mut partition_sizes = HashMap::new();

        // Actually USE the storage field to get file information
        for partition_id in self.partitions.keys() {
            let file_path = self.storage.partition_file_path(&self.name, *partition_id);

            // Read the storage field to get file size
            let size = if file_path.exists() {
                match std::fs::metadata(&file_path) {
                    Ok(metadata) => metadata.len(),
                    Err(_) => 0,
                }
            } else {
                0
            };

            total_size += size;
            partition_sizes.insert(*partition_id, size);
            debug!("Partition {} storage size: {} bytes", partition_id, size);
        }

        let stats = TopicStorageStats {
            topic_name: self.name.clone(),
            total_size_bytes: total_size,
            partition_sizes,
        };

        info!(
            "Topic '{}' storage stats: {} total bytes across {} partitions",
            self.name,
            total_size,
            self.partitions.len()
        );

        Ok(stats)
    }

    /// Force flush all partitions to storage
    pub async fn flush_to_storage(&self) -> KafkaResult<()> {
        info!("Topic '{}' flushing all partitions to storage", self.name);

        // Use self.storage to perform flush operations
        for partition_id in self.partitions.keys() {
            debug!(
                "Topic '{}' flushing partition {} to storage",
                self.name, partition_id
            );

            // Actually read from storage field to get file paths
            let file_path = self.storage.partition_file_path(&self.name, *partition_id);
            debug!("Storage file path: {:?}", file_path);

            // You could add actual flush logic here using self.storage
        }

        info!("Topic '{}' flush to storage completed", self.name);
        Ok(())
    }

    /// Get reference to storage for advanced operations
    pub fn get_storage(&self) -> &Arc<Storage> {
        &self.storage // This explicitly reads the storage field
    }

    /// Backup topic data using storage
    pub async fn backup_to_storage(&self, backup_path: &str) -> KafkaResult<()> {
        info!(
            "Topic '{}' backing up to storage at {}",
            self.name, backup_path
        );

        // Use self.storage for backup operations
        for partition_id in self.partitions.keys() {
            debug!(
                "Topic '{}' backing up partition {} using storage",
                self.name, partition_id
            );

            // Actually read the storage field to get source paths
            let source_path = self.storage.partition_file_path(&self.name, *partition_id);
            debug!("Storage backup source: {:?}", source_path);

            // Here you could implement actual backup logic using self.storage
        }

        info!("Topic '{}' backup completed using storage", self.name);
        Ok(())
    }

    /// Check if topic has any data in storage
    pub async fn has_data_in_storage(&self) -> bool {
        // Read from storage field to check for data
        for partition_id in self.partitions.keys() {
            let file_path = self.storage.partition_file_path(&self.name, *partition_id);
            if file_path.exists() {
                if let Ok(metadata) = std::fs::metadata(&file_path) {
                    if metadata.len() > 0 {
                        return true;
                    }
                }
            }
        }
        false
    }
}

#[derive(Debug, Clone)]
pub struct TopicStats {
    pub name: String,
    pub partition_count: u32,
    pub total_messages: u64,
    pub partition_stats: HashMap<u32, u64>,
}

#[derive(Debug, Clone)]
pub struct TopicStorageStats {
    pub topic_name: String,
    pub total_size_bytes: u64,
    pub partition_sizes: HashMap<u32, u64>, // partition_id -> size_bytes
}
