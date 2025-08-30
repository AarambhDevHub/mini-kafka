use crate::{KafkaResult, Message};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions, create_dir_all};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error};

pub struct Storage {
    data_dir: PathBuf,
}

impl Storage {
    pub async fn new(data_dir: &str) -> KafkaResult<Self> {
        let path = PathBuf::from(data_dir);
        create_dir_all(&path).await?;

        Ok(Self { data_dir: path })
    }

    pub async fn load_partition(&self, topic: &str, partition: u32) -> KafkaResult<Vec<Message>> {
        let file_path = self.partition_file_path(topic, partition);

        if !file_path.exists() {
            debug!("Partition file doesn't exist: {:?}", file_path);
            return Ok(Vec::new());
        }

        let mut file = File::open(&file_path).await?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).await?;

        if contents.is_empty() {
            return Ok(Vec::new());
        }

        match bincode::deserialize(&contents) {
            Ok(messages) => Ok(messages),
            Err(e) => {
                error!("Failed to deserialize partition file: {:?}", e);
                Ok(Vec::new())
            }
        }
    }

    pub async fn append_message(
        &self,
        topic: &str,
        partition: u32,
        message: &Message,
        _offset: u64,
    ) -> KafkaResult<()> {
        let mut messages = self.load_partition(topic, partition).await?;
        messages.push(message.clone());

        let file_path = self.partition_file_path(topic, partition);
        create_dir_all(file_path.parent().unwrap()).await?;

        let serialized = bincode::serialize(&messages)?;
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path)
            .await?;

        file.write_all(&serialized).await?;
        file.sync_all().await?;

        Ok(())
    }

    // New methods for consumer offset management
    pub async fn save_consumer_offset(&self, key: &str, offset: u64) -> KafkaResult<()> {
        let offsets_file = self.consumer_offsets_file_path();

        // Load existing offsets
        let mut offsets = self.load_consumer_offsets().await.unwrap_or_default();

        // Update with new offset
        offsets.insert(key.to_string(), offset);

        // Save back to file
        create_dir_all(offsets_file.parent().unwrap()).await?;
        let serialized = bincode::serialize(&offsets)?;
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&offsets_file)
            .await?;

        file.write_all(&serialized).await?;
        file.sync_all().await?;

        Ok(())
    }

    pub async fn load_consumer_offsets(&self) -> KafkaResult<HashMap<String, u64>> {
        let offsets_file = self.consumer_offsets_file_path();

        if !offsets_file.exists() {
            debug!("Consumer offsets file doesn't exist: {:?}", offsets_file);
            return Ok(HashMap::new());
        }

        let mut file = File::open(&offsets_file).await?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).await?;

        if contents.is_empty() {
            return Ok(HashMap::new());
        }

        match bincode::deserialize(&contents) {
            Ok(offsets) => Ok(offsets),
            Err(e) => {
                error!("Failed to deserialize consumer offsets file: {:?}", e);
                Ok(HashMap::new())
            }
        }
    }

    pub fn partition_file_path(&self, topic: &str, partition: u32) -> PathBuf {
        self.data_dir.join(topic).join(format!("{}.log", partition))
    }

    fn consumer_offsets_file_path(&self) -> PathBuf {
        self.data_dir.join("__consumer_offsets.db")
    }
}
