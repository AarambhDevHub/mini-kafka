pub mod broker;
pub mod client;
pub mod config;
pub mod logging;
pub mod network;

pub use broker::{Broker, BrokerConfig};
pub use client::{Consumer, Producer};
pub use config::Config;
pub use logging::DistributedLog;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Message {
    pub id: Uuid,
    pub topic: String,
    pub partition: u32,
    pub key: Option<String>,
    pub payload: Vec<u8>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub headers: HashMap<String, String>,
}

impl Message {
    pub fn new(topic: String, partition: u32, payload: Vec<u8>) -> Self {
        Self {
            id: Uuid::new_v4(),
            topic,
            partition,
            key: None,
            payload,
            timestamp: chrono::Utc::now(),
            headers: HashMap::new(),
        }
    }

    pub fn with_key(mut self, key: String) -> Self {
        self.key = Some(key);
        self
    }

    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
    #[error("Partition not found: {0}")]
    PartitionNotFound(u32),
    #[error("Broker error: {0}")]
    BrokerError(String),
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

pub type KafkaResult<T> = Result<T, KafkaError>;
