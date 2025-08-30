use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub broker: BrokerConfig,
    pub network: NetworkConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    pub id: u32,
    pub data_dir: String,
    pub default_partitions: u32,
    pub retention_ms: u64,
    pub max_message_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub bind_address: SocketAddr,
    pub max_connections: usize,
    pub request_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub log_dir: String,
    pub max_log_size: u64,
    pub retention_days: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            broker: BrokerConfig {
                id: 1,
                data_dir: "data/broker".to_string(),
                default_partitions: 3,
                retention_ms: 7 * 24 * 60 * 60 * 1000, // 7 days
                max_message_size: 1024 * 1024,         // 1MB
            },
            network: NetworkConfig {
                bind_address: "127.0.0.1:9092".parse().unwrap(),
                max_connections: 100,
                request_timeout_ms: 30000,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                log_dir: "data/logs".to_string(),
                max_log_size: 100 * 1024 * 1024, // 100MB
                retention_days: 7,
            },
        }
    }
}
