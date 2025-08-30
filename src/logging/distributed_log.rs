use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::RwLock;
use tracing::debug;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub node_id: u32,
    pub message: String,
    pub metadata: std::collections::HashMap<String, String>,
}

pub struct DistributedLog {
    entries: Arc<RwLock<VecDeque<LogEntry>>>,
    max_entries: usize,
    node_id: u32,
    log_file: Arc<tokio::sync::Mutex<File>>,
}

impl DistributedLog {
    pub async fn new(node_id: u32, log_path: &str, max_entries: usize) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(std::path::Path::new(log_path).parent().unwrap()).await?;

        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .await?;

        let log = Self {
            entries: Arc::new(RwLock::new(VecDeque::new())),
            max_entries,
            node_id,
            log_file: Arc::new(tokio::sync::Mutex::new(log_file)),
        };

        // Load existing logs
        log.load_existing_logs(log_path).await?;

        Ok(log)
    }

    async fn load_existing_logs(&self, log_path: &str) -> anyhow::Result<()> {
        if let Ok(file) = File::open(log_path).await {
            let reader = BufReader::new(file);
            let mut lines = reader.lines();

            while let Some(line) = lines.next_line().await? {
                if let Ok(entry) = serde_json::from_str::<LogEntry>(&line) {
                    let mut entries = self.entries.write().await;
                    entries.push_back(entry);

                    // Maintain max entries limit
                    if entries.len() > self.max_entries {
                        entries.pop_front();
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn log(
        &self,
        level: &str,
        message: &str,
        metadata: Option<std::collections::HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        let entry = LogEntry {
            timestamp: Utc::now(),
            level: level.to_string(),
            node_id: self.node_id,
            message: message.to_string(),
            metadata: metadata.unwrap_or_default(),
        };

        // Add to memory
        {
            let mut entries = self.entries.write().await;
            entries.push_back(entry.clone());

            if entries.len() > self.max_entries {
                entries.pop_front();
            }
        }

        // Write to file
        {
            let mut file = self.log_file.lock().await;
            let json = serde_json::to_string(&entry)?;
            file.write_all(format!("{}\n", json).as_bytes()).await?;
            file.flush().await?;
        }

        debug!("Logged: {} - {}", level, message);
        Ok(())
    }

    pub async fn get_recent_logs(&self, limit: usize) -> Vec<LogEntry> {
        let entries = self.entries.read().await;
        entries.iter().rev().take(limit).cloned().collect()
    }

    pub async fn search_logs(&self, query: &str) -> Vec<LogEntry> {
        let entries = self.entries.read().await;
        entries
            .iter()
            .filter(|entry| {
                entry.message.contains(query) || entry.metadata.values().any(|v| v.contains(query))
            })
            .cloned()
            .collect()
    }
}
