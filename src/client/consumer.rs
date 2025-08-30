use crate::network::protocol::{
    CommitOffsetRequest, ConsumeRequest, FetchOffsetRequest, Request, Response,
};
use crate::{KafkaResult, Message};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, interval};
use tracing::{debug, info, warn};

#[derive(Clone)]
pub struct Consumer {
    broker_address: String,
    group_id: String,
    client_id: String, // Now properly used
    subscriptions: HashMap<String, Vec<u32>>,
    offsets: HashMap<(String, u32), u64>,
    auto_commit: bool,
    commit_interval: Duration,
}

impl Consumer {
    pub fn new(broker_address: String, group_id: String, client_id: String) -> Self {
        info!("Creating consumer with client_id: '{}'", client_id); // Use client_id
        Self {
            broker_address,
            group_id,
            client_id,
            subscriptions: HashMap::new(),
            offsets: HashMap::new(),
            auto_commit: true,
            commit_interval: Duration::from_millis(5000),
        }
    }

    pub fn with_auto_commit(mut self, auto_commit: bool) -> Self {
        self.auto_commit = auto_commit;
        self
    }

    pub fn with_commit_interval(mut self, interval: Duration) -> Self {
        self.commit_interval = interval;
        self
    }

    pub async fn subscribe(&mut self, topic: String, partitions: Vec<u32>) -> KafkaResult<()> {
        info!(
            "Client '{}' subscribing to topic '{}' partitions {:?}",
            self.client_id,
            topic,
            partitions // Use client_id
        );

        // Fetch stored offsets for each partition
        for &partition in &partitions {
            let request = Request::FetchOffset(FetchOffsetRequest {
                group_id: self.group_id.clone(),
                topic: topic.clone(),
                partition,
            });

            match self.send_request(request).await {
                Ok(Response::FetchOffsetSuccess { offset }) => {
                    self.offsets.insert((topic.clone(), partition), offset);
                    info!(
                        "Client '{}' resuming from offset {} for topic '{}' partition {}",
                        self.client_id,
                        offset,
                        topic,
                        partition // Use client_id
                    );
                }
                Ok(Response::Error { message }) => {
                    warn!(
                        "Client '{}' could not fetch offset: {}",
                        self.client_id, message
                    ); // Use client_id
                    self.offsets.insert((topic.clone(), partition), 0);
                }
                Ok(_) => {
                    warn!(
                        "Client '{}' unexpected response when fetching offset",
                        self.client_id
                    ); // Use client_id
                    self.offsets.insert((topic.clone(), partition), 0);
                }
                Err(e) => {
                    warn!("Client '{}' error fetching offset: {}", self.client_id, e); // Use client_id
                    self.offsets.insert((topic.clone(), partition), 0);
                }
            }
        }

        self.subscriptions.insert(topic, partitions);
        Ok(())
    }

    pub async fn consume_one(&mut self) -> KafkaResult<Option<Message>> {
        for (topic, partitions) in &self.subscriptions.clone() {
            for &partition in partitions {
                let current_offset = *self.offsets.get(&(topic.clone(), partition)).unwrap_or(&0);

                let request = Request::Consume(ConsumeRequest {
                    topic: topic.clone(),
                    partition,
                    offset: current_offset,
                });

                let response = self.send_request(request).await?;

                match response {
                    Response::ConsumeSuccess {
                        message: Some(message),
                    } => {
                        self.offsets
                            .insert((topic.clone(), partition), current_offset + 1);
                        debug!(
                            "Client '{}' consumed message from topic '{}' partition {} at offset {}",
                            self.client_id,
                            topic,
                            partition,
                            current_offset // Use client_id
                        );
                        return Ok(Some(message));
                    }
                    Response::ConsumeSuccess { message: None } => {
                        continue;
                    }
                    Response::Error { message } => {
                        warn!("Client '{}' consume error: {}", self.client_id, message); // Use client_id
                        continue;
                    }
                    _ => continue,
                }
            }
        }

        Ok(None)
    }

    pub async fn commit_offsets(&mut self) -> KafkaResult<()> {
        for ((topic, partition), &offset) in &self.offsets {
            let request = Request::CommitOffset(CommitOffsetRequest {
                group_id: self.group_id.clone(),
                topic: topic.clone(),
                partition: *partition,
                offset,
            });

            match self.send_request(request).await {
                Ok(Response::CommitOffsetSuccess) => {
                    debug!(
                        "Client '{}' committed offset {} for topic '{}' partition {}",
                        self.client_id,
                        offset,
                        topic,
                        partition // Use client_id
                    );
                }
                Ok(Response::Error { message }) => {
                    warn!(
                        "Client '{}' failed to commit offset: {}",
                        self.client_id, message
                    ); // Use client_id
                }
                Ok(_) => {
                    warn!(
                        "Client '{}' unexpected response when committing offset",
                        self.client_id
                    ); // Use client_id
                }
                Err(e) => {
                    warn!("Client '{}' error committing offset: {}", self.client_id, e); // Use client_id
                }
            }
        }
        Ok(())
    }

    pub async fn start_consuming<F>(&mut self, mut callback: F) -> KafkaResult<()>
    where
        F: FnMut(Message) -> bool + Send,
    {
        let mut poll_interval = interval(Duration::from_millis(100));
        let mut commit_interval = if self.auto_commit {
            Some(interval(self.commit_interval))
        } else {
            None
        };

        info!(
            "Client '{}' starting consumer for group '{}' (auto_commit: {})",
            self.client_id,
            self.group_id,
            self.auto_commit // Use client_id
        );

        loop {
            tokio::select! {
                _ = poll_interval.tick() => {
                    if let Some(message) = self.consume_one().await? {
                        let should_continue = callback(message);
                        if !should_continue {
                            break;
                        }
                    }
                }
                _ = async {
                    if let Some(ref mut interval) = commit_interval {
                        interval.tick().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    if let Err(e) = self.commit_offsets().await {
                        warn!("Client '{}' auto-commit failed: {}", self.client_id, e); // Use client_id
                    }
                }
            }
        }

        // Final commit before exiting
        if let Err(e) = self.commit_offsets().await {
            warn!("Client '{}' final commit failed: {}", self.client_id, e); // Use client_id
        } else {
            info!("Client '{}' final commit successful", self.client_id); // Use client_id
        }

        Ok(())
    }

    // Add method to get client info
    pub fn client_info(&self) -> (&str, &str, &str) {
        (&self.client_id, &self.group_id, &self.broker_address) // Use client_id
    }

    async fn send_request(&self, request: Request) -> KafkaResult<Response> {
        debug!("Client '{}' sending request to broker", self.client_id); // Use client_id
        let mut stream = TcpStream::connect(&self.broker_address)
            .await
            .map_err(|e| {
                crate::KafkaError::NetworkError(
                    format!("Client '{}' connection failed: {}", self.client_id, e), // Use client_id
                )
            })?;

        let serialized = bincode::serialize(&request)?;
        stream.write_u32(serialized.len() as u32).await?;
        stream.write_all(&serialized).await?;

        let response_len = stream.read_u32().await?;
        let mut buffer = vec![0; response_len as usize];
        stream.read_exact(&mut buffer).await?;

        let response: Response = bincode::deserialize(&buffer)?;
        Ok(response)
    }
}
