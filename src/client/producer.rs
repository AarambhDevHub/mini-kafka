use crate::network::protocol::{ProduceRequest, Request, Response};
use crate::{KafkaResult, Message};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

#[derive(Clone)]
pub struct Producer {
    broker_address: String,
    client_id: String, // Now properly used
}

impl Producer {
    pub fn new(broker_address: String, client_id: String) -> Self {
        info!("Creating producer with client_id: '{}'", client_id); // Use client_id
        Self {
            broker_address,
            client_id,
        }
    }

    pub async fn send(
        &self,
        topic: String,
        key: Option<String>,
        payload: Vec<u8>,
    ) -> KafkaResult<u64> {
        let partition = self.select_partition(&topic, &key, 3);

        let message = Message::new(topic.clone(), partition, payload);
        let message = if let Some(k) = key.clone() {
            message.with_key(k)
        } else {
            message
        };

        let request = Request::Produce(ProduceRequest {
            topic: topic.clone(),
            partition,
            message,
        });

        debug!(
            "Producer '{}' sending message to topic '{}' partition {} with key: {:?}",
            self.client_id,
            topic,
            partition,
            key // Use client_id
        );

        let response = self.send_request(request).await?;

        match response {
            Response::ProduceSuccess { offset } => {
                info!(
                    "Producer '{}' sent message to topic '{}' partition {} at offset {} (key: {:?})",
                    self.client_id,
                    topic,
                    partition,
                    offset,
                    key // Use client_id
                );
                Ok(offset)
            }
            Response::Error { message } => {
                warn!("Producer '{}' error: {}", self.client_id, message); // Use client_id
                Err(crate::KafkaError::BrokerError(message))
            }
            _ => {
                warn!("Producer '{}' received unexpected response", self.client_id); // Use client_id
                Err(crate::KafkaError::BrokerError(
                    "Unexpected response".to_string(),
                ))
            }
        }
    }

    pub async fn send_batch(
        &self,
        messages: Vec<(String, Option<String>, Vec<u8>)>,
    ) -> KafkaResult<Vec<u64>> {
        info!(
            "Producer '{}' sending batch of {} messages",
            self.client_id,
            messages.len()
        ); // Use client_id
        let mut results = Vec::new();

        for (i, (topic, key, payload)) in messages.into_iter().enumerate() {
            debug!(
                "Producer '{}' processing batch message {}",
                self.client_id,
                i + 1
            ); // Use client_id
            let offset = self.send(topic, key, payload).await?;
            results.push(offset);
        }

        info!(
            "Producer '{}' completed batch send with {} results",
            self.client_id,
            results.len()
        ); // Use client_id
        Ok(results)
    }

    fn select_partition(&self, topic: &str, key: &Option<String>, partition_count: u32) -> u32 {
        let partition = match key {
            Some(k) => {
                let mut hasher = DefaultHasher::new();
                k.hash(&mut hasher);
                (hasher.finish() % partition_count as u64) as u32
            }
            None => {
                // Use client_id as part of partition selection for better distribution
                let mut hasher = DefaultHasher::new();
                format!("{}{}", self.client_id, topic).hash(&mut hasher); // Use client_id
                (hasher.finish() % partition_count as u64) as u32
            }
        };

        debug!(
            "Producer '{}' selected partition {} for topic '{}'",
            self.client_id, partition, topic
        ); // Use client_id
        partition
    }

    // Add method to get client info
    pub fn client_info(&self) -> (&str, &str) {
        (&self.client_id, &self.broker_address) // Use client_id
    }

    async fn send_request(&self, request: Request) -> KafkaResult<Response> {
        debug!(
            "Producer '{}' connecting to broker at {}",
            self.client_id, self.broker_address
        ); // Use client_id
        let mut stream = TcpStream::connect(&self.broker_address)
            .await
            .map_err(|e| {
                crate::KafkaError::NetworkError(
                    format!("Producer '{}' connection failed: {}", self.client_id, e), // Use client_id
                )
            })?;

        let serialized = bincode::serialize(&request)?;
        stream.write_u32(serialized.len() as u32).await?;
        stream.write_all(&serialized).await?;

        let response_len = stream.read_u32().await?;
        let mut buffer = vec![0; response_len as usize];
        stream.read_exact(&mut buffer).await?;

        let response: Response = bincode::deserialize(&buffer)?;
        debug!(
            "Producer '{}' received response from broker",
            self.client_id
        ); // Use client_id
        Ok(response)
    }
}
