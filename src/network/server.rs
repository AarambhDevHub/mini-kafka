use crate::KafkaResult;
use crate::broker::Broker;
use crate::network::protocol::{Request, Response};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

pub struct Server {
    broker: Arc<Broker>,
    listener: TcpListener,
}

impl Server {
    pub async fn new(broker: Broker, bind_address: &str) -> KafkaResult<Self> {
        let listener = TcpListener::bind(bind_address)
            .await
            .map_err(|e| crate::KafkaError::NetworkError(e.to_string()))?;

        info!("Server bound to {}", bind_address);

        Ok(Self {
            broker: Arc::new(broker),
            listener,
        })
    }

    pub async fn start(&self) -> KafkaResult<()> {
        info!("Starting Mini Kafka server...");

        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    info!("New connection from {}", addr);
                    let broker = self.broker.clone();

                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(broker, stream).await {
                            error!("Connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    async fn handle_connection(broker: Arc<Broker>, mut stream: TcpStream) -> KafkaResult<()> {
        loop {
            let request_len = match stream.read_u32().await {
                Ok(len) => len,
                Err(_) => break,
            };

            let mut buffer = vec![0; request_len as usize];
            stream.read_exact(&mut buffer).await?;

            let request: Request = match bincode::deserialize(&buffer) {
                Ok(req) => req,
                Err(e) => {
                    warn!("Failed to deserialize request: {}", e);
                    continue;
                }
            };

            let response = Self::process_request(&broker, request).await;

            let serialized = bincode::serialize(&response)?;
            stream.write_u32(serialized.len() as u32).await?;
            stream.write_all(&serialized).await?;
        }

        Ok(())
    }

    async fn process_request(broker: &Arc<Broker>, request: Request) -> Response {
        match request {
            Request::Produce(req) => match broker.produce(req.message).await {
                Ok(offset) => Response::ProduceSuccess { offset },
                Err(e) => Response::Error {
                    message: e.to_string(),
                },
            },
            Request::Consume(req) => {
                match broker.consume(&req.topic, req.partition, req.offset).await {
                    Ok(message) => Response::ConsumeSuccess { message },
                    Err(e) => Response::Error {
                        message: e.to_string(),
                    },
                }
            }
            Request::CreateTopic(req) => {
                match broker.create_topic(req.name, req.partitions).await {
                    Ok(_) => Response::CreateTopicSuccess,
                    Err(e) => Response::Error {
                        message: e.to_string(),
                    },
                }
            }
            Request::ListTopics => {
                let topics = broker.list_topics();
                Response::ListTopicsSuccess { topics }
            }
            // New offset management handlers
            Request::CommitOffset(req) => {
                match broker
                    .commit_offset(&req.group_id, &req.topic, req.partition, req.offset)
                    .await
                {
                    Ok(_) => Response::CommitOffsetSuccess,
                    Err(e) => Response::Error {
                        message: e.to_string(),
                    },
                }
            }
            Request::FetchOffset(req) => {
                match broker
                    .fetch_offset(&req.group_id, &req.topic, req.partition)
                    .await
                {
                    Ok(offset) => Response::FetchOffsetSuccess { offset },
                    Err(e) => Response::Error {
                        message: e.to_string(),
                    },
                }
            }
        }
    }
}
