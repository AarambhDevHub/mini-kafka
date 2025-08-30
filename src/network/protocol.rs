use crate::Message;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Produce(ProduceRequest),
    Consume(ConsumeRequest),
    CreateTopic(CreateTopicRequest),
    ListTopics,
    CommitOffset(CommitOffsetRequest), // New
    FetchOffset(FetchOffsetRequest),   // New
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProduceRequest {
    pub topic: String,
    pub partition: u32,
    pub message: Message,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumeRequest {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTopicRequest {
    pub name: String,
    pub partitions: Option<u32>,
}

// New offset management requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitOffsetRequest {
    pub group_id: String,
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchOffsetRequest {
    pub group_id: String,
    pub topic: String,
    pub partition: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    ProduceSuccess { offset: u64 },
    ConsumeSuccess { message: Option<Message> },
    CreateTopicSuccess,
    ListTopicsSuccess { topics: Vec<String> },
    CommitOffsetSuccess,                // New
    FetchOffsetSuccess { offset: u64 }, // New
    Error { message: String },
}
