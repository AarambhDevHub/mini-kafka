use clap::Parser;
use mini_kafka::network::Server;
use mini_kafka::{Broker, Config};
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Configuration file path
    #[arg(short, long, default_value = "config.json")]
    config: String,

    /// Broker ID
    #[arg(short, long, default_value = "1")]
    broker_id: u32,

    /// Bind address
    #[arg(short = 'a', long, default_value = "127.0.0.1:9092")]
    address: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Load or create default config
    let mut config = Config::default();
    config.broker.id = args.broker_id;
    config.network.bind_address = args.address.parse().unwrap();

    info!("Starting Mini Kafka Broker with ID: {}", config.broker.id);
    info!("Binding to: {}", config.network.bind_address);

    // Create broker
    let broker = Broker::new(config.broker.clone()).await?;

    // Create and start server
    let server = Server::new(broker, &config.network.bind_address.to_string()).await?;

    // Handle graceful shutdown
    let server = Arc::new(server);
    let server_clone = server.clone();

    tokio::select! {
        result = server_clone.start() => {
            if let Err(e) = result {
                error!("Server error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal, stopping server...");
        }
    }

    Ok(())
}
