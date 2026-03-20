#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]

mod proto {
    #![allow(dead_code)]
    #![allow(clippy::doc_markdown)]
    include!(concat!(env!("OUT_DIR"), "/lunaris.eventbus.rs"));
}

mod registry;
mod socket;
mod validation;

use anyhow::Result;
use registry::ConsumerRegistry;
use tracing::info;

const DEFAULT_PRODUCER_SOCKET: &str = "/run/lunaris/event-bus-producer.sock";
const DEFAULT_CONSUMER_SOCKET: &str = "/run/lunaris/event-bus-consumer.sock";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("event_bus=debug".parse()?),
        )
        .init();

    // Read socket paths from environment, fall back to production defaults.
    // This allows integration tests to use temporary paths without modifying
    // the binary.
    let producer_socket = std::env::var("LUNARIS_PRODUCER_SOCKET")
        .unwrap_or_else(|_| DEFAULT_PRODUCER_SOCKET.to_string());
    let consumer_socket = std::env::var("LUNARIS_CONSUMER_SOCKET")
        .unwrap_or_else(|_| DEFAULT_CONSUMER_SOCKET.to_string());

    info!("starting event bus daemon");

    let registry = ConsumerRegistry::new();
    socket::listen(&producer_socket, &consumer_socket, registry).await?;

    Ok(())
}
