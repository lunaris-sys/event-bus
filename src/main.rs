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

const PRODUCER_SOCKET: &str = "/run/lunaris/event-bus-producer.sock";
const CONSUMER_SOCKET: &str = "/run/lunaris/event-bus-consumer.sock";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("event_bus=debug".parse()?),
        )
        .init();

    info!("starting event bus daemon");

    // Create the shared registry. Arc::clone() is cheap (just increments a ref count).
    // Each socket handler gets its own Arc clone pointing to the same registry.
    let registry = ConsumerRegistry::new();

    socket::listen(PRODUCER_SOCKET, CONSUMER_SOCKET, registry).await?;

    Ok(())
}
