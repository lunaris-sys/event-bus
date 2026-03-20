#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]

mod proto {
    #![allow(dead_code)]
    #![allow(clippy::doc_markdown)]
    include!(concat!(env!("OUT_DIR"), "/lunaris.eventbus.rs"));
}

mod socket;
mod validation;

use anyhow::Result;
use tracing::info;

const SOCKET_PATH: &str = "/run/lunaris/event-bus.sock";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("event_bus=debug".parse()?),
        )
        .init();

    info!("starting event bus daemon");
    info!(socket = SOCKET_PATH, "listening on unix socket");

    socket::listen(SOCKET_PATH).await?;

    Ok(())
}
