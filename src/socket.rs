use crate::proto::Event;
use crate::validation;
use anyhow::Result;
use prost::Message;
use std::path::Path;
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, error, warn};

/// Start the Unix socket listener and accept incoming connections.
pub async fn listen(path: &str) -> Result<()> {
    // Remove stale socket file if it exists
    if Path::new(path).exists() {
        std::fs::remove_file(path)?;
    }

    // Ensure the socket directory exists
    if let Some(parent) = Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let listener = UnixListener::bind(path)?;

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream).await {
                        error!("connection error: {e}");
                    }
                });
            }
            Err(e) => {
                error!("accept error: {e}");
            }
        }
    }
}

/// Handle a single producer connection.
/// Reads length-prefixed protobuf messages until the connection closes.
async fn handle_connection(mut stream: UnixStream) -> Result<()> {
    debug!("new producer connection");

    loop {
        // Read 4-byte big-endian length prefix
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("producer disconnected");
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }

        let len = u32::from_be_bytes(len_buf) as usize;

        if len == 0 || len > 1024 * 1024 {
            warn!(len, "invalid message length, closing connection");
            return Ok(());
        }

        // Read the protobuf payload
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;

        match Event::decode(buf.as_slice()) {
            Ok(event) => {
                match validation::validate(&event) {
                    Ok(()) => {
                        debug!(
                            id = %event.id,
                            event_type = %event.r#type,
                            source = %event.source,
                            "received event"
                        );
                        // TODO: forward to registered consumers
                    }
                    Err(e) => {
                        warn!(error = %e, "dropping invalid event");
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to decode event, dropping");
            }
        }
    }
}
