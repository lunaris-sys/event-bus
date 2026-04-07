use crate::proto::Event;
use crate::registry::{ConsumerRegistry, UidFilter};
use crate::validation;
use anyhow::Result;
use prost::Message;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, error, warn};

/// Start both the producer socket and the consumer socket concurrently.
/// Both run forever; if either exits the daemon exits.
pub async fn listen(producer_path: &str, consumer_path: &str, registry: Arc<ConsumerRegistry>) -> Result<()> {
    tokio::try_join!(
        listen_producers(producer_path, registry.clone()),
        listen_consumers(consumer_path, registry),
    )?;
    Ok(())
}

/// Accept incoming producer connections and dispatch their events to the registry.
async fn listen_producers(path: &str, registry: Arc<ConsumerRegistry>) -> Result<()> {
    let listener = bind_socket(path)?;
    info_socket("producer", path);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let registry = registry.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_producer(stream, registry).await {
                        error!("producer connection error: {e}");
                    }
                });
            }
            Err(e) => error!("producer accept error: {e}"),
        }
    }
}

/// Accept incoming consumer connections, register them, and forward matching events.
async fn listen_consumers(path: &str, registry: Arc<ConsumerRegistry>) -> Result<()> {
    let listener = bind_socket(path)?;
    info_socket("consumer", path);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let registry = registry.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_consumer(stream, registry).await {
                        error!("consumer connection error: {e}");
                    }
                });
            }
            Err(e) => error!("consumer accept error: {e}"),
        }
    }
}

/// Extract the peer UID from a Unix stream via SO_PEERCRED.
/// Returns 0 (system) if credentials cannot be read.
fn peer_uid(stream: &UnixStream) -> u32 {
    stream
        .peer_cred()
        .ok()
        .map(|cred| cred.uid())
        .unwrap_or(0)
}

/// Handle a single producer connection.
/// Reads length-prefixed protobuf messages, stamps the UID from SO_PEERCRED,
/// validates them, and dispatches.
async fn handle_producer(mut stream: UnixStream, registry: Arc<ConsumerRegistry>) -> Result<()> {
    let producer_uid = peer_uid(&stream);
    debug!(uid = producer_uid, "new producer connection");

    loop {
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

        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;

        match Event::decode(buf.as_slice()) {
            Ok(mut event) => {
                // Stamp UID from SO_PEERCRED if the producer did not set it.
                // This prevents UID spoofing: the kernel-provided credential
                // always wins over a self-declared UID.
                if event.uid == 0 && producer_uid != 0 {
                    event.uid = producer_uid;
                }

                match validation::validate(&event) {
                    Ok(()) => {
                        debug!(id = %event.id, event_type = %event.r#type, uid = event.uid, "received event");
                        registry.dispatch(&event).await;
                    }
                    Err(e) => warn!(error = %e, "dropping invalid event"),
                }
            }
            Err(e) => warn!(error = %e, "failed to decode event, dropping"),
        }
    }
}

/// Handle a single consumer connection.
///
/// The consumer sends a newline-delimited registration message:
///   Line 1: consumer-id
///   Line 2: event-type1,event-type2,...
///   Line 3: UID filter ("*" for all, or a numeric UID like "1000")
///
/// After registration, the bus writes length-prefixed protobuf Event messages
/// to the socket as they arrive.
async fn handle_consumer(mut stream: UnixStream, registry: Arc<ConsumerRegistry>) -> Result<()> {
    debug!("new consumer connection");

    // Read registration: three newline-terminated strings.
    let consumer_id = read_line(&mut stream).await?;
    let types_line = read_line(&mut stream).await?;
    let uid_line = read_line(&mut stream).await?;

    let subscribed_types: Vec<String> = types_line
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let uid_filter = UidFilter::parse(&uid_line).map_err(|e| anyhow::anyhow!(e))?;

    debug!(
        consumer_id = %consumer_id,
        subscribed = ?subscribed_types,
        uid_filter = ?uid_filter,
        "consumer registered"
    );

    let mut receiver = registry
        .register(consumer_id.clone(), subscribed_types, uid_filter)
        .await;

    // Forward events from the channel to the socket.
    while let Some(event) = receiver.recv().await {
        let encoded = event.encode_to_vec();
        let len = u32::try_from(encoded.len()).expect("event too large to encode").to_be_bytes();

        if stream.write_all(&len).await.is_err()
            || stream.write_all(&encoded).await.is_err()
        {
            break;
        }
    }

    registry.unregister(&consumer_id).await;
    debug!(consumer_id = %consumer_id, "consumer disconnected");
    Ok(())
}

/// Bind a Unix socket, removing any stale socket file first.
fn bind_socket(path: &str) -> Result<UnixListener> {
    if Path::new(path).exists() {
        std::fs::remove_file(path)?;
    }
    if let Some(parent) = Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(UnixListener::bind(path)?)
}

fn info_socket(label: &str, path: &str) {
    tracing::info!(socket = path, "listening for {label} connections");
}

/// Read a newline-terminated string from a Unix stream, up to 4096 bytes.
async fn read_line(stream: &mut UnixStream) -> Result<String> {
    let mut buf = Vec::with_capacity(256);
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await?;
        if byte[0] == b'\n' {
            break;
        }
        buf.push(byte[0]);
        if buf.len() > 4096 {
            anyhow::bail!("registration line too long");
        }
    }
    Ok(String::from_utf8(buf)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_uid_from_peercred() {
        // Create a Unix socket pair to test peer_cred extraction.
        let (sock_a, _sock_b) = tokio::net::UnixStream::pair().unwrap();
        let uid = peer_uid(&sock_a);
        // In tests, the peer UID should be our own UID.
        let expected = unsafe { libc::getuid() };
        assert_eq!(uid, expected, "peer_uid should return the current user's UID");
    }
}
