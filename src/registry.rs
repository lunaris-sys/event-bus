use crate::proto::Event;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, warn};

/// How many events can queue up per consumer before we start dropping.
/// A consumer that cannot keep up will lose events rather than stalling the bus.
const CONSUMER_BUFFER: usize = 1024;

/// A registered consumer: an async task that reads from a Unix socket
/// and wants to receive a filtered subset of events.
struct ConsumerEntry {
    id: String,
    /// Event type prefixes this consumer subscribed to.
    /// "file.opened" matches only that type.
    /// "file." matches all file events (prefix match).
    /// "*" matches everything.
    subscribed_types: Vec<String>,
    /// The sending half of the per-consumer channel.
    /// Cloning this is cheap; it shares the same underlying queue.
    sender: mpsc::Sender<Event>,
}

impl ConsumerEntry {
    fn matches(&self, event_type: &str) -> bool {
        self.subscribed_types.iter().any(|sub| {
            if sub == "*" {
                true
            } else if let Some(prefix) = sub.strip_suffix('.') {
                // "file." matches "file.opened", "file.closed", etc.
                event_type.starts_with(prefix)
            } else {
                sub == event_type
            }
        })
    }
}

/// Shared registry of all active consumers.
/// Wrapped in `Arc<RwLock<...>>` so it can be shared across async tasks.
///
/// Arc = shared ownership across threads (like a thread-safe Rc in C#).
/// `RwLock` = multiple readers OR one writer at a time.
/// We read (dispatch) far more often than we write (register/unregister),
/// so `RwLock` is the right choice over Mutex here.
pub struct ConsumerRegistry {
    consumers: RwLock<Vec<ConsumerEntry>>,
}

impl ConsumerRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            consumers: RwLock::new(Vec::new()),
        })
    }

    /// Register a new consumer and return the receiving end of its channel.
    /// The caller (the consumer socket handler) reads from this receiver
    /// and forwards events over the Unix socket to the consumer process.
    pub async fn register(
        self: &Arc<Self>,
        id: String,
        subscribed_types: Vec<String>,
    ) -> mpsc::Receiver<Event> {
        let (sender, receiver) = mpsc::channel(CONSUMER_BUFFER);
        let entry = ConsumerEntry {
            id: id.clone(),
            subscribed_types,
            sender,
        };
        self.consumers.write().await.push(entry);
        debug!(consumer_id = %id, "consumer registered");
        receiver
    }

    /// Unregister a consumer by ID. Called when the consumer disconnects.
    pub async fn unregister(self: &Arc<Self>, id: &str) {
        let mut consumers = self.consumers.write().await;
        consumers.retain(|c| c.id != id);
        debug!(consumer_id = %id, "consumer unregistered");
    }

    /// Dispatch an event to all matching consumers.
    /// Consumers that cannot keep up (full buffer) receive a warning and
    /// the event is dropped for that consumer only.
    pub async fn dispatch(self: &Arc<Self>, event: &Event) {
        // Acquire a read lock: multiple dispatches can run concurrently.
        let consumers = self.consumers.read().await;

        for consumer in consumers.iter() {
            if !consumer.matches(&event.r#type) {
                continue;
            }

            // try_send returns Err immediately if the buffer is full,
            // rather than blocking. This is the backpressure mechanism:
            // a slow consumer loses events but does not stall the bus.
            match consumer.sender.try_send(event.clone()) {
                Ok(()) => {
                    debug!(
                        consumer_id = %consumer.id,
                        event_type = %event.r#type,
                        "dispatched event"
                    );
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!(
                        consumer_id = %consumer.id,
                        event_type = %event.r#type,
                        "consumer buffer full, dropping event"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // The receiver was dropped; the consumer disconnected
                    // without calling unregister. Will be cleaned up on
                    // next unregister call or registry sweep.
                    warn!(
                        consumer_id = %consumer.id,
                        "consumer channel closed unexpectedly"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(event_type: &str) -> Event {
        Event {
            id: "01950000-0000-7000-8000-000000000001".to_string(),
            r#type: event_type.to_string(),
            timestamp: 1_000_000,
            source: "test".to_string(),
            pid: 1,
            session_id: "session-test".to_string(),
            payload: vec![],
        }
    }

    #[test]
    fn exact_match() {
        let entry = ConsumerEntry {
            id: "test".to_string(),
            subscribed_types: vec!["file.opened".to_string()],
            sender: mpsc::channel(1).0,
        };
        assert!(entry.matches("file.opened"));
        assert!(!entry.matches("file.closed"));
        assert!(!entry.matches("window.focused"));
    }

    #[test]
    fn prefix_match() {
        let entry = ConsumerEntry {
            id: "test".to_string(),
            subscribed_types: vec!["file.".to_string()],
            sender: mpsc::channel(1).0,
        };
        assert!(entry.matches("file.opened"));
        assert!(entry.matches("file.closed"));
        assert!(!entry.matches("window.focused"));
    }

    #[test]
    fn wildcard_match() {
        let entry = ConsumerEntry {
            id: "test".to_string(),
            subscribed_types: vec!["*".to_string()],
            sender: mpsc::channel(1).0,
        };
        assert!(entry.matches("file.opened"));
        assert!(entry.matches("window.focused"));
        assert!(entry.matches("anything"));
    }

    #[tokio::test]
    async fn dispatch_reaches_matching_consumer() {
        let registry = ConsumerRegistry::new();
        let mut receiver = registry
            .register("consumer-1".to_string(), vec!["file.opened".to_string()])
            .await;

        registry.dispatch(&make_event("file.opened")).await;

        let event_received = receiver.try_recv().expect("should have received event");
        assert_eq!(event_received.r#type, "file.opened");
    }

    #[tokio::test]
    async fn dispatch_skips_non_matching_consumer() {
        let registry = ConsumerRegistry::new();
        let mut receiver = registry
            .register("consumer-1".to_string(), vec!["window.".to_string()])
            .await;

        registry.dispatch(&make_event("file.opened")).await;

        assert!(receiver.try_recv().is_err(), "should not have received event");
    }
}
