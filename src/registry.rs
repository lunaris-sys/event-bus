use crate::proto::Event;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, warn};

/// How many events can queue up per consumer before we start dropping.
/// A consumer that cannot keep up will lose events rather than stalling the bus.
const CONSUMER_BUFFER: usize = 1024;

/// UID filter for a consumer. Determines which user's events are delivered.
#[derive(Debug, Clone, PartialEq)]
pub enum UidFilter {
    /// Receive events from all users (system consumers like graph-writer).
    All,
    /// Receive events only from this specific user.
    Exact(u32),
}

impl UidFilter {
    /// Parse a UID filter from the registration line.
    /// "*" means all users, a number means that specific UID.
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if s == "*" {
            Ok(UidFilter::All)
        } else {
            s.parse::<u32>()
                .map(UidFilter::Exact)
                .map_err(|e| format!("invalid UID filter '{s}': {e}"))
        }
    }

    /// Check whether an event with the given UID passes this filter.
    /// System events (uid=0) always pass regardless of filter.
    pub fn accepts(&self, event_uid: u32) -> bool {
        // System events (uid=0) are delivered to all consumers.
        if event_uid == 0 {
            return true;
        }
        match self {
            UidFilter::All => true,
            UidFilter::Exact(uid) => event_uid == *uid,
        }
    }
}

/// A registered consumer: an async task that reads from a Unix socket
/// and wants to receive a filtered subset of events.
struct ConsumerEntry {
    id: String,
    /// Event type prefixes this consumer subscribed to.
    /// "file.opened" matches only that type.
    /// "file." matches all file events (prefix match).
    /// "*" matches everything.
    subscribed_types: Vec<String>,
    /// UID filter: which user's events this consumer receives.
    uid_filter: UidFilter,
    /// The sending half of the per-consumer channel.
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
    pub async fn register(
        self: &Arc<Self>,
        id: String,
        subscribed_types: Vec<String>,
        uid_filter: UidFilter,
    ) -> mpsc::Receiver<Event> {
        let (sender, receiver) = mpsc::channel(CONSUMER_BUFFER);
        let entry = ConsumerEntry {
            id: id.clone(),
            subscribed_types,
            uid_filter,
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
    /// Checks both event type pattern AND UID filter.
    pub async fn dispatch(self: &Arc<Self>, event: &Event) {
        let consumers = self.consumers.read().await;

        for consumer in consumers.iter() {
            // Check event type pattern match.
            if !consumer.matches(&event.r#type) {
                continue;
            }

            // Check UID filter.
            if !consumer.uid_filter.accepts(event.uid) {
                continue;
            }

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

    fn make_event(event_type: &str, uid: u32) -> Event {
        Event {
            id: "01950000-0000-7000-8000-000000000001".to_string(),
            r#type: event_type.to_string(),
            timestamp: 1_000_000,
            source: "test".to_string(),
            pid: 1,
            session_id: "session-test".to_string(),
            payload: vec![],
            uid,
            project_id: String::new(),
        }
    }

    #[test]
    fn exact_match() {
        let entry = ConsumerEntry {
            id: "test".to_string(),
            subscribed_types: vec!["file.opened".to_string()],
            uid_filter: UidFilter::All,
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
            uid_filter: UidFilter::All,
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
            uid_filter: UidFilter::All,
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
            .register("consumer-1".to_string(), vec!["file.opened".to_string()], UidFilter::All)
            .await;

        registry.dispatch(&make_event("file.opened", 1000)).await;

        let event_received = receiver.try_recv().expect("should have received event");
        assert_eq!(event_received.r#type, "file.opened");
    }

    #[tokio::test]
    async fn dispatch_skips_non_matching_consumer() {
        let registry = ConsumerRegistry::new();
        let mut receiver = registry
            .register("consumer-1".to_string(), vec!["window.".to_string()], UidFilter::All)
            .await;

        registry.dispatch(&make_event("file.opened", 1000)).await;

        assert!(receiver.try_recv().is_err(), "should not have received event");
    }

    // ── UID Filtering Tests ──

    #[tokio::test]
    async fn test_uid_filtering_same_user() {
        let registry = ConsumerRegistry::new();
        let mut receiver = registry
            .register("c1".to_string(), vec!["*".to_string()], UidFilter::Exact(1000))
            .await;

        registry.dispatch(&make_event("file.opened", 1000)).await;

        assert!(receiver.try_recv().is_ok(), "same UID should be delivered");
    }

    #[tokio::test]
    async fn test_uid_filtering_different_user() {
        let registry = ConsumerRegistry::new();
        let mut receiver = registry
            .register("c1".to_string(), vec!["*".to_string()], UidFilter::Exact(1000))
            .await;

        registry.dispatch(&make_event("file.opened", 2000)).await;

        assert!(receiver.try_recv().is_err(), "different UID should be filtered");
    }

    #[tokio::test]
    async fn test_uid_filtering_system_events() {
        let registry = ConsumerRegistry::new();
        let mut receiver = registry
            .register("c1".to_string(), vec!["*".to_string()], UidFilter::Exact(1000))
            .await;

        // uid=0 is a system event, should reach all consumers.
        registry.dispatch(&make_event("schema.registered", 0)).await;

        assert!(receiver.try_recv().is_ok(), "system event (uid=0) should reach all consumers");
    }

    #[tokio::test]
    async fn test_wildcard_uid_filter() {
        let registry = ConsumerRegistry::new();
        let mut receiver = registry
            .register("c1".to_string(), vec!["*".to_string()], UidFilter::All)
            .await;

        registry.dispatch(&make_event("file.opened", 1000)).await;
        registry.dispatch(&make_event("file.opened", 2000)).await;
        registry.dispatch(&make_event("schema.registered", 0)).await;

        // All three should arrive.
        assert!(receiver.try_recv().is_ok());
        assert!(receiver.try_recv().is_ok());
        assert!(receiver.try_recv().is_ok());
    }

    #[test]
    fn uid_filter_parse() {
        assert_eq!(UidFilter::parse("*").unwrap(), UidFilter::All);
        assert_eq!(UidFilter::parse("1000").unwrap(), UidFilter::Exact(1000));
        assert_eq!(UidFilter::parse("0").unwrap(), UidFilter::Exact(0));
        assert!(UidFilter::parse("abc").is_err());
        assert!(UidFilter::parse("").is_err());
    }

    #[test]
    fn uid_filter_accepts() {
        let all = UidFilter::All;
        assert!(all.accepts(0));
        assert!(all.accepts(1000));
        assert!(all.accepts(2000));

        let exact = UidFilter::Exact(1000);
        assert!(exact.accepts(0));     // system events always pass
        assert!(exact.accepts(1000));  // matching UID
        assert!(!exact.accepts(2000)); // different UID
    }
}
