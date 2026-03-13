//! Events module for real-time streaming and pub/sub
//!
//! Provides CDC (Change Data Capture) events and a PubSubManager for WebSocket subscriptions

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast;

/// Event types for CDC (Change Data Capture)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventType {
    Insert,
    Update,
    Delete,
}

/// CDC Event structure for real-time notifications
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcEvent {
    pub event_type: EventType,
    pub collection: String,
    pub id: String,
    pub data: Option<Value>,
    pub timestamp: i64,
}

/// PubSubManager handles event broadcasting via tokio broadcast channels
pub struct PubSubManager {
    tx: broadcast::Sender<CdcEvent>,
}

impl PubSubManager {
    /// Create a new PubSubManager with the specified channel capacity
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    /// Subscribe to events
    pub fn subscribe(&self) -> broadcast::Receiver<CdcEvent> {
        self.tx.subscribe()
    }

    /// Publish an event to all subscribers
    pub fn publish(&self, event: CdcEvent) {
        let _ = self.tx.send(event);
    }
}

impl Default for PubSubManager {
    fn default() -> Self {
        Self::new(1024)
    }
}
