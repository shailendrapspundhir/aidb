//! Session management module for tracking user sessions and associated logs
//!
//! Each login creates a new session with a unique ID. All operations within
//! that session are tagged with the session ID for easy log retrieval.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Represents a user session
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Session {
    pub id: String,
    pub username: String,
    pub created_at: u64,
    pub last_activity: u64,
    pub ended_at: Option<u64>,
}

/// A log entry associated with a session
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
    pub timestamp: u64,
    pub level: String,
    pub message: String,
    pub target: String,
    pub fields: HashMap<String, serde_json::Value>,
}

/// In-memory session store (persists to disk via sled)
pub struct SessionManager {
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    session_logs: Arc<Mutex<HashMap<String, Vec<LogEntry>>>>,
    user_sessions: Arc<Mutex<HashMap<String, Vec<String>>>>, // username -> session_ids
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            session_logs: Arc::new(Mutex::new(HashMap::new())),
            user_sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a new session for a user
    pub fn create_session(&self, username: &str) -> String {
        let session_id = Uuid::new_v4().to_string();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let session = Session {
            id: session_id.clone(),
            username: username.to_string(),
            created_at: now,
            last_activity: now,
            ended_at: None,
        };
        
        // Store session
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.insert(session_id.clone(), session);
        }
        
        // Track user's sessions
        if let Ok(mut user_sessions) = self.user_sessions.lock() {
            user_sessions
                .entry(username.to_string())
                .or_insert_with(Vec::new)
                .push(session_id.clone());
        }
        
        // Initialize empty log list for this session
        if let Ok(mut session_logs) = self.session_logs.lock() {
            session_logs.insert(session_id.clone(), Vec::new());
        }
        
        session_id
    }

    /// Get a session by ID
    pub fn get_session(&self, session_id: &str) -> Option<Session> {
        if let Ok(sessions) = self.sessions.lock() {
            sessions.get(session_id).cloned()
        } else {
            None
        }
    }

    /// Get all sessions for a user
    pub fn get_user_sessions(&self, username: &str) -> Vec<Session> {
        if let Ok(user_sessions) = self.user_sessions.lock() {
            if let Some(session_ids) = user_sessions.get(username) {
                if let Ok(sessions) = self.sessions.lock() {
                    return session_ids
                        .iter()
                        .filter_map(|id| sessions.get(id).cloned())
                        .collect();
                }
            }
        }
        Vec::new()
    }

    /// Update last activity time for a session
    pub fn touch_session(&self, session_id: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        if let Ok(mut sessions) = self.sessions.lock() {
            if let Some(session) = sessions.get_mut(session_id) {
                session.last_activity = now;
            }
        }
    }

    /// End a session
    pub fn end_session(&self, session_id: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        if let Ok(mut sessions) = self.sessions.lock() {
            if let Some(session) = sessions.get_mut(session_id) {
                session.ended_at = Some(now);
            }
        }
    }

    /// Add a log entry to a session
    pub fn add_log(&self, session_id: &str, entry: LogEntry) {
        if let Ok(mut session_logs) = self.session_logs.lock() {
            if let Some(logs) = session_logs.get_mut(session_id) {
                logs.push(entry);
            }
        }
    }

    /// Get all logs for a session
    pub fn get_session_logs(&self, session_id: &str) -> Vec<LogEntry> {
        if let Ok(session_logs) = self.session_logs.lock() {
            session_logs.get(session_id).cloned().unwrap_or_default()
        } else {
            Vec::new()
        }
    }

    /// Get logs for a session filtered by level
    pub fn get_session_logs_by_level(&self, session_id: &str, level: &str) -> Vec<LogEntry> {
        if let Ok(session_logs) = self.session_logs.lock() {
            session_logs
                .get(session_id)
                .map(|logs| {
                    logs.iter()
                        .filter(|log| log.level.eq_ignore_ascii_case(level))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    }

    /// Clear logs for a session
    pub fn clear_session_logs(&self, session_id: &str) {
        if let Ok(mut session_logs) = self.session_logs.lock() {
            if let Some(logs) = session_logs.get_mut(session_id) {
                logs.clear();
            }
        }
    }

    /// Delete old sessions (cleanup)
    pub fn cleanup_old_sessions(&self, max_age_secs: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let cutoff = now.saturating_sub(max_age_secs);
        
        if let Ok(mut sessions) = self.sessions.lock() {
            let old_session_ids: Vec<String> = sessions
                .iter()
                .filter(|(_, session)| session.last_activity < cutoff)
                .map(|(id, _)| id.clone())
                .collect();
            
            for id in old_session_ids {
                sessions.remove(&id);
            }
        }
        
        if let Ok(mut session_logs) = self.session_logs.lock() {
            session_logs.retain(|id, _| {
                if let Ok(sessions) = self.sessions.lock() {
                    sessions.contains_key(id)
                } else {
                    true
                }
            });
        }
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Global session manager instance
static SESSION_MANAGER: std::sync::OnceLock<Arc<SessionManager>> = std::sync::OnceLock::new();

/// Get or initialize the global session manager
pub fn get_session_manager() -> Arc<SessionManager> {
    SESSION_MANAGER
        .get_or_init(|| Arc::new(SessionManager::new()))
        .clone()
}
