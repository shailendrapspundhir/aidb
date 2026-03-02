//! Logging module for aiDB
//!
//! Provides JSON file logging with configurable log levels.
//! Logs are written to a file as JSON objects (one per line).
//! Each log entry includes a session_id field for tracking user sessions.

use std::path::PathBuf;
use std::io::{BufRead, BufReader};
use std::fs::File;
use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};
use tracing_appender::non_blocking::WorkerGuard;
use serde::{Deserialize, Serialize};

/// Configuration for logging
pub struct LogConfig {
    pub level: String,
    pub file_path: PathBuf,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            file_path: PathBuf::from("logs/aidb.log.json"),
        }
    }
}

impl LogConfig {
    /// Load logging configuration from environment variables
    pub fn from_env() -> Self {
        let level = std::env::var("AIDB_LOG_LEVEL")
            .unwrap_or_else(|_| "info".to_string());
        
        let file_path = std::env::var("AIDB_LOG_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("logs/aidb.log.json"));
        
        Self { level, file_path }
    }
    
    /// Get the log file path
    pub fn get_log_path() -> PathBuf {
        std::env::var("AIDB_LOG_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("logs/aidb.log.json"))
    }
}

/// A log entry from the JSON log file
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonLogEntry {
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub level: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub threadId: Option<u64>,
    #[serde(default)]
    pub threadName: Option<String>,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub line_number: Option<u32>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(flatten)]
    pub fields: serde_json::Map<String, serde_json::Value>,
}

fn extract_session_id(entry: &JsonLogEntry) -> Option<String> {
    if let Some(ref sid) = entry.session_id {
        return Some(sid.clone());
    }
    for key in ["session_id", "sessionId", "session"] {
        if let Some(value) = entry.fields.get(key) {
            if let Some(sid) = value.as_str() {
                return Some(sid.to_string());
            }
        }
    }
    None
}

fn extract_username(entry: &JsonLogEntry) -> Option<String> {
    if let Some(ref user) = entry.username {
        return Some(user.clone());
    }
    for key in ["username", "user"] {
        if let Some(value) = entry.fields.get(key) {
            if let Some(user) = value.as_str() {
                return Some(user.to_string());
            }
        }
    }
    None
}

/// Read logs from the JSON log file and filter by session_id
pub fn read_logs_by_session(session_id: &str) -> Result<Vec<JsonLogEntry>, Box<dyn std::error::Error>> {
    let log_path = LogConfig::get_log_path();
    
    if !log_path.exists() {
        return Ok(Vec::new());
    }
    
    let file = File::open(&log_path)?;
    let reader = BufReader::new(file);
    
    let mut logs = Vec::new();
    
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        
        if let Ok(entry) = serde_json::from_str::<JsonLogEntry>(&line) {
            if let Some(sid) = extract_session_id(&entry) {
                if sid == session_id {
                    logs.push(entry);
                }
            }
        }
    }
    
    Ok(logs)
}

/// Read logs from the JSON log file and filter by username
pub fn read_logs_by_username(username: &str) -> Result<Vec<JsonLogEntry>, Box<dyn std::error::Error>> {
    let log_path = LogConfig::get_log_path();
    
    if !log_path.exists() {
        return Ok(Vec::new());
    }
    
    let file = File::open(&log_path)?;
    let reader = BufReader::new(file);
    
    let mut logs = Vec::new();
    
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        
        if let Ok(entry) = serde_json::from_str::<JsonLogEntry>(&line) {
            if let Some(user) = extract_username(&entry) {
                if user == username {
                    logs.push(entry);
                }
            }
        }
    }
    
    Ok(logs)
}

/// Read all logs from the JSON log file
pub fn read_all_logs() -> Result<Vec<JsonLogEntry>, Box<dyn std::error::Error>> {
    let log_path = LogConfig::get_log_path();
    
    if !log_path.exists() {
        return Ok(Vec::new());
    }
    
    let file = File::open(&log_path)?;
    let reader = BufReader::new(file);
    
    let mut logs = Vec::new();
    
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        
        if let Ok(entry) = serde_json::from_str::<JsonLogEntry>(&line) {
            logs.push(entry);
        }
    }
    
    Ok(logs)
}

/// Initialize the logging system with JSON file output
/// Returns a WorkerGuard that must be kept alive for the duration of the application
pub fn init_logging() -> Result<WorkerGuard, Box<dyn std::error::Error>> {
    let config = LogConfig::from_env();
    
    // Create log directory if it doesn't exist
    if let Some(parent) = config.file_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    
    // Create the file appender
    let file_appender = tracing_appender::rolling::never(
        config.file_path.parent().unwrap_or(PathBuf::from(".").as_path()),
        config.file_path.file_name().unwrap_or(std::ffi::OsStr::new("aidb.log.json")),
    );
    
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    
    // Build the subscriber with JSON format
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.level));
    
    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt::layer()
                .with_writer(non_blocking)
                .json()
                .with_span_events(FmtSpan::CLOSE)
                .with_target(true)
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_line_number(true)
                .with_file(true),
        )
        .init();
    
    tracing::info!(
        level = %config.level,
        file = %config.file_path.display(),
        "Logging initialized"
    );
    
    Ok(guard)
}

/// Initialize logging for tests (writes to stdout instead of file)
pub fn init_test_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("debug"))
        )
        .json()
        .with_target(true)
        .with_test_writer()
        .try_init();
}
