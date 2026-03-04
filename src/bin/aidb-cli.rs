//! aiDB CLI Tool
//!
//! Command-line interface for interacting with aiDB.
//! Supports fetching logs by session ID.

use clap::{Parser, Subcommand};
use my_ai_db::logging::{read_logs_by_session, read_logs_by_username, read_all_logs, JsonLogEntry};
use serde::{Deserialize, Serialize};
use serde_json;
use std::fs;
use std::io::{self, Read};
use reqwest::blocking::Client;

#[derive(Parser)]
#[command(name = "aidb-cli")]
#[command(about = "aiDB CLI Tool - Manage and query aiDB", long_about = None)]
struct Cli {
    /// Server URL (default: http://localhost:11111)
    #[arg(short, long, default_value = "http://localhost:11111")]
    server_url: String,

    /// Authentication token
    #[arg(short, long)]
    token: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Fetch logs for a specific session
    FetchLogs {
        /// Session ID to fetch logs for
        #[arg(short, long)]
        session_id: Option<String>,
        
        /// Username to fetch logs for (alternative to session_id)
        #[arg(short, long)]
        username: Option<String>,
        
        /// Fetch all logs
        #[arg(short, long)]
        all: bool,
        
        /// Filter by log level (error, warn, info, debug)
        #[arg(short, long)]
        level: Option<String>,
    },

    /// Batch insert documents from a file or stdin
    BatchInsert {
        /// Collection ID to insert into
        #[arg(short, long)]
        collection_id: String,

        /// Path to JSON file (reads from stdin if not provided)
        #[arg(short, long)]
        file: Option<String>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct InsertDocRest {
    pub id: String,
    pub text: String,
    pub category: String,
    pub vector: Vec<f32>,
    pub metadata_json: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct BatchInsertDocRest {
    pub documents: Vec<InsertDocRest>,
}

fn main() {
    // Load .env file if present
    dotenvy::dotenv().ok();
    
    let cli = Cli::parse();
    let client = Client::new();
    
    match &cli.command {
        Commands::FetchLogs { session_id, username, all, level } => {
            let logs: Vec<JsonLogEntry> = if *all {
                read_all_logs().unwrap_or_else(|e| {
                    eprintln!("Error reading logs: {}", e);
                    std::process::exit(1);
                })
            } else if let Some(sid) = session_id {
                read_logs_by_session(sid).unwrap_or_else(|e| {
                    eprintln!("Error reading logs for session {}: {}", sid, e);
                    std::process::exit(1);
                })
            } else if let Some(user) = username {
                read_logs_by_username(user).unwrap_or_else(|e| {
                    eprintln!("Error reading logs for user {}: {}", user, e);
                    std::process::exit(1);
                })
            } else {
                eprintln!("Error: Must specify --session-id, --username, or --all");
                std::process::exit(1);
            };
            
            // Filter by level if specified
            let logs: Vec<JsonLogEntry> = if let Some(ref lvl) = level {
                logs.into_iter()
                    .filter(|log| {
                        log.level.as_ref()
                            .map(|l| l.eq_ignore_ascii_case(lvl))
                            .unwrap_or(false)
                    })
                    .collect()
            } else {
                logs
            };
            
            // Output as JSON array
            let output = serde_json::to_string_pretty(&logs).unwrap_or_else(|e| {
                eprintln!("Error serializing logs: {}", e);
                std::process::exit(1);
            });
            
            println!("{}", output);
            
            eprintln!("\n--- Summary ---");
            eprintln!("Total logs: {}", logs.len());
            if let Some(lvl) = level {
                eprintln!("Filtered by level: {}", lvl);
            }
        }

        Commands::BatchInsert { collection_id, file } => {
            let token = cli.token.expect("Error: --token is required for BatchInsert");
            
            let data = if let Some(path) = file {
                fs::read_to_string(path).unwrap_or_else(|e| {
                    eprintln!("Error reading file {}: {}", path, e);
                    std::process::exit(1);
                })
            } else {
                let mut buffer = String::new();
                io::stdin().read_to_string(&mut buffer).unwrap_or_else(|e| {
                    eprintln!("Error reading from stdin: {}", e);
                    std::process::exit(1);
                });
                buffer
            };

            // Support both a list of docs or the BatchInsertDocRest wrapper
            let payload: BatchInsertDocRest = if let Ok(docs) = serde_json::from_str::<Vec<InsertDocRest>>(&data) {
                BatchInsertDocRest { documents: docs }
            } else {
                serde_json::from_str(&data).unwrap_or_else(|e| {
                    eprintln!("Error parsing JSON: {}", e);
                    std::process::exit(1);
                })
            };

            let url = format!("{}/collections/{}/docs/batch", cli.server_url, collection_id);
            let response = client.post(&url)
                .header("Authorization", format!("Bearer {}", token))
                .json(&payload)
                .send()
                .unwrap_or_else(|e| {
                    eprintln!("Error sending request: {}", e);
                    std::process::exit(1);
                });

            if response.status().is_success() {
                println!("Successfully inserted {} documents", payload.documents.len());
            } else {
                eprintln!("Error: Server returned status {}", response.status());
                eprintln!("Body: {}", response.text().unwrap_or_default());
                std::process::exit(1);
            }
        }
    }
}
