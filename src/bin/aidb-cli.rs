//! aiDB CLI Tool
//!
//! Command-line interface for interacting with aiDB.
//! Supports fetching logs by session ID.

use clap::{Parser, Subcommand};
use my_ai_db::logging::{read_logs_by_session, read_logs_by_username, read_all_logs, JsonLogEntry};
use serde_json;

#[derive(Parser)]
#[command(name = "aidb-cli")]
#[command(about = "aiDB CLI Tool - Manage and query aiDB", long_about = None)]
struct Cli {
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
}

fn main() {
    // Load .env file if present
    dotenvy::dotenv().ok();
    
    let cli = Cli::parse();
    
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
    }
}
