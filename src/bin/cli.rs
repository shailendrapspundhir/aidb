use clap::{Parser, Subcommand};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::fs;

#[derive(Parser)]
#[command(name = "aidb-cli")]
#[command(about = "CLI for aiDB", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(short, long, default_value = "http://localhost:11111")]
    url: String,
}

#[derive(Subcommand)]
enum Commands {
    Register {
        #[arg(short, long)]
        username: String,
        #[arg(short, long)]
        password: String,
    },
    Login {
        #[arg(short, long)]
        username: String,
        #[arg(short, long)]
        password: String,
    },
    CreateTenant {
        #[arg(short, long)]
        id: String,
        #[arg(short, long)]
        name: String,
    },
    CreateEnv {
        #[arg(short = 't', long)]
        tenant_id: String,
        #[arg(short, long)]
        id: String,
        #[arg(short, long)]
        name: String,
    },
    CreateCollection {
        #[arg(short = 'e', long)]
        env_id: String,
        #[arg(short, long)]
        id: String,
        #[arg(short, long)]
        name: String,
    },
    Insert {
        #[arg(short = 'C', long = "collection")]
        collection_id: String,
        #[arg(short, long)]
        id: String,
        #[arg(short = 't', long)]
        text: String,
        #[arg(short = 'c', long)]
        category: String,
        #[arg(short = 'm', long, default_value = "{}")]
        metadata: String,
    },
    Update {
        #[arg(short = 'C', long = "collection")]
        collection_id: String,
        #[arg(short, long)]
        id: String,
        #[arg(short = 't', long)]
        text: String,
        #[arg(short = 'c', long)]
        category: String,
        #[arg(short = 'm', long, default_value = "{}")]
        metadata: String,
    },
    GetDoc {
        #[arg(short = 'C', long = "collection")]
        collection_id: String,
        #[arg(short, long)]
        id: String,
    },
    ListDocs {
        #[arg(short = 'C', long = "collection")]
        collection_id: String,
    },
    DeleteDoc {
        #[arg(short = 'C', long = "collection")]
        collection_id: String,
        #[arg(short, long)]
        id: String,
    },
    DeleteCollection {
        #[arg(short = 'e', long)]
        env_id: String,
        #[arg(short, long)]
        id: String,
    },
    Sql {
        #[arg(short = 'C', long = "collection")]
        collection_id: String,
        #[arg(short, long)]
        query: String,
    },
    RagIngest {
        #[arg(short = 'C', long = "collection")]
        collection_id: String,
        #[arg(short, long)]
        doc_id: String,
        #[arg(short, long)]
        text: String,
        #[arg(short = 'm', long)]
        metadata_json: Option<String>,
        #[arg(short, long)]
        source: Option<String>,
    },
    RagIngestFile {
        #[arg(short = 'C', long = "collection")]
        collection_id: String,
        #[arg(short, long)]
        doc_id: String,
        #[arg(short, long)]
        path: String,
        #[arg(short = 'm', long)]
        metadata_json: Option<String>,
        #[arg(short, long)]
        source: Option<String>,
    },
    RagSearch {
        #[arg(short = 'C', long = "collection")]
        collection_id: String,
        #[arg(short, long)]
        query: String,
        #[arg(short, long, default_value_t = 5)]
        top_k: usize,
    },
    Logout,
}

#[derive(Deserialize)]
struct LoginResponse {
    token: String,
    session_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let client = Client::new();

    match cli.command {
        Commands::Register { username, password } => {
            let res = client.post(format!("{}/register", cli.url))
                .json(&json!({ "username": username, "password": password }))
                .send()
                .await?;
            println!("Response: {}", res.text().await?);
        }
        Commands::Login { username, password } => {
            let res = client.post(format!("{}/login", cli.url))
                .json(&json!({ "username": username, "password": password }))
                .send()
                .await?;
            if res.status().is_success() {
                let body: LoginResponse = res.json().await?;
                // Save token
                fs::write(".aidb_token", body.token)?;
                println!("Logged in. Token saved to .aidb_token");
                println!("Session ID: {}", body.session_id);
            } else {
                println!("Login failed: {}", res.text().await?);
            }
        }
        Commands::CreateTenant { id, name } => {
            let token = fs::read_to_string(".aidb_token").unwrap_or_default();
            let res = client.post(format!("{}/tenants", cli.url))
                .header("Authorization", format!("Bearer {}", token))
                .json(&json!({ "id": id, "name": name }))
                .send()
                .await?;
            println!("Response: {}", res.text().await?);
        }
        Commands::CreateEnv { tenant_id, id, name } => {
            let token = fs::read_to_string(".aidb_token").unwrap_or_default();
            let res = client.post(format!("{}/tenants/{}/environments", cli.url, tenant_id))
                .header("Authorization", format!("Bearer {}", token))
                .json(&json!({ "id": id, "name": name }))
                .send()
                .await?;
            println!("Response: {}", res.text().await?);
        }
        Commands::CreateCollection { env_id, id, name } => {
            let token = fs::read_to_string(".aidb_token").unwrap_or_default();
            let res = client.post(format!("{}/environments/{}/collections", cli.url, env_id))
                .header("Authorization", format!("Bearer {}", token))
                .json(&json!({ "id": id, "name": name }))
                .send()
                .await?;
            println!("Response: {}", res.text().await?);
        }
        Commands::Insert { collection_id, id, text, category, metadata } => {
            let token = fs::read_to_string(".aidb_token").unwrap_or_default();
            let res = client.post(format!("{}/collections/{}/docs", cli.url, collection_id))
                .header("Authorization", format!("Bearer {}", token))
                .json(&json!({
                    "id": id,
                    "text": text,
                    "category": category,
                    "vector": vec![0.0; 4], // Placeholder
                    "metadata_json": metadata
                }))
                .send()
                .await?;
            println!("Response: {}", res.text().await?);
        }
        Commands::Update { collection_id, id, text, category, metadata } => {
            let token = fs::read_to_string(".aidb_token").unwrap_or_default();
            let res = client.put(format!("{}/collections/{}/docs", cli.url, collection_id))
                .header("Authorization", format!("Bearer {}", token))
                .json(&json!({
                    "id": id,
                    "text": text,
                    "category": category,
                    "vector": vec![0.0; 4], // Placeholder
                    "metadata_json": metadata
                }))
                .send()
                .await?;
            println!("Response: {}", res.text().await?);
        }
        Commands::GetDoc { collection_id, id } => {
            let token = fs::read_to_string(".aidb_token").unwrap_or_default();
            let res = client.get(format!("{}/collections/{}/docs/{}", cli.url, collection_id, id))
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await?;
            println!("Response: {}", res.text().await?);
        }
        Commands::ListDocs { collection_id } => {
            let token = fs::read_to_string(".aidb_token").unwrap_or_default();
            let res = client.get(format!("{}/collections/{}/docs", cli.url, collection_id))
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await?;
            println!("Response: {}", res.text().await?);
        }
        Commands::DeleteDoc { collection_id, id } => {
            let token = fs::read_to_string(".aidb_token").unwrap_or_default();
            let res = client.delete(format!("{}/collections/{}/docs/{}", cli.url, collection_id, id))
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await?;
            println!("Response: {}", res.text().await?);
        }
        Commands::DeleteCollection { env_id, id } => {
            let token = fs::read_to_string(".aidb_token").unwrap_or_default();
            let res = client.delete(format!("{}/environments/{}/collections/{}", cli.url, env_id, id))
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await?;
            println!("Response: {}", res.text().await?);
        }
        Commands::Sql { collection_id, query } => {
            let token = fs::read_to_string(".aidb_token").unwrap_or_default();
            let res = client.post(format!("{}/collections/{}/sql", cli.url, collection_id))
                .header("Authorization", format!("Bearer {}", token))
                .json(&json!({ "sql": query }))
                .send()
                .await?;
            println!("Response: {}", res.text().await?);
        }
        Commands::RagIngest {
            collection_id,
            doc_id,
            text,
            metadata_json,
            source,
        } => {
            let token = fs::read_to_string(".aidb_token").unwrap_or_default();
            let res = client.post(format!("{}/collections/{}/rag/ingest", cli.url, collection_id))
                .header("Authorization", format!("Bearer {}", token))
                .json(&json!({
                    "doc_id": doc_id,
                    "text": text,
                    "metadata_json": metadata_json,
                    "source": source,
                }))
                .send()
                .await?;
            println!("Response: {}", res.text().await?);
        }
        Commands::RagIngestFile {
            collection_id,
            doc_id,
            path,
            metadata_json,
            source,
        } => {
            let token = fs::read_to_string(".aidb_token").unwrap_or_default();
            let text = fs::read_to_string(&path)?;
            let res = client.post(format!("{}/collections/{}/rag/ingest", cli.url, collection_id))
                .header("Authorization", format!("Bearer {}", token))
                .json(&json!({
                    "doc_id": doc_id,
                    "text": text,
                    "metadata_json": metadata_json,
                    "source": source.or(Some(path)),
                }))
                .send()
                .await?;
            println!("Response: {}", res.text().await?);
        }
        Commands::RagSearch {
            collection_id,
            query,
            top_k,
        } => {
            let token = fs::read_to_string(".aidb_token").unwrap_or_default();
            let res = client.post(format!("{}/collections/{}/rag/search", cli.url, collection_id))
                .header("Authorization", format!("Bearer {}", token))
                .json(&json!({ "query": query, "top_k": top_k }))
                .send()
                .await?;
            println!("Response: {}", res.text().await?);
        }
        Commands::Logout => {
            let _ = fs::remove_file(".aidb_token");
            println!("Logged out (token removed).");
        }
    }

    Ok(())
}
