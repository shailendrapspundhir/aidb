# aiDB - Hybrid Vector Database in Rust

aiDB combines high-performance storage, advanced indexing, and a robust networking layer in a modular Rust stack. Started with storage + indexing engine as specified.

## Modular Architecture
- **Storage Engine**: Sled (persistent KV store) + Apache Arrow (RecordBatch for rich metadata)
  - Proven: Write Arrow record (metadata) + vector by ID to Sled and retrieve it
- **Indexing Engine**: [instant-distance](https://crates.io/crates/instant-distance) (HNSW for ANN similarity search)
- **Networking Layer**: Tonic + Tokio (async gRPC)
- **Query/Processing**: DataFusion (integrated for SQL/Arrow)
- **Consensus/Distrib**: raft-engine (for future HA)
- **Build/Proto**: tonic-build, prost

Future phases: full DataFusion integration, Raft clustering, HTTP gateway.

## Core Functionality
- Hybrid: metadata (Arrow) + vectors stored/retrieved by ID
- Vector search: approximate nearest neighbors
- gRPC API for production use

## Quick Start
```bash
# 1. Populate database with sample data (10 docs w/ Arrow metadata + vectors)
cargo run --bin load_data

# 2. (Optional) Set cache size in MB (defaults to 64 if unset)
export AIDB_CACHE_MB=128

# 3. Start the aiDB gRPC server
cargo run --bin my_ai_db
```

## Usage with gRPC (curl-like via grpcurl)
Use [grpcurl](https://github.com/fullstorydev/grpcurl) (the "curl for gRPC"):

```bash
# VectorSearch: retrieve by vector similarity using indexing engine
grpcurl -plaintext -d '{
  "query_vector": [1.0, 0.1, 0.1, 0.1],
  "top_k": 3
}' [::1]:50051 aidb.AiDbService/VectorSearch

# Example response:
# {
#   "results": ["doc0", "doc4", "doc8"]
# }

# Insert: write new Arrow metadata record + vector to Sled
grpcurl -plaintext -d '{
  "id": "doc_new",
  "text": "Advanced hybrid vector DB metadata",
  "vector": [0.9, 0.2, 0.1, 0.1]
}' [::1]:50051 aidb.AiDbService/Insert

# Text search (placeholder, extend with DataFusion)
grpcurl -plaintext -d '{"query": "vector database"}' [::1]:50051 aidb.AiDbService/Search
```

## Tests & Validation
- Unit tests for storage/retrieval and indexing: `cargo test`
- Verified: Arrow RecordBatch + vector insert/retrieve by ID in Sled
- Load script for data ingestion

## Debian Package & Systemd Service
Build a Debian package that installs the aiDB server, configures a systemd service (auto-restart on failure), and includes an uninstall helper.

```bash
# Build the release .deb package
./scripts/build_deb.sh

# Install the package (creates aidb user, systemd service)
sudo dpkg -i dist/aidb_<version>_amd64.deb

# Start / check status
sudo systemctl status aidb.service
sudo systemctl restart aidb.service

# Uninstall (keep data/logs)
sudo aidb-uninstall

# Uninstall and purge data/logs
sudo aidb-uninstall --purge
```

Service details:
- Binary installed to `/usr/bin/aidb-server`
- Data dir: `/var/lib/aidb`
- Logs: `/var/log/aidb/aidb.log.json`
- Systemd service: `/lib/systemd/system/aidb.service`

## Build & Run with Docker
```bash
docker build -t aidb .
docker run -p 50051:50051 aidb
```

## Multi-Model Extension: SQL + NoSQL + Vector
Extended to unified multi-model DB per high-perf strategy:
- **NoSQL (JSON)**: Serde docs in Sled KV (schema-flexible unstructured).
- **SQL (Structured)**: DataFusion on Arrow projections from Sled (virtual 'docs' table, vectorized).
- **Vector**: Existing HNSW integrated.
- **Hybrid Planner**: Custom logic for SQL filter + vector ANN + NoSQL fetch (predicate push-down: e.g., `SELECT * FROM docs WHERE category='AI' AND similarity(vec, [0.1,...]) > 0.8`).
- **Performance**: No data movement; parallel candidates from index + SQL scan.

See table in initial design for libs.

## Project Structure
- `src/storage.rs`: Unified Sled (NoSQL JSON + vectors/Arrow)
- `src/indexing.rs`: HNSW
- `src/query.rs`: DataFusion SQL + hybrid planner
- `src/main.rs`: Multi-model gRPC
- `scripts/load_data.rs`: Multi-model loader (JSON/SQL demo)

## Updated gRPC Examples (Multi-Model)
```bash
# NoSQL JSON insert
grpcurl -plaintext -d '{
  "id": "doc_json", "text": "NoSQL doc", "category": "AI",
  "vector": [0.5,0.5,0.5,0.5], "metadata_json": "{\"tags\":[\"test\"]}"
}' [::1]:50051 aidb.AiDbService/InsertDoc

# SQL query (DataFusion on JSON projection)
grpcurl -plaintext -d '{"sql": "SELECT id, category FROM docs WHERE category = '\''AI'\''"}' [::1]:50051 aidb.AiDbService/ExecuteSql

# Hybrid: SQL + vector (push-down)
grpcurl -plaintext -d '{
  "sql_filter": "category = '\''AI'\''", "query_vector": [1.0,0.1,0.1,0.1], "top_k": 3
}' [::1]:50051 aidb.AiDbService/HybridSearch
```

## REST API Exposure (on port 11111)
Exposed via Axum HTTP/JSON (concurrent with gRPC; curl-friendly):
- Endpoints mirror multi-model: `/insert_doc`, `/sql`, `/hybrid_search`, `/health`.
- Start server: `cargo run --bin my_ai_db` (both gRPC:50051 + REST:11111).

### cURL Examples (Direct HTTP)
```bash
# Health check
curl -X GET http://localhost:11111/health

# NoSQL JSON insert (to Sled)
curl -X POST http://localhost:11111/insert_doc \
  -H "Content-Type: application/json" \
  -d '{
    "id": "rest_doc", "text": "REST NoSQL", "category": "AI",
    "vector": [0.5,0.5,0.5,0.5], "metadata_json": "{\"tags\":[\"curl\"]}"
  }'

# SQL query (DataFusion)
curl -X POST http://localhost:11111/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT id, category FROM docs WHERE category = \"AI\""}'

# Hybrid search (SQL filter + vector)
curl -X POST http://localhost:11111/hybrid_search \
  -H "Content-Type: application/json" \
  -d '{
    "sql_filter": "category = \"AI\"", "query_vector": [1.0,0.1,0.1,0.1], "top_k": 3
  }'
```

## Dummy Data & CRUD Examples (SQL/NoSQL via REST)
Use the dummy insert script for JSON/NoSQL:

```bash
# Insert dummy NoSQL/JSON data (run server first)
./scripts/insert_dummy_nosql.sh
```

Script populates dummy docs , provides example cURL for CRUD.

### cURL Examples for CRUD (NoSQL + SQL on port 11111)
```bash
# Insert (NoSQL JSON to Sled)
curl -X POST http://localhost:11111/insert_doc -H "Content-Type: application/json" -d '{
  "id": "dummy_nosql_1", "text": "Dummy NoSQL", "category": "AI",
  "vector": [0.1,0.2,0.3,0.4], "metadata_json": "{\"type\":\"dummy\",\"value\":42}"
}'

# Query (SQL on NoSQL projection)
curl -X POST http://localhost:11111/sql -H "Content-Type: application/json" -d '{
  "sql": "SELECT id, category FROM docs WHERE category = \"AI\""
}'

# Edit/Update (NoSQL upsert)
curl -X POST http://localhost:11111/update_doc -H "Content-Type: application/json" -d '{
  "id": "dummy_nosql_1", "text": "Updated dummy", "category": "AI",
  "vector": [0.9,0.9,0.9,0.9], "metadata_json": "{\"updated\":true}"
}'

# Delete (NoSQL)
curl -X DELETE http://localhost:11111/delete_doc/dummy_nosql_2

# Advanced: SQL for edit/delete/query (e.g., UPDATE/DELETE)
curl -X POST http://localhost:11111/sql -H "Content-Type: application/json" -d '{
  "sql": "SELECT * FROM docs WHERE category = \"AI\""
}'
```

Built for performance, modularity, and AI workloads (e.g., embeddings, RAG, analytics).

## RAG System (Retrieval-Augmented Generation)

aiDB now includes a built-in RAG system for ingesting text data and performing semantic similarity search. This is ideal for building AI-powered applications like chatbots, document search, and knowledge bases.

### Features

- **Text Chunking**: Automatically split large texts into manageable chunks with configurable size and overlap
- **Embedding Generation**: Generate vector embeddings using n-gram hashing (lightweight, no external dependencies)
- **Semantic Search**: Find similar documents using vector similarity (HNSW-based)
- **Dual Storage**: Both text and embeddings stored together for efficient retrieval
- **REST & gRPC APIs**: Full API support for all RAG operations

### Quick Start with RAG

```bash
# 1. Start the server
cargo run --bin my_ai_db

# 2. Ingest text into a collection (requires authentication)
curl -X POST http://localhost:11111/collections/my_collection/rag/ingest \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "doc_id": "doc_1",
    "text": "This is a sample document about artificial intelligence and machine learning.",
    "metadata_json": "{\"category\": \"tech\"}",
    "source": "example.com"
  }'

# 3. Search for similar documents
curl -X POST http://localhost:11111/collections/my_collection/rag/search \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "query": "machine learning and AI",
    "top_k": 5
  }'

# 4. Generate embeddings for text
curl -X POST http://localhost:11111/rag/embed \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{"text": "Hello, world!"}'
```

### RAG API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/collections/:id/rag/ingest` | POST | Ingest text into RAG system |
| `/collections/:id/rag/search` | POST | Semantic search for similar documents |
| `/collections/:id/rag/docs` | GET | List all RAG document IDs |
| `/collections/:id/rag/docs/:doc_id` | GET | Get document chunks |
| `/collections/:id/rag/docs/:doc_id` | DELETE | Delete a document |
| `/rag/embed` | POST | Generate embedding for text |

### Configuration

The RAG system can be configured with:

- **Embedding Dimension**: Size of the embedding vector (default: 384)
- **Chunk Size**: Maximum tokens per chunk (default: 512)
- **Chunk Overlap**: Overlap between chunks (default: 50)
- **N-gram Range**: Range for n-gram hashing (default: 1-4)

### gRPC RAG Methods

```bash
# Ingest text
grpcurl -plaintext -d '{
  "doc_id": "doc_1",
  "text": "Sample text for RAG",
  "collection_id": "my_collection"
}' [::1]:50051 aidb.AiDbService/RagIngest

# Search
grpcurl -plaintext -d '{
  "query": "search query",
  "collection_id": "my_collection",
  "top_k": 5
}' [::1]:50051 aidb.AiDbService/RagSearch

# Generate embedding
grpcurl -plaintext -d '{"text": "Hello"}' [::1]:50051 aidb.AiDbService/RagEmbed
```

### Embedding Model

The default embedding model uses **n-gram hashing** for generating text embeddings:

- **Fast**: No model download or GPU required
- **Lightweight**: Pure Rust implementation
- **Deterministic**: Same text always produces the same embedding
- **Suitable for**: Development, testing, and basic semantic similarity

For production use with high-quality semantic embeddings, consider integrating with external embedding services like:
- OpenAI Embeddings API
- Cohere Embed API
- HuggingFace Inference API
- Self-hosted sentence-transformers

See issues for roadmap!
