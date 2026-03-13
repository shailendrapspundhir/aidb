# aiDB WebSocket Real-Time Streaming Test Suite

This directory contains test scripts for demonstrating aiDB's real-time streaming and pub/sub capabilities using WebSocket connections with CDC (Change Data Capture) events.

## Overview

The aiDB database now supports:

1. **WebSocket Endpoint** (`/ws`) - Real-time bidirectional communication
2. **CDC Events** - Automatic events fired on every insert/update/delete
3. **Pub/Sub System** - Subscribe to collections or specific documents
4. **Collection-Level Feeds** - Subscribe to entire collections and receive all changes
5. **Change Streaming** - Receive real-time updates as they happen

## Subscription Types

### 1. Collection-Level Subscription (NEW)
Subscribe to an entire collection and receive **ALL** changes (inserts, updates, deletes) for **ANY** document in that collection.

```json
{"action": "subscribe", "collection": "products"}
```

**Use Cases**:
- Real-time dashboards showing all products
- Activity feeds for a specific data type
- Cache invalidation for entire collections
- Multi-user collaboration on a dataset

### 2. Document-Level Subscription
Subscribe to a specific document and receive changes only for that document.

```json
{"action": "subscribe", "collection": "products", "id": "product_123"}
```

**Use Cases**:
- Watching a specific order's status
- Tracking a single user's profile changes
- Monitoring a specific configuration document

### 3. Full CDC Stream
Connect without subscribing to receive **ALL** events from **ALL** collections (system-wide CDC).

**Use Cases**:
- Audit logging
- Data replication
- Analytics pipelines
- Backup/sync systems

## Test Scripts

### 1. `test_websocket_subscriber.py`

**Purpose**: Sets up the environment and listens for CDC events with collection-level subscriptions

**What it does**:
1. Registers a new user
2. Logs in to get an auth token
3. Creates a tenant, environment, and **multiple collections** (products, orders)
4. Inserts documents across collections
5. Connects to WebSocket at `/ws`
6. **Subscribes to collections** (receives all changes in those collections)
7. Listens for and displays CDC events in real-time

**Usage**:
```bash
python3 test_websocket_subscriber.py [--server URL] [--mode {collection,document,mixed}]
```

**Examples**:

```bash
# Subscribe to entire collections (default mode)
python3 test_websocket_subscriber.py --server http://localhost:11111 --mode collection

# Subscribe to specific documents only
python3 test_websocket_subscriber.py --server http://localhost:11111 --mode document

# Mixed: subscribe to one collection + specific docs from another
python3 test_websocket_subscriber.py --server http://localhost:11111 --mode mixed
```

**Subscription Modes**:
- `collection`: Subscribe to entire collections (all documents)
- `document`: Subscribe to specific documents only
- `mixed`: Combination of collection + document subscriptions

### 2. `test_websocket_publisher.py`

**Purpose**: Generates CDC events by updating documents across multiple collections

**What it does**:
1. Logs in as a publisher user
2. Works with **multiple collections** (products, orders by default)
3. Updates documents across different collections
4. Each update triggers a CDC event sent to collection subscribers

**Usage**:
```bash
python3 test_websocket_publisher.py [--server URL] [--collections COL1,COL2] [--mode {single,multi}] [--num-updates N]
```

**Examples**:

```bash
# Update documents across multiple collections (round-robin)
python3 test_websocket_publisher.py --server http://localhost:11111 --mode multi

# Update a single document many times
python3 test_websocket_publisher.py --server http://localhost:11111 --mode single --num-updates 20

# Work with specific collections
python3 test_websocket_publisher.py --server http://localhost:11111 --collections products,orders,customers
```

**Modes**:
- `single`: Update one document many times (good for testing document-level subscriptions)
- `multi`: Update documents across collections (good for testing collection-level subscriptions)

### 3. `test_websocket_demo.sh`

**Purpose**: Combined demo script that orchestrates the full test

**What it does**:
1. Checks/starts the aiDB server
2. Launches the subscriber script
3. Provides instructions for running the publisher
4. Demonstrates the complete CDC workflow

**Usage**:
```bash
./test_websocket_demo.sh [--server URL] [--ws-server URL]
```

**Example**:
```bash
./test_websocket_demo.sh
```

## Prerequisites

1. **Python 3.7+** with the following packages:
   ```bash
   pip3 install aiohttp websockets
   ```

2. **aiDB Server** running on the specified port (default: 11111)

3. **Rust toolchain** (if starting the server via the demo script)

## Running the Test

### Quick Start (Recommended)

1. Open a terminal and run the demo script:
   ```bash
   cd scripts
   ./test_websocket_demo.sh
   ```

2. The subscriber will set up the environment and start listening

3. Open a **second terminal** and run the publisher:
   ```bash
   cd scripts
   python3 test_websocket_publisher.py
   ```

4. Watch the subscriber terminal receive real-time CDC events!

### Manual Test (Step by Step)

1. **Start the aiDB server**:
   ```bash
   cargo run --bin my_ai_db
   ```

2. **In Terminal 1 - Start the subscriber**:
   ```bash
   python3 scripts/test_websocket_subscriber.py
   ```
   Wait for it to complete setup and start listening.

3. **In Terminal 2 - Run the publisher**:
   ```bash
   python3 scripts/test_websocket_publisher.py --collection testcollection --doc-id doc_1
   ```

4. **Observe** the CDC events appearing in Terminal 1

## WebSocket Protocol

### Connect
```
ws://localhost:11111/ws
```

### Subscribe to Collection
```json
{
  "action": "subscribe",
  "collection": "my_collection"
}
```

### Subscribe to Specific Document
```json
{
  "action": "subscribe",
  "collection": "my_collection",
  "id": "doc_id"
}
```

### Unsubscribe
```json
{
  "action": "unsubscribe",
  "collection": "my_collection",
  "id": "doc_id"
}
```

### CDC Event Format (Received from Server)
```json
{
  "event_type": "update",
  "collection": "testcollection",
  "id": "doc_1",
  "data": {
    "id": "doc_1",
    "text": "Updated content...",
    "category": "test",
    "vector": [0.1, 0.2, 0.3, 0.4],
    "metadata": {...}
  },
  "timestamp": 1709901234
}
```

Event types: `insert`, `update`, `delete`

## Architecture

The real-time streaming system consists of:

1. **Events Module** (`src/events.rs`):
   - `PubSubManager` - Manages broadcast channels
   - `CdcEvent` - CDC event structure
   - `EventType` - Insert/Update/Delete enum

2. **CDC Wrapper** (`src/storage/mod.rs`):
   - `CdcWrapper<T>` - Wraps storage engines
   - Automatically emits events on mutations
   - Works with all storage backends

3. **WebSocket Handler** (`src/rest.rs`):
   - `ws_handler` - HTTP upgrade handler
   - `handle_socket` - WebSocket connection handler
   - Supports collection-level and document-level subscriptions

4. **CDC Integration** (in REST handlers):
   - `insert_doc_handler` - Emits Insert events
   - `update_doc_handler` - Emits Update events
   - `delete_doc_handler` - Emits Delete events

## Troubleshooting

### Connection Refused
- Ensure the aiDB server is running
- Check the server URL and port
- Verify firewall settings

### WebSocket Errors
- Check that `aiohttp` and `websockets` are installed
- Verify WebSocket URL (ws:// for HTTP, wss:// for HTTPS)

### No CDC Events Received
- Ensure the subscriber is connected before running the publisher
- Check that the collection and document IDs match
- Verify the document exists before updating

### Authentication Errors
- The subscriber creates a new user; the publisher uses a different user
- Both need valid JWT tokens for protected endpoints

## Expected Output

### Subscriber Terminal (Collection Mode)
```
============================================================
aiDB WebSocket Subscriber Test - Collection Subscriptions
============================================================

Mode: COLLECTION
Collections: products, orders

[1/8] Registering user 'testuser_1234567890'...
    ✓ User registered: User registered

[2/8] Logging in as 'testuser_1234567890'...
    ✓ Logged in successfully
    ✓ Token: eyJ0eXAiOiJKV1QiLCJhbGc...
    ✓ Session ID: abc123

...

[8/8] Subscribing to COLLECTIONS (collection-level feeds)...
    This subscriber will receive ALL changes in these collections:

    [SUBSCRIBE] Collection: products
                ↳ Will receive: insert, update, delete for ANY document
    [SUBSCRIBE] Collection: orders
                ↳ Will receive: insert, update, delete for ANY document

============================================================
SUBSCRIBER READY - Listening for CDC events
============================================================

➕ [CDC EVENT] INSERT
   Collection: products
   Document: products_doc_1
   Timestamp: 1709901234
   Stats: 1 inserts, 0 updates, 0 deletes
   Content: Test document 1 in products...

✏️ [CDC EVENT] UPDATE
   Collection: products
   Document: products_doc_1
   Timestamp: 1709901235
   Stats: 1 inserts, 1 updates, 0 deletes
   Content: [products] Updated content - Update #1 at 12:34:56...

✏️ [CDC EVENT] UPDATE
   Collection: orders
   Document: orders_doc_2
   Timestamp: 1709901236
   Stats: 1 inserts, 2 updates, 0 deletes
   Content: [orders] Updated content - Update #2 at 12:34:57...
```

### Publisher Terminal (Multi-Collection Mode)
```
============================================================
aiDB WebSocket Publisher Test - Multi-Collection
============================================================

Mode: MULTI
Collections: products, orders
Total Updates: 12

[1/4] Registering user 'testuser_publisher'...
    ℹ User may already exist, continuing...

[2/4] Logging in as 'testuser_publisher'...
    ✓ Logged in successfully
    ✓ Token: eyJ0eXAiOiJKV1QiLCJhbGc...

[3/4] Ensuring documents exist in collections...

    Checking collection 'products':
      ✓ products_doc_1 exists
      ✓ products_doc_2 exists
      ✓ products_doc_3 exists

    Checking collection 'orders':
      ✓ orders_doc_1 exists
      ✓ orders_doc_2 exists
      ✓ orders_doc_3 exists

============================================================
[4/4] Publishing updates across 2 collections
      Collections: products, orders
      Mode: MULTI-COLLECTION (round-robin)
============================================================

  📁 Collection: products
    [UPDATE 1/12] products_doc_1
    [UPDATE 2/12] products_doc_2
    [UPDATE 3/12] products_doc_3

  📁 Collection: orders
    [UPDATE 4/12] orders_doc_1
    [UPDATE 5/12] orders_doc_2
    [UPDATE 6/12] orders_doc_3
...

============================================================
✓ All updates published successfully!
============================================================
```

## Collection-Level Subscription Examples

### Example 1: Subscribe to Products Collection
**Subscriber** (Terminal 1):
```bash
python3 test_websocket_subscriber.py --mode collection
```

**Publisher** (Terminal 2):
```bash
python3 test_websocket_publisher.py --collections products --mode multi
```

**Result**: Subscriber receives ALL updates to ANY document in the `products` collection.

### Example 2: Subscribe to Multiple Collections
**Subscriber** (Terminal 1):
```bash
python3 test_websocket_subscriber.py --mode collection
```

**Publisher** (Terminal 2):
```bash
python3 test_websocket_publisher.py --collections products,orders,customers --mode multi --num-updates 30
```

**Result**: Subscriber receives updates from BOTH `products` AND `orders` collections.

### Example 3: Mixed Subscriptions
**Subscriber** (Terminal 1):
```bash
python3 test_websocket_subscriber.py --mode mixed
```
This subscribes to:
- ALL documents in the `products` collection
- SPECIFIC documents in the `orders` collection

**Publisher** (Terminal 2):
```bash
python3 test_websocket_publisher.py --mode multi
```

**Result**: 
- Updates to `products:doc_1` → Subscriber receives ✓
- Updates to `products:doc_2` → Subscriber receives ✓
- Updates to `orders:doc_1` (subscribed) → Subscriber receives ✓
- Updates to `orders:doc_3` (not subscribed) → Subscriber does NOT receive ✗

## Additional Resources

- [aiDB README](../README.md)
- [REST API Documentation](../README.md#rest-api) (Swagger UI at `/swagger-ui`)
- [Protocol Buffers](../proto/service.proto)
