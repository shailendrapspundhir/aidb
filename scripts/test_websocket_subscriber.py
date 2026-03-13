#!/usr/bin/env python3
"""
WebSocket Subscriber Test Script for aiDB - Collection-Level Subscriptions

This script demonstrates collection-level CDC subscriptions:
1. Registers a new user
2. Logs in to get an auth token
3. Creates a tenant, environment, and MULTIPLE collections
4. Inserts documents across different collections
5. Connects to the WebSocket endpoint
6. Subscribes to COLLECTIONS (receives all changes in those collections)
7. Listens for real-time updates from any document in subscribed collections

Usage:
    python3 test_websocket_subscriber.py [--server URL] [--mode {collection,document,mixed}]
"""

import argparse
import asyncio
import json
import sys
import time
import websockets
import aiohttp

# Configuration
DEFAULT_SERVER = "http://localhost:11111"
WS_SERVER = "ws://localhost:11111"

# Test data
TEST_USER = f"testuser_{int(time.time())}"
TEST_PASSWORD = "testpassword123"
TEST_TENANT = f"testtenant_{int(time.time())}"
TEST_ENV = "testenv"
TEST_COLLECTION_1 = "products"
TEST_COLLECTION_2 = "orders"


class AidbSubscriber:
    def __init__(self, server_url: str, ws_url: str, mode: str = "collection"):
        self.server_url = server_url
        self.ws_url = ws_url
        self.mode = mode  # "collection", "document", or "mixed"
        self.token = None
        self.session_id = None
        self.collections = [TEST_COLLECTION_1, TEST_COLLECTION_2]
        self.doc_ids = {col: [] for col in self.collections}
        
    async def register(self):
        """Register a new user"""
        print(f"\n[1/8] Registering user '{TEST_USER}'...")
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.server_url}/register",
                json={"username": TEST_USER, "password": TEST_PASSWORD}
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    print(f"    ✓ User registered: {result.get('message', 'OK')}")
                    return True
                else:
                    text = await resp.text()
                    print(f"    ✗ Registration failed: {text}")
                    return False
    
    async def login(self):
        """Login and get auth token"""
        print(f"\n[2/8] Logging in as '{TEST_USER}'...")
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.server_url}/login",
                json={"username": TEST_USER, "password": TEST_PASSWORD}
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    self.token = result.get("token")
                    self.session_id = result.get("session_id")
                    print(f"    ✓ Logged in successfully")
                    print(f"    ✓ Token: {self.token[:20]}...")
                    print(f"    ✓ Session ID: {self.session_id}")
                    return True
                else:
                    text = await resp.text()
                    print(f"    ✗ Login failed: {text}")
                    return False
    
    async def create_tenant(self):
        """Create a tenant"""
        print(f"\n[3/8] Creating tenant '{TEST_TENANT}'...")
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.server_url}/tenants",
                headers={"Authorization": f"Bearer {self.token}"},
                json={"id": TEST_TENANT, "name": f"Test Tenant {TEST_TENANT}"}
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    print(f"    ✓ Tenant created: {result.get('message', 'OK')}")
                    return True
                else:
                    text = await resp.text()
                    print(f"    ✗ Tenant creation failed: {text}")
                    return False
    
    async def create_environment(self):
        """Create an environment"""
        print(f"\n[4/8] Creating environment '{TEST_ENV}'...")
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.server_url}/tenants/{TEST_TENANT}/environments",
                headers={"Authorization": f"Bearer {self.token}"},
                json={"id": TEST_ENV, "name": f"Test Environment {TEST_ENV}"}
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    print(f"    ✓ Environment created: {result.get('message', 'OK')}")
                    return True
                else:
                    text = await resp.text()
                    print(f"    ✗ Environment creation failed: {text}")
                    return False
    
    async def create_collections(self):
        """Create multiple collections"""
        print(f"\n[5/8] Creating collections...")
        for collection in self.collections:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.server_url}/environments/{TEST_ENV}/collections",
                    headers={"Authorization": f"Bearer {self.token}"},
                    json={"id": collection, "name": f"Test Collection {collection}"}
                ) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        print(f"    ✓ Collection created: {collection}")
                    else:
                        text = await resp.text()
                        print(f"    ✗ Collection creation failed for {collection}: {text}")
                        return False
        return True
    
    async def insert_documents(self):
        """Insert test documents across multiple collections"""
        print(f"\n[6/8] Inserting test documents into collections...")
        
        for collection in self.collections:
            print(f"\n    Inserting into '{collection}':")
            for i in range(1, 4):  # 3 documents per collection
                doc = {
                    "id": f"{collection}_doc_{i}",
                    "text": f"Test document {i} in {collection}",
                    "category": collection,
                    "vector": [0.1 * i, 0.2 * i, 0.3 * i, 0.4 * i],
                    "metadata_json": json.dumps({
                        "collection": collection,
                        "index": i,
                        "created_by": TEST_USER,
                        "timestamp": time.time(),
                        "status": "active"
                    })
                }
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.server_url}/collections/{collection}/docs",
                        headers={"Authorization": f"Bearer {self.token}"},
                        json=doc
                    ) as resp:
                        if resp.status == 200:
                            self.doc_ids[collection].append(doc["id"])
                            print(f"      ✓ {doc['id']}")
                        else:
                            text = await resp.text()
                            print(f"      ✗ Failed to insert {doc['id']}: {text}")
        
        total = sum(len(docs) for docs in self.doc_ids.values())
        print(f"\n    ✓ Total documents inserted: {total}")
        return total > 0
    
    async def websocket_listener(self):
        """Connect to WebSocket and listen for updates based on subscription mode"""
        print(f"\n[7/8] Connecting to WebSocket...")
        print(f"    WebSocket URL: {self.ws_url}/ws")
        print(f"    Subscription Mode: {self.mode.upper()}")
        
        try:
            async with websockets.connect(f"{self.ws_url}/ws") as ws:
                # Subscribe based on mode
                if self.mode == "collection":
                    await self._subscribe_collections(ws)
                elif self.mode == "document":
                    await self._subscribe_documents(ws)
                elif self.mode == "mixed":
                    await self._subscribe_mixed(ws)
                
                print(f"\n{'='*60}")
                print("SUBSCRIBER READY - Listening for CDC events")
                print(f"{'='*60}\n")
                
                # Listen for messages
                await self._listen_loop(ws)
                        
        except websockets.exceptions.ConnectionClosed:
            print("\n[DISCONNECTED] WebSocket connection closed")
        except Exception as e:
            print(f"\n[ERROR] WebSocket error: {e}")
    
    async def _subscribe_collections(self, ws):
        """Subscribe to entire collections (receive all changes in these collections)"""
        print(f"\n[8/8] Subscribing to COLLECTIONS (collection-level feeds)...")
        print(f"    This subscriber will receive ALL changes in these collections:\n")
        
        for collection in self.collections:
            subscribe_msg = {
                "action": "subscribe",
                "collection": collection
            }
            await ws.send(json.dumps(subscribe_msg))
            print(f"    [SUBSCRIBE] Collection: {collection}")
            print(f"                ↳ Will receive: insert, update, delete for ANY document")
    
    async def _subscribe_documents(self, ws):
        """Subscribe to specific documents only"""
        print(f"\n[8/8] Subscribing to SPECIFIC DOCUMENTS...")
        
        for collection, doc_ids in self.doc_ids.items():
            for doc_id in doc_ids[:2]:  # Subscribe to first 2 docs per collection
                subscribe_msg = {
                    "action": "subscribe",
                    "collection": collection,
                    "id": doc_id
                }
                await ws.send(json.dumps(subscribe_msg))
                print(f"    [SUBSCRIBE] Document: {collection}:{doc_id}")
    
    async def _subscribe_mixed(self, ws):
        """Mixed subscription: subscribe to one collection and specific docs from another"""
        print(f"\n[8/8] Subscribing with MIXED mode...")
        
        # Subscribe to entire first collection
        subscribe_msg = {
            "action": "subscribe",
            "collection": self.collections[0]
        }
        await ws.send(json.dumps(subscribe_msg))
        print(f"    [SUBSCRIBE] Collection: {self.collections[0]} (all documents)")
        
        # Subscribe to specific docs from second collection
        for doc_id in self.doc_ids[self.collections[1]][:2]:
            subscribe_msg = {
                "action": "subscribe",
                "collection": self.collections[1],
                "id": doc_id
            }
            await ws.send(json.dumps(subscribe_msg))
            print(f"    [SUBSCRIBE] Document: {self.collections[1]}:{doc_id}")
    
    async def _listen_loop(self, ws):
        """Main listening loop for CDC events"""
        event_counts = {"insert": 0, "update": 0, "delete": 0}
        
        while True:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=60.0)
                data = json.loads(message)
                
                # Check if it's a CDC event
                if "event_type" in data:
                    event_type = data['event_type'].upper()
                    collection = data.get('collection', 'unknown')
                    doc_id = data.get('id', 'unknown')
                    
                    event_counts[data['event_type'].lower()] += 1
                    
                    # Color-code different event types
                    icon = {"INSERT": "➕", "UPDATE": "✏️", "DELETE": "🗑️"}.get(event_type, "📄")
                    
                    print(f"{icon} [CDC EVENT] {event_type}")
                    print(f"   Collection: {collection}")
                    print(f"   Document: {doc_id}")
                    print(f"   Timestamp: {data.get('timestamp')}")
                    print(f"   Stats: {event_counts['insert']} inserts, {event_counts['update']} updates, {event_counts['delete']} deletes")
                    
                    if data.get('data'):
                        text = data['data'].get('text', '')
                        if text:
                            print(f"   Content: {text[:80]}...")
                    print()
                    
                elif "status" in data:
                    status = data.get('status')
                    collection = data.get('collection', '')
                    doc_id = data.get('id', '')
                    if doc_id:
                        print(f"[ACK] {status}: {collection}:{doc_id}")
                    else:
                        print(f"[ACK] {status}: {collection} (collection)")
                else:
                    print(f"[MESSAGE] {json.dumps(data, indent=2)}")
                    
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                await ws.send(json.dumps({"action": "ping"}))
                
    async def run(self):
        """Run the complete subscriber workflow"""
        print("="*60)
        print("aiDB WebSocket Subscriber Test - Collection Subscriptions")
        print("="*60)
        print(f"\nMode: {self.mode.upper()}")
        print(f"Collections: {', '.join(self.collections)}")
        
        # Step 1: Register
        if not await self.register():
            return False
        
        # Step 2: Login
        if not await self.login():
            return False
        
        # Step 3: Create tenant
        if not await self.create_tenant():
            return False
        
        # Step 4: Create environment
        if not await self.create_environment():
            return False
        
        # Step 5: Create collections
        if not await self.create_collections():
            return False
        
        # Step 6: Insert documents
        if not await self.insert_documents():
            return False
        
        # Step 7 & 8: WebSocket listener with subscriptions
        await self.websocket_listener()
        
        return True


def main():
    parser = argparse.ArgumentParser(
        description="aiDB WebSocket Subscriber Test - Collection-Level Subscriptions"
    )
    parser.add_argument(
        "--server",
        default=DEFAULT_SERVER,
        help=f"REST API server URL (default: {DEFAULT_SERVER})"
    )
    parser.add_argument(
        "--ws-server",
        default=None,
        help="WebSocket server URL (default: ws://<server_host>:<server_port>)"
    )
    parser.add_argument(
        "--mode",
        choices=["collection", "document", "mixed"],
        default="collection",
        help="Subscription mode: 'collection' (subscribe to entire collections), "
             "'document' (subscribe to specific docs), 'mixed' (combination). Default: collection"
    )
    args = parser.parse_args()
    
    # Determine WebSocket URL
    ws_url = args.ws_server
    if not ws_url:
        # Convert http:// to ws://
        ws_url = args.server.replace("http://", "ws://").replace("https://", "wss://")
    
    subscriber = AidbSubscriber(args.server, ws_url, args.mode)
    
    try:
        asyncio.run(subscriber.run())
    except KeyboardInterrupt:
        print("\n\n[EXIT] Subscriber stopped by user")
        sys.exit(0)


if __name__ == "__main__":
    main()
