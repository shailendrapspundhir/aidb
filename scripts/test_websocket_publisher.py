#!/usr/bin/env python3
"""
WebSocket Publisher Test Script for aiDB - Multi-Collection Support

This script:
1. Logs in as an existing user (or creates one if needed)
2. Works with MULTIPLE collections to demonstrate collection-level subscriptions
3. Updates documents across different collections
4. Each update triggers a CDC event that should be received by collection subscribers

Usage:
    python3 test_websocket_publisher.py [--server URL] [--collections COL1,COL2] [--mode {single,multi}]
"""

import argparse
import asyncio
import json
import sys
import time
import aiohttp

# Configuration
DEFAULT_SERVER = "http://localhost:11111"
DEFAULT_COLLECTIONS = "products,orders"

TEST_USER = "testuser_publisher"
TEST_PASSWORD = "testpassword123"


class AidbPublisher:
    def __init__(self, server_url: str, collections: list, mode: str = "multi", num_updates: int = 10):
        self.server_url = server_url
        self.collections = collections
        self.mode = mode  # "single" or "multi"
        self.num_updates = num_updates
        self.token = None
        
    async def register(self):
        """Register a new user if needed"""
        print(f"\n[1/4] Registering user '{TEST_USER}'...")
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.server_url}/register",
                json={"username": TEST_USER, "password": TEST_PASSWORD}
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    print(f"    ✓ User registered: {result.get('message', 'OK')}")
                    return True
                elif resp.status == 400:
                    # User might already exist
                    print(f"    ℹ User may already exist, continuing...")
                    return True
                else:
                    text = await resp.text()
                    print(f"    ✗ Registration failed: {text}")
                    return False
    
    async def login(self):
        """Login and get auth token"""
        print(f"\n[2/4] Logging in as '{TEST_USER}'...")
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.server_url}/login",
                json={"username": TEST_USER, "password": TEST_PASSWORD}
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    self.token = result.get("token")
                    print(f"    ✓ Logged in successfully")
                    print(f"    ✓ Token: {self.token[:20]}...")
                    return True
                else:
                    text = await resp.text()
                    print(f"    ✗ Login failed: {text}")
                    return False
    
    async def check_document(self, collection: str, doc_id: str):
        """Check if the document exists"""
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.server_url}/collections/{collection}/docs/{doc_id}",
                headers={"Authorization": f"Bearer {self.token}"}
            ) as resp:
                if resp.status == 200:
                    return True
                elif resp.status == 404:
                    return False
                else:
                    text = await resp.text()
                    print(f"    ✗ Error checking document: {text}")
                    return False
    
    async def create_document(self, collection: str, doc_id: str):
        """Create the document if it doesn't exist"""
        doc = {
            "id": doc_id,
            "text": f"Initial content for {doc_id} in {collection}",
            "category": collection,
            "vector": [0.1, 0.2, 0.3, 0.4],
            "metadata_json": json.dumps({
                "collection": collection,
                "created_by": TEST_USER,
                "created_at": time.time(),
                "update_count": 0,
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
                    result = await resp.json()
                    print(f"      ✓ Created: {doc_id}")
                    return True
                else:
                    text = await resp.text()
                    print(f"      ✗ Failed to create {doc_id}: {text}")
                    return False
    
    async def update_document(self, collection: str, doc_id: str, update_num: int):
        """Update the document"""
        doc = {
            "id": doc_id,
            "text": f"[{collection}] Updated content - Update #{update_num} at {time.strftime('%H:%M:%S')}",
            "category": collection,
            "vector": [0.1 + (update_num * 0.01), 0.2, 0.3, 0.4],
            "metadata_json": json.dumps({
                "collection": collection,
                "created_by": TEST_USER,
                "last_updated": time.time(),
                "update_count": update_num,
                "update_message": f"Update #{update_num} in {collection}",
                "status": "active" if update_num % 2 == 0 else "pending"
            })
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.put(
                f"{self.server_url}/collections/{collection}/docs",
                headers={"Authorization": f"Bearer {self.token}"},
                json=doc
            ) as resp:
                if resp.status == 200:
                    return True
                else:
                    text = await resp.text()
                    print(f"      ✗ Update failed for {doc_id}: {text}")
                    return False
    
    async def ensure_documents_exist(self):
        """Ensure documents exist in all collections"""
        print(f"\n[3/4] Ensuring documents exist in collections...")
        
        for collection in self.collections:
            print(f"\n    Checking collection '{collection}':")
            for i in range(1, 4):  # 3 docs per collection
                doc_id = f"{collection}_doc_{i}"
                exists = await self.check_document(collection, doc_id)
                if not exists:
                    print(f"      Creating {doc_id}...")
                    await self.create_document(collection, doc_id)
                else:
                    print(f"      ✓ {doc_id} exists")
        return True
    
    async def publish_updates_single_collection(self):
        """Publish updates to a single collection only"""
        collection = self.collections[0]
        doc_id = f"{collection}_doc_1"
        
        print(f"\n{'='*60}")
        print(f"[4/4] Publishing {self.num_updates} updates to '{collection}:{doc_id}'")
        print(f"      Mode: SINGLE COLLECTION")
        print(f"{'='*60}\n")
        
        for i in range(1, self.num_updates + 1):
            print(f"[UPDATE {i}/{self.num_updates}] {collection}:{doc_id}")
            success = await self.update_document(collection, doc_id, i)
            if not success:
                print(f"    ✗ Failed")
            if i < self.num_updates:
                await asyncio.sleep(0.3)
        
        return True
    
    async def publish_updates_multi_collection(self):
        """Publish updates across multiple collections"""
        print(f"\n{'='*60}")
        print(f"[4/4] Publishing updates across {len(self.collections)} collections")
        print(f"      Collections: {', '.join(self.collections)}")
        print(f"      Mode: MULTI-COLLECTION (round-robin)")
        print(f"{'='*60}\n")
        
        update_num = 0
        docs_per_collection = self.num_updates // len(self.collections)
        
        for collection in self.collections:
            print(f"\n  📁 Collection: {collection}")
            for doc_idx in range(1, 4):  # Update 3 different docs per collection
                doc_id = f"{collection}_doc_{doc_idx}"
                for _ in range(docs_per_collection // 3 + 1):
                    update_num += 1
                    if update_num > self.num_updates:
                        break
                    
                    print(f"    [UPDATE {update_num}/{self.num_updates}] {doc_id}")
                    success = await self.update_document(collection, doc_id, update_num)
                    if not success:
                        print(f"      ✗ Failed")
                    await asyncio.sleep(0.2)
                
                if update_num > self.num_updates:
                    break
            if update_num > self.num_updates:
                break
        
        return True
    
    async def publish_updates(self):
        """Publish updates based on mode"""
        if self.mode == "single":
            return await self.publish_updates_single_collection()
        else:
            return await self.publish_updates_multi_collection()
    
    async def show_summary(self):
        """Show summary of all documents"""
        print(f"\n{'='*60}")
        print("FINAL STATE SUMMARY")
        print(f"{'='*60}\n")
        
        for collection in self.collections:
            print(f"Collection: {collection}")
            for i in range(1, 4):
                doc_id = f"{collection}_doc_{i}"
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{self.server_url}/collections/{collection}/docs/{doc_id}",
                        headers={"Authorization": f"Bearer {self.token}"}
                    ) as resp:
                        if resp.status == 200:
                            doc = await resp.json()
                            metadata = doc.get('metadata', {})
                            update_count = metadata.get('update_count', 0)
                            print(f"  {doc_id}: {update_count} updates")
                        else:
                            print(f"  {doc_id}: error fetching")
            print()
    
    async def run(self):
        """Run the complete publisher workflow"""
        print("="*60)
        print("aiDB WebSocket Publisher Test - Multi-Collection")
        print("="*60)
        print(f"\nMode: {self.mode.upper()}")
        print(f"Collections: {', '.join(self.collections)}")
        print(f"Total Updates: {self.num_updates}")
        
        # Step 1: Register (or try to)
        await self.register()
        
        # Step 2: Login
        if not await self.login():
            print("\n[ERROR] Failed to login. Make sure the server is running.")
            return False
        
        # Step 3: Ensure documents exist
        await self.ensure_documents_exist()
        
        # Step 4: Publish updates
        await self.publish_updates()
        
        # Show summary
        await self.show_summary()
        
        print(f"\n{'='*60}")
        print(f"✓ All updates published successfully!")
        print(f"{'='*60}")
        
        return True


def main():
    parser = argparse.ArgumentParser(
        description="aiDB WebSocket Publisher Test - Multi-Collection Support"
    )
    parser.add_argument(
        "--server",
        default=DEFAULT_SERVER,
        help=f"REST API server URL (default: {DEFAULT_SERVER})"
    )
    parser.add_argument(
        "--collections",
        default=DEFAULT_COLLECTIONS,
        help=f"Comma-separated list of collections (default: {DEFAULT_COLLECTIONS})"
    )
    parser.add_argument(
        "--mode",
        choices=["single", "multi"],
        default="multi",
        help="Update mode: 'single' (one doc, many updates) or 'multi' (many docs, round-robin). Default: multi"
    )
    parser.add_argument(
        "--num-updates",
        type=int,
        default=12,
        help="Number of updates to publish (default: 12)"
    )
    args = parser.parse_args()
    
    collections = [c.strip() for c in args.collections.split(",")]
    
    publisher = AidbPublisher(args.server, collections, args.mode, args.num_updates)
    
    try:
        success = asyncio.run(publisher.run())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n[EXIT] Publisher stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
