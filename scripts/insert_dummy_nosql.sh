#!/bin/bash
# Script to insert dummy data for aiDB JSON/NoSQL capabilities via REST API
# Assumes server running on :11111 (start with `cargo run --bin my_ai_db`)
# Demonstrates NoSQL inserts , and example curls for full CRUD (SQL/NoSQL)

set -e

BASE_URL="http://localhost:11111"

echo "🚀 Inserting dummy NoSQL/JSON data into aiDB (via REST on 11111)..."

# Dummy NoSQL JSON docs (unstructured , with vector for hybrid , category for SQL)
curl -s -X POST "$BASE_URL/insert_doc" -H "Content-Type: application/json" -d '{
  "id": "dummy_nosql_1",
  "text": "Dummy NoSQL document 1 for JSON capabilities",
  "category": "AI",
  "vector": [0.1, 0.2, 0.3, 0.4],
  "metadata_json": "{\"type\": \"dummy\", \"value\": 42, \"tags\": [\"test\", \"nosql\"]}"
}' && echo "✅ Inserted dummy_nosql_1"

curl -s -X POST "$BASE_URL/insert_doc" -H "Content-Type: application/json" -d '{
  "id": "dummy_nosql_2",
  "text": "Dummy NoSQL document 2 for multi-model DB",
  "category": "DB",
  "vector": [0.5, 0.6, 0.7, 0.8],
  "metadata_json": "{\"type\": \"dummy\", \"value\": 100, \"tags\": [\"example\", \"json\"]}"
}' && echo "✅ Inserted dummy_nosql_2"

echo "✅ Dummy NoSQL/JSON data inserted (Sled storage)."

# Example cURL requests for full CRUD (SQL + NoSQL)
echo ""
echo "📋 Example cURL requests for CRUD operations:"
echo ""
echo "# Query (SQL: SELECT on NoSQL projected data)"
echo "curl -X POST $BASE_URL/sql -H \"Content-Type: application/json\" -d '{\"sql\": \"SELECT id, category FROM docs WHERE category = \\\"AI\\\"\"}'"
echo ""
echo "# Edit/Update (NoSQL: update JSON doc)"
echo "curl -X POST $BASE_URL/update_doc -H \"Content-Type: application/json\" -d '{
  \"id\": \"dummy_nosql_1\",
  \"text\": \"Updated dummy doc\",
  \"category\": \"AI\",
  \"vector\": [0.9,0.9,0.9,0.9],
  \"metadata_json\": \"{\\\"updated\\\": true}\"
}'"
echo ""
echo "# Delete (NoSQL: remove by ID)"
echo "curl -X DELETE $BASE_URL/delete_doc/dummy_nosql_2"
echo ""
echo "# Advanced SQL (e.g., edit via UPDATE - note: re-project for full ; query post)"
echo "curl -X POST $BASE_URL/sql -H \"Content-Type: application/json\" -d '{\"sql\": \"SELECT * FROM docs WHERE category = \\\"AI\\\"\"}'"
echo ""
echo "# Hybrid (SQL filter + vector)"
echo "curl -X POST $BASE_URL/hybrid_search -H \"Content-Type: application/json\" -d '{
  \"sql_filter\": \"category = \\\"AI\\\"\",
  \"query_vector\": [0.1,0.2,0.3,0.4],
  \"top_k\": 2
}'"

echo ""
echo "Run these examples with server up for CRUD on JSON/NoSQL data!"
