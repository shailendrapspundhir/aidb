#!/bin/bash

# REST API Test Script for aiDB
# Uses only curl/REST APIs, no CLI binary

set -e

# Configuration
URL="http://localhost:11111"
SERVER_BIN="cargo run --quiet --bin my_ai_db"
LOG_FILE_PATH="$(pwd)/logs/aidb.log.json"
export AIDB_LOG_FILE="$LOG_FILE_PATH"

REST_TOKEN=""

echo "=========================================="
echo "aiDB REST API Test Suite"
echo "=========================================="
echo ""

echo "Starting my_ai_db server..."
$SERVER_BIN &
PID=$!
echo "Server started with PID: $PID"

# Wait for server to start
sleep 10

# Helper function for authenticated API calls
auth_api_call() {
  local METHOD=$1
  local ENDPOINT=$2
  local DATA=$3

  if [ -n "$DATA" ]; then
    curl -s -X "$METHOD" "$URL$ENDPOINT" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $REST_TOKEN" \
      -d "$DATA"
  else
    curl -s -X "$METHOD" "$URL$ENDPOINT" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $REST_TOKEN"
  fi
}

# Helper for unauthenticated calls (register/login)
api_call() {
  local METHOD=$1
  local ENDPOINT=$2
  local DATA=$3

  if [ -n "$DATA" ]; then
    curl -s -X "$METHOD" "$URL$ENDPOINT" \
      -H "Content-Type: application/json" \
      -d "$DATA"
  else
    curl -s -X "$METHOD" "$URL$ENDPOINT" \
      -H "Content-Type: application/json"
  fi
}

echo "1. Registering user 'admin'..."
api_call POST /register '{"username":"admin","password":"admin"}'
echo ""

echo "2. Logging in and getting token..."
LOGIN_RESPONSE=$(api_call POST /login '{"username":"admin","password":"admin"}')
echo "$LOGIN_RESPONSE"
REST_TOKEN=$(echo "$LOGIN_RESPONSE" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
SESSION_ID=$(echo "$LOGIN_RESPONSE" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
echo ""

echo "3. Creating tenant 'tenant1'..."
auth_api_call POST /tenants '{"id":"tenant1","name":"Tenant One"}'
echo ""

echo "4. Creating environment 'dev'..."
auth_api_call POST /tenants/tenant1/environments '{"id":"dev","name":"Development"}'
echo ""

echo "5. Creating collections..."
auth_api_call POST /environments/dev/collections '{"id":"users","name":"Users"}'
auth_api_call POST /environments/dev/collections '{"id":"addresses","name":"Addresses"}'
auth_api_call POST /environments/dev/collections '{"id":"orders","name":"Orders"}'
auth_api_call POST /environments/dev/collections '{"id":"payments","name":"Payments"}'
auth_api_call POST /environments/dev/collections '{"id":"products","name":"Products"}'
echo "Collections created"
echo ""

# echo "6. Storing 10 documents in each collection..."
# for i in {1..10}; do
#   # Users collection (5+ keys)
#   run_cli insert -C users --id user$i -t "User $i text" -c "person" \
#     -m "{\"email\":\"user$i@example.com\",\"age\":$((20+i)),\"city\":\"City$i\",\"status\":\"active\",\"role\":\"user\"}"
#   # Addresses collection (5+ keys)
#   run_cli insert -C addresses --id addr$i -t "Address $i text" -c "place" \
#     -m "{\"street\":\"Street $i\",\"zip\":\"1000$i\",\"country\":\"USA\",\"type\":\"residential\",\"verified\":true}"
#   # Data collection (5+ keys)
#   run_cli insert -C data --id item$i -t "Data item $i" -c "metric" \
#     -m "{\"value\":$((i*10)),\"unit\":\"ms\",\"timestamp\":\"2026-03-01\",\"node\":\"node-$i\",\"priority\":\"high\"}"
# done

echo "6a. Inserting 50 documents for text search testing..."
TEXT_BATCH_FILE="$(pwd)/scripts/text_search_data.json"

# Define some varied content for the 50 documents
CONTENTS=(
    "Rust is a systems programming language focused on safety and speed."
    "Elasticsearch is a distributed, RESTful search and analytics engine."
    "Machine learning models often use vector embeddings for similarity."
    "The quick brown fox jumps over the lazy dog."
    "Database indexing improves query performance significantly."
    "Full-text search allows finding documents based on keywords."
    "Partial match searching can be useful for autocomplete features."
    "Case-sensitive search distinguishes between 'Apple' and 'apple'."
    "Distributed systems require careful coordination and consensus."
    "Cloud computing provides scalable resources on demand."
)

cat <<EOF > "$TEXT_BATCH_FILE"
[
$(for i in {1..50}; do
  CONTENT_IDX=$(( (i-1) % 10 ))
  CONTENT="${CONTENTS[$CONTENT_IDX]}"
  if [ $i -lt 50 ]; then
    echo "  {\"id\":\"text-$i\",\"text\":\"$CONTENT (Doc #$i)\",\"category\":\"search\",\"vector\":[0.0,0.0,0.0,0.0],\"metadata_json\":\"{\\\"tag\\\":\\\"test-$i\\\",\\\"type\\\":\\\"sample\\\"}\"},"
  else
    echo "  {\"id\":\"text-$i\",\"text\":\"$CONTENT (Doc #$i)\",\"category\":\"search\",\"vector\":[0.0,0.0,0.0,0.0],\"metadata_json\":\"{\\\"tag\\\":\\\"test-$i\\\",\\\"type\\\":\\\"sample\\\"}\"}"
  fi
done)
]
EOF
run_cli batch-insert -C data --file "$TEXT_BATCH_FILE"

echo "6b. Testing Batch Insertion with specific items..."
BATCH_FILE="$(pwd)/scripts/batch_data.json"
cat <<EOF > "$BATCH_FILE"
[
  {
    "id": "batch1",
    "text": "Batch document 1: Advanced indexing techniques",
    "category": "batch",
    "vector": [0.1, 0.2, 0.3, 0.4],
    "metadata_json": "{\"source\":\"batch_process\",\"priority\":1}"
  },
  {
    "id": "batch2",
    "text": "Batch document 2: Distributed consensus algorithms",
    "category": "batch",
    "vector": [0.5, 0.6, 0.7, 0.8],
    "metadata_json": "{\"source\":\"batch_process\",\"priority\":2}"
  }
]
EOF
run_cli batch-insert -C data --file "$BATCH_FILE"

echo "6c. 10 Full-text search examples..."

# Helper for search
run_search() {
  local DESC=$1
  local CMD_ARGS=$2
  echo "--- Search Example: $DESC ---"
  run_cli search -C data $CMD_ARGS
  echo -e "\n"
}

# 1. Basic search for "Rust"
run_search "Basic search for 'Rust'" '--query Rust'

# 2. Case-sensitive search for "Elasticsearch"
run_search "Case-sensitive search for 'Elasticsearch'" '--query Elasticsearch --case-sensitive'

# 3. Case-sensitive search for "elasticsearch" (should fail/return nothing if exact)
run_search "Case-sensitive search for 'elasticsearch' (lowercase)" '--query elasticsearch --case-sensitive'

# 4. Partial match for "distrib"
run_search "Partial match for 'distrib'" '--query distrib --partial-match'

# 5. Partial match for "index"
run_search "Partial match for 'index'" '--query index --partial-match'

# 6. Search for "fox" (The quick brown fox...)
run_search "Search for 'fox'" '--query fox'

# 7. Search for "consensus"
run_search "Search for 'consensus'" '--query consensus'

# 8. Search for "Machine learning" with metadata
run_search "Search for 'Machine learning' with metadata" '--query "Machine learning" --partial-match --include-metadata'

# 9. Search for "Doc #42" (Testing specific document retrieval)
run_search "Search for 'Doc #42'" '--query "Doc #42" --partial-match'

# 10. Search for "safety"
run_search "Search for 'safety'" '--query safety'

# echo "7. Ingesting RAG text examples (embeddings stored in DB)..."
# run_cli rag-ingest -C rag --doc-id rag-doc-1 --text "Rust is a systems programming language focused on safety and speed." \
#   --metadata-json "{\"topic\":\"rust\",\"source\":\"docs\"}" --source "docs"
# run_cli rag-ingest -C rag --doc-id rag-doc-2 --text "Vector search uses embeddings to compare semantic similarity between chunks." \
#   --metadata-json "{\"topic\":\"vector-search\"}" --source "notes"

# RAG_TEXT_FILE="$(pwd)/scripts/rag_sample.txt"
# cat <<EOF > "$RAG_TEXT_FILE"
# Retrieval-augmented generation combines search with generation.
# Chunking text helps match relevant passages.
# Embeddings represent text as vectors.
# EOF
# run_cli rag-ingest-file -C rag --doc-id rag-doc-file --path "$RAG_TEXT_FILE" \
#   --metadata-json "{\"topic\":\"rag\",\"source\":\"file\"}"


# echo "8. Fetching stored data..."
# echo "--- Bulk fetch users ---"
# run_cli list-docs -C users
# echo "--- Bulk fetch addresses ---"
# run_cli list-docs -C addresses
# echo "--- Bulk fetch data ---"
# run_cli list-docs -C data

# echo "--- Individual fetch user1 ---"
# run_cli get-doc -C users --id user1

# echo "9. Searching RAG embeddings..."
# run_cli rag-search -C rag --query "How does vector search compare embeddings?" --top-k 5


# echo "10. Deleting 5 documents from each collection..."
# for i in {1..5}; do
#   run_cli delete-doc -C users --id user$i
#   run_cli delete-doc -C addresses --id addr$i
#   run_cli delete-doc -C data --id item$i
# done

# Cross-collection demos

echo "7. Inserting cross-collection data (users, addresses, orders, payments)..."
run_cli insert -C users --id user1 -t "User 1" -c "person" -m "{\"email\":\"user1@example.com\",\"region\":\"north\"}"
run_cli insert -C users --id user2 -t "User 2" -c "person" -m "{\"email\":\"user2@example.com\",\"region\":\"south\"}"
run_cli insert -C addresses --id addr1 -t "Address 1" -c "place" -m "{\"user_id\":\"user1\",\"city\":\"Austin\",\"state\":\"TX\"}"
run_cli insert -C addresses --id addr2 -t "Address 2" -c "place" -m "{\"user_id\":\"user2\",\"city\":\"Seattle\",\"state\":\"WA\"}"
run_cli insert -C orders --id order1 -t "Order 1" -c "order" -m "{\"user_id\":\"user1\",\"total\":120.5,\"status\":\"shipped\"}"
run_cli insert -C orders --id order2 -t "Order 2" -c "order" -m "{\"user_id\":\"user1\",\"total\":42.0,\"status\":\"processing\"}"
run_cli insert -C orders --id order3 -t "Order 3" -c "order" -m "{\"user_id\":\"user2\",\"total\":99.9,\"status\":\"delivered\"}"
run_cli insert -C payments --id payment1 -t "Payment 1" -c "payment" -m "{\"order_id\":\"order1\",\"amount\":120.5,\"method\":\"card\"}"
run_cli insert -C payments --id payment2 -t "Payment 2" -c "payment" -m "{\"order_id\":\"order2\",\"amount\":42.0,\"method\":\"cash\"}"
run_cli insert -C payments --id payment3 -t "Payment 3" -c "payment" -m "{\"order_id\":\"order3\",\"amount\":99.9,\"method\":\"card\"}"

echo "8. Cross-collection lookup: users with addresses"
auth_api_call POST /collections/cross/query '{"source":"users","stages":[{"lookup":{"from":"addresses","local_field":"id","foreign_field":"metadata.user_id","as":"user_addresses"}}]}'

echo "9. Cross-collection join: orders with payments"
auth_api_call POST /collections/cross/query '{"source":"orders","stages":[{"join":{"join_type":"left","from":"payments","local_field":"id","foreign_field":"metadata.order_id","as":"payment"}}]}'

echo "10. Cross-collection union: users + orders + payments"
auth_api_call POST /collections/cross/query '{"source":"users","stages":[{"union":{"collections":["orders","payments"]}}]}'

echo "11. Multi-collection insert (users + orders)"
auth_api_call POST /collections/cross/operation '{"operation":"insert","collections":["users","orders"],"documents":[{"id":"user3","text":"User 3","category":"person","vector":[0.0,0.0,0.0,0.0],"metadata":{"email":"user3@example.com","region":"west"}},{"id":"order4","text":"Order 4","category":"order","vector":[0.0,0.0,0.0,0.0],"metadata":{"user_id":"user3","total":250.0,"status":"processing"}}]}'

echo "12. Multi-collection update (orders + payments)"
auth_api_call POST /collections/cross/operation '{"operation":"update","collections":["orders","payments"],"documents":[{"id":"order2","metadata":{"status":"delivered"}},{"id":"payment2","metadata":{"method":"wire"}}]}'

echo "13. Multi-collection delete (orders + payments)"
auth_api_call POST /collections/cross/operation '{"operation":"delete","collections":["orders","payments"],"documents":[{"id":"order4"},{"id":"payment2"}]}'

echo "14. Removing 'data' collection..."
run_cli delete-collection -e dev --id data

if [ -z "$SESSION_ID" ]; then
  echo "Session ID not found in login output; skipping log fetch."
else
  echo "15. Fetching logs for session: $SESSION_ID"
  $LOG_CLI fetch-logs --session-id "$SESSION_ID"
fi

echo "16. Logging out..."
run_cli logout

echo "Stopping server (PID: $PID)..."
kill $PID

echo "Script completed successfully."
exit 0
