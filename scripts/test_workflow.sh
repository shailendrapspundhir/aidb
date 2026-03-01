#!/bin/bash

# Configuration
URL="http://localhost:11111"
CLI="cargo run --quiet --bin cli -- --url $URL"
SERVER_BIN="cargo run --quiet --bin my_ai_db"

echo "Starting my_ai_db server..."
$SERVER_BIN &
PID=$!
echo "Server started with PID: $PID"

# Wait for server to start
sleep 10

# Helper to run CLI
run_cli() {
  $CLI "$@"
}

echo "1. Registering user 'admin'..."
run_cli register --username admin --password admin

echo "2. Logging in..."
run_cli login --username admin --password admin

echo "3. Creating tenant 'tenant1'..."
run_cli create-tenant --id tenant1 --name tenant1

echo "4. Creating environment 'dev'..."
run_cli create-env -t tenant1 --id dev --name dev

echo "5. Creating collections: users, addresses, data..."
run_cli create-collection -e dev --id users --name users
run_cli create-collection -e dev --id addresses --name addresses
run_cli create-collection -e dev --id data --name data

echo "6. Storing 10 documents in each collection..."
for i in {1..10}; do
  # Users collection (5+ keys)
  run_cli insert -C users --id user$i -t "User $i text" -c "person" \
    -m "{\"email\":\"user$i@example.com\",\"age\":$((20+i)),\"city\":\"City$i\",\"status\":\"active\",\"role\":\"user\"}"
  # Addresses collection (5+ keys)
  run_cli insert -C addresses --id addr$i -t "Address $i text" -c "place" \
    -m "{\"street\":\"Street $i\",\"zip\":\"1000$i\",\"country\":\"USA\",\"type\":\"residential\",\"verified\":true}"
  # Data collection (5+ keys)
  run_cli insert -C data --id item$i -t "Data item $i" -c "metric" \
    -m "{\"value\":$((i*10)),\"unit\":\"ms\",\"timestamp\":\"2026-03-01\",\"node\":\"node-$i\",\"priority\":\"high\"}"
done

echo "7. Fetching stored data..."
echo "--- Bulk fetch users ---"
run_cli list-docs -C users
echo "--- Bulk fetch addresses ---"
run_cli list-docs -C addresses
echo "--- Bulk fetch data ---"
run_cli list-docs -C data

echo "--- Individual fetch user1 ---"
run_cli get-doc -C users --id user1

echo "8. Deleting 5 documents from each collection..."
for i in {1..5}; do
  run_cli delete-doc -C users --id user$i
  run_cli delete-doc -C addresses --id addr$i
  run_cli delete-doc -C data --id item$i
done

echo "9. Removing 'data' collection..."
run_cli delete-collection -e dev --id data

echo "10. Logging out..."
run_cli logout

echo "Stopping server (PID: $PID)..."
kill $PID

echo "Script completed successfully."
exit 0
