#!/bin/bash
#
# Combined WebSocket CDC Demo Script for aiDB
#
# This script demonstrates the real-time CDC streaming and pub/sub functionality:
# 1. Starts the aiDB server
# 2. Runs the subscriber script (creates tenant/env/collection/docs, subscribes via WebSocket)
# 3. In a separate terminal/window, runs the publisher script (updates documents)
# 4. The subscriber receives real-time CDC events for each update
#
# Usage:
#   ./test_websocket_demo.sh [--server URL]
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default configuration
SERVER_URL="http://localhost:11111"
WS_URL="ws://localhost:11111"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_PID=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --server)
            SERVER_URL="$2"
            shift 2
            ;;
        --ws-server)
            WS_URL="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [--server URL] [--ws-server URL]"
            echo ""
            echo "Options:"
            echo "  --server URL      REST API server URL (default: http://localhost:11111)"
            echo "  --ws-server URL   WebSocket server URL (default: ws://localhost:11111)"
            echo "  --help            Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Helper functions
print_header() {
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}============================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

cleanup() {
    print_info "Cleaning up..."
    if [ -n "$SERVER_PID" ]; then
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
        print_success "Server stopped"
    fi
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Main script
print_header "aiDB WebSocket CDC Demo"

echo ""
print_info "Configuration:"
echo "  REST API: $SERVER_URL"
echo "  WebSocket: $WS_URL"
echo ""

# Check if Python scripts exist
SUBSCRIBER_SCRIPT="$SCRIPT_DIR/test_websocket_subscriber.py"
PUBLISHER_SCRIPT="$SCRIPT_DIR/test_websocket_publisher.py"

if [ ! -f "$SUBSCRIBER_SCRIPT" ]; then
    print_error "Subscriber script not found: $SUBSCRIBER_SCRIPT"
    exit 1
fi

if [ ! -f "$PUBLISHER_SCRIPT" ]; then
    print_error "Publisher script not found: $PUBLISHER_SCRIPT"
    exit 1
fi

print_success "Found test scripts"

# Check Python dependencies
print_info "Checking Python dependencies..."
python3 -c "import aiohttp, websockets" 2>/dev/null || {
    print_error "Missing Python dependencies. Installing..."
    pip3 install aiohttp websockets
}
print_success "Python dependencies OK"

echo ""
print_header "Starting Demo"

echo ""
print_info "This demo will:"
echo "  1. Start the aiDB server (if not already running)"
echo "  2. Run the SUBSCRIBER script to:"
echo "     - Register a new user"
echo "     - Create tenant, environment, and collection"
echo "     - Insert test documents"
echo "     - Connect to WebSocket and subscribe to documents"
echo "  3. Run the PUBLISHER script to:"
echo "     - Update a document 10 times"
echo "     - Each update triggers a CDC event"
echo "  4. The subscriber will receive and display all CDC events"
echo ""

read -p "Press Enter to start the demo..."

# Check if server is already running
print_info "Checking if aiDB server is running..."
if curl -s "$SERVER_URL/health" > /dev/null 2>&1; then
    print_success "Server is already running at $SERVER_URL"
    SERVER_ALREADY_RUNNING=1
else
    print_info "Starting aiDB server..."
    cd "$SCRIPT_DIR/.."
    cargo run --quiet --bin my_ai_db > /tmp/aidb_server.log 2>&1 &
    SERVER_PID=$!
    
    # Wait for server to start
    print_info "Waiting for server to start..."
    for i in {1..30}; do
        if curl -s "$SERVER_URL/health" > /dev/null 2>&1; then
            print_success "Server started successfully"
            break
        fi
        sleep 1
    done
    
    if ! curl -s "$SERVER_URL/health" > /dev/null 2>&1; then
        print_error "Server failed to start. Check /tmp/aidb_server.log"
        exit 1
    fi
fi

echo ""
print_header "Phase 1: Starting Subscriber"
print_info "The subscriber will set up the environment and listen for updates."
print_info "After the subscriber is ready, run the publisher in another terminal."
echo ""
print_info "To run the publisher in another terminal, execute:"
echo "  python3 $PUBLISHER_SCRIPT --server $SERVER_URL"
echo ""

# Run subscriber
python3 "$SUBSCRIBER_SCRIPT" --server "$SERVER_URL" --ws-server "$WS_URL"

echo ""
print_header "Demo Complete"
print_success "WebSocket CDC demo finished!"

if [ -n "$SERVER_PID" ]; then
    echo ""
    print_info "Server logs available at: /tmp/aidb_server.log"
fi
