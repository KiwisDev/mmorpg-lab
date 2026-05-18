#!/bin/bash
set -e

echo "==> Building all services..."
cargo build -p orchestrator -p gatekeeper -p dedicated_server

echo "==> Starting Redis..."
docker start redis-mmorpg 2>/dev/null \
  || docker run -d --name redis-mmorpg -p 6379:6379 redis:7-alpine

# Wait until Redis accepts connections.
until docker exec redis-mmorpg redis-cli ping 2>/dev/null | grep -q PONG; do
  sleep 0.5
done
echo "Redis ready."

echo "==> Starting Orchestrator (background)..."
./target/debug/orchestrator &
ORCH_PID=$!

sleep 2

echo "==> Starting Gatekeeper (background)..."
./target/debug/gatekeeper &
GK_PID=$!

echo ""
echo "All services are running."
echo "  Orchestrator PID : $ORCH_PID"
echo "  Gatekeeper PID   : $GK_PID"
echo ""
echo "Test the login endpoint:"
echo "  curl -X POST http://localhost:3000/login \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"username\": \"alice\", \"password\": \"1234\"}'"
echo ""
echo "Press Ctrl+C to stop."

# Wait and clean up on exit
trap "kill $ORCH_PID $GK_PID 2>/dev/null; pkill -f target/debug/dedicated_server 2>/dev/null; docker stop redis-mmorpg 2>/dev/null" EXIT
wait