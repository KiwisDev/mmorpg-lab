#!/usr/bin/env bash
# dev launcher: builds the workspace, starts all backend services with colored
# prefixed logs, and cleans everything up on Ctrl+C.
# Launch clients manually: cargo run -p client

CYAN=$'\033[36m'
GREEN=$'\033[32m'
YELLOW=$'\033[33m'
MAGENTA=$'\033[35m'
BOLD=$'\033[1m'
RST=$'\033[0m'

ROOT="$(cd "$(dirname "$0")" && pwd)"
BIN="$ROOT/target/debug"

# Parse flags
SHOW_HB=0
for arg in "$@"; do [[ "$arg" == "--heartbeat" ]] && SHOW_HB=1; done

# ── Log prefix helper ─────────────────────────────────────────────────────────
# Prepends a colored label to every line. fflush() keeps output live when piped.
# Heartbeat lines are suppressed unless --heartbeat was passed.
tagged() {
    local color="$1" label="$2"
    awk -v c="$color" -v l="$label" -v r="$RST" -v hb="$SHOW_HB" \
        '!hb && /Heartbeat sent|Heartbeat from/ { next }
         { print c l r " " $0; fflush() }'
}

# ── Process tracking ──────────────────────────────────────────────────────────
BGPIDS=()

start() {
    local color="$1" label="$2"; shift 2
    "$@" 2>&1 | tagged "$color" "$label" &
    BGPIDS+=($!)   # PID of the tagged awk process; service binary exits via SIGPIPE
}

cleanup() {
    echo ""
    printf '%s%s==> Shutting down all services...%s\n' "$BOLD" "$YELLOW" "$RST"

    # SIGKILL is required: Bevy's App::run() has no SIGINT handler and will never
    # exit on its own. SIGKILL cannot be caught or ignored — instant, guaranteed.
    pkill -KILL -f "$BIN/broker"          2>/dev/null || true
    pkill -KILL -f "$BIN/spatial_service" 2>/dev/null || true
    pkill -KILL -f "$BIN/orchestrator"    2>/dev/null || true
    pkill -KILL -f "$BIN/gatekeeper"      2>/dev/null || true
    pkill -KILL -f "$BIN/dedicated_server" 2>/dev/null || true

    # Kill the awk log-filter processes too.
    kill -KILL "${BGPIDS[@]}" 2>/dev/null || true

    printf '%s%s==> All stopped.%s\n' "$BOLD" "$YELLOW" "$RST"
    exit 0
}
trap cleanup INT TERM

# ── Build ─────────────────────────────────────────────────────────────────────
printf '%s==> Building workspace...%s\n' "$BOLD" "$RST"
cargo build --workspace --quiet 2>&1
printf '%s==> Build complete.%s\n\n' "$BOLD" "$RST"

# ── Redis ─────────────────────────────────────────────────────────────────────
printf '%s==> Starting Redis...%s\n' "$BOLD" "$RST"
docker start redis-mmorpg 2>/dev/null \
    || docker run -d --name redis-mmorpg -p 6379:6379 redis:7-alpine
until docker exec redis-mmorpg redis-cli ping 2>/dev/null | grep -q PONG; do
    sleep 0.3
done
printf '%s==> Redis ready.%s\n\n' "$BOLD" "$RST"

# ── Services (dependency order) ───────────────────────────────────────────────
printf '%s[BROKER] %s :9010\n' "$CYAN" "$RST"
start "$CYAN" "[BROKER] " "$BIN/broker"

sleep 0.5   # broker must bind before spatial and dedicated servers connect

printf '%s[SPATIAL]%s :9001 → broker :9010\n' "$GREEN" "$RST"
start "$GREEN" "[SPATIAL]" \
    env SPATIAL_PORT=9001 BROKER_ADDR=127.0.0.1:9010 "$BIN/spatial_service"

printf '%s[ORCH]   %s :9000 → redis, auto-spawns dedicated servers\n' "$YELLOW" "$RST"
start "$YELLOW" "[ORCH]   " \
    env NUM_SHARDS=4 BROKER_ADDR=127.0.0.1:9010 SPATIAL_ADDR=127.0.0.1:9001 \
    "$BIN/orchestrator"

sleep 0.5   # orchestrator must bind before gatekeeper queries it

printf '%s[GATE]   %s :3000 → redis :6379\n' "$MAGENTA" "$RST"
start "$MAGENTA" "[GATE]   " "$BIN/gatekeeper"

printf '\n%s%s==> All services started.%s\n' "$BOLD" "$GREEN" "$RST"
echo "Dedicated servers appear in ~10 s (orchestrator scaler interval)."
echo "Launch clients manually:  cargo run -p client"
printf 'Press %sCtrl+C%s to stop everything.\n\n' "$BOLD" "$RST"

wait
