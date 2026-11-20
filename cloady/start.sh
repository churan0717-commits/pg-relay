#!/bin/sh
# Single-container front door: oai-bridge on :8080 (public), aistudio-api on :8081 (internal).
set -e

CONFIG_FILE="${AISTUDIO_CONFIG_FILE:-/app/data/config.yaml}"
TEMPLATE_FILE="/app/defaults/config.yaml"
FALLBACK_TEMPLATE="/app/config.yaml"

if [ ! -f "$CONFIG_FILE" ]; then
    if [ -f "$TEMPLATE_FILE" ]; then
        mkdir -p "$(dirname "$CONFIG_FILE")"
        cp "$TEMPLATE_FILE" "$CONFIG_FILE"
        echo "[start] Initialized default config.yaml at $CONFIG_FILE"
    elif [ -f "$FALLBACK_TEMPLATE" ]; then
        mkdir -p "$(dirname "$CONFIG_FILE")"
        cp "$FALLBACK_TEMPLATE" "$CONFIG_FILE"
        echo "[start] Initialized default config.yaml at $CONFIG_FILE"
    fi
fi

cd "$(dirname "$0")"
python3 main.py server --port 8081 &
API_PID=$!
OAI_BRIDGE_HOST=0.0.0.0 OAI_BRIDGE_PORT=8080 OAI_BRIDGE_UPSTREAM=http://127.0.0.1:8081 \
    python3 oai-bridge.py &
BR_PID=$!

while kill -0 $API_PID 2>/dev/null && kill -0 $BR_PID 2>/dev/null; do
    sleep 2
done
echo "[start] a core process exited; shutting down"
kill $API_PID $BR_PID 2>/dev/null || true
exit 1
