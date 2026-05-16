#!/usr/bin/env bash
# Starts a local nats-server with JetStream enabled for smoke/stress runs.
# Default: docker (no install needed). Set USE_BINARY=1 to use a local
# nats-server binary instead.
#
# Listens on 127.0.0.1:4222. Storage in /tmp/hirnok-nats (host) or
# inside the container (ephemeral). Stops cleanly on Ctrl-C.
set -euo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-hirnok-nats}"
PORT="${PORT:-4222}"
IMAGE="${IMAGE:-nats:latest}"

cleanup() {
  if [[ "${USE_BINARY:-0}" != "1" ]]; then
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

if [[ "${USE_BINARY:-0}" == "1" ]]; then
  if ! command -v nats-server >/dev/null 2>&1; then
    echo "USE_BINARY=1 but nats-server is not in PATH. Install it or unset USE_BINARY." >&2
    exit 1
  fi
  STORE="${STORE:-/tmp/hirnok-nats}"
  mkdir -p "$STORE"
  echo "starting nats-server -js (binary mode, store=$STORE, port=$PORT)"
  exec nats-server -js -sd "$STORE" -p "$PORT"
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not found. Install Docker, or run with USE_BINARY=1 and nats-server in PATH." >&2
  exit 1
fi

# Remove a stale container of the same name, if any.
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

echo "starting nats-server -js (docker, image=$IMAGE, port=$PORT)"
exec docker run --rm \
  --name "$CONTAINER_NAME" \
  -p "$PORT:4222" \
  "$IMAGE" \
  -js
