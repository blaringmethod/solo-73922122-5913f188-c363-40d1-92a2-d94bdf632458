#!/usr/bin/env sh
# Reset the demo to a pristine state: remove containers and volumes (Redpanda
# logs + SQLite correction state), rebuild, and wait for health.
set -e
cd "$(dirname "$0")/.."

echo "==> tearing down containers and volumes"
docker compose down -v --remove-orphans

echo "==> building and starting redpanda + api"
docker compose up -d --build redpanda api

echo "==> waiting for API health"
for i in $(seq 1 60); do
  if curl -fs http://localhost:8000/health >/dev/null 2>&1; then
    echo "==> API healthy"
    curl -s http://localhost:8000/health
    echo
    exit 0
  fi
  sleep 2
done
echo "API did not become healthy" >&2
exit 1
