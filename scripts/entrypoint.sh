#!/usr/bin/env sh
# Wait for Redpanda, then launch the single uvicorn worker.
set -e

echo "entrypoint: waiting for Redpanda at ${KAFKA_BOOTSTRAP}..."
python - <<'PY'
import os
import time

from confluent_kafka import AdminClient

bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
admin = AdminClient({"bootstrap.servers": bootstrap})
for attempt in range(60):
    try:
        admin.list_topics(timeout=5)
        print(f"Redpanda reachable at {bootstrap}")
        break
    except Exception as exc:  # noqa: BLE001
        print(f"  attempt {attempt + 1}: {exc}")
        time.sleep(2)
else:
    raise SystemExit("Redpanda did not become reachable")
PY

# Exactly one worker process: offset ownership and the fixed transactional id
# assume a single consumer instance.
exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
