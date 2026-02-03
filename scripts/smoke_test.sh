#!/bin/bash
# Smoke test: wait for services, call inference, verify response
# Run after: docker compose up --build (producer runs continuously)

set -e

echo "Waiting for inference to become healthy..."

for i in $(seq 1 60); do
  if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
    echo "Inference is up"
    break
  fi
  echo "Waiting for inference... ($i/60)"
  sleep 2
done

if ! curl -sf http://localhost:8000/health > /dev/null 2>&1; then
  echo "Inference failed to become healthy"
  exit 1
fi

# Give producer/processor time to generate and process events
echo "Waiting 15s for events to flow..."
sleep 15

# Call inference for a known user/item (may be cold_start if no features yet)
echo "Calling inference endpoint..."
RESPONSE=$(curl -sf -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"user_id": "u0", "item_id": "i0"}')

echo "Response: $RESPONSE"

# Verify probability is between 0 and 1
echo "$RESPONSE" | python3 -c "
import sys, json
d = json.load(sys.stdin)
p = d.get('probability', -1)
if not (0 <= p <= 1):
    print('FAIL: probability', p, 'is not between 0 and 1')
    sys.exit(1)
print('SUCCESS: probability=', p, 'is valid')
"

