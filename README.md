# Streaming ML Platform

A production-grade **Streaming Feature Pipeline + Real-time ML Inference** system for CTR (Click-Through Rate) prediction. Built with Python, Docker Compose, Redpanda (Kafka-compatible), Redis, Postgres, FastAPI, Prometheus, and Grafana.

## Architecture

```
                                    ┌─────────────────┐
                                    │    Producer     │
                                    │ (synthetic      │
                                    │  clickstream)   │
                                    └────────┬────────┘
                                             │ events.raw
                                             ▼
┌──────────────┐    ┌──────────────────────────────────────────────┐    ┌─────────────────┐
│   Redpanda   │◄───│              Stream Processor                │───►│     Redis       │
│  (Kafka API) │    │  • Deduplication (Redis)                     │    │  • Online store │
└──────┬───────┘    │  • Rolling windows (sorted sets)             │    │  • Dedup store  │
       │            │  • Feature computation                       │    └────────┬────────┘
       │            │  • Bayesian CTR smoothing                    │             │
       │            └──────────────────────────────────────────────┘             │
       │                              │                     │                    │
       │                              │ features.online     │ INSERT             │
       │                              ▼                     ▼                    │
       │                       ┌──────────────┐      ┌──────────────┐            │
       │                       │   Postgres   │      │   Inference  │◄───────────┘
       │                       │  (offline)   │      │   (FastAPI)  │   GET features
       │                       └──────┬───────┘      └──────┬───────┘
       │                              │                     │
       │                              │                     │ POST /predict
       │                              │                     ▼
       │                              │              ┌──────────────┐
       │                              │              │   Clients    │
       │                              ▼              └──────────────┘
       │                       ┌──────────────┐
       │                       │   Trainer    │
       │                       │ (offline LR) │
       │                       └──────────────┘
       │
       │  Prometheus scrapes
       ▼
┌──────────────┐      ┌──────────────┐
│  Prometheus  │─────►│   Grafana    │
└──────────────┘      └──────────────┘
```

## Data Contract

### Event (JSON)
- `event_id` (UUID)
- `event_type`: `"impression"` or `"click"`
- `ts` (epoch milliseconds)
- `user_id`, `item_id`, `session_id`
- `device`, `country`

### Feature Record (JSON)
- `timestamp`, `user_id`, `item_id`, `session_id`
- `features`: dict of numeric features

### Streaming Features
| Feature | Description | Window |
|---------|-------------|--------|
| user_impressions_1m | User impression count | 1 min |
| user_clicks_1m | User click count | 1 min |
| user_ctr_10m | Bayesian CTR: (clicks+1)/(impressions+11) | 10 min |
| item_impressions_10m | Item impression count | 10 min |
| item_clicks_10m | Item click count | 10 min |
| item_ctr_1h | Bayesian CTR | 1 hour |
| time_since_last_click_user | Seconds since user's last click | - |
| session_events_5m | Session event count | 5 min |
| hour_of_day | 0–23 | - |
| day_of_week | 0–6 | - |

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Git

### Run Everything

```bash
cd streaming-ml-platform
docker compose up --build
```

This starts:
- **Redpanda** (Kafka) on 9092
- **Postgres** on 5432
- **Redis** on 6379
- **Producer** (continuous synthetic events)
- **Processor** (stream processing)
- **Inference** (FastAPI) on 8000
- **Prometheus** on 9090
- **Grafana** on 3000

Allow ~60 seconds for all services to become healthy.

### Example cURL Commands

**Health check:**
```bash
curl http://localhost:8000/health
```

**Predict (CTR probability):**
```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"user_id": "u0", "item_id": "i0"}'
```

Example response:
```json
{
  "probability": 0.05,
  "model_version": "1.0.0",
  "cold_start": false
}
```

**Prometheus metrics:**
```bash
curl http://localhost:8000/metrics
```

## Running the Trainer

The trainer builds a logistic regression model from Postgres events and saves it to `models/model.pkl`.

```bash
# Ensure producer and processor have run for a while to populate Postgres
# Then run trainer with the "training" profile:
docker compose --profile training run --rm trainer
```

To train on a specific time range:
```bash
docker compose --profile training run --rm -e TRAIN_START_TS=1738454400000 -e TRAIN_END_TS=1738540800000 trainer
```

The model and metrics are written to `models/`. Restart the inference service to pick up the new model:
```bash
docker compose restart inference
```

## Grafana

1. Open http://localhost:3000
2. Login: `admin` / `admin`
3. Navigate to **Dashboards** → **ML Platform Overview**

The dashboard shows:
- Inference request latency (p95)
- Stream processor throughput
- Inference request rate
- Deduplication skips

## Performance & Load Testing

- **Event rate**: Configure via `EVENTS_PER_SEC` (default: 5)
  ```bash
  EVENTS_PER_SEC=50 docker compose up --build
  ```

- **Producer**: Generates non-uniform user/item distributions for meaningful features
- **Processor**: Deduplication and rolling windows use Redis; Postgres receives raw events
- **Inference**: Fetches features from Redis; returns baseline (0.05) on cold start

For load testing the inference endpoint:
```bash
# Install hey or use Apache Bench
hey -n 1000 -c 10 -m POST -d '{"user_id":"u0","item_id":"i0"}' \
  -H "Content-Type: application/json" http://localhost:8000/predict
```

## Smoke Test

**Linux/macOS:**
```bash
chmod +x scripts/smoke_test.sh
./scripts/smoke_test.sh
```

**Windows PowerShell:**
```powershell
.\scripts\smoke_test.ps1
```

The smoke test waits for inference to be healthy, allows events to flow, then calls `/predict` and verifies the probability is between 0 and 1.

## Troubleshooting

- **Redpanda / init_kafka timeouts**: Ensure Redpanda uses a single advertised listener (`redpanda:9092`) so Kafka clients receive a reachable broker address.
- **Redis connection refused**: Wait for the redis service to be healthy before starting the processor; check with `docker compose ps`.
- **Cold start on every predict**: Features are keyed by `(user_id, item_id)`; run the producer and processor for a short while so features are written to Redis for the user/item you query.

## Project Structure

```
streaming-ml-platform/
├── docker-compose.yml
├── README.md
├── schemas/
│   └── event.py           # Data contract
├── scripts/
│   ├── init_kafka.py      # Create Kafka topics
│   ├── smoke_test.sh
│   └── smoke_test.ps1
├── models/                # Model output (model.pkl, metrics.json)
├── services/
│   ├── producer/          # Synthetic event producer
│   ├── stream_processor/  # Feature pipeline
│   │   └── feature_defs.py
│   ├── inference/         # FastAPI prediction service
│   ├── trainer/           # Offline training
│   └── init/              # Kafka topic init
└── infra/
    ├── prometheus/
    │   └── prometheus.yml
    └── grafana/
        ├── provisioning/
        └── dashboards/
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| EVENTS_PER_SEC | 5 | Producer event rate |
| KAFKA_BOOTSTRAP_SERVERS | redpanda:9092 | Kafka brokers |
| REDIS_URL | redis://redis:6379/0 | Redis connection |
| POSTGRES_* | mlplatform | Postgres credentials |
| MODEL_PATH | /models/model.pkl | Inference model path |

## Exact Step-by-Step Commands (Run & Validate Locally)

1. **Clone and enter the repo** (if starting fresh):
   ```bash
   git clone https://github.com/jerrylin118/Real-Time-Streaming-ML-Feature-Platform.git
   cd Real-Time-Streaming-ML-Feature-Platform
   ```

2. **Start the full stack** (single command):
   ```bash
   docker compose up --build
   ```
   Wait ~60 seconds for Redpanda, Postgres, Redis, init_kafka, producer, processor, inference, Prometheus, and Grafana to be ready.

3. **Health check**:
   ```bash
   curl http://localhost:8000/health
   ```
   Expected: `{"status":"ok"}`

4. **Request a prediction**:
   ```bash
   curl -X POST http://localhost:8000/predict -H "Content-Type: application/json" -d "{\"user_id\":\"u0\",\"item_id\":\"i0\"}"
   ```
   Expected: JSON with `probability` (0–1), `model_version`, and `cold_start` (true until features exist for that user/item).

5. **Run the smoke test** (after services are up):
   - **Linux/macOS**: `chmod +x scripts/smoke_test.sh && ./scripts/smoke_test.sh`
   - **Windows PowerShell**: `.\scripts\smoke_test.ps1`
   The script waits for inference, lets events flow ~15s, calls `/predict`, and verifies probability is in [0, 1].

6. **Run offline training** (optional; after some events are in Postgres):
   ```bash
   docker compose --profile training run --rm trainer
   ```
   Then restart inference to load the new model: `docker compose restart inference`

7. **View Grafana**: Open http://localhost:3000, login `admin`/`admin`, go to **Dashboards** → **ML Platform Overview**.

8. **Stop the stack**:
   ```bash
   docker compose down
   ```

## Repository

- **GitHub**: [jerrylin118/Real-Time-Streaming-ML-Feature-Platform](https://github.com/jerrylin118/Real-Time-Streaming-ML-Feature-Platform)

## License

MIT
