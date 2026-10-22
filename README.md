# Cryptocurrency Market Data & Replay API

A high-performance, asynchronous backend engine designed to query, stream, and replay historical cryptocurrency tick-by-tick and candlestick datasets stored in Apache Parquet format across S3-compatible storage.

## Key Architecture & Features

```
                   +----------------------------------+
                   |           Client Apps            |
                   +-----------------+----------------+
                                     |
                REST (API Key Auth)  |  WebSocket (Replay)
                                     v
                   +----------------------------------+
                   |       FastAPI Gateway Engine     |
                   |   (Async / Concurrency / Quotas) |
                   +--------+----------------+--------+
                            |                |
             Redis (Rate-Limit)              | Predicate Pushdown (DuckDB)
                            v                v
                   +----------------+  +----------------------+
                   | Redis Cache /  |  | S3 / MinIO Storage   |
                   | Quotas DB      |  | (Parquet Files)      |
                   +----------------+  +----------------------+
```

1. **Zero-Copy Parquet Query Engine**: Uses **DuckDB** and **PyArrow** to query compressed Parquet archives directly on S3 without reading full multi-gigabyte files into RAM.
2. **WebSocket Market Replay**: Replays historical trades and order book snapshots with customizable pacing multipliers (1x, 5x, 10x) and asynchronous backpressure handling.
3. **Presigned S3 Downloads**: Generates secure, short-lived presigned download URLs for bulk historical datasets, offloading heavy bandwidth egress from API servers.
4. **Token-Bucket Rate Limiter & Bandwidth Quotas**: Backed by **Redis** to strictly monitor and enforce requests/sec and monthly gigabyte transfer limits per API key.
5. **PostgreSQL Security & Audit**: Secure API key hashing, permission scoping, and usage telemetry.

---

## Tech Stack

- **Language & Runtime**: Python 3.11+, Asyncio
- **API Framework**: FastAPI, Uvicorn, WebSockets
- **Columnar Big Data**: DuckDB, PyArrow, Apache Parquet
- **Storage**: AWS S3 / MinIO / Cloudflare R2
- **State & Caching**: Redis (Sliding window rate-limiter, Pub/Sub)
- **Database**: PostgreSQL
- **Containerization**: Docker, Docker Compose

---

## Quickstart

### 1. Run with Docker Compose
```bash
docker-compose up -d --build
```

### 2. Endpoints
- `GET /api/v1/market/trades` - Query historical trades with time-range filtering.
- `GET /api/v1/market/download-url` - Generate presigned S3 download link for bulk Parquet files.
- `WS /ws/v1/market/replay` - WebSocket stream replaying historical tick data.
- `GET /docs` - Interactive Swagger/OpenAPI documentation.

---

## Author
**Soroush Sharif** - Senior Backend Engineer
