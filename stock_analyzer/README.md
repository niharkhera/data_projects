# Stock Index Engine

> A financial data pipeline and dashboard for constructing, tracking, and visualising
> custom stock market indices — migrated from a legacy Windows 7 / Python 3.6 environment
> to a modern microservice architecture on macOS / Docker.

---

## What This Project Does

This component fetches daily OHLCV market data from the Polygon.io API, constructs
Equal-Weighted and Market-Cap Weighted stock indices from the top N stocks by market
capitalisation, tracks index performance over time, and exposes everything through an
interactive Streamlit dashboard.

**Business problem solved:** Institutional and retail analysts need a reproducible,
auditable pipeline for building custom indices — something not available in free-tier
financial tools. This component fills that gap as a self-hosted, extensible engine.

---

## Architecture Overview

```
Browser (localhost:8501)
        │  HTTP
        ▼
  dashboard-ui          ← Streamlit: charts, composition, performance
        │  REST HTTP
        ▼
   api-service          ← FastAPI: routing, validation, auth
    │         │
    ▼         ▼
index-engine  export-service
    │                 │
    ▼                 ▼
  PostgreSQL  ←── ingestion-service  ←── Polygon.io API
  (Docker)         (APScheduler,
                    nightly 6pm)
```

All services run as Docker containers orchestrated by `docker-compose`.
See [`docs/architecture/`](./docs/architecture/) for UML class diagrams,
sequence diagrams, and deployment diagrams.

---

## Project Evolution

| Phase | Environment | Status |
|---|---|---|
| v0.1 (legacy) | Windows 7, Python 3.6, SQLite, monolithic script | Archived in `legacy_code/` |
| v0.2 (current) | macOS M4, Python 3.13, SQLite, Streamlit monolith | `stock_analyzer/` |
| v0.3 (in progress) | Docker, PostgreSQL, FastAPI microservices | This branch |

---

## Quickstart (5 minutes)

### Prerequisites
- Docker Desktop
- A free [Polygon.io](https://polygon.io) API key

### 1. Clone and configure
```bash
git clone https://github.com/niharkhera/data_projects.git
cd data_projects/stock_analyzer
cp .env.example .env
# Edit .env and add your POLYGON_API_KEY
```

### 2. Start all services
```bash
docker-compose up --build
```

### 3. Open the dashboard
Navigate to [http://localhost:8501](http://localhost:8501)

### 4. Fetch your first data
- Click **"Fetch Market Data"** tab
- Set a date range (last 5 trading days recommended for free tier)
- Click **"Download Prices"**
- Switch to **"Build Index"** tab → Generate weights
- Switch to **"View Dashboard"** tab → View charts

---

## Environment Variables

Copy `.env.example` to `.env` and fill in your values.

```
POLYGON_API_KEY=your_polygon_api_key_here
POSTGRES_USER=stockuser
POSTGRES_PASSWORD=changeme
POSTGRES_DB=stock_data
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
```

---

## Installation (local dev, no Docker)

```bash
cd stock_analyzer
conda create -n stock_env python=3.13
conda activate stock_env
pip install -r requirements.txt
streamlit run src/stock_index_dashboard.py
```

---

## Key Components

| Module | Responsibility |
|---|---|
| `fetch_data.py` | Polygon.io API client — symbols, OHLCV, rate-limit aware |
| `save_db.py` | Database abstraction layer — SQLite/PostgreSQL |
| `index_construction.py` | Equal-weighted and market-cap-weighted index logic |
| `export_data.py` | CSV export of performance and composition data |
| `stock_index_dashboard.py` | Streamlit UI — charts, data fetch triggers |
| `logger_config.py` | Structured logging (console + file) |

---

## Index Construction Methods

**Equal-Weighted (EW):** Each of the top N stocks receives an identical weight of `1/N`.
Rebalanced daily. Tracks the average stock performance irrespective of company size.

**Market-Cap Weighted (MCW):** Each stock's weight is proportional to its market
capitalisation relative to the total market cap of the index. Mirrors how indices like
the S&P 500 are constructed.

---

## Known Limitations

- Polygon.io free tier: 5 API calls per minute. Fetching 1,000 symbols takes ~3.3 hours.
- SQLite is used in dev/test mode. PostgreSQL is required for concurrent multi-service writes.
- No authentication on the dashboard in the current version.

---

## Changelog

### v0.3.0 (in progress)
- Microservice decomposition: ingestion, index-engine, api-service, dashboard-ui, export
- PostgreSQL replacing SQLite for production
- Docker Compose orchestration
- FastAPI gateway between UI and data layer

### v0.2.0
- Migrated from Windows 7 / Python 3.6 to macOS M4 / Python 3.13
- Refactored monolithic script into 6 modules
- Added Streamlit dashboard with Plotly charts
- Introduced structured logging

### v0.1.0 (legacy)
- Windows 7, Python 3.6, SQLite
- Single-file pipeline: fetch → save → display

---

## Tech Stack

Python 3.13 · Streamlit · FastAPI · PostgreSQL · SQLite · Docker · Pandas ·
Plotly · Polygon.io API · APScheduler · SQLAlchemy · Conda

---

## License

MIT License — see [LICENSE](./LICENSE)

---

## Author

**Nihar Khera** · [github.com/niharkhera](https://github.com/niharkhera)
