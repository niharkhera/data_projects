# data_projects

> Portfolio of data engineering projects by [Nihar Khera](https://github.com/niharkhera) —
> covering financial data pipelines, stock index construction, and production-grade
> microservice architecture.

---

## Repository Structure

```
data_projects/
├── stock_analyzer/        ← Active: stock index pipeline (Python 3.13, Docker, FastAPI)
├── legacy_code/           ← Archive: original Windows 7 / Python 3.6 monolith
├── future_data_project/   ← Planned: next project (TBD)
└── README.md              ← You are here
```

---

## Projects

### [stock_analyzer](./stock_analyzer/README.md) — Stock Index Engine `[active]`

A financial data pipeline that fetches daily OHLCV market data from Polygon.io,
constructs Equal-Weighted and Market-Cap Weighted indices, tracks performance, and
serves everything through an interactive Streamlit dashboard.

**Stack:** Python 3.13 · FastAPI · Streamlit · PostgreSQL · Docker · Pandas · Plotly

**Architecture evolution:**

| Version | Environment | Key change |
|---|---|---|
| v0.1 | Windows 7, Python 3.6, SQLite | Original monolith |
| v0.2 | macOS M4, Python 3.13, SQLite | Full refactor + dashboard |
| v0.3 *(in progress)* | Docker, PostgreSQL, FastAPI | Microservice decomposition |

→ [See full README](./stock_analyzer/README.md)

---

### [legacy_code](./legacy_code/README.md) — Windows 7 Archive `[archived]`

The original codebase that ran on a Windows 7 / Intel i5 / 6GB RAM machine with
Python 3.6. Kept as a migration reference and to document the architectural journey.
Not intended for use — see `stock_analyzer/` for the current version.

→ [See archive notes](./legacy_code/README.md)

---

### future_data_project `[planned]`

Placeholder for upcoming work. Candidates under consideration:

- Real-time streaming pipeline (Kafka + Spark)
- dbt + data warehouse project (Redshift or BigQuery)
- ML feature engineering pipeline

---

## Skills Demonstrated Across This Repo

`Python` · `SQL / PostgreSQL` · `REST API design` · `Docker` · `Microservices`  
`Financial data engineering` · `Streamlit` · `FastAPI` · `Pandas` · `Plotly`  
`Git` · `Conventional Commits` · `SQLite → PostgreSQL migration`

---

## Contact

**Nihar Khera** · [github.com/niharkhera](https://github.com/niharkhera)
