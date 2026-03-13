# legacy_code — Archive (Windows 7 / Python 3.6)

> **Status: Archived. Do not use for new development.**  
> This folder is preserved as a migration reference only.
> The active codebase lives in [`../stock_analyzer/`](../stock_analyzer/README.md).

---

## What This Is

This is the original stock index dashboard built in 2023 on a Windows 7 machine
(Intel Core i5-3230M, 6 GB RAM, Python 3.6.8). It was a single-file monolith with
no logging, no structured error handling, and SQLite as the only data store.

It is kept here deliberately — to show the starting point of the architectural
migration, and as evidence of the problem-space understanding that drove the rewrite.

---

## Why It's Archived

| Limitation | Impact |
|---|---|
| Python 3.6.8 (EOL 2021) | Security vulnerabilities, no modern type hints |
| Windows-only venv (`Scripts/`, `Lib/`) | Not portable to macOS or Linux |
| Monolithic single script | No separation of concerns, untestable |
| No logging | Silent failures in production |
| No rate-limit handling | API bans on Polygon.io free tier |
| SQLite only | No concurrent writes from multiple services |

---

## Original Environment

```
OS:        Windows 7 Professional (64-bit)
CPU:       Intel Core i5-3230M @ 2.60 GHz
RAM:       6.00 GB
Python:    3.6.8
Java:      1.8.0_102
Git:       2.35.2.windows.1
```

---

## Source Files (for reference only)

```
legacy_code/src/
├── fetch_data.py           ← Polygon.io API client (no rate limiting)
├── save_db.py              ← SQLite operations (no error handling)
├── index_construction.py   ← EW index logic (no MCW support)
├── export_data.py          ← CSV export
└── stock_index_dashboard.py ← Streamlit monolith (all logic in one file)
```

---

## What Changed in the Migration

See [`../stock_analyzer/README.md`](../stock_analyzer/README.md) for the full
changelog. In summary:

- Separated into 6 focused modules with single responsibilities
- Added `logger_config.py` with structured logging (console + file)
- Added Market-Cap Weighted index alongside Equal-Weighted
- Added robust SQL with `WITH` / `ROW_NUMBER()` deduplication
- Fixed rate-limit handling (12s sleep between API calls)
- Upgraded to Python 3.13 on macOS M4

---

*This archive is intentionally not installable. The `Lib/` and `Scripts/`
directories contain Windows Python runtime binaries and are excluded from
version control via `.gitignore`.*
