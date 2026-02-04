Legend: [ ] Incomplete, [X] Complete

_Evidence rule:_ When executing this plan, every completed checklist item must include:
1) the exact verification command (wrapped with backticks),
2) its exit code, and
3) any artifacts (logs, screenshots, `.scratch` transcripts) stored under `.scratch/verification/SPRINT-001/...`.

# Sprint #001-003 - GitHub Trending Archive Analytics Webserver (DuckDB/Parquet -> Cache -> Rollups)

## Objective
Deliver a local-first Python webserver + UI that makes it instant to flip between daily GitHub Trending snapshots and to compute dynamic toplists across arbitrary date ranges (global and filtered by language, repo owner/author, etc.).

This plan intentionally stages the work:
- **Sprint 001 (Approach 2):** Parquet partitions + DuckDB analytics (baseline correctness + query ergonomics)
- **Sprint 002 (Approach 4):** Result caching + pre-warming (baseline UX latency)
- **Sprint 003 (Approach 3):** Materialized aggregates / rollups for the most-common metrics (baseline scalability)

At the end of Sprint 003 we note when **Approach 1 (Star-schema SQL, e.g. SQLite/Postgres)** becomes the best follow-up.

## Context & Problem
Today the archive lives as many tiny JSON files, optimized for storage and scraping, not analytics:
- `archive/repository/<year>/<date>/<language>.json` contains `list: ["owner/repo", ...]`
- `archive/developer/<year>/<date>/<language>.json` contains `list: ["username", ...]`

The dataset is small in bytes but large in file count, so naive “read JSON on every request” approaches become I/O-bound quickly, especially for range queries (e.g., “most frequently re-appearing repos between 2024-01-01 and 2024-03-31”).

We want:
- Fast day navigation (prev/next, jump-to-date)
- Fast dynamic analytics (group-by, distinct-days, streaks, filters)
- A data model that is explicit about semantics (per-day vs per-(day,language) appearances, and how to treat “all languages” `(null).json`)

## Current State Snapshot (repo review)
- Scraper writes JSON in `src/main.ts` into `archive/*/<year>/<date>/*.json` with:
  - `date: "YYYY-MM-DD"`
  - `language: null | "<slug>"` (null means “All languages” and is stored in `(null).json`)
  - `list: string[]` (repo full_name or developer username)
- GitHub Actions runs hourly but commits only once per day (directory-exists guardrail) in `.github/workflows/*.yml`.

## Architecture (target)

### High-level components
- **ETL (build step):** Convert archive JSON -> canonical row format -> Parquet dataset
- **Analytics engine:** DuckDB embedded (in-process) reading Parquet
- **Web server:** FastAPI (HTTP API + minimal HTML UI)
- **Cache layer (Sprint 002):** in-memory LRU + optional disk-backed cache
- **Rollup builder (Sprint 003):** incremental materialization of high-value aggregates

### Proposed local directory layout
Keep generated artifacts out of `archive/` and clearly separate from source:
- `analytics/parquet/` - canonical row datasets (read-mostly)
- `analytics/rollups/` - derived aggregates (read-mostly)
- `analytics/duckdb/analytics.duckdb` - optional persisted DuckDB catalog (views + metadata)
- `.scratch/verification/SPRINT-001/` - evidence artifacts while implementing

## Data Model (canonical rows)

### Canonical tables (logical)
We keep **two** core entry tables to avoid ambiguity between repo and developer shapes:

1) `repo_trend_entry`
- `date` (DATE)
- `language` (VARCHAR, nullable; NULL => “all languages”)
- `rank` (INTEGER, 1-based position inside that file’s list)
- `full_name` (VARCHAR; `"owner/repo"`)
- `owner` (VARCHAR)
- `repo` (VARCHAR)

2) `dev_trend_entry`
- `date` (DATE)
- `language` (VARCHAR, nullable; NULL => “all languages”)
- `rank` (INTEGER, 1-based)
- `username` (VARCHAR)

### Semantics (MUST be explicit in code + docs)
- **Appearance unit options**
  - `occurrence`: a row in `*_trend_entry` (counts per (date, language) file)
  - `day_presence`: distinct `date` for an entity (dedupes across languages per day)
- **“All languages”**
  - Default UI shows “all languages” view if present for that kind+date.
  - Analytics endpoints must allow `include_all_languages={true|false}` because mixing “all languages” with per-language lists can double-count.

## API + UX (baseline)

### UI (minimal but fast)
- `/repositories` and `/developers` pages
- Day navigation: Prev/Next buttons + date picker
- Language picker: `All` + list of languages present for that day/kind
- Metrics panel: a few dynamic toplists backed by the API

### HTTP API (v1)
- `GET /api/v1/dates?kind={repository|developer}` -> available dates (sorted)
- `GET /api/v1/day?kind=repository&date=YYYY-MM-DD&language=<slug|__all__>` -> ranked list + metadata
- `GET /api/v1/top/reappearing?kind=repository&start=YYYY-MM-DD&end=YYYY-MM-DD&language=<optional>&presence={day|occurrence}&include_all_languages={true|false}`
- `GET /api/v1/top/owners?start=...&end=...` (repository only)
- `GET /api/v1/top/languages?start=...&end=...` (counts of entries per language; both kinds)

## Sprint 001 (Approach 2) - Parquet + DuckDB baseline

Execution order: 001A -> 001B -> 001C -> 001D -> 001E.

### 001A - Python project scaffold (FastAPI + DuckDB + Parquet)
- [ ] Add `pyproject.toml` (or `requirements.txt` if preferred) for:
  - runtime: `fastapi`, `uvicorn`, `duckdb`, `pyarrow`, `jinja2`
  - test: `pytest`, `httpx`
- [ ] Add module layout (example):
  - `py/gh_trending_web/` (server)
  - `py/gh_trending_analytics/` (ETL + query layer)
  - `py/tests/` (pytest)
- [ ] Add `Makefile` or scripts:
  - `make py-test`
  - `make py-run` (starts server)
  - `make analytics-build` (build parquet from archive)

Verification:
- `python3 -m pytest -q` (exit 0)
- `python3 -c "import duckdb, pyarrow, fastapi"` (exit 0)

### 001B - ETL: archive JSON -> canonical Parquet datasets
- [ ] Implement `analytics-build` to (re)build Parquet from `archive/`:
  - Read `archive/repository/**/<language>.json` and emit `repo_trend_entry` rows
  - Read `archive/developer/**/<language>.json` and emit `dev_trend_entry` rows
  - Validate/normalize:
    - `language` can be NULL (from `(null).json`)
    - `rank` is 1..N
    - For repos, split `full_name` into `owner` and `repo` (safe handling if malformed)
  - Write Parquet in a layout optimized for “flip day” + range scans:
    - Recommended: **one parquet file per kind per year** (small dataset, low file count)
      - `analytics/parquet/repository/year=2025/repo_trend_entry.parquet`
      - `analytics/parquet/developer/year=2025/dev_trend_entry.parquet`
    - Ensure stable schema and append-friendly pipeline
- [ ] Emit a `analytics/parquet/manifest.json`:
  - min/max date per kind
  - available languages (global list) per kind
  - row counts per year file

Verification:
- `python3 -m gh_trending_analytics build --help` (exit 0)
- `python3 -m gh_trending_analytics build --kind repository --year 2025` (exit 0; parquet + manifest updated)
- `python3 -m gh_trending_analytics build --kind developer --year 2025` (exit 0; parquet + manifest updated)

### 001C - DuckDB query layer (parameterized SQL)
- [ ] Implement a small query library that:
  - Opens DuckDB in-process
  - Reads the Parquet datasets via `read_parquet(...)`
  - Exposes functions for:
    - `list_dates(kind)` (from manifest, not from scanning parquet)
    - `get_day(kind, date, language)`
    - `top_reappearing(kind, start, end, language?, presence_mode, include_all_languages)`
    - `top_owners(start, end, ...)` (repo only)
  - Uses only parameterized queries (no string interpolation of user inputs)
- [ ] Define precise SQL semantics for `presence=day`:
  - For repository:
    - `COUNT(DISTINCT date)` grouped by `full_name`
  - For developer:
    - `COUNT(DISTINCT date)` grouped by `username`
- [ ] Add unit tests with a tiny synthetic archive fixture:
  - at least 2 dates, 2 languages, include `(null).json`, and one entity that appears in multiple languages on the same day
  - tests must prove the difference between `presence=occurrence` and `presence=day`

Verification:
- `python3 -m pytest -q` (exit 0)

### 001D - FastAPI server + minimal UI (day flip + analytics)
- [ ] Implement FastAPI app:
  - `GET /repositories` and `GET /developers` render HTML (Jinja2 templates)
  - `GET /api/v1/...` returns JSON
  - Server reads from `analytics/parquet/` and `manifest.json`
- [ ] UI “flip day” requirements:
  - Prev/Next day navigation works even if there are missing days (skip to nearest available)
  - Language dropdown is based on available languages for that day (or global list if we keep it simple in Sprint 001)
- [ ] Add an initial metrics panel:
  - “Top re-appearing repos (distinct days)” over a chosen date range
  - “Top owners by re-appearing repos” over a chosen date range

Verification:
- `python3 -m gh_trending_web --help` (exit 0)
- `python3 -m gh_trending_web --archive ./archive --analytics ./analytics --port 8000` (exit 0; server starts)
- `curl -sf http://127.0.0.1:8000/api/v1/dates?kind=repository | head` (exit 0)

### 001E - E2E smoke tests (proof the whole stack works)
- [ ] Add a `scripts/e2e_smoke.sh` (or pytest e2e) that:
  - builds parquet from a tiny fixture
  - starts the server on an ephemeral port
  - verifies:
    - day endpoint returns stable ordering (rank)
    - top_reappearing returns expected counts
    - language filtering does not crash on `c++` / `c#` / `(null)`

Verification:
- `bash scripts/e2e_smoke.sh` (exit 0)

## Sprint 002 (Approach 4) - Result caching + pre-warming

Execution order: 002A -> 002B -> 002C.

### 002A - Cache primitives + cache keys
- [ ] Add caching for:
  - day payloads `(kind, date, language)`
  - toplists `(kind, metric, start, end, filters...)`
- [ ] Define stable cache keys (JSON-serialized params, sorted keys)
- [ ] Add TTL defaults and max-size bounds

Verification:
- `python3 -m pytest -q` (exit 0; includes cache behavior tests)

### 002B - Pre-warm strategy for fast day flipping
- [ ] When serving a day view, enqueue pre-warm for:
  - previous available date
  - next available date
  - (optional) “all languages” + currently selected language
- [ ] Add simple instrumentation (log + counters) for:
  - cache hit ratio
  - pre-warm success/failure

Verification:
- `python3 -m pytest -q` (exit 0)

### 002C - Performance budget + regression guardrails
- [ ] Add a lightweight perf test that asserts:
  - cached day view endpoint responds under a target threshold on local machine
  - top_reappearing over 30/90 days stays within a reasonable bound

Verification:
- `python3 -m pytest -q` (exit 0)

## Sprint 003 (Approach 3) - Materialized aggregates / rollups

Execution order: 003A -> 003B -> 003C.

### 003A - Identify the “top 5” expensive queries + decide rollups
- [ ] Capture real usage patterns (or assume likely ones):
  - re-appearing repos over 7/30/90-day windows
  - longest streaks over a window
  - per-language leaders over a window
  - top owners by distinct repos over a window
- [ ] Decide rollups that preserve semantics and reduce scan cost:
  - `repo_day_presence(date, full_name, owner, best_rank, languages_count, in_all_languages)`
  - `dev_day_presence(date, username, best_rank, languages_count, in_all_languages)`

Verification:
- `python3 -m pytest -q` (exit 0; includes rollup semantics tests)

### 003B - Incremental rollup builder + storage format
- [ ] Implement `analytics-rollup` command that:
  - builds rollups from Parquet
  - supports incremental rebuild “from date X”
  - writes rollups as Parquet into `analytics/rollups/`
- [ ] Update query layer to use rollups when the query can be answered from them
  - (must be correctness-preserving; fall back to raw tables if unsure)

Verification:
- `python3 -m gh_trending_analytics rollup --help` (exit 0)
- `python3 -m pytest -q` (exit 0)

### 003C - Extend analytics endpoints (streaks + advanced toplists)
- [ ] Add endpoints:
  - `GET /api/v1/top/streaks?kind=repository&start=...&end=...&language=...`
  - `GET /api/v1/top/newcomers?kind=repository&start=...&end=...` (first_seen within window)
- [ ] Add tests for streak calculations (edge cases: gaps, duplicates across languages, include/exclude all-languages)

Verification:
- `bash scripts/e2e_smoke.sh` (exit 0)

### Note (post-003 follow-up): When to consider Approach 1 (Star-schema SQL)
If any of the following becomes true, consider migrating the analytics store to SQLite/Postgres (or generating a SQLite file for distribution):
- You need multiple concurrent users over a network (DuckDB embedded concurrency becomes limiting)
- You need richer indexing and point-lookups over huge history (beyond “scan + group”)
- You want to publish this as a hosted service with auth, rate limiting, and heavy traffic

In that follow-up, keep the Parquet datasets as the immutable “source of truth” and build a SQL warehouse as a derived artifact.

## Risks & Mitigations
- Many tiny JSON inputs: ETL must be incremental and should not `glob` the entire tree on every request.
- Double counting across `(null)` and per-language lists: must make `include_all_languages` explicit and tested.
- Special characters in language slugs and filenames (`c++`, `c#`): ensure URL encoding/decoding is correct end-to-end.
- “Developers” data begins later than repositories: UI must handle missing kinds/dates gracefully.

## Non-goals (for these sprints)
- No GitHub API enrichment (stars, descriptions, topics) - can be added later.
- No user accounts/auth.
- No attempt to replicate GitHub Trending ranking logic beyond archived ordering.

## Appendix - Diagrams

### Domain model (class diagram)
```mermaid
classDiagram
  class ArchiveJSONReader {
    +iter_repo_files()
    +iter_dev_files()
    +parse_repo_json()
    +parse_dev_json()
  }
  class ParquetBuilder {
    +build_repo_year(year)
    +build_dev_year(year)
    +write_manifest()
  }
  class DuckDBQueryService {
    +get_day(kind, date, language)
    +top_reappearing(kind, start, end, filters)
    +top_owners(start, end, filters)
  }
  class ResultCache {
    +get(key)
    +set(key, value, ttl)
  }
  class WebServer {
    +routes()
  }

  ArchiveJSONReader --> ParquetBuilder : feeds rows
  ParquetBuilder --> DuckDBQueryService : writes parquet
  DuckDBQueryService --> ResultCache : optional cache (Sprint 002)
  WebServer --> DuckDBQueryService : queries
```

### E-R diagram (logical)
```mermaid
erDiagram
  REPO {
    string full_name PK
    string owner
    string repo
  }
  DEVELOPER {
    string username PK
  }
  LANGUAGE {
    string slug PK
  }
  REPO_TREND_ENTRY {
    date date
    string language_slug "nullable"
    int rank
    string full_name FK
  }
  DEV_TREND_ENTRY {
    date date
    string language_slug "nullable"
    int rank
    string username FK
  }

  REPO ||--o{ REPO_TREND_ENTRY : appears_in
  LANGUAGE ||--o{ REPO_TREND_ENTRY : in_language
  DEVELOPER ||--o{ DEV_TREND_ENTRY : appears_in
  LANGUAGE ||--o{ DEV_TREND_ENTRY : in_language
```

### Dataflow (ETL + request path)
```mermaid
flowchart TD
  subgraph Build[Build time]
    A[archive/**/*.json] --> B[ETL: parse + normalize]
    B --> C[analytics/parquet/**/*.parquet]
    C --> D[manifest.json]
  end

  subgraph Serve[Request time]
    U[Browser] -->|HTTP| S[FastAPI]
    S -->|lookup| M[manifest.json]
    S -->|query| Q[DuckDB]
    Q -->|scan| P[Parquet datasets]
    S -->|optional| K[Cache]
    S --> U
  end
```

