# Architecture Decision Record (ADR)

## ADR-001: Parquet + DuckDB analytics pipeline for GitHub Trending archive

- Status: Accepted
- Date: 2026-02-04

### Context
We need a local-first analytics stack that can query historical GitHub Trending snapshots quickly, while keeping build artifacts separate from the JSON archive and ensuring explicit semantics around per-day vs per-occurrence presence and the `(null)` all-languages lists.

### Decision
- Use a Parquet-based canonical dataset derived from `archive/` JSON, partitioned by kind/year under `analytics/parquet/`.
- Maintain a `manifest.json` for available dates/languages and metadata, rather than scanning Parquet at request time.
- Use DuckDB embedded in-process with a connection-per-request model for read-only analytics.
- Place Python sources under `py/` and add a repo-level `sitecustomize.py` so `python -m` can locate the packages without extra environment configuration.

### Consequences
- Build artifacts are clearly separated from source data and can be regenerated without data loss.
- Query semantics are centralized and testable (parameterized SQL, explicit presence modes).
- Connection-per-request avoids cross-thread shared state while keeping the runtime simple for local use.
- The `sitecustomize.py` approach keeps CLI usage simple, but applies to any Python invocation from the repo root.

## ADR-002: In-memory cache with TTL + pre-warm for day navigation

- Status: Accepted
- Date: 2026-02-04

### Context
Day navigation and toplists should feel fast for local use. The dataset is relatively small, so a lightweight cache can reduce repeated queries without introducing operational complexity.

### Decision
- Add an in-memory LRU cache with TTL defaults for day payloads and toplist responses.
- Use background pre-warm for the previous/next available day after serving a request, and track hit/miss/pre-warm counters.

### Consequences
- Cache improves perceived latency for day flipping while keeping the system local-first.
- Cache state is ephemeral and safe to discard; correctness still depends on the source Parquet data.

## ADR-003: Rollup schema for day-presence analytics

- Status: Accepted
- Date: 2026-02-04

### Context
Some analytics (reappearing, streaks) rely on day-level presence, which can be computed once and reused to reduce full-table scans.

### Decision
- Materialize `repo_day_presence` and `dev_day_presence` rollups with the fields `best_rank_any`, `best_rank_non_null`, `non_null_languages`, and `has_all_languages`.
- Prefer rollups when no language filter is applied and presence mode is `day`, with safe fallback to raw Parquet queries.

### Consequences
- Rollups accelerate common day-based queries without changing underlying semantics.
- Rollups must be regenerated whenever Parquet inputs change and should be treated as derived artifacts.

## ADR-004: FastAPI + Jinja2 web stack for local analytics UI

- Status: Accepted
- Date: 2026-02-04

### Context
We need a lightweight local web server to expose JSON APIs and a minimal HTML UI for day navigation and toplist analytics without introducing a heavy frontend build pipeline.

### Decision
- Use FastAPI for HTTP routing and request validation.
- Render the minimal UI using Jinja2 templates with lightweight client-side fetch calls.
- Keep the server local-first (no auth, no rate limiting) and read from the analytics Parquet datasets plus manifest metadata.

### Consequences
- The UI is simple and fast to iterate without a dedicated frontend build step.
- API and UI live in the same Python process, simplifying deployment for local use.
