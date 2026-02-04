# src

This directory contains the TypeScript runtime that scrapes GitHub Trending and serves a
local UI over the archived data.

## Layout
- `main.ts` CLI entrypoint for scraping, imports, and the UI server.
- `scraper/` Puppeteer-based page parsers for trending repository and developer pages.
- `ui/` HTTP server and API that serves the static UI and archive search endpoints.

## Architecture
```mermaid
flowchart LR
    CLI["ts-node src/main.ts"] -->|scrape| Scraper["scraper/ (Puppeteer)"]
    Scraper -->|writes JSON| Archive["archive/<kind>/<date>/*.json"]
    CLI -->|import| Importer["import hf-projects"]
    Importer -->|writes JSON| Archive
    CLI -->|ui| Server["ui/server.ts"]
    Server -->|reads| Archive
    Server -->|serves| Browser["Local UI"]
```

## Common commands
```bash
npm run start scrape <developer|repository> <archive-dir>
npm run start ui <archive-root> [--port=8787] [--bind=127.0.0.1]
npm run start import hf-projects <csv-path-or-url> <archive-root> [--skip-existing] [--overwrite] [--dry-run]
```
