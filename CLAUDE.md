# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Purpose

Wine deal-comparison API: scrapes Platinum Wine Club and Grand Cru Wines prices, cross-references Vivino ratings/market prices, scores each wine 0-100, and serves the results via FastAPI + a vanilla JS dashboard. Live at [wine.kooexperience.com](https://wine.kooexperience.com).

## Tech Stack

- **Backend**: FastAPI, SQLAlchemy 2.0 (ORM), Pydantic v2
- **DB**: SQLite locally (`data/wines.db`), PostgreSQL on Railway (`DATABASE_URL`)
- **Scraping**: Selenium (Platinum), Shopify `products.json` (Grand Cru)
- **Enrichment**: Brave Search API + Google Gemini (`google-generativeai`) for Vivino matching
- **Frontend**: `web/` — vanilla JS (`app.js`, ~2000 lines), Leaflet.js map, no build step
- **Deployment**: Docker on Railway, auto-deploy from `main`

## Commands

```bash
# Install
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in API keys

# Run locally
uvicorn app.main:app --reload
# open http://localhost:8000

# Test
PYTHONPATH=. pytest

# Pipeline (reimport from cached CSVs)
python scripts/refresh_pipeline.py

# Pre-commit (secrets scan, deploy-command check, pipeline compile check)
pre-commit run --all-files
```

CI (`.github/workflows/ci.yml`) runs secrets scan, deploy-command check, `py_compile` on pipeline scripts, then `pytest`.

## Architecture

Seven-stage pipeline orchestrated by `scripts/refresh_pipeline.py`: scrape → match/build comparison summary → resolve Vivino URLs (Brave) → LLM-resolve gaps (Gemini) → validate market prices → import to DB (`import_wine_data.py`, computes deal scores) → validate completeness. Daily/weekly Railway cron services run this; the web service only re-imports from seed CSVs if the DB has no fresh ingestion (`--skip-if-fresh 20` on startup, see `Dockerfile`).

The **identity cache** (`data/identity_cache.json`) stores validated wine→URL mappings so resolvers skip repeat API calls.

## Key Files

- `app/main.py` — FastAPI app, routes, middleware (CORS, rate limiting, security headers, ops auth)
- `app/config.py` — `Settings` dataclass, all env vars with defaults
- `app/models.py` — SQLAlchemy models: `WineDeal`, `WineDealSnapshot`, `IngestionRun`
- `app/service.py`, `app/scoring.py` — query logic (filters/stats/map/history) and deal-score computation
- `app/security.py`, `app/ops.py` — rate limiter/HMAC auth, and refresh runner for `/ops/*`
- `scripts/refresh_pipeline.py` — pipeline orchestrator entrypoint
- `scripts/data_quality_rules.py` — documented allowlist for accepted data gaps
- `web/` — frontend (`index.html`, `app.js`, `styles.css`), no build step
- `seed/` — committed fallback CSVs used when no fresh DB ingestion exists
- `deploy/*.txt` — checked-in Railway cron commands, must match `check_deploy_commands.py`

## Notes

- Public endpoints only expose `GET`; ops endpoints (`/ops/*`) require `X-Ops-Key` header.
- `README.md` has the full endpoint list, deal-score formula, and env var table — check it for details beyond this file.
- Do not commit `.env` or real API keys; `.env.example` documents required vars.
