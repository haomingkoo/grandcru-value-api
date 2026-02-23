# GrandCru Value API

Refactor of the notebook scraper into a backend service that helps users find high-value wines on Platinum by comparing:

1. Platinum price
2. Grand Cru price
3. Vivino quality signals
4. Direct links back to source product pages

## What Is Included

1. FastAPI backend (`app/main.py`)
2. SQLAlchemy models for deals and ingestion health (`app/models.py`)
3. Deal ranking logic (`app/scoring.py`)
4. CSV to database import pipeline (`scripts/import_wine_data.py`)
5. Reusable source scraper (`scripts/scrape_sources.py`)
5. API endpoints:
   - `GET /deals`
   - `GET /deals/{deal_id}`
   - `GET /deals/{deal_id}/history`
   - `GET /health`
   - `GET /legal`

## Local Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/import_wine_data.py
uvicorn app.main:app --reload
```

Open:

1. API docs: `http://127.0.0.1:8000/docs`
2. Health: `http://127.0.0.1:8000/health`
3. Deals: `http://127.0.0.1:8000/deals?limit=20&only_platinum_cheaper=true`

## Refreshing Source Data

If website formatting changes, avoid notebook edits first. Run the refactored scraper with a small page cap and debug HTML output:

```bash
pip install -r requirements-scraper.txt
python scripts/scrape_sources.py \
  --grandcru-base-url https://grandcruwines.com \
  --platinum-base-url https://platwineclub.wineportal.com \
  --max-pages 2 \
  --debug-dir seed/debug_html \
  --headed
```

Outputs:

1. `seed/grandcru_wines.csv`
2. `seed/platinum_wines.csv`
3. `seed/scrape_run.json`
4. Debug HTML snapshots in `seed/debug_html/` (for selector troubleshooting)

## Data Model

Primary table: `wine_deals`

Key fields:

1. `wine_name`, `vintage`, `quantity`, `volume`
2. `price_platinum`, `price_grand_cru`, `price_diff`, `price_diff_pct`
3. `cheaper_side`, `deal_score`
4. `platinum_url`, `grand_cru_url`, `vivino_url`
5. `vivino_rating`, `vivino_num_ratings`

Health table: `ingestion_runs` tracks last import status/time so the app can expose freshness.

History table: `wine_deal_snapshots` stores timestamped deal snapshots for each import run.
Old history rows are auto-pruned using `HISTORY_RETENTION_DAYS`.

## Hosting Recommendation

Given your current setup (static homepage on GitHub and prior Railway use), this split is the cleanest:

1. Keep homepage on GitHub Pages (static marketing/front page).
2. Host this API on Railway (good DX, simple deploys, managed env vars, Postgres option).
3. Point homepage JS/API client to Railway URL (for example: `https://grandcru-api.up.railway.app`).

If traffic grows, switch `DATABASE_URL` from SQLite to Railway Postgres without app code changes.

### Railway Quick Start

1. Create a new service from your backend repo.
2. Add environment variables:
   - `DATABASE_URL` (optional at first; default SQLite works)
   - `INGESTION_STALE_HOURS` (optional)
   - `HISTORY_RETENTION_DAYS` (optional, default `90`)
   - `CORS_ORIGINS` (set to your GitHub Pages domain in production)
   - `RATE_LIMIT_ENABLED` (default `true`)
   - `RATE_LIMIT_REQUESTS_PER_MINUTE` (default `120`)
3. Start command:
   - Railway will usually pick up `Procfile`.
   - Fallback command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
4. Run ingestion after deploy:
   - `python scripts/import_wine_data.py`

### API Abuse Controls

The API includes in-app IP rate limiting with `429` responses when a client exceeds quota.
For stronger protection and cost control, also configure:

1. Railway spend alert and hard budget cap
2. Edge/WAF protection (Cloudflare or similar)
3. Strict `CORS_ORIGINS` (avoid `*` in production)

## Repository Recommendation

For maintainability, create a new repo for this backend.

Suggested structure:

1. `grandcru-value-api` (new repo, private initially)
2. Existing homepage repo remains separate (public static site)

Reason:

1. Clear deployment pipeline per app
2. Cleaner secrets management for backend
3. Easier CI/CD and issue tracking

### New Repo Steps

```bash
mkdir grandcru-value-api
cd grandcru-value-api
# copy this project files in
git init
git add .
git commit -m "Initial GrandCru value API scaffold"
gh repo create grandcru-value-api --private --source=. --remote=origin --push
```

## Suggested Product Features

1. User watchlist for favorite wines and price-drop alerts
2. Price history graph per wine
3. "Why this is a deal" explanation (discount + rating + confidence)
4. Region/style filters
5. Daily automated ingestion with run logs and alerting

## Legal/Compliance

`LEGAL_NOTICE.md` contains a responsible scraping and usage statement for in-app display (`GET /legal`).

Important: this is not legal advice. For enforceable terms and jurisdiction-specific compliance, work with counsel.

## One-Command Refresh Pipeline

Use this helper to import refreshed CSVs and optionally validate API health:

```bash
python scripts/refresh_pipeline.py \
  --comparison seed/comparison_summary.csv \
  --vivino seed/vivino_results.csv \
  --health-url http://127.0.0.1:8010/health
```

For Railway production imports, run the same command with your production `DATABASE_URL` and Railway API health URL.
