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
4. Deal history: `http://127.0.0.1:8000/deals/1/history?days=180&sort_order=asc`

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
6. Daily delta fields on `GET /deals`:
   - `price_platinum_7d_ago`, `price_platinum_change_7d`
   - `price_grand_cru_7d_ago`, `price_grand_cru_change_7d`
   - `price_platinum_30d_ago`, `price_platinum_change_30d`
   - `price_grand_cru_30d_ago`, `price_grand_cru_change_30d`

Health table: `ingestion_runs` tracks last import status/time so the app can expose freshness.

History table: `wine_deal_snapshots` stores timestamped deal snapshots for each import run.
Old history rows are auto-pruned using `HISTORY_RETENTION_DAYS`.

History endpoint notes (`GET /deals/{deal_id}/history`):

1. `days`: lookback window (default `90`)
2. `limit`: max rows returned (default `30`)
3. `sort_order`: `asc` (chart-friendly) or `desc`

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


### Vivino Overrides

If a wine is valid on Vivino but missing from `seed/vivino_results.csv`, add a manual row to `seed/vivino_overrides.csv`.
The importer will apply overrides automatically (exact `match_name` takes priority).

CSV columns:
`match_name,wine_name,vivino_rating,vivino_num_ratings,vivino_price,vivino_url,notes`

Example:
```csv
match_name,wine_name,vivino_rating,vivino_num_ratings,vivino_price,vivino_url,notes
2022 Daou Vineyards - Cabernet Sauvignon Reserve - Red - 750 ml - Standard Bottle,DAOU Reserve Cabernet Sauvignon,4.1,661,99.96,https://www.vivino.com/en/daou-reserve-cabernet-sauvignon/w/1189764?year=2022&price_id=39581886,manual override
```

## Automatic Vivino Matching (Deterministic + Confidence)

Use the resolver to avoid random search strings and keep matching repeatable:

```bash
python scripts/resolve_vivino_matches.py \
  --comparison seed/comparison_summary.csv \
  --vivino seed/vivino_results.csv \
  --vivino-overrides seed/vivino_overrides.csv \
  --provider none \
  --only-new-unresolved
```

What this does:

1. Parses each unresolved wine into structured fields (`year`, `producer`, `label`, `color`).
2. Generates deterministic Vivino-oriented queries (`query_1..query_3`).
3. Writes review queue and unmatched outputs:
   - `data/vivino_review_queue.csv`
   - `data/vivino_unmatched.csv`
4. Optionally auto-applies high-confidence matches into `seed/vivino_overrides.csv`.

To enable automatic web search retrieval for candidates, you now have a free-first resolver mode.

Free-first resolver strategy:

1. `google_cse` first (free daily quota)
2. `brave` second (free monthly tier)
3. `serper` fallback (starter credits)

The resolver now supports:

1. `--provider auto` with fallback order
2. Local query cache (`data/vivino_query_cache.json`) to avoid repeated paid/free API hits
3. Optional API-call cap per run (`--max-api-queries`)
4. Delta-only mode (`--only-new-unresolved`, default enabled) so only newly unresolved wines are queried

Example (free-first + cache):

```bash
export GOOGLE_API_KEY="your_google_api_key"
export GOOGLE_CSE_ID="your_programmable_search_engine_id"
export BRAVE_API_KEY="your_brave_api_key"        # optional but recommended
export SERPER_API_KEY="your_serper_api_key"      # optional fallback

python scripts/resolve_vivino_matches.py \
  --provider auto \
  --auto-provider-order google_cse,brave,serper \
  --query-cache data/vivino_query_cache.json \
  --state-file data/vivino_resolver_state.json \
  --cache-ttl-hours 168 \
  --max-api-queries 40 \
  --auto-apply
```

Direct provider modes still work:

```bash
python scripts/resolve_vivino_matches.py --provider google_cse --auto-apply
python scripts/resolve_vivino_matches.py --provider brave --auto-apply
python scripts/resolve_vivino_matches.py --provider serper --auto-apply
```

Brave-only (recommended current free path):

```bash
export BRAVE_API_KEY="your_brave_api_key"
python scripts/resolve_vivino_matches.py \
  --provider brave \
  --query-cache data/vivino_query_cache.json \
  --state-file data/vivino_resolver_state.json \
  --max-api-queries 40 \
  --auto-apply
```

Then run the normal import pipeline:

```bash
python scripts/refresh_pipeline.py \
  --comparison seed/comparison_summary.csv \
  --vivino seed/vivino_results.csv \
  --vivino-overrides seed/vivino_overrides.csv \
  --resolve-vivino \
  --resolver-provider auto \
  --resolver-auto-provider-order google_cse,brave,serper \
  --resolver-query-cache data/vivino_query_cache.json \
  --resolver-state-file data/vivino_resolver_state.json \
  --resolver-cache-ttl-hours 168 \
  --resolver-max-api-queries 40 \
  --health-url http://127.0.0.1:8010/health
```

### Reset DB (Local or Railway)

Reset schema and re-import from seed files:

```bash
python scripts/reset_database.py --drop-all
python scripts/import_wine_data.py \
  --comparison seed/comparison_summary.csv \
  --vivino seed/vivino_results.csv \
  --vivino-overrides seed/vivino_overrides.csv
```

Railway example:

```bash
railway run python scripts/reset_database.py --drop-all
railway run python scripts/import_wine_data.py \
  --comparison seed/comparison_summary.csv \
  --vivino seed/vivino_results.csv \
  --vivino-overrides seed/vivino_overrides.csv
```

### Logging

1. API request logging includes `X-Request-ID`, path, status, client IP, and duration.
2. Import script logs start/success/failure details.
3. Configure with:
   - `LOG_LEVEL` (default `INFO`)
   - `ACCESS_LOG_ENABLED` (`true`/`false`, default `true`)
