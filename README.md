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

## Maintainer Quick Start

The backend has 3 distinct responsibilities:

1. `scripts/scrape_sources.py` pulls raw retailer catalog data into CSVs.
2. `scripts/build_comparison_summary.py` pairs Platinum rows with Grand Cru rows and computes retailer-vs-retailer price deltas.
3. `scripts/import_wine_data.py` hydrates Vivino metadata, computes `deal_score`, and writes `wine_deals` plus `wine_deal_snapshots`.

When debugging, keep these layers separate:

1. Retailer match issue: inspect `seed/comparison_summary.csv`
2. Vivino enrichment issue: inspect `seed/vivino_results.csv`, `seed/vivino_overrides.csv`, and `vivino_match_method`
3. API ranking or filter issue: inspect `app/service.py` and `/deals`

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
3. `cheaper_side`, `deal_score`, `vivino_match_method`
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

## Ranking, Sorting, and UI Semantics

These fields are easy to confuse when picking the project up for the first time.

### Signed Price Difference

`price_diff_pct` is signed and uses this formula:

```text
(price_platinum - price_grand_cru) / price_grand_cru * 100
```

Interpretation:

1. Negative: Platinum is cheaper
2. Positive: Grand Cru is cheaper
3. Zero: same price
4. `null`: no comparable Grand Cru price was found

Sorting examples:

1. `GET /deals?sort_by=price_diff_pct&sort_order=asc` returns the biggest Platinum discounts first
2. `GET /deals?sort_by=price_diff_pct&sort_order=desc` returns the biggest Platinum markups or Grand Cru advantages first

### Deal Score

`deal_score` is not the same thing as Vivino rating.

Formula from [app/scoring.py](app/scoring.py):

1. Discount component: up to `60` points
2. Vivino rating component: up to `30` points
3. Vivino rating-count confidence: up to `10` points

Because quality and confidence still contribute even when price difference is `0` or missing:

1. `Same Price` rows can still have a non-zero `deal_score`
2. `No Match` rows can still have a non-zero `deal_score` if they have strong Vivino data

### Stable Tie-Break Sorting

The API now uses deterministic tie-breakers so equal-looking UI values do not jump around between deploys.

Current sort behavior from [app/service.py](app/service.py):

1. `sort_by=deal_score`: tie-break by `price_diff_pct`, then `vivino_rating`, then `vivino_num_ratings`, then `wine_name`
2. `sort_by=price_diff_pct`: tie-break by `deal_score`, then `vivino_rating`, then `vivino_num_ratings`, then `wine_name`

If the frontend rounds scores to whole numbers, multiple rows may still look tied in the UI even when the backend is correctly sorting raw decimals like `35.82`, `35.80`, and `35.20`.

### Match Labels

`cheaper_side` and `vivino_match_method` are independent.

`cheaper_side` answers the retailer comparison question:

1. `Platinum Cheaper`
2. `Grand Cru Cheaper`
3. `Same Price`
4. `No Match`

`vivino_match_method` answers how Vivino metadata was attached:

1. `exact`: exact normalized-name match in the Vivino dataset
2. `canonical`: normalized-name family match after token cleanup
3. `fuzzy`: best similarity match that passed safety gates
4. `platinum`: Vivino data came from Platinum-embedded metadata because dataset matching found nothing
5. `none`: no Vivino metadata was attached

This means a card can legitimately show:

1. `cheaper_side = No Match`
2. `vivino_match_method = exact`

That combination means the project found the right Vivino wine, but did not find a comparable Grand Cru listing.

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
   - `CORS_ORIGINS` (comma-separated exact origins, for example `https://kooexperience.com,https://www.kooexperience.com,https://wine.kooexperience.com`)
   - `RATE_LIMIT_ENABLED` (default `true`)
   - `RATE_LIMIT_REQUESTS_PER_MINUTE` (default `120`)
3. Start command:
   - Railway will usually pick up `Procfile`.
   - Fallback command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
4. Run ingestion after deploy:
   - `python scripts/import_wine_data.py`

Important CORS note:

1. Origins must be exact scheme + host + optional port matches
2. `https://kooexperience.com` and `https://wine.kooexperience.com` are different origins
3. If the browser shows a CORS error after adding a new frontend hostname, update Railway `CORS_ORIGINS` first

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

### Full Daily Refresh (Websites + Vivino + Import)

Use this when you want fresh website data and fresh Vivino matching in one run:

```bash
export BRAVE_API_KEY="your_brave_api_key"
python scripts/refresh_pipeline.py \
  --scrape-and-build \
  --grandcru-base-url https://grandcruwines.com \
  --platinum-base-url https://platwineclub.wineportal.com \
  --scrape-output-dir seed/latest_refresh \
  --scrape-max-pages 50 \
  --comparison seed/comparison_summary.csv \
  --vivino seed/vivino_results.csv \
  --vivino-overrides seed/vivino_overrides.csv \
  --resolve-vivino \
  --resolver-provider brave \
  --resolver-max-api-queries 40 \
  --resolver-only-new-unresolved \
  --health-url http://127.0.0.1:8010/health
```

For one-time backfill (reprocess all unresolved rows):

```bash
python scripts/refresh_pipeline.py \
  --scrape-and-build \
  --comparison seed/comparison_summary.csv \
  --vivino seed/vivino_results.csv \
  --vivino-overrides seed/vivino_overrides.csv \
  --resolve-vivino \
  --resolver-provider brave \
  --resolver-max-api-queries 200 \
  --no-resolver-only-new-unresolved
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

### Ops Endpoints (Production Diagnostics + Manual Trigger)

Use these to inspect Railway runtime state and trigger refresh jobs from the hosted API itself.

Required env vars:

1. `OPS_API_KEY` (required to enable `/ops/*` endpoints)
2. `OPS_DEFAULT_HEALTH_URL` (optional fallback health URL for triggered runs)

Endpoints:

1. `GET /ops/diagnostics`:
   - runtime metadata (`git_commit`, service name, hostname)
   - DB counts (`total_deals`, `total_snapshots`)
   - file row counts for seed/queue CSVs
   - current refresh runner status
2. `GET /ops/refresh/status`: current/last refresh run state
3. `GET /ops/refresh/log?lines=300`: tail of latest refresh log
4. `POST /ops/refresh/trigger`: trigger `daily`, `weekly`, or `import_only` run mode

Examples:

```bash
export OPS_KEY="replace_me"
export API_BASE="https://web-production-f2effe.up.railway.app"

curl -sS "$API_BASE/ops/diagnostics" -H "X-Ops-Key: $OPS_KEY"
curl -sS "$API_BASE/ops/refresh/status" -H "X-Ops-Key: $OPS_KEY"
curl -sS "$API_BASE/ops/refresh/log?lines=200" -H "X-Ops-Key: $OPS_KEY"

curl -sS -X POST "$API_BASE/ops/refresh/trigger" \
  -H "Content-Type: application/json" \
  -H "X-Ops-Key: $OPS_KEY" \
  -d '{"mode":"daily","strict_health":false}'

curl -sS -X POST "$API_BASE/ops/refresh/trigger" \
  -H "Content-Type: application/json" \
  -H "X-Ops-Key: $OPS_KEY" \
  -d '{"mode":"weekly","strict_health":false}'
```
