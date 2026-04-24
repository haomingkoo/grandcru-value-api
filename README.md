# Wine Intelligence API

Wine price comparison engine for Singapore. Compares **Platinum Wine Club** and **Grand Cru** retailer prices with **Vivino** community ratings and market pricing to surface genuine deals with AI-powered scoring.

**Live at** [wine.kooexperience.com](https://wine.kooexperience.com/)

---

## How It Works

```
Scrape             Match & Compare        Resolve Vivino        Enrich            Score & Serve
───────────────    ──────────────────     ────────────────     ───────────       ──────────────
Platinum (DOM)  →  Fuzzy name matching →  Brave Search API  →  Ratings        →  Deal score 0-100
Grand Cru (API)    Bundle detection       Gemini LLM fallback  Prices (SGD)      FastAPI + JS UI
                   Total listing prices   Identity cache        Grapes, region    Filters, map, trends
```

Platinum Wine Club is scoped as the source of truth for inventory — only in-stock items are shown. Grand Cru products are included for price comparison even if sold out. Vivino provides independent market pricing (SGD) and community ratings.

All prices are displayed as **total listing prices** matching Platinum's bundle size — if Platinum sells a bundle of 3, Grand Cru and Vivino prices are also shown for 3 bottles. This ensures apples-to-apples comparison.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | FastAPI, SQLAlchemy 2.0, Pydantic v2 |
| Database | SQLite (dev) / PostgreSQL (prod) |
| Scraping | Selenium + Shopify products.json |
| Search | Brave Search API, Google Generative AI (Gemini Flash) |
| Frontend | Vanilla JS, Leaflet.js, Tailwind CSS |
| Deployment | Docker, Railway (auto-deploy from `main`) |

## Infrastructure

Three Railway services under project **zonal-purpose**:

| Service | Role | Schedule |
|---------|------|----------|
| **web** | FastAPI app + static frontend | Always on |
| **daily-ingest** | Scrape Platinum/Grand Cru, Brave resolver, import | Daily cron |
| **weekly-ingest** | Full pipeline + LLM resolver (Vivino prices/descriptions) | Mondays 02:00 UTC |

Data flows into a shared PostgreSQL database. The web service reads from the DB — it does not re-import from CSVs if the daily cron has already refreshed the data within 20 hours.

## Quick Start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in API keys
python scripts/refresh_pipeline.py
uvicorn app.main:app --reload
```

Open [localhost:8000](http://localhost:8000).

## API

### Public

| Endpoint | Description |
|----------|-------------|
| `GET /` | Web dashboard |
| `GET /health` | DB status, ingestion freshness, last update timestamp |
| `GET /deals` | List deals (filters, sort, pagination, limit 500) |
| `GET /deals/filters` | Filter dimensions with counts |
| `GET /deals/stats` | Aggregates by offering type |
| `GET /deals/map` | Origin coordinates for map markers |
| `GET /deals/{id}` | Single deal |
| `GET /deals/{id}/history` | Price snapshots (up to 365 points) |

**Query parameters on `/deals`:** `search`, `min_score`, `min_vivino_rating`, `min_vivino_num_ratings`, `max_platinum_price`, `comparable_only`, `only_platinum_cheaper`, `cheaper_side`, `country`, `region`, `wine_type`, `style_family`, `grape`, `offering_type`, `producer`.

### Ops (requires `X-Ops-Key` header)

| Endpoint | Description |
|----------|-------------|
| `POST /ops/refresh/trigger` | Start pipeline (`daily`, `weekly`, or `import_only`) |
| `GET /ops/refresh/status` | Current run state |
| `GET /ops/refresh/log` | Tail of refresh log |
| `GET /ops/diagnostics` | Runtime metadata and DB counts |

## Data Pipeline

Seven stages, orchestrated by `scripts/refresh_pipeline.py`:

| Stage | Script | What it does |
|-------|--------|-------------|
| 1. Scrape | `scrape_sources.py` | Selenium for Platinum portal; Shopify API for Grand Cru |
| 2. Match | `build_comparison_summary.py` | Fuzzy name matching, bundle detection, total listing prices |
| 3. Resolve | `resolve_vivino_matches.py` | Brave Search for Vivino URLs (skips wines in identity cache) |
| 4. LLM Resolve | `llm_vivino_resolver.py` | Gemini Flash extracts wine identity, fetches Vivino prices/ratings (uses cached URLs when available) |
| 5. Market Validate | `validate_market_prices.py` | Checks URL matches against wine identity (producer, label, classification) and price sanity |
| 6. Import | `import_wine_data.py` | Merge sources, compute deal scores, write to DB |
| 7. Completeness Validate | `validate_wine_completeness.py` | Fails unexpected missing Vivino URL/rating/count gaps after import |

### Identity Cache

Wine→URL mappings are permanent. The **identity cache** (`data/identity_cache.json`) stores validated Vivino and Wine-Searcher URLs so resolvers skip Brave searches for known wines. Only new/flagged wines trigger API calls, reducing Brave usage by ~84%.

### Startup Import Logic

On deploy, the web service runs `import_wine_data.py --skip-if-fresh 20`:
- If the daily cron imported data in the last 20 hours, the CSV import is **skipped** (trusts the DB)
- If no recent ingestion exists (first deploy or cron failure), it imports from the committed seed CSVs as a safety net

This ensures code pushes never overwrite fresh cron data.

### Deal Score (0-100)

| Component | Max | How |
|-----------|-----|-----|
| Retailer discount | 30 | Platinum vs Grand Cru price gap |
| Market discount | 30 | Platinum vs Vivino market price |
| Rating quality | 25 | Vivino rating / 5.0 |
| Confidence | 10 | log10(rating count) |
| Bonus | 5 | Both discounts > 5% |

### Refresh Modes

```bash
# Quick reimport from cached CSVs
python scripts/refresh_pipeline.py

# Daily (light — only new wines, 40 Brave calls max)
python scripts/refresh_pipeline.py \
  --scrape-and-build \
  --resolve-vivino --resolver-provider brave \
  --resolver-auto-apply --resolver-require-vivino-metrics \
  --resolver-max-api-queries 40 \
  --resolver-only-new-unresolved

# Weekly (full — all wines, LLM resolve for descriptions/prices)
python scripts/refresh_pipeline.py \
  --scrape-and-build \
  --resolve-vivino --resolver-provider brave --resolver-auto-apply \
  --resolver-require-vivino-metrics \
  --resolver-max-api-queries 50 --no-resolver-only-new-unresolved \
  --llm-resolve --llm-resolve-all

# Seed identity cache (one-time, after fresh resolve)
python scripts/build_identity_cache.py
```

Both daily and weekly modes run as Railway cron services. The identity cache ensures known wines skip Brave searches — only new wines use API calls.

Post-import completeness validation runs by default. New wines without a Vivino URL or rating/count fail the refresh unless the gap is explicitly documented in `scripts/data_quality_rules.py`. Resolver auto-apply also requires Vivino rating/count metrics by default, so URL-only search hits stay in review instead of becoming blank-rating matches.

### Residential Vivino Review Loop

For guarded Vivino maintenance, use `scripts/local_vivino_refresh.sh` on a Mac or other residential-IP machine.

- It fetches the current unresolved live rows from `GET /ops/vivino/unresolved.csv`
- It resolves only those rows locally on your residential connection
- It respects locked manual overrides in `seed/vivino_overrides.csv`
- It can be paired with local AI review assistants such as Codex or Claude Code to inspect suspicious matches before you push
- It pushes reviewed overrides, waits for deploy, then triggers `POST /ops/refresh/trigger` with `mode=daily`

This is not a zero-touch claim. Scrape/import is automated; edge-case Vivino identity still benefits from a local review loop because a missing price is safer than a wrong match.

Manual or reviewed overrides should be locked by either:

- Setting `locked=1` in `seed/vivino_overrides.csv`
- Or using a `notes` value that starts with `manual`

## Environment Variables

Copy `.env.example` to `.env`.

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `DATABASE_URL` | No | `sqlite:///./data/wines.db` | PostgreSQL in production |
| `OPS_API_KEY` | For ops | - | HMAC auth for `/ops/*` |
| `BRAVE_API_KEY` | For resolver | - | Brave Search API |
| `GEMINI_API_KEY` | For LLM | - | Google Gemini (falls back to `GOOGLE_API_KEY`) |
| `CORS_ORIGINS` | No | kooexperience.com | Comma-separated origins |
| `RATE_LIMIT_REQUESTS_PER_MINUTE` | No | `120` | Per-IP rate limit |
| `INGESTION_STALE_HOURS` | No | `24` | Hours before health reports stale |
| `HISTORY_RETENTION_DAYS` | No | `90` | Snapshot auto-prune window |

## Data Model

| Table | Purpose |
|-------|---------|
| `wine_deals` | Current deals (60+ fields: prices, ratings, metadata, geography, scoring) |
| `wine_deal_snapshots` | Daily price history for trend charts |
| `ingestion_runs` | Pipeline execution tracking |

## Security

- Environment variables for all secrets
- HMAC-authenticated ops endpoints (`hmac.compare_digest`)
- Per-IP rate limiting with `429 Retry-After` responses
- HTML escaping on all rendered values
- Security headers: `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`
- CORS restricted to explicit origins (no wildcard in production)

## Project Structure

```
app/
  main.py              FastAPI app, routes, middleware
  config.py            Settings from environment
  models.py            SQLAlchemy ORM (WineDeal, Snapshot, IngestionRun)
  service.py           Query logic (filters, stats, map, history)
  scoring.py           Deal score computation
  security.py          Rate limiter, HMAC auth
  ops.py               Refresh runner and diagnostics
scripts/
  refresh_pipeline.py  Pipeline orchestrator
  scrape_sources.py    Selenium scraper
  build_comparison_summary.py
  resolve_vivino_matches.py
  llm_vivino_resolver.py
  llm_market_resolver.py  Wine-Searcher price resolver (disabled — no reliable SGD source)
  validate_market_prices.py  URL + price sanity validator
  build_identity_cache.py  Seed identity cache from existing data
  enrich_vivino_results.py
  import_wine_data.py
  llm_utils.py         Shared cache, identity cache, Gemini helpers
web/
  index.html           Dashboard UI
  app.js               Client-side logic (~2000 lines)
  styles.css           Dark theme styles
seed/
  comparison_summary.csv   Platinum vs Grand Cru (committed, fallback for deploys)
  vivino_results.csv       Pre-known Vivino URLs
  vivino_overrides.csv     LLM resolver output (committed, fallback for deploys)
```

## Legal

See [`LEGAL_NOTICE.md`](LEGAL_NOTICE.md) for the responsible data use policy, or `GET /legal` on the live API.
