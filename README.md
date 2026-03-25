# Wine Intelligence API

Three-way wine price comparison engine for Singapore. Compares **Platinum Wine Club**, **Grand Cru**, and **Vivino market prices** to surface genuine deals with AI-powered scoring.

**Live at** [wine.kooexperience.com](https://wine.kooexperience.com/)

---

## How It Works

```
Scrape             Match & Compare        Resolve Vivino        Enrich            Score & Serve
───────────────    ──────────────────     ────────────────     ───────────       ──────────────
Platinum (DOM)  →  Fuzzy name matching →  Brave Search API  →  Ratings        →  Deal score 0-100
Grand Cru (API)    Bundle detection       Gemini LLM fallback  Prices            FastAPI + JS UI
                   Per-bottle pricing     Direct Vivino search  Grapes, region    Filters, map, trends
```

Platinum Wine Club is scoped as the source of truth for inventory — only in-stock items are shown. Grand Cru products are included for price comparison even if sold out. Vivino provides independent market pricing and community ratings.

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
| **weekly-ingest** | Full pipeline + LLM resolver with cache bypass | Mondays 02:00 UTC |

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

Five stages, orchestrated by `scripts/refresh_pipeline.py`:

| Stage | Script | What it does |
|-------|--------|-------------|
| 1. Scrape | `scrape_sources.py` | Selenium for Platinum portal; Shopify API for Grand Cru |
| 2. Match | `build_comparison_summary.py` | Fuzzy name matching, bundle detection, per-bottle normalization |
| 3. Resolve | `resolve_vivino_matches.py` | Brave Search for Vivino URLs (fallback: Google CSE, Serper) |
| 4. LLM Resolve | `llm_vivino_resolver.py` | Gemini Flash extracts wine identity, finds Vivino pages, scrapes prices/ratings |
| 5. Import | `import_wine_data.py` | Merge sources, compute deal scores, write to DB |

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

# Full pipeline (scrape + resolve + enrich)
python scripts/refresh_pipeline.py \
  --scrape-and-build \
  --resolve-vivino --resolver-provider brave \
  --resolver-auto-apply --resolver-max-api-queries 40

# With LLM resolver (Gemini + Brave, bypasses 30-day cache)
python scripts/refresh_pipeline.py \
  --scrape-and-build \
  --resolve-vivino --resolver-provider brave --resolver-auto-apply \
  --llm-resolve --llm-resolve-all --llm-resolve-force

# Run LLM resolver locally (Vivino blocks Railway IPs)
railway run python scripts/llm_vivino_resolver.py \
  --force --all --auto-apply --sleep 3

# Trigger on Railway
curl -X POST https://wine.kooexperience.com/ops/refresh/trigger \
  -H "X-Ops-Key: $OPS_KEY" \
  -H "Content-Type: application/json" \
  -d '{"mode": "weekly"}'
```

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
  enrich_vivino_results.py
  import_wine_data.py
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
