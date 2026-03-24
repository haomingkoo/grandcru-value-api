# Wine Intelligence API

Three-way wine price comparison engine for Singapore — compares **Platinum Wine Club**, **Grand Cru**, and **Vivino market prices** to surface genuine deals with AI-powered scoring.

**Live**: [wine.kooexperience.com](https://wine.kooexperience.com/)

## Architecture

```
Scrape (Selenium)  →  Match & Compare  →  Vivino Enrich  →  Import to DB  →  FastAPI + Vanilla JS
  Platinum             Normalize names     Ratings, price     Deal scoring       Filters, map, search
  Grand Cru            Bundle detection    Grapes, region     Metadata derive    Deal signal pills
                       Per-bottle prices   Tasting notes      Gap-fill           Price trends
```

### Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | FastAPI, SQLAlchemy, Pydantic |
| Database | SQLite (local), PostgreSQL (Railway prod) |
| Scraping | Selenium, Shopify products.json API |
| Vivino | HTML parsing, JSON-LD, Vivino tastes/reviews API |
| LLM | Gemini Flash for wine identity extraction |
| Frontend | Vanilla JS, Leaflet maps, Tailwind CSS |
| Deploy | Railway (auto-deploy from `main`) |

## Quick Start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scripts/refresh_pipeline.py
uvicorn app.main:app --reload
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000)

## API Endpoints

### Public

| Endpoint | Description |
|----------|-------------|
| `GET /` | Web UI |
| `GET /deals` | List deals with filters, sorting, pagination |
| `GET /deals/filters` | Available filter options with counts |
| `GET /deals/stats` | Aggregated stats (counts, averages by offering type) |
| `GET /deals/map` | Geographic origin points for map markers |
| `GET /deals/{id}` | Single deal detail |
| `GET /deals/{id}/history` | Price history snapshots (7d/30d trends) |
| `GET /health` | DB status, ingestion freshness |

### Ops (requires `X-Ops-Key` header)

| Endpoint | Description |
|----------|-------------|
| `POST /ops/refresh/trigger` | Trigger scrape + import pipeline |
| `GET /ops/refresh/status` | Current/last refresh run state |
| `GET /ops/refresh/log` | Tail of latest refresh log |
| `GET /ops/diagnostics` | Runtime metadata, DB counts, env flags |

## Data Pipeline

The pipeline runs in 5 stages:

1. **Scrape** — `scripts/scrape_sources.py` pulls Platinum (DOM) and Grand Cru (Shopify API) catalogs
2. **Match** — `scripts/build_comparison_summary.py` pairs wines across retailers, normalizes to per-bottle prices
3. **Resolve Vivino** — `scripts/resolve_vivino_matches.py` or `scripts/llm_vivino_resolver.py` finds Vivino URLs via search APIs + Gemini
4. **Enrich** — `scripts/enrich_vivino_results.py` fetches ratings, prices, grapes, region, tasting notes from Vivino pages
5. **Import** — `scripts/import_wine_data.py` merges all data, computes deal scores, writes to DB

### Deal Score (0-100)

| Component | Max Points | Source |
|-----------|-----------|--------|
| Retailer discount (Platinum vs Grand Cru) | 30 | Price comparison |
| Market discount (Platinum vs Vivino) | 30 | Vivino market price |
| Rating quality | 25 | Vivino rating (0-5) |
| Confidence (sample size) | 10 | log10(rating count) |
| Bonus (beats both) | 5 | Both discounts > 5% |

## Refresh Commands

```bash
# Quick reimport from existing CSVs
python scripts/refresh_pipeline.py

# Full scrape + match + enrich + import
python scripts/refresh_pipeline.py \
  --scrape-and-build \
  --resolve-vivino --resolver-provider brave \
  --resolver-auto-apply --resolver-max-api-queries 40

# Trigger on Railway
curl -X POST https://wine.kooexperience.com/ops/refresh/trigger \
  -H "X-Ops-Key: $OPS_KEY" \
  -H "Content-Type: application/json" \
  -d '{"mode":"daily"}'
```

## Environment Variables

Copy `.env.example` to `.env` and fill in your keys.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | No | `sqlite:///./data/wines.db` | PostgreSQL URL for production |
| `CORS_ORIGINS` | No | kooexperience.com domains | Comma-separated allowed origins |
| `OPS_API_KEY` | For ops | — | HMAC key for `/ops/*` endpoints |
| `BRAVE_API_KEY` | For resolver | — | Brave Search API key |
| `GEMINI_API_KEY` | For LLM | — | Google Gemini API key |
| `RATE_LIMIT_REQUESTS_PER_MINUTE` | No | `120` | Per-IP rate limit |

## Search & Filters

The search box queries: **wine name, producer, grapes, region, and tasting notes**.

Filter dimensions: country, region, wine type, style family, grape variety, offering type, producer. Toggle switches for comparable-only, Platinum-cheaper-only, 4.0+ rated, and 100+ ratings.

## Data Model

| Table | Purpose |
|-------|---------|
| `wine_deals` | Current deal snapshot (60+ fields) |
| `wine_deal_snapshots` | Daily price history (auto-pruned by `HISTORY_RETENTION_DAYS`) |
| `ingestion_runs` | Pipeline execution tracking |

## Security

- Secrets via environment variables only (no hardcoded keys)
- HMAC-authenticated ops endpoints with `hmac.compare_digest()`
- Per-IP rate limiting with 429 responses
- XSS protection via `escapeHtml()` on all rendered values
- Security headers: `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`
- CORS restricted to explicit allowed origins (no wildcard in production)

## Legal

`GET /legal` serves a responsible scraping and usage statement. This is informational, not legal advice.
