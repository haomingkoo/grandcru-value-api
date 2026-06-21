# Project Status

**Last updated:** 2026-06-21
**Live URL:** https://wine.kooexperience.com
**Railway project:** zonal-purpose (service: web)

---

## What This Is

MinMax Wine — a Singapore wine deal-finder. Compares Platinum Wine Club offers against Grand Cru retail prices and Vivino market benchmarks. Scores deals by Vivino rating + price gap and shows them in a filterable table.

---

## Current Deployment State

| Layer | Status | Notes |
|-------|--------|-------|
| Backend (FastAPI) | ✅ Healthy | Last container start: 2026-06-21 02:14 |
| Database | ✅ Live | 61 current deals, 61 snapshots |
| Vivino coverage | ✅ 100% | 61 canonical/exact matches |
| Country/grapes coverage | ✅ 100% | Completeness gate passing |
| Grand Cru benchmarks | ⚡ 26 of 61 | 22 Vivino-only, 13 unmatched |
| Daily cron | ✅ Running | Via Railway cron service |

---

## What's Deployed vs What's Local

### Deployed (main branch, HEAD = 9cee4c5)
- Offer table with flat list of 61 deals
- Mobile price strip, verdict chips, trend indicators
- Country map, style browsing, discovery hub
- Grand Cru LLM resolver, Vivino identity cache
- Data completeness gate, quarantine CSV, freshness guard

### Local (uncommitted — ready to commit)
- **`web/app.js`** — Offer family grouping: groups same-wine vintages/formats into collapsible shelf rows. 54 wine groups from 61 offers.
- **`web/styles.css`** — Grouped shelf styling + simplified mobile layout (hides non-essential columns)
- **`tests/test_frontend_browser.py`** — New test `test_offer_shelf_groups_duplicate_wine_families` + updated mobile test

**All 4 browser tests pass.** Production already running new code (deployed via `railway up`). Needs a proper commit + PR.

---

## Architecture

```
Railway (Docker)
├── FastAPI app (app/)
│   ├── main.py          — API routes, lifespan, static serving
│   ├── service.py       — DB queries, deal aggregation
│   ├── ops.py           — Refresh pipeline orchestrator
│   ├── scoring.py       — Deal score formula
│   ├── deal_insights.py — Verdict labels and tone
│   └── database.py      — SQLAlchemy engine + migrations
├── Web frontend (web/)
│   ├── index.html       — Shell with section nav
│   ├── app.js           — All UI logic (2400 lines, vanilla JS)
│   └── styles.css       — All styles (2900 lines)
├── Data pipeline (scripts/)
│   ├── platinum_scraper.py     — Selenium → Platinum offers
│   ├── grandcru_scraper.py     — Shopify API → GC catalog
│   ├── vivino_resolver.py      — Brave Search + Gemini → Vivino URL
│   ├── llm_grandcru_resolver.py— Gemini → GC catalog match
│   └── refresh_pipeline.py     — Orchestrates all scrapers + import
└── Seed data (seed/)
    ├── comparison_summary.csv  — Platinum vs Grand Cru prices (bundle totals, do NOT divide)
    ├── vivino_results.csv      — Vivino URLs + resolved metadata
    └── vivino_overrides.csv    — Locked Vivino prices (source of truth)
```

---

## Invariants (things that must NOT change without careful thought)

1. **Platinum prices from CSV are bundle totals** — do not divide by quantity
2. **Vivino override CSV is the price source of truth** — live scrape can differ; override wins
3. **Gemini grounding for prices is permanently disabled** — hallucinated values (see CHANGELOG)
4. **Market prices (Wine-Searcher / USD) are disabled** — conversion noise for SG users
5. **Freshness guard on startup import** — do not remove; prevents stale seed from overwriting live DB
6. **No hidden data substitution** — every price/rating must be traceable to a scrape or override

---

## Prioritised Next Work

### High (blocks daily usability)
- [ ] Commit offer grouping work on `feat/offer-grouping` branch + PR
- [ ] Grand Cru benchmark coverage: 26/61 is low; investigate remaining 13 unmatched

### Medium (quality of life)
- [ ] Data quality dashboard: show Vivino override %, market price %, freshness status in `/ops`
- [ ] Sentry integration: silent cron failures have no alerting
- [ ] Break `app.js` into modules (`shelf.js`, `filters.js`, `table.js`) — 2400 lines is the limit

### Low (nice to have)
- [ ] Backend `/deals?group_families=true` — avoid re-grouping on every frontend load
- [ ] Vivino confidence column in override CSV (`manual_review` / `high_llm` / `auto_accept`)
- [ ] SEO meta tags refresh on daily import (currently only set at startup)

---

## Session Handoff Protocol

**Start of session:** Read this file + CHANGELOG.md "Unreleased" section + `git status`.
**End of session:** Update "Local (uncommitted)" section above + add Unreleased entry to CHANGELOG if new work done.

This prevents the "what were we doing last time?" loop.
