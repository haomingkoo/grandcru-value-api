# Changelog

All notable changes to grandcru-value-api are recorded here. Format: what changed, why, and what it closes (to prevent re-opening the same fix).

---

## [Unreleased] — Offer Family Grouping

**What:** Groups multiple vintages/formats of the same wine into a collapsible shelf row. Best offer shown by default; tap "View N offers" to expand.

**Why:** Users were confused by seeing the same wine 2-3 times in the table with no indication they were related.

**Changed files:** `web/app.js`, `web/styles.css`, `tests/test_frontend_browser.py`

**Closes:** Flat table duplication of wine families (same wine, different vintage/format showing as separate rows).

---

## [0.5] — 2026-06-09/10 — UI Polish & Mobile Hardening

**What:**
- Wine imagery thumbnails in offer table
- Table header contrast and visual hierarchy improvements
- Hero filter pills and map styling
- Mobile price strip moved above action links (user couldn't see prices without scrolling)
- Offers table stays visible below explorer section after filter navigation
- SEO discovery metadata + CSP fixes

**Why:** Several user-reported mobile issues where prices were hidden below the fold or clipped. Map CSP errors prevented tile loading.

**Closes:** Mobile price visibility (fixes #be1eadb, #356e0cd, #89727a5). This was patched 3× before — root cause was column ordering; final fix hides non-essential columns on mobile and uses the `mobile-price-strip` component as the sole price display.

---

## [0.4] — 2026-06-08 — Grand Cru LLM Resolver & CI Gates

**What:**
- LLM-powered Grand Cru catalog matcher (Gemini Flash) for wines without a direct URL match
- Grand Cru resolver runs in daily refresh pipeline
- Source coverage exposed in API + UI (benchmarked / Vivino-only / unmatched)
- CI guardrails: import blocked if completeness gate fails
- Bundle schema preserved through Grand Cru resolution pipeline

**Why:** Grand Cru matching was purely fuzzy-string. ~25% of wines had no benchmark because the name formats diverged (e.g., "Chambolle-Musigny" vs "Chambolle Musigny"). LLM closes that gap.

**Closes:** Missing Grand Cru benchmarks for wines with name-format divergence. Completeness gate prevents silent data quality regressions from reaching production.

---

## [0.3] — 2026-03-29 / 2026-05-19 — Data Quality & Price Normalization

**What:**
- Platinum prices used as-is from CSV (already bundle totals — do not divide)
- Vivino price normalization for packaged offers (magnum ×2, bundles ×qty)
- Per-unit annotations removed (cluttered, misleading)
- Market prices (Wine-Searcher / USD→SGD) disabled — conversion too unreliable for SG
- Gemini grounding fallback disabled — returned hallucinated prices (e.g., $35k)
- Vivino override CSV as locked source of truth for validated prices
- Startup import guarded by freshness check (won't overwrite fresh data with stale seed)
- Data completeness gate: import fails if country/grapes/price coverage drops below threshold
- Quarantine CSV for incomplete imports requiring manual review
- Auto-fill metadata (country, grapes) from API for quarantined wines

**Why:** Price bugs caused user trust issues. Platinum CSV already contains totals; dividing by quantity was double-counting. Gemini grounding invented prices. Market USD conversion added noise for SG buyers.

**Closes:**
- Bundle price double-counting (Platinum ÷ qty was wrong — CSV is already total).
- Gemini hallucinated prices — permanently removed, memory stored in `feedback_no_gemini_fallback.md`.
- Market price noise — disabled until a reliable SGD source exists.
- Stale seed overwrite on deploy — now freshness-guarded.

---

## [0.2] — 2026-03-17/19 — LLM Vivino Matching + Discovery UI

**What:**
- LLM-powered Vivino resolver: Brave Search → Gemini match → identity cache (permanent URL store)
- Vivino price and description ingested from Vivino product pages (JSON-LD)
- Transparent match method exposed in API (`exact`, `fuzzy`, `llm`, `none`)
- Discovery hub: country map with continent colour coding, style browsing, filter drill-down
- Deal score formula: Vivino rating + price gap vs Grand Cru + Vivino market gap
- 7-day and 30-day price history and trend chips

**Why:** Manual Vivino URL matching covered ~40% of wines. LLM + Brave Search gets to ~95%.

**Closes:** Low Vivino coverage. Hidden fallback policy (no silent data substitution — memory in `feedback_no_hidden_fallbacks.md`).

---

## [0.1] — 2026-02-23 — Foundation

**What:**
- FastAPI + SQLAlchemy + Railway deployment scaffold
- Selenium scraper for Platinum Wine Club portal (stock-aware)
- Shopify API integration for Grand Cru catalog
- Bundle normalization: magnum ×2, cases ×qty for apples-to-apples price comparison
- Daily cron refresh pipeline
- Ops endpoints: manual refresh trigger, diagnostics, health check

**Why:** Initial build to compare Platinum Wine Club offers vs Grand Cru retail prices in Singapore.

---

## Known Recurring Issues (read before opening a new fix)

| Area | What happened | Current solution | Don't repeat |
|------|---------------|------------------|--------------|
| Vivino prices | Hallucinated by Gemini; locale confusion (USD vs SGD); magnum vs per-bottle | Locked via `vivino_overrides.csv`; JSON-LD parser with SG locale | Never re-enable Gemini grounding for prices |
| Platinum prices | Was dividing CSV totals by qty (double-counting) | Use CSV price as-is (it's already the bundle total) | Never divide Platinum price from CSV |
| Mobile layout | Fixed 5× — each fix broke another column | Hidden non-essential columns; `mobile-price-strip` as sole price view | Don't add new columns without mobile `display:none` |
| Market prices | USD→SGD conversion noise | Disabled; only show if `price_market` field is populated | Don't re-enable USD conversion without a reliable SGD source |
| Gemini grounding | Returned $35k prices | Permanently removed | See `feedback_no_gemini_fallback.md` |
| Stale seed on deploy | Deploy was overwriting fresh DB with old CSV | Freshness guard on startup import | Startup import is now safe — do not remove freshness check |
