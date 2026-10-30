# Changelog

All notable changes to grandcru-value-api are recorded here. Format: what changed, why, and what it closes (to prevent re-opening the same fix).

---

## [Unreleased] Valpolicella and Bordeaux blend grapes

**What:** Amarone, Valpolicella and Ripasso listings guess "Corvina Blend" (regional inference, medium), since the appellation rules make Corvina the main grape. Listings that say "Bordeaux Blend" keep that label. Both are red in `GRAPE_COLOURS`, and Corvina has a wine note.

**Why:** On 30 Oct 2026, 29 live offers had no grape. These rules fill 7 of them and change no existing guess.

**Changed files:** `app/wine_metadata.py`, `app/wine_notes.py`, `tests/test_grape_colours.py`, `tests/test_wine_notes.py`

---

## [Unreleased] Glassware leaves the offer list

**What:** The import sets aside any Platinum listing without a wine type, with the reason `not_a_wine`, next to the existing `missing_price_platinum`. The run summary now counts quarantined rows by reason, for example "quarantined 2 rows (not_a_wine=2)".

**Why:** Every Platinum wine listing names its colour. On 30 Oct 2026 the only two listings without one were wine glasses ("Wine Glass - Conterno Symphony" and "Wine Glass - Italesse Masterclass 70 720ml (Bundle of 6)"), and they showed as offers and counted in "237 wines".

**Changed files:** `scripts/import_wine_data.py`, `tests/test_import_wine_data.py`

---

## [Unreleased] Section links land on their section

**What:** `web/anchor.js` holds the section named in the URL hash in place while the page grows, and lets go on the first wheel, touch, pointer or key event. It runs on page load and on every `hashchange`, so it covers every `#section` link, including ones added later.

**Why:** A link such as `/#offers` jumped before the basket, map and offers loaded, and that content then pushed the section down. With the seed data, sections ended up 800 to 1,250 px below the header on desktop and up to 5,650 px on a phone. They now land at the header's scroll padding (72 px). A section near the end of the page stops where the page ends.

**Changed files:** `web/anchor.js`, `web/index.html`, `tests/test_frontend_browser.py`

---

## [Unreleased] Grape guesses match the wine's colour

**What:** `app/wine_metadata.py` now has one colour table, `GRAPE_COLOURS`, for every grape or blend label a guess can return. A grape rule whose grapes do not fit the wine's colour is skipped, and matching moves on to the next rule. Burgundy reds without a named grape guess Pinot Noir and Burgundy whites guess Chardonnay, from the detected region. This replaces the two village lists that ignored colour. The Rhone rules list a white blend first and a Grenache blend second for Cotes du Rhone and Chateauneuf du Pape, so each colour lands on its own rule. An "aligote" rule keeps Aligote out of the Chardonnay fallback. `app/wine_notes.py` reads the same colour table. A test fails when a rule returns a grape with no colour.

**Why:** The guesses tagged red Chassagne-Montrachet and Saint-Romain as Chardonnay, Coudoulet de Beaucastel Rouge as a white Rhone blend, and Silk & Spice White Blend with red grapes. Checking colour inside the guesser fixes this class of error for future rules too. Against the 237 live wines on 30 Oct 2026, exactly these 5 change; the saved grapes update at the next ingest.

**Changed files:** `app/wine_metadata.py`, `app/wine_notes.py`, `tests/test_grape_colours.py`, `tests/test_wine_notes.py`

---

## [Unreleased] Wine notes

**What:** Each offer row shows a one-line wine note under the details line, for example "Nebbiolo. Pale but high in tannin and acidity; Barolo's grape, built to age." `/deals` returns it as `wine_note`. Notes are written per grape in `app/wine_notes.py`, with separate notes for blanc de blancs, blanc de noirs, the Champagne blend and Prosecco. A guessed grape starts with "Likely". Blends list their grapes without a note. There is no line for low-confidence guesses, catch-all labels such as "Red Blend", or a grape that does not fit the wine's colour. On 30 Oct 2026, 170 of 237 live offers got a line.

**Why:** Shoppers asked for a short description of each wine, and the row never showed the grape. Notes describe the grape in general, so nothing is copied from Vivino reviews or retailer text.

**Changed files:** `app/wine_notes.py`, `app/service.py`, `app/schemas.py`, `web/app.js`, `web/styles.css`, `tests/test_wine_notes.py`, `tests/test_frontend_browser.py`, `DESIGN.md`

---

## [Unreleased] Origin map returns

**What:** Brought back the world map from before the offers-first redesign as "Where the wines come from", between the basket and the offers, with a "Map" link in the header. It shows one bubble per country from `/deals/map`, sized by offer count. A bubble or a country chip filters the offer list to that country and lists its regions as chips, and a region chip narrows further. Leaflet 1.9.4 is served from `/static/vendor/leaflet` (checked against the official SRI hashes), tiles come from OpenStreetMap, and the map loads only when scrolled near. CSP `img-src` allows `tile.openstreetmap.org`, and `/privacy` says the map fetches tiles from OpenStreetMap. Tile seams use the half-pixel overlap from #60 with Leaflet's `plus-lighter` blend turned off, which otherwise drew white lines.

**Why:** The map was a popular way to browse and was removed in #64. Exploring by country and region gives another way into the list besides search, filters and the price chart.

**Changed files:** `web/originmap.js`, `web/vendor/leaflet/*`, `web/index.html`, `web/app.js`, `web/styles.css`, `web/privacy.html`, `app/main.py`, `tests/test_frontend_browser.py`, `tests/test_seo_security.py`, `DESIGN.md`

---

## [Unreleased] Basket promotions check themselves

**What:** Each basket rule carries `checked_on` and `ends_on`. The daily and weekly ingest now read the Platinum Wine Club home page (`scripts/check_promos.py`) and store which promotion lines are still there in `app_state`. The shop's S$50 off turns off when its banner goes. The Amex S$200 credit turns off after its end date (31 Dec 2026); a missing home-page line for it only warns, since Amex's terms decide it. A failed read keeps the last good reading. `/health` gains `promo_ok`, `promo_status` and `promo_checked_at`, and warns 30 days before a rule ends. `/basket` returns each rule's `active` and `note`, refuses a spend level whose rule has ended, and stops counting the credit or promo in the totals; `GET /basket/rules` serves the same states. The page greys out ended spend levels with the reason, hides the "S$200 back" button and rewrites the basket intro when the credit ends. The promo text now mentions the free product the cart lists at S$600.

**Why:** The S$50 promo and the Amex credit were typed into `app/basket.py` by hand, so the page would keep promising them after they ended.

**Changed files:** `app/promos.py`, `app/basket.py`, `app/main.py`, `app/schemas.py`, `scripts/check_promos.py`, `scripts/refresh_pipeline.py`, `web/basket.js`, `web/styles.css`, `tests/test_promos.py`, `CLAUDE.md`

---

## [Unreleased] Offer list pages

**What:** The offer list shows 5 wines per page, with 10 or 25 in a "Per page" picker, Previous and Next buttons, and a "1–5 of N wines" line. Pages split wine families, never single offers. The page and page size live in the URL (`page`, `per_page`), changing a filter or sort returns to page 1, and "Show in list" from the value map, stat strip or basket opens the page holding that offer. Browser tests now get a fresh rate limiter each, so more of them can run without throttling each other.

**Why:** All live offers rendered at once, which made the list too long to scan.

**Changed files:** `web/app.js`, `web/index.html`, `web/styles.css`, `tests/test_frontend_browser.py`, `DESIGN.md`

---

## [Unreleased] Design system: offers first

**What:** Rebuilt the page as one price sheet. The offer list sits right under a one-sentence summary counted from live data. Removed the world map, the readout circles, the price-position chart, the best-value, regions and styles sections, eyebrow labels and most pills. Light paper theme with one wine-red accent, Lora and Figtree self-hosted (Oswald retired). Each offer shows Platinum, Grand Cru and Vivino prices with the difference written out, one Buy button and plain links. Designed loading, empty, filtered-empty and error states, plus a bottle silhouette when a photo fails. Added a favicon, a PNG social card, `/privacy` and `/terms`, the shared footer and `DESIGN.md`. CSP no longer allows unpkg or OpenStreetMap tiles.

**Why:** The answer (is this bottle a good price?) was buried under decoration about 870px down on desktop and much further down on phones. Coral labels failed AA contrast, and there was no favicon, privacy page or usable social card.

**Changed files:** `web/*`, `app/main.py`, `tests/test_frontend_browser.py`, `tests/test_seo_security.py`, `DESIGN.md`

**Closes:** Design audit 2026-10-26 section 3.3 (wine).

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
