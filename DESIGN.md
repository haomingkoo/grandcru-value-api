# MinMax Wine design

Inherits the kooexperience design core (`~/.agent-memory/design/DESIGN-core.md`). This file only adds what is specific to this app.

## Audience and job
Platinum Wine Club buyers in Singapore deciding, bottle by bottle, whether a Platinum price is fair before they spend. The screen is a price sheet: offers and prices come first, everything else supports them.

## Mood
A wine merchant's price list on good paper. Plain, factual, no "intelligence" theatre. Mode: Operate.

## Tokens (see `web/styles.css` `:root`)
- Ground `--bg #faf6f0`, surface `#fffdf9`, table head `#f3ece2`, hairline `#e6dbcd`, control border `#8f7d74` (3:1 on paper).
- Ink `#2a1a1e`; secondary `#6b5b5f` (5.4:1 or better on every surface).
- One accent, wine red `#8e2340`: the Buy button, links, focus ring, the lead number in the summary. Nothing else.
- Status: good `#2e6b3f` (Platinum cheaper), bad `#a6361b` (Platinum dearer), warn `#8a5a00` (stale, out of stock). Used as text colour only, never as fills or pills.
- Type: Lora 600 for the page title, section titles, wine names and the wordmark. Figtree for everything else. Sizes 13 / 15 / 18 / 24 / 40 (32 on phones). Tabular numerals on prices, ratings and scores.
- Radius 6 controls, 10 table and panels. No shadows on content.

## Rules for this app
- Every number on screen comes from the API for the current data: offer counts in the summary from `/deals/stats`, prices, ratings and scores from `/deals`, the check time from `/health`. No hard-coded counts anywhere, including the social card.
- Price differences are written as sentences ("Platinum 12.0% less"), not badges. A 7-day trend shows only when the price moved; jumps far larger than the price itself are data corrections and stay hidden.
- One primary action per offer: "Buy on Platinum". Grand Cru and Vivino are plain text links.
- No Vivino review text or images. Every Vivino figure has a "Vivino" link to that wine's page, and each Vivino price shows its "Checked" date. A "Market prices paused" notice appears under the summary when `/health` reports `vivino_ok: false`.
- "Compare Singapore prices on Wine-Searcher" is a quiet secondary text link (no Wine-Searcher data is fetched).
- Bottle photos are hotlinked from the shop. A drawn bottle silhouette replaces a missing or broken image.
- Phones (900px and below): each offer becomes a card with Platinum, Grand Cru and Vivino side by side above the Buy button. Sort by the select, not the table headers.

## Price history
- Each offer keeps one price per source per Singapore day (`price_observations`), never pruned. Platinum and Grand Cru come from the daily check; Vivino from the dated page reads (about weekly), drawn as dots on their check dates. A new source (a retailer feed) is a new `source` value and draws as another line with no code change.
- All history is per 750 ml, and the chart says so. Lines are step lines; a missing day is a gap, never interpolated. Points before daily tracking began come from saved price lists (Platinum only, dated by commit) and are drawn hollow.
- Row line: a 90-day sparkline (Platinum solid, Grand Cru dashed) plus one plain sentence ("Up S$3 since 24 Oct", "Same Platinum price at every check since 29 Jul"). Claims about the past need at least 14 days of history; otherwise the row says "Tracked since <date>".
- Series colours: Platinum `--src-platinum #8e2340`, Grand Cru `--src-grand-cru #2f6db5` (dashed), Vivino `--src-vivino #9a7608`. Validated together as a set. A fourth source draws in ink with a dotted line and a direct label rather than a new hue.

## States
Loading: four skeleton rows. Empty after filtering: "No offers match these filters." with Clear all filters. Empty data set: says the list is empty or refreshing. Error: "The offers could not load." with Try again. Stale data: the check time turns warn-coloured and says the daily check is late.
