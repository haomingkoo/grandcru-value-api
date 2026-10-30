# MinMax Wine design

Inherits the kooexperience design core (`~/.agent-memory/design/DESIGN-core.md`). This file only adds what is specific to this app.

## Audience and job
Platinum Wine Club buyers in Singapore deciding, bottle by bottle, whether a Platinum price is fair before they spend. The screen is a price sheet: offers and prices come first, everything else supports them.

## Mood
A wine merchant's price list on good paper. Plain, factual, no "intelligence" theatre. Mode: Operate.

## Tokens (see `web/styles.css` `:root`)
- Ground `--bg #faf6f0`, surface `#fffdf9`, table head `#f3ece2`, hairline `#e6dbcd`, control border `#8f7d74` (3:1 on paper).
- Ink `#2a1a1e`; secondary `#6b5b5f` (5.4:1 or better on every surface).
- One accent, wine red `#8e2340`: the Buy button, links, focus ring, the lead number in the summary, the selected country on the origin map. Nothing else.
- Status: good `#2e6b3f` (Platinum cheaper), bad `#a6361b` (Platinum dearer), warn `#8a5a00` (stale, out of stock). Used as text colour only, never as fills or pills.
- Type: Lora 600 for the page title, section titles, wine names and the wordmark. Figtree for everything else. Sizes 13 / 15 / 18 / 24 / 40, plus the hero headline at 50 (34 on phones). Tabular numerals on prices, ratings and scores in tables and axes; stat figures use proportional numerals.
- Radius 6 controls, 10 table and panels. No shadows on content.

## Rules for this app
- Every number on screen comes from the API for the current data: offer counts in the summary from `/deals/stats`, prices, ratings and scores from `/deals`, the check time from `/health`. No hard-coded counts anywhere, including the social card.
- Each row has four columns: the wine; Platinum's price with a price strip (one line, a dot per source: Platinum larger, Grand Cru, Vivino market, each labelled with its price) and one plain line per comparison ("S$18 (13%) more than Grand Cru", "S$22 (12%) below the Vivino market price · checked 26 Oct", or "Not at Grand Cru" / "No Vivino market price yet"); the Vivino rating with a quiet "Score 57 of 100"; and the links. No strip when only Platinum has a price.
- The score has no column and no "good/poor" label: it is mostly the rating when no discount is known, so a label would mislead.
- One primary action per offer: "Buy on Platinum". Grand Cru, Vivino and Wine-Searcher are plain text links on one line.
- No Vivino review text or images. Every Vivino figure has a "Vivino" link to that wine's page, and each Vivino price shows its "Checked" date. A "Market prices paused" notice appears under the summary when `/health` reports `vivino_ok: false`.
- The Wine-Searcher link opens a Singapore search; no Wine-Searcher data is fetched.
- Bottle photos are hotlinked from the shop. A drawn bottle silhouette replaces a missing or broken image.
- Phones (900px and below): each offer becomes a card: the wine, then the price and strip, then Buy and links beside the rating. Sort by the select, not the table headers.
- Split bar (first screen): offers in view against Grand Cru: cheaper on Platinum (claret), same price (control grey), cheaper at Grand Cru (blue), not at Grand Cru (outlined). Its legend buttons filter by that outcome. Counts come from `/deals/stats` with the other filters applied.
- Quick filters sit above the table as toggle buttons (cheaper on Platinum, rated 4.0+, 100+ ratings, listed at Grand Cru). Region, type, grape, format and producer stay under More filters.

## First screen
- Left: the headline, one line under it, the split bar, and two stats that set a filter or link: the lowest price per 750 ml among wines rated 4.0 or higher (with its bottle), and how long since prices were checked. Right (below on tablets and phones): the value map.
- Value map: x is Platinum price per 750 ml on a doubling scale, y is the Vivino rating (3.4 to 4.6, widened if a rating falls outside). Dot size follows the number of Vivino ratings; a hollow dot is a rating shown by Platinum without a count. Wines with no rating sit in a "Not rated yet" strip at their true price, never at a made-up height.
- The frontier is the highest rating at or below each price among the offers in view, drawn as an ink step line; its wines get an ink ring. At most three get a direct label (two on tablets, none on phones), on a light plate, offset with a leader line when needed, and never over another dot or label; the rest show on hover. Filters dim the other dots, and the frontier is redrawn for what is in view.
- Hover or focus previews a wine; click, tap or Enter pins a card with "Show in list", Platinum and Vivino links. Arrow keys move through offers by price. The style legend doubles as a style filter.
- Style colours (validated as a set, all pairs): Red `#8a2846`, White and Orange `#9a7608`, Rosé `#d4587a`, Sparkling and Champagne `#3268b8`. Six styles would fail the all-pairs check, so styles are grouped into four.
- One authored motion: dots settle in price order, then the frontier draws left to right. Nothing moves with reduced motion.
- Price filter: a two-handle slider on a log scale of S$ per 750 ml, exact From and To inputs, and presets. It drives the table and the map together.

## Basket builder
- Purpose: reach a Platinum spend level with as little extra as possible. Modes are thresholds, not caps: "S$300 or more" (S$200 back on the Amex statement), "Over S$350" (also free delivery), "S$600 or more" (the shop's S$50 off, check at checkout), or your own amount.
- The S$200 is always written as money back on the Amex statement after payment, never as a discount, with its conditions in one line and each rule linked to its source (Amex 2026 Platinum terms, Platinum Wine Club terms and home page).
- Up to three baskets: closest to the threshold, most rating for the money, highest rated. Each shows the total, how far over, pay at checkout, money back, cost after the credit, delivery, and the difference against Grand Cru only for wines also sold there. Each wine has one plain line on why it is there and links to its row.
- In-stock wines only, live listed prices only; unrated wines only when the visitor includes them.

## Full list
- The daily scrape keeps every Platinum listing, including sold-out ones. The page shows in-stock offers by default, with "Include sold out (N)" among the quick filters, and the line "Platinum lists 237 wines: 69 in stock, 168 sold out." from the full list.
- A sold-out row is muted (grey Platinum dot, faded bottle). It says "Sold out since <date>" when a daily check saw it in stock before, otherwise "Sold out when we first saw it, <date>", and in both cases that the listed price is shown. Its action is a plain "View on Platinum" button, never "Buy".
- On the value map, sold-out wines appear only when included, as faded dots, with a legend note.

## Price history
- Each offer keeps one price per source per Singapore day (`price_observations`), never pruned. Platinum and Grand Cru come from the daily check; Vivino from the dated page reads (about weekly), drawn as dots on their check dates. A new source (a retailer feed) is a new `source` value and draws as another line with no code change.
- All history is per 750 ml, and the chart says so. Lines are step lines; a missing day is a gap, never interpolated. Points before daily tracking began come from saved price lists (Platinum only, dated by commit) and are drawn hollow.
- Row line: a 90-day sparkline (Platinum solid, Grand Cru dashed) plus one plain sentence ("Up S$3 since 24 Oct", "Same Platinum price at every check since 29 Jul"). Claims about the past need at least 14 days of history; otherwise the row says "Tracked since <date>".
- Series colours: Platinum `--src-platinum #8e2340`, Grand Cru `--src-grand-cru #2f6db5` (dashed), Vivino `--src-vivino #9a7608`. Validated together as a set. A fourth source draws in ink with a dotted line and a direct label rather than a new hue.

## Origin map
"Where the wines come from" sits between the basket and the offers: Leaflet (served from `/static/vendor/leaflet`) on OpenStreetMap tiles, greyed to sit under the paper theme. One bubble per country, sized by offer count, in secondary ink; the selected country is wine red. Country chips under the map do the same as the bubbles, and a picked country lists its regions as chips. The map ignores the country and region filters so every country stays pickable. It loads only when scrolled near; on phones one finger scrolls the page, not the map. The tile host is disclosed on `/privacy`.

## Paging
The offer list shows 5 wines per page by default; readers can choose 10 or 25. Pages count wine families, so a family is never split. Page and size are kept in the URL; any filter or sort change returns to page 1.

## States
Loading: four skeleton rows. Empty after filtering: "No offers match these filters." with Clear all filters. Empty data set: says the list is empty or refreshing. Error: "The offers could not load." with Try again. Stale data: the check time turns warn-coloured and says the daily check is late.
