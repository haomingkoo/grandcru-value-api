const defaultState = {
  search: "",
  sortBy: "deal_score",
  sortOrder: "desc",
  country: "",
  region: "",
  wineType: "",
  styleFamily: "",
  grape: "",
  offeringType: "",
  producer: "",
  comparableOnly: false,
  onlyPlatinumCheaper: false,
  minVivinoRating: "",
  minVivinoNumRatings: "",
}

const sortDefaults = {
  wine_name: "asc",
  price_platinum: "asc",
  price_per_750: "asc",
  price_diff_pct: "asc",
  vivino_rating: "desc",
  deal_score: "desc",
}

const sortLabels = {
  "deal_score:desc": "best score first",
  "price_diff_pct:asc": "biggest saving vs Grand Cru first",
  "price_diff_pct:desc": "Grand Cru cheapest first",
  "vivino_rating:desc": "highest rating first",
  "vivino_rating:asc": "lowest rating first",
  "price_platinum:asc": "lowest price first",
  "price_platinum:desc": "highest price first",
  "price_per_750:asc": "best price per 750 ml first",
  "price_per_750:desc": "highest price per 750 ml first",
  "wine_name:asc": "name, A to Z",
  "wine_name:desc": "name, Z to A",
  "deal_score:asc": "lowest score first",
}

const STYLE_ORDER = ["Red", "White", "Champagne", "Sparkling", "Rose", "Sweet / Dessert", "Orange"]

const state = { ...defaultState }
let filterOptions = null
let priceHistory = null
let requestSerial = 0
let searchDebounce = 0
const els = {}

document.addEventListener("DOMContentLoaded", async () => {
  captureElements()
  hydrateStateFromUrl()
  bindEvents()
  renderLoadingRows()
  loadSummary()
  loadFreshness()
  loadPriceHistory()
  try {
    await loadFilterOptions()
  } catch (_) {
    // The offer list shows the error state; filters stay empty.
  }
  syncControlsFromState()
  await loadOffers()
})

function captureElements() {
  for (const id of [
    "searchInput", "sortSelect", "countrySelect", "regionSelect", "wineTypeSelect",
    "styleFamilySelect", "grapeSelect", "offeringSelect", "producerSelect",
    "comparableOnlyToggle", "platinumOnlyToggle", "ratingToggle", "confidenceToggle",
    "resultsMeta", "activeFilters", "dealTableBody", "tableState", "summary", "freshness", "marketNotice",
  ]) {
    els[id] = document.getElementById(id)
  }
  els.sortButtons = Array.from(document.querySelectorAll(".sort-button"))
}

function bindEvents() {
  document.getElementById("filtersForm").addEventListener("submit", (event) => event.preventDefault())

  els.searchInput.addEventListener("input", (event) => {
    window.clearTimeout(searchDebounce)
    searchDebounce = window.setTimeout(() => {
      state.search = event.target.value.trim()
      loadOffers()
    }, 220)
  })

  els.sortSelect.addEventListener("change", (event) => {
    const [sortBy, sortOrder] = event.target.value.split(":")
    state.sortBy = sortBy
    state.sortOrder = sortOrder
    syncSortButtons()
    loadOffers()
  })

  const selectBindings = [
    ["countrySelect", "country", () => { state.region = "" }],
    ["regionSelect", "region"],
    ["wineTypeSelect", "wineType"],
    ["styleFamilySelect", "styleFamily"],
    ["grapeSelect", "grape"],
    ["offeringSelect", "offeringType"],
    ["producerSelect", "producer"],
  ]
  selectBindings.forEach(([elementId, key, after]) => {
    els[elementId].addEventListener("change", (event) => {
      state[key] = event.target.value
      if (after) after()
      syncControlsFromState()
      loadOffers()
    })
  })

  const toggleBindings = [
    ["comparableOnlyToggle", (checked) => { state.comparableOnly = checked }],
    ["platinumOnlyToggle", (checked) => { state.onlyPlatinumCheaper = checked }],
    ["ratingToggle", (checked) => { state.minVivinoRating = checked ? "4.0" : "" }],
    ["confidenceToggle", (checked) => { state.minVivinoNumRatings = checked ? "100" : "" }],
  ]
  toggleBindings.forEach(([elementId, apply]) => {
    els[elementId].addEventListener("change", (event) => {
      apply(event.target.checked)
      loadOffers()
    })
  })

  els.activeFilters.addEventListener("click", (event) => {
    const button = event.target.closest("[data-reset]")
    if (!button) return
    resetFilter(button.dataset.reset)
    syncControlsFromState()
    loadOffers()
  })

  els.tableState.addEventListener("click", (event) => {
    if (event.target.closest("[data-action='clear']")) {
      resetFilter("all")
      syncControlsFromState()
      loadOffers()
    }
    if (event.target.closest("[data-action='retry']")) {
      window.location.reload()
    }
  })

  els.dealTableBody.addEventListener("click", (event) => {
    const toggle = event.target.closest(".history-toggle")
    if (toggle) toggleHistory(toggle)
  })

  els.sortButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const sortBy = button.dataset.sort
      if (state.sortBy === sortBy) {
        state.sortOrder = state.sortOrder === "desc" ? "asc" : "desc"
      } else {
        state.sortBy = sortBy
        state.sortOrder = sortDefaults[sortBy] || "desc"
      }
      syncControlsFromState()
      loadOffers()
    })
  })
}

async function loadSummary() {
  try {
    const stats = await fetchJson("/deals/stats")
    const counts = Object.fromEntries((stats.cheaper_sides || []).map((item) => [item.value, item.count]))
    const total = stats.total_deals || 0
    if (!total) {
      els.summary.textContent = "There are no Platinum Wine Club offers to compare right now."
      return
    }
    const cheaperPlatinum = counts["Platinum Cheaper"] || 0
    const cheaperGrandCru = counts["Grand Cru Cheaper"] || 0
    const same = counts["Same Price"] || 0
    const unlisted = counts["No Match"] || 0
    const withMarket = stats.with_market_price || 0
    const comparisons = [
      `${formatInteger(cheaperPlatinum)} ${plural(cheaperPlatinum, "is", "are")} cheaper on Platinum than at Grand Cru`,
      cheaperGrandCru ? `${formatInteger(cheaperGrandCru)} ${plural(cheaperGrandCru, "is", "are")} cheaper at Grand Cru` : "",
      same ? `${formatInteger(same)} ${plural(same, "costs", "cost")} the same` : "",
      unlisted ? `${formatInteger(unlisted)} ${plural(unlisted, "is", "are")} not sold at Grand Cru` : "",
    ].filter(Boolean)
    els.summary.innerHTML = [
      `Platinum Wine Club lists <strong>${formatInteger(total)} ${plural(total, "offer", "offers")}</strong> in stock today.`,
      `${capitalize(joinList(comparisons))}.`,
      `${formatInteger(withMarket)} of ${formatInteger(total)} ${plural(withMarket, "has", "have")} a Vivino market price to compare.`,
    ].join(" ")
  } catch (_) {
    els.summary.textContent = "Today’s price summary could not load."
  }
}

async function loadFreshness() {
  try {
    const data = await fetchJson("/health")
    const finished = data.latest_ingestion?.finished_at
    if (!finished) return
    const ago = formatTimeAgo(new Date(finished))
    els.freshness.textContent = data.ingestion_stale
      ? `Prices in Singapore dollars, last checked ${ago}. Today’s check is late, so some prices may have changed.`
      : `Prices in Singapore dollars, checked ${ago}.`
    els.freshness.classList.toggle("is-stale", Boolean(data.ingestion_stale))
    els.freshness.hidden = false
    if (data.vivino_ok === false) {
      els.marketNotice.textContent = "Market prices paused. Vivino prices are not being updated right now, so each one shows the date it was last checked."
      els.marketNotice.hidden = false
    }
  } catch (_) {
    els.freshness.hidden = true
  }
}

async function loadPriceHistory() {
  try {
    const data = await fetchJson("/deals/price-history")
    priceHistory = { today: data.today, firstDate: data.first_date, byId: new Map(data.offers.map((offer) => [offer.deal_id, offer])) }
    fillHistorySlots()
  } catch (_) {
    // Rows simply show no history line.
  }
}

function fillHistorySlots() {
  if (!priceHistory) return
  els.dealTableBody.querySelectorAll(".history-slot:empty").forEach((slot) => {
    const offer = priceHistory.byId.get(Number(slot.dataset.dealId))
    if (offer && offer.sources.length) slot.innerHTML = historyLineHtml(offer)
  })
}

function historyLineHtml(offer) {
  const spark = WineCharts.sparkline(offer, priceHistory.today)
  const verdict = offer.verdicts.find((v) => !v.kind.startsWith("cross"))
  const text = verdict
    ? `<span class="history-verdict tone-${escapeHtml(verdict.tone)}">${escapeHtml(verdict.text)}</span>`
    : `<span class="history-verdict tone-calm">Tracked since ${escapeHtml(formatDay(offer.first_date))}</span>`
  return `
    <div class="history-line">
      ${spark}
      ${text}
      <button class="text-button history-toggle" type="button" aria-expanded="false" aria-controls="history-${offer.deal_id}">Price history</button>
    </div>
  `
}

function toggleHistory(button) {
  const row = document.getElementById(button.getAttribute("aria-controls"))
  if (!row) return
  const open = row.hidden
  row.hidden = !open
  button.setAttribute("aria-expanded", String(open))
  button.textContent = open ? "Hide price history" : "Price history"
  if (open && !row.dataset.drawn) {
    const offer = priceHistory.byId.get(Number(row.dataset.dealId))
    const deal = JSON.parse(row.dataset.deal)
    row.querySelector(".history-panel").innerHTML = historyPanelHtml(offer, deal)
    WineCharts.history(row.querySelector(".history-chart"), offer, priceHistory.today)
    row.dataset.drawn = "true"
  }
}

const SOURCE_NOTES = { vivino: "dated checks" }

function historyPanelHtml(offer, deal) {
  const sizeNote = isStandardBottle(deal)
    ? ""
    : ` This offer is ${escapeHtml(formatVolume(deal))} at ${escapeHtml(formatMoney(deal.price_platinum))}.`
  const verdicts = offer.verdicts.length
    ? `<ul class="history-verdicts">${offer.verdicts.map((v) => `<li class="tone-${escapeHtml(v.tone)}">${escapeHtml(v.text)}</li>`).join("")}</ul>`
    : ""
  const legend = offer.sources.map((s) => `
    <li class="legend-item legend-${escapeHtml(s.source)}"><span class="legend-mark" aria-hidden="true"></span>${escapeHtml(s.label)}${SOURCE_NOTES[s.source] ? `<span class="sub"> ${SOURCE_NOTES[s.source]}</span>` : ""}</li>
  `).join("")
  const daily = offer.sources.flatMap((s) => (s.segments || []).filter((g) => g.origin !== "saved_list").map((g) => g.start)).sort()
  const notes = ["A break in a line is a day we have no price from that shop."]
  if (offer.saved_list_dates.length) {
    notes.push(`Hollow dots${daily.length ? ` before ${escapeHtml(formatDay(daily[0]))}` : ""} come from saved price lists, dated the day each list was saved.`)
  }
  if (offer.sources.some((s) => s.source === "vivino")) notes.push("Vivino dots are the days we read the wine’s Vivino page.")
  return `
    <div class="history-head">
      <h3 class="history-title">Price history</h3>
      <p class="sub">Prices per 750 ml.${sizeNote} Tracking began ${escapeHtml(formatDay(offer.first_date, true))}.</p>
    </div>
    ${verdicts}
    <ul class="history-legend">${legend}</ul>
    <div class="history-chart"></div>
    <p class="history-notes">${notes.join(" ")}</p>
  `
}

async function loadFilterOptions() {
  filterOptions = await fetchJson("/deals/filters")
  renderSelectOptions()
}

async function loadOffers() {
  const requestId = ++requestSerial
  writeStateToUrl()
  document.querySelector(".table-wrap").setAttribute("aria-busy", "true")

  try {
    const deals = await fetchJson(`/deals?${buildParams()}`)
    if (requestId !== requestSerial) return
    const families = groupDealsForShelf(deals)
    renderResultsMeta(deals, families)
    renderTable(families)
    renderActiveFilters()
  } catch (error) {
    if (requestId !== requestSerial) return
    console.error(error)
    renderErrorState()
  } finally {
    if (requestId === requestSerial) {
      document.querySelector(".table-wrap").setAttribute("aria-busy", "false")
    }
  }
}

function buildParams() {
  const params = filterParams()
  params.set("sort_by", state.sortBy)
  params.set("sort_order", state.sortOrder)
  params.set("limit", "500")
  return params.toString()
}

function filterParams() {
  const params = new URLSearchParams()
  if (state.search) params.set("search", state.search)
  if (state.country) params.set("country", state.country)
  if (state.region) params.set("region", state.region)
  if (state.wineType) params.set("wine_type", state.wineType)
  if (state.styleFamily) params.set("style_family", state.styleFamily)
  if (state.grape) params.set("grape", state.grape)
  if (state.offeringType) params.set("offering_type", state.offeringType)
  if (state.producer) params.set("producer", state.producer)
  if (state.comparableOnly) params.set("comparable_only", "true")
  if (state.onlyPlatinumCheaper) params.set("only_platinum_cheaper", "true")
  if (state.minVivinoRating) params.set("min_vivino_rating", state.minVivinoRating)
  if (state.minVivinoNumRatings) params.set("min_vivino_num_ratings", state.minVivinoNumRatings)
  return params
}

function hydrateStateFromUrl() {
  const params = new URLSearchParams(window.location.search)
  state.search = params.get("search") || ""
  state.country = params.get("country") || ""
  state.region = params.get("region") || ""
  state.wineType = params.get("wine_type") || ""
  state.styleFamily = params.get("style_family") || ""
  state.grape = params.get("grape") || ""
  state.offeringType = params.get("offering_type") || ""
  state.producer = params.get("producer") || ""
  state.sortBy = params.get("sort_by") || defaultState.sortBy
  state.sortOrder = params.get("sort_order") || defaultState.sortOrder
  state.comparableOnly = params.get("comparable_only") === "true"
  state.onlyPlatinumCheaper = params.get("only_platinum_cheaper") === "true"
  state.minVivinoRating = params.get("min_vivino_rating") || ""
  state.minVivinoNumRatings = params.get("min_vivino_num_ratings") || ""
}

function writeStateToUrl() {
  const params = filterParams()
  if (state.sortBy !== defaultState.sortBy) params.set("sort_by", state.sortBy)
  if (state.sortOrder !== defaultState.sortOrder) params.set("sort_order", state.sortOrder)
  const query = params.toString()
  window.history.replaceState({}, "", `${window.location.pathname}${query ? `?${query}` : ""}${window.location.hash}`)
}

function renderSelectOptions() {
  if (!filterOptions) return
  const styles = (filterOptions.style_families || []).slice().sort((a, b) => styleRank(a.value) - styleRank(b.value) || a.value.localeCompare(b.value))
  populateSelect(els.countrySelect, filterOptions.countries, state.country, "All countries")
  populateSelect(els.styleFamilySelect, styles, state.styleFamily, "All styles")
  populateSelect(els.regionSelect, filterOptions.regions, state.region, "All regions")
  populateSelect(els.wineTypeSelect, filterOptions.wine_types, state.wineType, "All types")
  populateSelect(els.grapeSelect, filterOptions.grapes, state.grape, "All grapes")
  populateSelect(els.offeringSelect, filterOptions.offering_types, state.offeringType, "All formats")
  populateSelect(els.producerSelect, filterOptions.producers, state.producer, "All producers")
}

function styleRank(value) {
  const index = STYLE_ORDER.indexOf(value)
  return index === -1 ? STYLE_ORDER.length : index
}

function populateSelect(select, options, selectedValue, placeholder) {
  const items = Array.isArray(options) ? options.slice() : []
  if (selectedValue && !items.some((item) => item.value === selectedValue)) {
    items.unshift({ value: selectedValue, count: 0 })
  }
  select.innerHTML = [
    `<option value="">${escapeHtml(placeholder)}</option>`,
    ...items.map((item) => {
      const label = item.count ? `${item.value} (${item.count})` : item.value
      return `<option value="${escapeHtml(item.value)}">${escapeHtml(label)}</option>`
    }),
  ].join("")
  select.value = selectedValue || ""
}

function syncControlsFromState() {
  els.searchInput.value = state.search
  syncSortSelect()
  els.countrySelect.value = state.country
  els.regionSelect.value = state.region
  els.wineTypeSelect.value = state.wineType
  els.styleFamilySelect.value = state.styleFamily
  els.grapeSelect.value = state.grape
  els.offeringSelect.value = state.offeringType
  els.producerSelect.value = state.producer
  els.comparableOnlyToggle.checked = state.comparableOnly
  els.platinumOnlyToggle.checked = state.onlyPlatinumCheaper
  els.ratingToggle.checked = Number(state.minVivinoRating || 0) >= 4
  els.confidenceToggle.checked = Number(state.minVivinoNumRatings || 0) >= 100
  syncSortButtons()
}

function syncSortSelect() {
  const value = `${state.sortBy}:${state.sortOrder}`
  if (!Array.from(els.sortSelect.options).some((option) => option.value === value)) {
    const option = document.createElement("option")
    option.value = value
    option.textContent = capitalize(sortLabels[value] || "Custom order")
    els.sortSelect.appendChild(option)
  }
  els.sortSelect.value = value
}

function syncSortButtons() {
  els.sortButtons.forEach((button) => {
    const active = button.dataset.sort === state.sortBy
    button.classList.toggle("is-active", active)
    button.dataset.dir = active ? state.sortOrder : ""
    button.closest("th").setAttribute("aria-sort", active ? (state.sortOrder === "asc" ? "ascending" : "descending") : "none")
  })
}

function renderResultsMeta(deals, families) {
  const order = sortLabels[`${state.sortBy}:${state.sortOrder}`] || "custom order"
  const wineCopy = families.length === deals.length
    ? `${formatInteger(deals.length)} ${plural(deals.length, "offer", "offers")}`
    : `${formatInteger(families.length)} wine groups, ${formatInteger(deals.length)} live offers`
  els.resultsMeta.textContent = deals.length ? `${wineCopy}, ${order}` : ""
}

function renderLoadingRows() {
  els.dealTableBody.innerHTML = Array.from({ length: 4 }, () => `
    <tr class="skeleton-row" aria-hidden="true">
      <td colspan="7"><div class="skeleton"><span class="skeleton-block"></span><span class="skeleton-line"></span><span class="skeleton-line short"></span></div></td>
    </tr>
  `).join("")
  els.resultsMeta.textContent = "Loading offers…"
}

function showTableState(html) {
  els.dealTableBody.innerHTML = ""
  els.tableState.innerHTML = html
  els.tableState.hidden = false
  document.querySelector(".table-wrap").hidden = true
}

function renderTable(families) {
  els.tableState.hidden = true
  document.querySelector(".table-wrap").hidden = false
  if (!families.length) {
    showTableState(hasActiveFilters()
      ? `<p class="state-title">No offers match these filters.</p>
         <p>Try a different search, or remove a filter.</p>
         <button class="button" type="button" data-action="clear">Clear all filters</button>`
      : `<p class="state-title">No offers to show right now.</p>
         <p>The Platinum Wine Club list is empty or being refreshed. Check back later today.</p>`)
    return
  }
  els.dealTableBody.innerHTML = families.map(renderFamily).join("")
  fillHistorySlots()
}

function renderErrorState() {
  els.resultsMeta.textContent = ""
  showTableState(`
    <p class="state-title">The offers could not load.</p>
    <p>Check your connection, then try again.</p>
    <button class="button" type="button" data-action="retry">Try again</button>
  `)
}

function renderFamily(family) {
  const row = renderOfferRow(family.bestOffer, family) + renderHistoryRow(family.bestOffer)
  return family.offers.length > 1 ? `${row}${renderGroupedOffersRow(family)}` : row
}

function renderHistoryRow(deal) {
  const keep = { price_platinum: deal.price_platinum, volume: deal.volume, quantity: deal.quantity, volume_ml: deal.volume_ml }
  return `
    <tr class="history-row" id="history-${deal.id}" data-deal-id="${deal.id}" data-deal="${escapeHtml(JSON.stringify(keep))}" hidden>
      <td colspan="7"><div class="history-panel"></div></td>
    </tr>
  `
}

function displayName(deal) {
  return deal.wine_name
    ? deal.wine_name.replace(/ - (Red|White|Rose|Rosé|Sparkling|Orange)( - .+)?$/, "")
    : deal.label_name || "Unnamed wine"
}

function renderOfferRow(deal, family) {
  const verdict = resolveVerdict(deal)
  const style = deal.style_family || deal.wine_type || ""
  const origin = deal.country ? [deal.region, deal.country].filter(Boolean).join(", ") : "Country not listed"
  const meta = [deal.producer, origin, style, formatVolume(deal)].filter(Boolean).join(" · ")
  const grouped = family && family.offers.length > 1
    ? `<p class="group-summary-line">${formatInteger(family.offers.length)} offers for this wine${family.vintageLabel ? ` (${escapeHtml(family.vintageLabel)})` : ""}</p>`
    : ""

  return `
    <tr class="deal-row">
      <td class="cell-wine">
        <div class="wine-cell">
          ${wineImageHtml(deal)}
          <div class="wine-main">
            <p class="wine-title">${escapeHtml(displayName(deal))}</p>
            <p class="wine-meta">${escapeHtml(meta)}</p>
            <p class="verdict tone-${escapeHtml(verdict.tone)}">${escapeHtml(verdict.label)}</p>
            ${grouped}
            <div class="history-slot" data-deal-id="${deal.id}"></div>
          </div>
        </div>
      </td>
      <td class="num cell-price">
        <span class="cell-label">Platinum</span>
        <span class="price">${formatMoney(deal.price_platinum)}</span>
        ${sizeNotes(deal)}
        ${deal.platinum_in_stock === false ? `<span class="sub tone-warn">Out of stock</span>` : ""}
        ${trendNote(deal.price_platinum_change_7d, deal.platinum_trend_7d, deal.price_platinum)}
      </td>
      <td class="num cell-price">
        <span class="cell-label">Grand Cru</span>
        ${grandCruCell(deal)}
      </td>
      <td class="num cell-price">
        <span class="cell-label">Vivino</span>
        ${vivinoCell(deal)}
      </td>
      <td class="num cell-rating">
        <span class="cell-label">Rating</span>
        ${deal.vivino_rating != null
          ? `<span class="figure">${deal.vivino_rating.toFixed(1)}</span>${deal.vivino_num_ratings ? `<span class="sub">${formatInteger(deal.vivino_num_ratings)} ratings</span>` : deal.vivino_match_method === "platinum" ? `<span class="sub">As shown on Platinum</span>` : ""}${vivinoSource(deal)}`
          : `<span class="sub">No rating</span>`}
      </td>
      <td class="num cell-score">
        <span class="cell-label">Score</span>
        ${deal.deal_score != null
          ? `<span class="figure">${Math.round(deal.deal_score)}</span><span class="sub">of 100</span>`
          : `<span class="sub">Not enough data to score</span>`}
      </td>
      <td class="cell-links">
        <div class="wine-links">
          ${deal.platinum_url ? `<a class="button button-primary" href="${escapeHtml(deal.platinum_url)}" target="_blank" rel="noopener noreferrer">Buy on Platinum</a>` : ""}
          <span class="compare-links">
            ${textLink(deal.grand_cru_url, "Grand Cru")}
            ${textLink(deal.vivino_url, "Vivino")}
          </span>
          ${quietLink(wineSearcherUrl(deal), "Compare Singapore prices on Wine-Searcher")}
        </div>
      </td>
    </tr>
  `
}

function grandCruCell(deal) {
  if (deal.price_grand_cru == null) {
    return `<span class="sub">Not sold at Grand Cru</span>`
  }
  const pct = deal.price_diff_pct_abs != null ? deal.price_diff_pct_abs : Math.abs(deal.price_diff_pct || 0)
  let diff = `<span class="sub">Same price</span>`
  if (deal.cheaper_side === "Platinum Cheaper") diff = `<span class="sub tone-good">Platinum ${formatPct(pct)} less</span>`
  if (deal.cheaper_side === "Grand Cru Cheaper") diff = `<span class="sub tone-bad">Platinum ${formatPct(pct)} more</span>`
  return `
    <span class="price">${formatMoney(deal.price_grand_cru)}</span>
    ${diff}
    ${deal.grand_cru_in_stock === false ? `<span class="sub">Out of stock there</span>` : ""}
    ${trendNote(deal.price_grand_cru_change_7d, deal.grand_cru_trend_7d, deal.price_grand_cru)}
  `
}

function vivinoCell(deal) {
  const lines = []
  if (deal.vivino_price != null && deal.vivino_price > 0) {
    lines.push(`<span class="price" title="Vivino price for the same number of bottles">${formatMoney(deal.vivino_price)}</span>`)
    const gap = marketGapPct(deal)
    if (gap != null) {
      if (Math.abs(gap) < 1) lines.push(`<span class="sub">Same as Platinum</span>`)
      else if (gap < 0) lines.push(`<span class="sub tone-good">Platinum ${formatPct(Math.abs(gap), 0)} less</span>`)
      else lines.push(`<span class="sub tone-bad">Platinum ${formatPct(gap, 0)} more</span>`)
    }
    if (deal.vivino_checked_at) lines.push(`<span class="sub">Checked ${formatDay(deal.vivino_checked_at)}</span>`)
    lines.push(vivinoSource(deal))
  } else if (!deal.vivino_url && deal.vivino_rating == null) {
    lines.push(`<span class="sub">No Vivino match</span>`)
  } else {
    lines.push(`<span class="sub">No market price</span>`)
  }
  if (deal.price_market != null) {
    lines.push(`<span class="sub" title="Wine-Searcher average for the same number of bottles">Wine-Searcher ${formatMoney(deal.price_market)}</span>`)
  }
  return lines.join("")
}

function renderGroupedOffersRow(family) {
  const variants = family.offers.slice(1).map((offer) => `
    <li class="variant">
      <span class="variant-name">${escapeHtml(displayName(offer))}<span class="sub">${escapeHtml(volumeQuantityCopy(offer))}</span></span>
      <span class="variant-prices">
        <span>Platinum <strong class="price">${formatMoney(offer.price_platinum)}</strong></span>
        ${offer.price_grand_cru != null ? `<span>Grand Cru ${formatMoney(offer.price_grand_cru)}</span>` : ""}
      </span>
      ${offer.platinum_url ? `<a class="text-link" href="${escapeHtml(offer.platinum_url)}" target="_blank" rel="noopener noreferrer">Buy on Platinum</a>` : ""}
    </li>
  `).join("")
  const others = family.offers.length - 1

  return `
    <tr class="deal-group-row">
      <td colspan="7">
        <details class="offer-group">
          <summary>${formatInteger(others)} more ${plural(others, "offer", "offers")} for ${escapeHtml(family.title)}${family.priceBand ? `, ${escapeHtml(family.priceBand)}` : ""}</summary>
          <ul class="variant-list">${variants}</ul>
        </details>
      </td>
    </tr>
  `
}

function groupDealsForShelf(deals) {
  const families = new Map()
  deals.forEach((deal) => {
    const descriptor = describeWineFamily(deal)
    if (!families.has(descriptor.key)) families.set(descriptor.key, { ...descriptor, offers: [] })
    families.get(descriptor.key).offers.push(deal)
  })
  return Array.from(families.values()).map((family) => {
    const prices = family.offers.map((offer) => offer.price_platinum).filter((value) => typeof value === "number")
    const vintages = Array.from(new Set(family.offers.map((offer) => formatVintage(offer.vintage)))).filter(Boolean)
    const low = Math.min(...prices)
    const high = Math.max(...prices)
    return {
      ...family,
      bestOffer: family.offers[0],
      priceBand: prices.length ? `${formatMoney(low)}${low !== high ? ` to ${formatMoney(high)}` : ""}` : null,
      vintageLabel: vintages.join(", "),
    }
  })
}

function describeWineFamily(deal) {
  const producer = (deal.producer || "").trim()
  const labelName = (deal.label_name || fallbackLabelName(deal)).trim()
  const title = [producer, labelName].filter(Boolean).join(" ").trim() || deal.wine_name
  const key = [producer, labelName, deal.region || "", deal.country || ""].map((part) => part.toLowerCase()).join("|")
  return { key, title }
}

function fallbackLabelName(deal) {
  const parts = String(deal.wine_name || "").split(" - ").map((part) => part.trim()).filter(Boolean)
  const colorIndex = parts.findIndex((part) => ["red", "white", "rose", "orange"].includes(part.toLowerCase()))
  const body = colorIndex >= 0 ? parts.slice(0, colorIndex) : parts
  if (!body.length) return deal.wine_name || "Wine"
  const normalized = [body[0].replace(/^(?:19|20)\d{2}\s+|^nv\s+/i, "").trim(), ...body.slice(1)]
  if (normalized.length >= 2) return normalized[normalized.length - 1]
  if (deal.producer && normalized[0].toLowerCase() === String(deal.producer).trim().toLowerCase()) return deal.wine_name || "Wine"
  return normalized[0]
}

function hasActiveFilters() {
  return filterParams().toString() !== ""
}

function renderActiveFilters() {
  const chips = []
  if (state.search) chips.push([`“${state.search}”`, "search"])
  if (state.country) chips.push([state.country, "country"])
  if (state.region) chips.push([state.region, "region"])
  if (state.wineType) chips.push([state.wineType, "wineType"])
  if (state.styleFamily) chips.push([state.styleFamily, "styleFamily"])
  if (state.grape) chips.push([state.grape, "grape"])
  if (state.offeringType) chips.push([state.offeringType, "offeringType"])
  if (state.producer) chips.push([state.producer, "producer"])
  if (state.comparableOnly) chips.push(["Listed at Grand Cru", "comparableOnly"])
  if (state.onlyPlatinumCheaper) chips.push(["Cheaper on Platinum", "onlyPlatinumCheaper"])
  if (state.minVivinoRating) chips.push([`Rated ${state.minVivinoRating}+`, "minVivinoRating"])
  if (state.minVivinoNumRatings) chips.push([`${state.minVivinoNumRatings}+ ratings`, "minVivinoNumRatings"])

  if (!chips.length) {
    els.activeFilters.innerHTML = ""
    return
  }
  els.activeFilters.innerHTML = `
    <span class="active-label">Filtered by</span>
    ${chips.map(([label, key]) => `<button class="filter-chip" type="button" data-reset="${escapeHtml(key)}" aria-label="Remove filter: ${escapeHtml(label)}">${escapeHtml(label)}<svg aria-hidden="true" viewBox="0 0 12 12" width="12" height="12"><path d="M3 3l6 6M9 3l-6 6" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg></button>`).join("")}
    ${chips.length > 1 ? `<button class="text-button" type="button" data-reset="all">Clear all</button>` : ""}
  `
}

function resetFilter(key) {
  if (key === "all") {
    const { sortBy, sortOrder } = state
    Object.assign(state, defaultState, { sortBy, sortOrder })
    return
  }
  if (key === "country") state.region = ""
  state[key] = defaultState[key]
}

function resolveVerdict(deal) {
  // Offers Grand Cru doesn't sell are judged against Vivino below.
  if (deal.value_verdict && deal.value_verdict_tone && deal.value_verdict !== "No retail benchmark") {
    return { label: normalizeVerdictLabel(deal.value_verdict), tone: deal.value_verdict_tone }
  }
  if (deal.cheaper_side === "Platinum Cheaper") {
    return (deal.vivino_rating || 0) >= 4 && (deal.vivino_num_ratings || 0) >= 100
      ? { label: "Best Platinum buy", tone: "good" }
      : { label: "Good Platinum price", tone: "good" }
  }
  if (deal.cheaper_side === "Same Price") {
    return (deal.vivino_rating || 0) >= 4.1
      ? { label: "Good bottle, fair price", tone: "calm" }
      : { label: "Same price", tone: "calm" }
  }
  if (deal.cheaper_side === "Grand Cru Cheaper") return { label: "Grand Cru cheaper", tone: "warn" }
  if (deal.cheaper_side === "No Match") {
    const gap = marketGapPct(deal)
    if (gap == null) return { label: "No price to compare yet", tone: "ghost" }
    if (Math.abs(gap) < 1) return { label: "At market price", tone: "calm" }
    return gap < 0 ? { label: "Below market price", tone: "good" } : { label: "Above market price", tone: "warn" }
  }
  return { label: "Check before buying", tone: "ghost" }
}

function normalizeVerdictLabel(label) {
  const map = {
    "Strong Credit Spend": "Best Platinum buy",
    "Solid Value": "Good Platinum price",
    "Quality Buy": "Good bottle, fair price",
    "Retail Match": "Same price",
    "Platinum Markup": "Grand Cru cheaper",
    "Market Value": "Below market price",
    "Below market": "Below market price",
    "At Market": "At market price",
    "At market": "At market price",
    "Market Risk": "Above market price",
    "Above market": "Above market price",
    "Needs Source": "No price to compare yet",
    "Quality Only": "No price to compare yet",
    "Benchmark pending": "No price to compare yet",
    "Needs Review": "Check before buying",
    "Needs review": "Check before buying",
  }
  return map[label] || label
}

function hasVivinoMarket(deal) {
  return deal.price_platinum != null && deal.vivino_price != null && deal.vivino_price > 0
}

function marketGapPct(deal) {
  if (!hasVivinoMarket(deal)) return null
  return ((deal.price_platinum - deal.vivino_price) / deal.vivino_price) * 100
}

function trendNote(change, direction, currentPrice) {
  if (direction !== "up" && direction !== "down") return ""
  if (isLikelyTrendCorrection(change, currentPrice)) return ""
  const amount = formatMoney(Math.abs(change || 0))
  return direction === "down"
    ? `<span class="sub tone-good">Down ${amount} this week</span>`
    : `<span class="sub tone-bad">Up ${amount} this week</span>`
}

function isLikelyTrendCorrection(change, currentPrice) {
  const absoluteChange = Math.abs(Number(change) || 0)
  const price = Math.abs(Number(currentPrice) || 0)
  if (!absoluteChange || !price) return false
  return absoluteChange >= 1000 && absoluteChange >= price * 2
}

const BOTTLE_SVG = `<svg class="bottle-fallback" viewBox="0 0 24 64" aria-hidden="true"><path d="M9 2h6v14c0 3 5 6 5 12v32a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V28c0-6 5-9 5-12V2z" fill="currentColor"/></svg>`

function wineImageHtml(deal) {
  const image = deal.image_url
    ? `<img src="${escapeHtml(deal.image_url)}" alt="" loading="lazy" decoding="async" referrerpolicy="no-referrer" onerror="this.parentElement.classList.add('is-empty'); this.remove()">`
    : ""
  return `<div class="wine-thumb${deal.image_url ? "" : " is-empty"}">${image}${BOTTLE_SVG}</div>`
}

function vivinoSource(deal) {
  if (!deal.vivino_url) return ""
  return `<a class="source-link" href="${escapeHtml(deal.vivino_url)}" target="_blank" rel="noopener noreferrer">Vivino<span class="visually-hidden"> page for this wine (opens in a new tab)</span></a>`
}

function formatDay(value, withYear = false) {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return ""
  const sameYear = date.getFullYear() === new Date().getFullYear()
  return date.toLocaleDateString("en-SG", { day: "numeric", month: "short", ...(sameYear && !withYear ? {} : { year: "numeric" }) })
}

function wineSearcherUrl(deal) {
  const name = displayName(deal).replace(/^(\d{4}|NV|MV)\s+/i, "").replace(/\s+-\s+/g, " ")
  const terms = [name, deal.vintage].filter(Boolean).join(" ").trim()
  if (!terms) return ""
  return `https://www.wine-searcher.com/find/${encodeURIComponent(terms).replace(/%20/g, "+")}/1/singapore`
}

function quietLink(url, label) {
  if (!url) return ""
  return `<a class="quiet-link" href="${escapeHtml(url)}" target="_blank" rel="noopener">${escapeHtml(label)}<span class="visually-hidden"> (opens in a new tab)</span></a>`
}

function textLink(url, label) {
  if (!url) return ""
  return `<a class="text-link" href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}<span class="visually-hidden"> (opens in a new tab)</span></a>`
}

function formatVolume(deal) {
  const quantity = Number(deal.quantity || 1)
  const raw = String(deal.volume || "").trim()
  const volume = /ml$/i.test(raw) ? raw.replace(/\s*ml$/i, "\u00a0ml") : raw.replace(/\s*l$/i, "\u00a0L")
  if (!volume) return ""
  return quantity > 1 ? `${quantity}\u00a0\u00d7\u00a0${volume}` : volume
}

function volumeQuantityCopy(offer) {
  return [formatVintage(offer.vintage), formatVolume(offer)].filter(Boolean).join(" · ")
}

function formatVintage(vintage) {
  return vintage || "NV"
}

function isStandardBottle(deal) {
  return (deal.volume_ml == null || deal.volume_ml === 750) && Math.max(deal.quantity || 1, 1) === 1
}

function sizeNotes(deal) {
  if (deal.price_platinum == null || !deal.volume_ml) return ""
  const totalMl = deal.volume_ml * Math.max(deal.quantity || 1, 1)
  const per100 = `${WineCharts.money((deal.price_platinum / totalMl) * 100)}/100\u00a0ml`
  if (isStandardBottle(deal)) return `<span class="sub">${per100}</span>`
  return `
    <span class="sub size-note">for ${escapeHtml(formatVolume(deal))}</span>
    <span class="sub per-750">${WineCharts.money(deal.price_platinum_per_750)} per 750\u00a0ml</span>
    <span class="sub">${per100}</span>
  `
}

async function fetchJson(path) {
  const response = await fetch(path, { headers: { Accept: "application/json" } })
  if (!response.ok) throw new Error(`Request failed for ${path}: ${response.status}`)
  return response.json()
}

function formatMoney(value) {
  if (value == null) return ""
  return `S$${new Intl.NumberFormat("en-SG", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(value)}`
}

function formatPct(value, digits = 1) {
  return `${Number(value).toFixed(digits)}%`
}

function formatInteger(value) {
  return new Intl.NumberFormat("en-SG").format(value)
}

function formatTimeAgo(date) {
  const minutes = Math.floor((Date.now() - date.getTime()) / 60000)
  if (minutes < 1) return "just now"
  if (minutes < 60) return `${minutes} ${plural(minutes, "minute", "minutes")} ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours} ${plural(hours, "hour", "hours")} ago`
  const days = Math.floor(hours / 24)
  return `${days} ${plural(days, "day", "days")} ago`
}

function plural(count, one, many) {
  return count === 1 ? one : many
}

function capitalize(text) {
  return text ? text[0].toUpperCase() + text.slice(1) : text
}

function joinList(items) {
  if (items.length <= 1) return items.join("")
  return `${items.slice(0, -1).join(", ")} and ${items[items.length - 1]}`
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;")
}
