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
  cheaperSide: "",
  minVivinoRating: "",
  minVivinoNumRatings: "",
  priceMin: "",
  priceMax: "",
  includeSoldOut: false,
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

const QUICK_FILTERS = {
  platinum: { isOn: () => state.cheaperSide === "Platinum Cheaper", apply: (on) => { state.cheaperSide = on ? "Platinum Cheaper" : "" } },
  rated: { isOn: () => Number(state.minVivinoRating || 0) >= 4, apply: (on) => { state.minVivinoRating = on ? "4.0" : "" } },
  confident: { isOn: () => Number(state.minVivinoNumRatings || 0) >= 100, apply: (on) => { state.minVivinoNumRatings = on ? "100" : "" } },
  listed: { isOn: () => state.comparableOnly, apply: (on) => { state.comparableOnly = on } },
  soldout: { isOn: () => state.includeSoldOut, apply: (on) => { state.includeSoldOut = on } },
}

const SIDES = [
  { value: "Platinum Cheaper", label: "cheaper on Platinum", cls: "platinum" },
  { value: "Same Price", label: "same price", cls: "same" },
  { value: "Grand Cru Cheaper", label: "cheaper at Grand Cru", cls: "grand-cru" },
  { value: "No Match", label: "not at Grand Cru", cls: "none" },
]

const STYLE_ORDER = ["Red", "White", "Champagne", "Sparkling", "Rose", "Sweet / Dessert", "Orange"]

const state = { ...defaultState }
let filterOptions = null
let priceHistory = null
let allDeals = []
let valueMap = null
let priceBounds = null
let pendingHighlight = null
let sliderDebounce = 0
let requestSerial = 0
let lastDeals = null
let splitSerial = 0
let searchDebounce = 0
const els = {}

document.addEventListener("DOMContentLoaded", async () => {
  captureElements()
  hydrateStateFromUrl()
  bindEvents()
  renderLoadingRows()
  loadFreshness()
  loadPriceHistory()
  setupValueMap()
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
    "splitBar", "quickFilters",
    "resultsMeta", "activeFilters", "dealTableBody", "tableState", "freshness", "marketNotice",
    "statStrip", "priceMinRange", "priceMaxRange", "priceMinInput", "priceMaxInput", "priceFilter",
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

  els.quickFilters.addEventListener("click", (event) => {
    const button = event.target.closest("[data-quick]")
    if (!button) return
    const on = button.getAttribute("aria-pressed") !== "true"
    QUICK_FILTERS[button.dataset.quick].apply(on)
    syncControlsFromState()
    loadOffers()
  })

  els.splitBar.addEventListener("click", (event) => {
    const button = event.target.closest("[data-side]")
    if (!button) return
    state.cheaperSide = state.cheaperSide === button.dataset.side ? "" : button.dataset.side
    syncControlsFromState()
    loadOffers()
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

  bindPriceFilter()
  els.statStrip.addEventListener("click", (event) => {
    const button = event.target.closest("[data-stat]")
    if (button) applyStat(button.dataset.stat, button.dataset.dealId)
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

// One bar across the offers in view: how each compares with Grand Cru. Its own filter is left out.
async function loadSplit() {
  const params = filterParams()
  params.delete("cheaper_side")
  const serial = ++splitSerial
  let stats
  try {
    stats = await fetchJson(`/deals/stats?${params}`)
  } catch (_) {
    return
  }
  if (serial !== splitSerial) return
  const counts = Object.fromEntries((stats.cheaper_sides || []).map((item) => [item.value, item.count]))
  const total = stats.total_deals || 0
  if (!total) {
    els.splitBar.hidden = true
    return
  }
  const sides = SIDES.map((side) => ({ ...side, count: counts[side.value] || 0 }))
  els.splitBar.innerHTML = `
    <p class="split-title"><strong>${formatInteger(total)}</strong> ${plural(total, "offer", "offers")}${hasActiveFilters() ? " in view" : state.includeSoldOut ? ", in stock and sold out" : " in stock today"}, against Grand Cru</p>
    <div class="split-bar" aria-hidden="true">
      ${sides.filter((side) => side.count).map((side) => `<span class="split-seg seg-${side.cls}" style="flex-grow:${side.count}" data-side="${escapeHtml(side.value)}"></span>`).join("")}
    </div>
    <ul class="split-legend">
      ${sides.map((side) => `<li><button type="button" class="split-key key-${side.cls}" data-side="${escapeHtml(side.value)}" aria-pressed="${state.cheaperSide === side.value}" ${side.count ? "" : "disabled"}><span class="split-swatch" aria-hidden="true"></span><strong>${formatInteger(side.count)}</strong> ${escapeHtml(side.label)}</button></li>`).join("")}
    </ul>
  `
  els.splitBar.hidden = false
}

async function loadFreshness() {
  try {
    const data = await fetchJson("/health")
    const finished = data.latest_ingestion?.finished_at
    if (!finished) return
    const ago = formatTimeAgo(new Date(finished))
    els.freshness.textContent = data.ingestion_stale
      ? `Prices in Singapore dollars, last checked ${ago}. Today’s check is late, so some prices may have changed.`
      : "All prices in Singapore dollars."
    els.freshness.classList.toggle("is-stale", Boolean(data.ingestion_stale))
    els.freshness.hidden = false
    renderFreshStat(data)
    if (data.vivino_ok === false) {
      els.marketNotice.textContent = "Market prices paused. Vivino prices are not being updated right now, so each one shows the date it was last checked."
      els.marketNotice.hidden = false
    }
  } catch (_) {
    els.freshness.hidden = true
  }
}

// --- Value map, stat strip and price filter ---

async function setupValueMap() {
  const root = document.getElementById("valueMap")
  const stateLine = root.querySelector(".map-state")
  try {
    allDeals = await fetchJson("/deals?limit=500&sort_by=price_per_750&sort_order=asc")
  } catch (_) {
    stateLine.textContent = "The map could not load. The list below still works."
    return
  }
  const prices = allDeals.map((d) => d.price_platinum_per_750).filter((v) => v != null)
  if (!prices.length) {
    stateLine.textContent = "No offers to plot right now."
    return
  }
  stateLine.remove()
  priceBounds = { min: Math.floor(Math.min(...prices)), max: Math.ceil(Math.max(...prices)) }
  syncPriceControls()
  renderStats()
  valueMap = new ValueMap(root, { onShowInList: showDealInList, onStyleToggle: toggleStyleGroup })
  valueMap.setDeals(allDeals)
  if (lastDeals) valueMap.setVisible(lastDeals.map((d) => d.id), styleFilterList(), state.includeSoldOut)
  renderStockLine()
}

// Honest totals for the whole Platinum list, whatever the filters.
function renderStockLine() {
  const line = document.getElementById("stockLine")
  const soldOut = allDeals.filter((d) => d.platinum_in_stock === false).length
  const inStock = allDeals.length - soldOut
  line.textContent = soldOut
    ? `Platinum lists ${formatInteger(allDeals.length)} wines: ${formatInteger(inStock)} in stock, ${formatInteger(soldOut)} sold out.`
    : `Platinum lists ${formatInteger(inStock)} ${plural(inStock, "wine", "wines")}, all in stock.`
  const toggle = els.quickFilters.querySelector("[data-quick='soldout']")
  toggle.textContent = `Include sold out (${formatInteger(soldOut)})`
  toggle.hidden = !soldOut
}

function styleFilterList() {
  return state.styleFamily ? state.styleFamily.split(",") : []
}

function toggleStyleGroup(group) {
  const value = group.members.filter((m) => allDeals.some((d) => d.style_family === m)).join(",")
  state.styleFamily = state.styleFamily === value ? "" : value
  renderSelectOptions()
  syncControlsFromState()
  loadOffers()
}

function renderStats() {
  const rated = allDeals.filter((d) => d.platinum_in_stock !== false && (d.vivino_rating || 0) >= 4 && d.price_platinum_per_750 != null)
  const best = rated.sort((a, b) => a.price_platinum_per_750 - b.price_platinum_per_750)[0]
  const items = []
  if (best) {
    items.push(`<li><button type="button" class="stat stat-feature" data-stat="best" data-deal-id="${best.id}">
      ${best.image_url ? `<img src="${escapeHtml(best.image_url)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()">` : ""}
      <strong>${WineCharts.money(best.price_platinum_per_750)}</strong><span>per 750&nbsp;ml for ${escapeHtml(displayName(best))}, the lowest price among wines rated 4.0 or higher</span></button></li>`)
  }
  items.push(`<li id="statFresh" hidden></li>`)
  els.statStrip.innerHTML = items.join("")
  if (lastHealth) renderFreshStat(lastHealth)
}

let lastHealth = null
function renderFreshStat(health) {
  lastHealth = health
  const item = document.getElementById("statFresh")
  const finished = health.latest_ingestion?.finished_at
  if (!item || !finished) return
  const ago = formatTimeAgo(new Date(finished)).replace(/ ago$/, "")
  item.innerHTML = `<a class="stat${health.ingestion_stale ? " is-stale" : ""}" href="#sources"><strong>${escapeHtml(ago === "just now" ? "Just now" : ago)}</strong><span>${ago === "just now" ? "prices were checked" : "since prices were last checked"}${health.ingestion_stale ? ", later than usual" : ""}</span></a>`
  item.hidden = false
}

function applyStat(kind, dealId) {
  if (kind === "best") {
    resetFilter("all")
    state.minVivinoRating = "4.0"
    state.sortBy = "price_per_750"
    state.sortOrder = "asc"
    pendingHighlight = Number(dealId)
  }
  syncControlsFromState()
  loadOffers().then(() => document.getElementById("offers").scrollIntoView({ behavior: prefersReducedMotion() ? "auto" : "smooth" }))
}

function prefersReducedMotion() {
  return window.matchMedia("(prefers-reduced-motion: reduce)").matches
}

function showDealInList(deal) {
  const row = els.dealTableBody.querySelector(`[data-offer-ids~="${deal.id}"]`)
  if (!row) return
  const group = row.nextElementSibling?.classList.contains("history-row") ? row.nextElementSibling.nextElementSibling : row.nextElementSibling
  if (row.dataset.bestId !== String(deal.id) && group?.classList.contains("deal-group-row")) group.querySelector("details").open = true
  row.scrollIntoView({ behavior: prefersReducedMotion() ? "auto" : "smooth", block: "center" })
  row.classList.remove("is-highlighted")
  void row.offsetWidth
  row.classList.add("is-highlighted")
  const link = row.querySelector(".wine-title")
  if (link) { link.setAttribute("tabindex", "-1"); link.focus({ preventScroll: true }) }
}

// Slider positions 0..1000 map onto a log scale of S$ per 750 ml.
function priceToSlider(price) {
  const { min, max } = priceBounds
  return Math.round(((Math.log(price) - Math.log(min)) / (Math.log(max) - Math.log(min))) * 1000)
}

function sliderToPrice(position) {
  const { min, max } = priceBounds
  const raw = Math.exp(Math.log(min) + (position / 1000) * (Math.log(max) - Math.log(min)))
  const step = raw < 100 ? 1 : raw < 500 ? 5 : 10
  return Math.round(raw / step) * step
}

function syncPriceControls({ fromSlider = false } = {}) {
  els.priceMinInput.value = state.priceMin
  els.priceMaxInput.value = state.priceMax
  if (priceBounds) {
    // While dragging, the thumbs stay where the user put them; rounding the price must not move them.
    if (!fromSlider) {
      els.priceMinRange.value = state.priceMin ? Math.max(0, priceToSlider(Number(state.priceMin))) : 0
      els.priceMaxRange.value = state.priceMax ? Math.min(1000, priceToSlider(Number(state.priceMax))) : 1000
    }
    els.priceMinRange.setAttribute("aria-valuetext", state.priceMin ? `S$${state.priceMin}` : "No minimum")
    els.priceMaxRange.setAttribute("aria-valuetext", state.priceMax ? `S$${state.priceMax}` : "No maximum")
    const fill = els.priceFilter.querySelector(".range-fill")
    fill.style.left = `${els.priceMinRange.value / 10}%`
    fill.style.right = `${100 - els.priceMaxRange.value / 10}%`
  }
  els.priceFilter.querySelectorAll(".preset").forEach((b) => {
    b.setAttribute("aria-pressed", String(b.dataset.min === state.priceMin && b.dataset.max === state.priceMax))
  })
}

function bindPriceFilter() {
  const fromRange = (commit) => {
    if (!priceBounds) return
    let lo = Number(els.priceMinRange.value)
    let hi = Number(els.priceMaxRange.value)
    if (lo > hi) {
      if (document.activeElement === els.priceMinRange) els.priceMaxRange.value = hi = lo
      else els.priceMinRange.value = lo = hi
    }
    state.priceMin = lo <= 0 ? "" : String(sliderToPrice(lo))
    state.priceMax = hi >= 1000 ? "" : String(sliderToPrice(hi))
    syncPriceControls({ fromSlider: true })
    if (commit) {
      // Arrow keys fire change on every press; send one request when they stop.
      window.clearTimeout(sliderDebounce)
      sliderDebounce = window.setTimeout(loadOffers, 350)
    }
  }
  for (const range of [els.priceMinRange, els.priceMaxRange]) {
    range.addEventListener("input", () => fromRange(false))
    range.addEventListener("change", () => fromRange(true))
  }
  for (const [input, key] of [[els.priceMinInput, "priceMin"], [els.priceMaxInput, "priceMax"]]) {
    input.addEventListener("change", () => {
      const value = input.value.trim()
      state[key] = value && Number(value) >= 0 ? String(Math.round(Number(value))) : ""
      if (state.priceMin && state.priceMax && Number(state.priceMin) > Number(state.priceMax)) {
        [state.priceMin, state.priceMax] = [state.priceMax, state.priceMin]
      }
      syncPriceControls()
      loadOffers()
    })
  }
  els.priceFilter.addEventListener("click", (event) => {
    const preset = event.target.closest(".preset")
    if (!preset) return
    const same = preset.getAttribute("aria-pressed") === "true"
    state.priceMin = same ? "" : preset.dataset.min
    state.priceMax = same ? "" : preset.dataset.max
    syncPriceControls()
    loadOffers()
  })
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
    lastDeals = deals
    renderResultsMeta(deals, families)
    renderTable(families)
    renderActiveFilters()
    if (valueMap) valueMap.setVisible(deals.map((d) => d.id), styleFilterList(), state.includeSoldOut)
    loadSplit()
    if (pendingHighlight) {
      const target = deals.find((d) => d.id === pendingHighlight)
      pendingHighlight = null
      if (target) window.setTimeout(() => showDealInList(target), 60)
    }
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
  if (state.cheaperSide) params.set("cheaper_side", state.cheaperSide)
  if (state.minVivinoRating) params.set("min_vivino_rating", state.minVivinoRating)
  if (state.minVivinoNumRatings) params.set("min_vivino_num_ratings", state.minVivinoNumRatings)
  if (state.priceMin) params.set("min_price_per_750", state.priceMin)
  if (state.priceMax) params.set("max_price_per_750", state.priceMax)
  if (!state.includeSoldOut) params.set("availability", "in_stock")
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
  state.cheaperSide = params.get("cheaper_side") || (params.get("only_platinum_cheaper") === "true" ? "Platinum Cheaper" : "")
  state.minVivinoRating = params.get("min_vivino_rating") || ""
  state.minVivinoNumRatings = params.get("min_vivino_num_ratings") || ""
  state.priceMin = params.get("min_price_per_750") || ""
  state.priceMax = params.get("max_price_per_750") || ""
  state.includeSoldOut = params.get("sold_out") === "include"
}

function writeStateToUrl() {
  const params = filterParams()
  params.delete("availability")
  if (state.includeSoldOut) params.set("sold_out", "include")
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
      const label = item.count ? `${item.value} (${item.count})` : item.value.split(",").join(" and ")
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
  els.quickFilters.querySelectorAll("[data-quick]").forEach((button) => {
    button.setAttribute("aria-pressed", String(QUICK_FILTERS[button.dataset.quick].isOn()))
  })
  els.splitBar.querySelectorAll("[data-side]").forEach((button) => {
    button.setAttribute("aria-pressed", String(state.cheaperSide === button.dataset.side))
  })
  syncPriceControls()
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
      <td colspan="4"><div class="skeleton"><span class="skeleton-block"></span><span class="skeleton-line"></span><span class="skeleton-line short"></span></div></td>
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
      <td colspan="4"><div class="history-panel"></div></td>
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
    <tr class="deal-row${deal.platinum_in_stock === false ? " is-sold-out" : ""}" data-best-id="${deal.id}" data-offer-ids="${escapeHtml((family ? family.offers : [deal]).map((o) => o.id).join(" "))}">
      <td class="cell-wine">
        <div class="wine-cell">
          ${wineImageHtml(deal)}
          <div class="wine-main">
            <p class="wine-title">${escapeHtml(displayName(deal))}</p>
            <p class="wine-meta">${escapeHtml(meta)}</p>
            <p class="verdict tone-${escapeHtml(verdict.tone)}">${escapeHtml(verdict.label)}</p>
            ${grouped}
          </div>
        </div>
      </td>
      <td class="cell-compare">
        <div class="price-head">
          <span class="price">${formatMoney(deal.price_platinum)}</span>
          ${sizeNotes(deal)}
        </div>
        ${deal.platinum_in_stock === false ? `<p class="sold-out-line">${escapeHtml(soldOutText(deal))}</p>` : ""}
        ${priceStrip(deal)}
        <ul class="compare-lines">${compareLines(deal)}</ul>
        <div class="history-slot" data-deal-id="${deal.id}"></div>
      </td>
      <td class="num cell-rating">
        <span class="cell-label">Vivino rating</span>
        ${deal.vivino_rating != null
          ? `<span class="figure">${deal.vivino_rating.toFixed(1)}</span>${deal.vivino_num_ratings ? `<span class="sub">${formatInteger(deal.vivino_num_ratings)} ratings</span>` : deal.vivino_match_method === "platinum" ? `<span class="sub">As shown on Platinum</span>` : ""}${vivinoSource(deal)}`
          : `<span class="sub">Not rated yet</span>`}
        ${deal.deal_score != null ? `<span class="sub score-line">Score ${Math.round(deal.deal_score)} of 100</span>` : ""}
      </td>
      <td class="cell-links">
        <div class="wine-links">
          ${deal.platinum_url
            ? deal.platinum_in_stock === false
              ? `<a class="button" href="${escapeHtml(deal.platinum_url)}" target="_blank" rel="noopener noreferrer">View on Platinum<span class="visually-hidden"> (sold out, opens in a new tab)</span></a>`
              : `<a class="button button-primary" href="${escapeHtml(deal.platinum_url)}" target="_blank" rel="noopener noreferrer">Buy on Platinum</a>`
            : ""}
          <span class="compare-links">
            ${textLink(deal.grand_cru_url, "Grand Cru")}
            ${textLink(deal.vivino_url, "Vivino")}
            ${textLink(wineSearcherUrl(deal), "Wine-Searcher", "Compare Singapore prices on Wine-Searcher")}
          </span>
        </div>
      </td>
    </tr>
  `
}

function soldOutText(deal) {
  if (deal.sold_out_since) return `Sold out since ${formatDay(deal.sold_out_since)}. Last listed price shown.`
  if (deal.first_seen_on) return `Sold out when we first saw it, ${formatDay(deal.first_seen_on)}. Listed price shown.`
  return "Sold out. Listed price shown."
}

// Where Platinum sits against every other price we have for the same listing size.
function priceStrip(deal) {
  if (deal.price_platinum == null) return ""
  const points = [{ key: "platinum", name: "Platinum", value: deal.price_platinum }]
  if (deal.price_grand_cru != null) points.push({ key: "grand_cru", name: "Grand Cru", value: deal.price_grand_cru })
  if (hasVivinoMarket(deal)) points.push({ key: "vivino", name: "Vivino market", value: deal.vivino_price })
  if (points.length < 2) return ""
  const values = points.map((p) => p.value)
  const pad = Math.max((Math.max(...values) - Math.min(...values)) * 0.2, Math.max(...values) * 0.05)
  const lo = Math.min(...values) - pad
  const hi = Math.max(...values) + pad
  const pos = (v) => ((v - lo) / (hi - lo)) * 100
  const groups = []
  points.slice().sort((a, b) => a.value - b.value).forEach((p) => {
    const last = groups[groups.length - 1]
    if (last && Math.abs(pos(p.value) - last.pos) < 4) last.items.push(p)
    else groups.push({ pos: pos(p.value), items: [p] })
  })
  groups.forEach((g) => {
    g.items.sort((a, b) => (a.key === "platinum" ? -1 : b.key === "platinum" ? 1 : 0))
    g.hasPlatinum = g.items[0].key === "platinum"
  })
  // Platinum's label above the line, the others below; a crowded label moves to the free row.
  const rows = { top: [], bottom: [] }
  groups.filter((g) => g.hasPlatinum).forEach((g) => { g.row = "top"; rows.top.push(g.pos) })
  groups.filter((g) => !g.hasPlatinum).forEach((g) => {
    const clash = (row) => rows[row].some((x) => Math.abs(x - g.pos) < 34)
    g.row = !clash("bottom") ? "bottom" : !clash("top") ? "top" : "bottom"
    rows[g.row].push(g.pos)
  })
  const align = (p) => (p < 20 ? "start" : p > 80 ? "end" : "middle")
  const label = (g) => `<span class="strip-label align-${align(g.pos)}${g.hasPlatinum ? " is-platinum" : ""}" style="left:${g.pos.toFixed(1)}%">${escapeHtml(g.items.map((i) => i.name).join(" = "))} ${escapeHtml(WineCharts.money(g.items[0].value))}</span>`
  const aria = points.map((p) => `${p.name} ${WineCharts.money(p.value)}`).join(", ")
  return `
    <div class="strip" role="img" aria-label="${escapeHtml(aria)}">
      <div class="strip-row">${groups.filter((g) => g.row === "top").map(label).join("")}</div>
      <div class="strip-track">${groups.map((g) => g.items.slice().reverse().map((i) => `<span class="strip-dot dot-${i.key}" style="left:${g.pos.toFixed(1)}%"></span>`).join("")).join("")}</div>
      <div class="strip-row">${groups.filter((g) => g.row === "bottom").map(label).join("")}</div>
    </div>
  `
}

function compareLines(deal) {
  const lines = []
  const money = WineCharts.money
  if (deal.price_grand_cru == null || deal.price_platinum == null) {
    lines.push(`<li class="tone-calm">Not at Grand Cru</li>`)
  } else {
    const diff = deal.price_platinum - deal.price_grand_cru
    const pct = Math.round((Math.abs(diff) / deal.price_grand_cru) * 100)
    const stock = deal.grand_cru_in_stock === false ? " · out of stock there" : ""
    if (Math.abs(diff) < 0.5) lines.push(`<li class="tone-calm">Same price as Grand Cru${stock}</li>`)
    else if (diff < 0) lines.push(`<li class="tone-good">${money(-diff)} (${pct}%) less than Grand Cru${stock}</li>`)
    else lines.push(`<li class="tone-bad">${money(diff)} (${pct}%) more than Grand Cru${stock}</li>`)
  }
  if (hasVivinoMarket(deal)) {
    const diff = deal.price_platinum - deal.vivino_price
    const pct = Math.round((Math.abs(diff) / deal.vivino_price) * 100)
    const checked = deal.vivino_checked_at ? ` · checked ${escapeHtml(formatDay(deal.vivino_checked_at))}` : ""
    if (Math.abs(diff) < 0.5) lines.push(`<li class="tone-calm">Same as the Vivino market price${checked}</li>`)
    else if (diff < 0) lines.push(`<li class="tone-good">${money(-diff)} (${pct}%) below the Vivino market price${checked}</li>`)
    else lines.push(`<li class="tone-bad">${money(diff)} (${pct}%) above the Vivino market price${checked}</li>`)
  } else {
    lines.push(`<li class="tone-calm">${!deal.vivino_url && deal.vivino_rating == null ? "No Vivino match yet" : "No Vivino market price yet"}</li>`)
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
      <td colspan="4">
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
  const params = filterParams()
  params.delete("availability")
  return params.toString() !== ""
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
  if (state.cheaperSide) chips.push([capitalize(SIDES.find((side) => side.value === state.cheaperSide)?.label || state.cheaperSide), "cheaperSide"])
  if (state.minVivinoRating) chips.push([`Rated ${state.minVivinoRating}+`, "minVivinoRating"])
  if (state.minVivinoNumRatings) chips.push([`${state.minVivinoNumRatings}+ ratings`, "minVivinoNumRatings"])
  if (state.includeSoldOut) chips.push(["Including sold out", "includeSoldOut"])
  if (state.priceMin || state.priceMax) {
    const range = state.priceMin && state.priceMax ? `S$${state.priceMin} to S$${state.priceMax}` : state.priceMin ? `S$${state.priceMin} and up` : `Under S$${state.priceMax}`
    chips.push([`${range} per 750 ml`, "price"])
  }

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
  if (key === "price") {
    state.priceMin = ""
    state.priceMax = ""
    return
  }
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

function textLink(url, label, title) {
  if (!url) return ""
  return `<a class="text-link" href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer"${title ? ` title="${escapeHtml(title)}"` : ""}>${escapeHtml(label)}<span class="visually-hidden"> (opens in a new tab)</span></a>`
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
