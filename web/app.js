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
  comparableOnly: true,
  onlyPlatinumCheaper: false,
  minVivinoRating: "",
  minVivinoNumRatings: "",
}

const sortDefaults = {
  wine_name: "asc",
  price_platinum: "asc",
  price_diff_pct_abs: "desc",
  vivino_rating: "desc",
  deal_score: "desc",
}

const sortLabels = {
  "deal_score:desc": "Best overall",
  "price_diff_pct:asc": "Biggest Platinum discount",
  "price_diff_pct_abs:desc": "Largest gap either way",
  "vivino_rating:desc": "Highest Vivino rating",
  "price_platinum:asc": "Lowest Platinum price",
  "wine_name:asc": "Alphabetical",
}

const EUROPEAN_COUNTRIES = new Set([
  "Austria",
  "France",
  "Germany",
  "Italy",
  "Portugal",
  "Spain",
  "Switzerland",
])

const state = { ...defaultState }
let filterOptions = null
let requestSerial = 0
let searchDebounce = 0
let originMap = null
let originMarkerLayer = null
let latestMapPoints = []
let mapFocusCountry = ""

const els = {}

document.addEventListener("DOMContentLoaded", async () => {
  captureElements()
  hydrateStateFromUrl()
  bindEvents()
  await loadFilterOptions()
  syncControlsFromState()
  await loadDashboard()
})

function captureElements() {
  els.searchInput = document.getElementById("searchInput")
  els.sortSelect = document.getElementById("sortSelect")
  els.countrySelect = document.getElementById("countrySelect")
  els.regionSelect = document.getElementById("regionSelect")
  els.wineTypeSelect = document.getElementById("wineTypeSelect")
  els.styleFamilySelect = document.getElementById("styleFamilySelect")
  els.grapeSelect = document.getElementById("grapeSelect")
  els.offeringSelect = document.getElementById("offeringSelect")
  els.producerSelect = document.getElementById("producerSelect")
  els.countryQuickFilters = document.getElementById("countryQuickFilters")
  els.styleQuickFilters = document.getElementById("styleQuickFilters")
  els.comparableOnlyToggle = document.getElementById("comparableOnlyToggle")
  els.platinumOnlyToggle = document.getElementById("platinumOnlyToggle")
  els.ratingToggle = document.getElementById("ratingToggle")
  els.confidenceToggle = document.getElementById("confidenceToggle")
  els.clearMapFocus = document.getElementById("clearMapFocus")
  els.heroStats = document.getElementById("heroStats")
  els.dealMixChart = document.getElementById("dealMixChart")
  els.offeringChart = document.getElementById("offeringChart")
  els.topPicks = document.getElementById("topPicks")
  els.resultsMeta = document.getElementById("resultsMeta")
  els.activeFilters = document.getElementById("activeFilters")
  els.familyBoard = document.getElementById("familyBoard")
  els.dealTableBody = document.getElementById("dealTableBody")
  els.originMap = document.getElementById("originMap")
  els.mapSelection = document.getElementById("mapSelection")
  els.regionGuide = document.getElementById("regionGuide")
  els.sortButtons = Array.from(document.querySelectorAll(".sort-button"))
}

function bindEvents() {
  els.searchInput.addEventListener("input", (event) => {
    window.clearTimeout(searchDebounce)
    searchDebounce = window.setTimeout(() => {
      state.search = event.target.value.trim()
      loadDashboard()
    }, 220)
  })

  els.sortSelect.addEventListener("change", (event) => {
    const [sortBy, sortOrder] = event.target.value.split(":")
    state.sortBy = sortBy
    state.sortOrder = sortOrder
    syncSortButtons()
    loadDashboard()
  })

  els.countrySelect.addEventListener("change", (event) => {
    state.country = event.target.value
    state.region = ""
    syncControlsFromState()
    loadDashboard()
  })

  els.regionSelect.addEventListener("change", (event) => {
    state.region = event.target.value
    loadDashboard()
  })

  els.wineTypeSelect.addEventListener("change", (event) => {
    state.wineType = event.target.value
    loadDashboard()
  })

  els.styleFamilySelect.addEventListener("change", (event) => {
    state.styleFamily = event.target.value
    loadDashboard()
  })

  els.grapeSelect.addEventListener("change", (event) => {
    state.grape = event.target.value
    loadDashboard()
  })

  els.offeringSelect.addEventListener("change", (event) => {
    state.offeringType = event.target.value
    loadDashboard()
  })

  els.producerSelect.addEventListener("change", (event) => {
    state.producer = event.target.value
    loadDashboard()
  })

  els.comparableOnlyToggle.addEventListener("change", (event) => {
    state.comparableOnly = event.target.checked
    loadDashboard()
  })

  els.platinumOnlyToggle.addEventListener("change", (event) => {
    state.onlyPlatinumCheaper = event.target.checked
    loadDashboard()
  })

  els.ratingToggle.addEventListener("change", (event) => {
    state.minVivinoRating = event.target.checked ? "4.0" : ""
    loadDashboard()
  })

  els.confidenceToggle.addEventListener("change", (event) => {
    state.minVivinoNumRatings = event.target.checked ? "100" : ""
    loadDashboard()
  })

  els.clearMapFocus.addEventListener("click", () => {
    mapFocusCountry = ""
    renderMap(latestMapPoints)
  })

  els.activeFilters.addEventListener("click", (event) => {
    const resetKey = event.target.getAttribute("data-reset")
    if (!resetKey) {
      return
    }
    resetFilter(resetKey)
    syncControlsFromState()
    loadDashboard()
  })

  els.mapSelection.addEventListener("click", (event) => {
    const filterButton = event.target.closest("[data-map-country-filter]")
    if (filterButton) {
      mapFocusCountry = ""
      state.country = filterButton.dataset.mapCountryFilter || ""
      state.region = ""
      syncControlsFromState()
      loadDashboard()
      return
    }

    const clearButton = event.target.closest("[data-map-clear-filter]")
    if (!clearButton) {
      return
    }
    mapFocusCountry = ""
    state.country = ""
    state.region = ""
    syncControlsFromState()
    loadDashboard()
  })

  els.countryQuickFilters.addEventListener("click", (event) => {
    const button = event.target.closest("[data-country-filter]")
    if (!button) {
      return
    }
    state.country = button.dataset.countryFilter || ""
    state.region = ""
    syncControlsFromState()
    loadDashboard()
  })

  els.styleQuickFilters.addEventListener("click", (event) => {
    const button = event.target.closest("[data-style-filter]")
    if (!button) {
      return
    }
    state.styleFamily = button.dataset.styleFilter || ""
    syncControlsFromState()
    loadDashboard()
  })

  els.regionGuide.addEventListener("click", (event) => {
    const button = event.target.closest("[data-country-pick]")
    if (!button || button.dataset.regionDisabled === "true") {
      return
    }
    state.country = button.dataset.countryPick || ""
    state.region = button.dataset.regionPick || ""
    syncControlsFromState()
    loadDashboard()
  })

  els.familyBoard.addEventListener("click", (event) => {
    const filterButton = event.target.closest("[data-style-group-filter]")
    if (filterButton) {
      event.preventDefault()
      event.stopPropagation()
      state.styleFamily = filterButton.dataset.styleGroupFilter || ""
      syncControlsFromState()
      loadDashboard()
      return
    }

    const clearButton = event.target.closest("[data-style-group-clear]")
    if (!clearButton) {
      return
    }
    event.preventDefault()
    event.stopPropagation()
    state.styleFamily = ""
    syncControlsFromState()
    loadDashboard()
  })

  els.sortButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const sortBy = button.dataset.sort
      if (!sortBy) {
        return
      }
      if (state.sortBy === sortBy) {
        state.sortOrder = state.sortOrder === "desc" ? "asc" : "desc"
      } else {
        state.sortBy = sortBy
        state.sortOrder = sortDefaults[sortBy] || "desc"
      }
      syncControlsFromState()
      loadDashboard()
    })
  })
}

async function loadFilterOptions() {
  filterOptions = await fetchJson("/deals/filters")
  renderSelectOptions()
}

async function loadDashboard() {
  mapFocusCountry = ""
  const requestId = ++requestSerial
  setLoading(true)
  writeStateToUrl()

  try {
    const [deals, stats, mapPoints] = await Promise.all([
      fetchJson(`/deals?${buildParams({ includeSort: true, includeLimit: true })}`),
      fetchJson(`/deals/stats?${buildParams({ includeSort: false, includeLimit: false })}`),
      fetchJson(`/deals/map?${buildParams({ includeSort: false, includeLimit: false })}`),
    ])

    if (requestId !== requestSerial) {
      return
    }

    latestMapPoints = mapPoints
    renderResultsMeta(deals)
    renderHeroStats(deals, mapPoints)
    renderDealMix(stats.cheaper_sides || [])
    renderOfferingMix(stats.offering_types || [])
    renderCountryQuickFilters(stats.countries || [])
    renderStyleQuickFilters(stats.style_families || [])
    renderRegionGuide(deals)
    renderStyleGroups(deals)
    renderTopPicks(deals)
    renderMap(mapPoints)
    renderTable(deals)
    renderActiveFilters()
  } catch (error) {
    console.error(error)
    renderErrorState(error)
  } finally {
    if (requestId === requestSerial) {
      setLoading(false)
    }
  }
}

function buildParams({ includeSort, includeLimit }) {
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
  if (includeSort) {
    params.set("sort_by", state.sortBy)
    params.set("sort_order", state.sortOrder)
  }
  if (includeLimit) {
    params.set("limit", "500")
  }

  return params.toString()
}

function hydrateStateFromUrl() {
  const params = new URLSearchParams(window.location.search)
  state.search = params.get("search") || defaultState.search
  state.country = params.get("country") || defaultState.country
  state.region = params.get("region") || defaultState.region
  state.wineType = params.get("wine_type") || defaultState.wineType
  state.styleFamily = params.get("style_family") || defaultState.styleFamily
  state.grape = params.get("grape") || defaultState.grape
  state.offeringType = params.get("offering_type") || defaultState.offeringType
  state.producer = params.get("producer") || defaultState.producer
  state.sortBy = params.get("sort_by") || defaultState.sortBy
  state.sortOrder = params.get("sort_order") || defaultState.sortOrder
  state.comparableOnly = params.get("comparable_only") !== "false"
  state.onlyPlatinumCheaper = params.get("only_platinum_cheaper") === "true"
  state.minVivinoRating = params.get("min_vivino_rating") || defaultState.minVivinoRating
  state.minVivinoNumRatings = params.get("min_vivino_num_ratings") || defaultState.minVivinoNumRatings
}

function writeStateToUrl() {
  const params = new URLSearchParams()
  if (state.search) params.set("search", state.search)
  if (state.country) params.set("country", state.country)
  if (state.region) params.set("region", state.region)
  if (state.wineType) params.set("wine_type", state.wineType)
  if (state.styleFamily) params.set("style_family", state.styleFamily)
  if (state.grape) params.set("grape", state.grape)
  if (state.offeringType) params.set("offering_type", state.offeringType)
  if (state.producer) params.set("producer", state.producer)
  if (state.comparableOnly !== defaultState.comparableOnly) params.set("comparable_only", String(state.comparableOnly))
  if (state.onlyPlatinumCheaper) params.set("only_platinum_cheaper", "true")
  if (state.minVivinoRating) params.set("min_vivino_rating", state.minVivinoRating)
  if (state.minVivinoNumRatings) params.set("min_vivino_num_ratings", state.minVivinoNumRatings)
  if (state.sortBy !== defaultState.sortBy) params.set("sort_by", state.sortBy)
  if (state.sortOrder !== defaultState.sortOrder) params.set("sort_order", state.sortOrder)
  const query = params.toString()
  window.history.replaceState({}, "", `${window.location.pathname}${query ? `?${query}` : ""}`)
}

function renderSelectOptions() {
  if (!filterOptions) {
    return
  }

  populateSelect(els.countrySelect, filterOptions.countries, state.country, "All countries")
  populateSelect(els.regionSelect, filterOptions.regions, state.region, "All regions")
  populateSelect(els.wineTypeSelect, filterOptions.wine_types, state.wineType, "All wine types")
  populateSelect(els.styleFamilySelect, filterOptions.style_families, state.styleFamily, "All browse styles")
  populateSelect(els.grapeSelect, filterOptions.grapes, state.grape, "All grapes")
  populateSelect(els.offeringSelect, filterOptions.offering_types, state.offeringType, "All offer shapes")
  populateSelect(els.producerSelect, filterOptions.producers, state.producer, "All producers")
}

function populateSelect(select, options, selectedValue, placeholder) {
  const items = Array.isArray(options) ? options.slice() : []
  if (selectedValue && !items.some((item) => item.value === selectedValue)) {
    items.unshift({ value: selectedValue, count: 0 })
  }

  const markup = [
    `<option value="">${escapeHtml(placeholder)}</option>`,
    ...items.map((item) => {
      const label = item.count ? `${item.value} (${item.count})` : item.value
      return `<option value="${escapeHtml(item.value)}">${escapeHtml(label)}</option>`
    }),
  ]

  select.innerHTML = markup.join("")
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
  const desiredValue = `${state.sortBy}:${state.sortOrder}`
  const existingOption = Array.from(els.sortSelect.options).find((option) => option.value === desiredValue)
  if (!existingOption) {
    const option = document.createElement("option")
    option.value = desiredValue
    option.textContent = sortLabels[desiredValue] || "Custom sort"
    els.sortSelect.appendChild(option)
  }
  els.sortSelect.value = desiredValue
}

function syncSortButtons() {
  els.sortButtons.forEach((button) => {
    const isActive = button.dataset.sort === state.sortBy
    button.classList.toggle("is-active", isActive)
    button.dataset.arrow = isActive ? (state.sortOrder === "desc" ? "↓" : "↑") : "↕"
  })
}

function renderResultsMeta(deals) {
  const wineCount = groupDealsIntoFamilies(deals).length
  const comparableCopy = state.comparableOnly ? "comparable" : "visible"
  els.resultsMeta.textContent = `${wineCount} wines · ${deals.length} ${comparableCopy} offers - ${sortLabels[`${state.sortBy}:${state.sortOrder}`] || "Custom sort"}`
}

function renderHeroStats(deals, mapPoints) {
  const comparableCount = deals.filter((deal) => deal.price_diff_pct !== null).length
  const platinumCheaper = deals.filter((deal) => deal.cheaper_side === "Platinum Cheaper").length
  const strongSpends = deals.filter((deal) => resolveVerdict(deal).label === "Strong Credit Spend").length
  const countries = new Set(mapPoints.map((point) => point.country).filter(Boolean)).size

  const comparableDiffs = deals
    .filter((deal) => typeof deal.price_diff_pct === "number")
    .map((deal) => deal.price_diff_pct)
  const avgGap = comparableDiffs.length
    ? `${formatSignedPct(average(comparableDiffs), 1)} avg gap`
    : "No comparable gap yet"

  const cards = [
    {
      label: "Comparable now",
      value: String(comparableCount),
      detail: "Wines with a real retailer price comparison.",
    },
    {
      label: "Platinum cheaper",
      value: String(platinumCheaper),
      detail: "Bottles where Platinum currently beats Grand Cru.",
    },
    {
      label: "Strong spends",
      value: String(strongSpends),
      detail: "4.0+ wines with 100+ ratings and no Platinum markup.",
    },
    {
      label: "Map coverage",
      value: String(countries),
      detail: avgGap,
    },
  ]

  els.heroStats.innerHTML = cards
    .map(
      (card) => `
        <article class="stat-card">
          <span class="label">${escapeHtml(card.label)}</span>
          <span class="value">${escapeHtml(card.value)}</span>
          <p class="detail">${escapeHtml(card.detail)}</p>
        </article>
      `
    )
    .join("")
}

function renderDealMix(items) {
  renderBarList(els.dealMixChart, items, (item) => ({
    label: item.value,
    value: item.count,
    detail: `${item.count} wines`,
    tone: toneForCheaperSide(item.value),
  }))
}

function renderOfferingMix(items) {
  renderBarList(els.offeringChart, items, (item) => ({
    label: item.value,
    value: item.count,
    detail: `${item.platinum_cheaper_count} cheaper on Platinum`,
    tone: item.platinum_cheaper_count > item.grand_cru_cheaper_count ? "gain" : item.grand_cru_cheaper_count > 0 ? "loss" : "flat",
  }))
}

function renderCountryQuickFilters(items) {
  const buttons = [
    {
      label: "All countries",
      value: "",
      count: null,
      active: !state.country,
    },
    ...items.slice(0, 8).map((item) => ({
      label: item.value,
      value: item.value,
      count: item.count,
      active: state.country === item.value,
    })),
  ]

  els.countryQuickFilters.innerHTML = buttons
    .map((item) => renderQuickFilterButton(item, "country-filter"))
    .join("")
}

function renderStyleQuickFilters(items) {
  const preferredOrder = ["Red", "White", "Sparkling", "Champagne", "Sweet / Dessert", "Rose", "Orange"]
  const order = new Map(preferredOrder.map((value, index) => [value, index]))
  const rankForStyle = (value) => (order.has(value) ? order.get(value) : preferredOrder.length)
  const sorted = items
    .slice()
    .sort((left, right) => {
      const leftRank = rankForStyle(left.value)
      const rightRank = rankForStyle(right.value)
      if (leftRank !== rightRank) {
        return leftRank - rightRank
      }
      if (leftRank === preferredOrder.length && rightRank === preferredOrder.length) {
        return left.value.localeCompare(right.value)
      }
      return right.count - left.count || left.value.localeCompare(right.value)
    })

  const buttons = [
    {
      label: "All styles",
      value: "",
      count: null,
      active: !state.styleFamily,
    },
    ...sorted.map((item) => ({
      label: item.value,
      value: item.value,
      count: item.count,
      active: state.styleFamily === item.value,
    })),
  ]

  els.styleQuickFilters.innerHTML = buttons
    .map((item) => renderQuickFilterButton(item, "style-filter"))
    .join("")
}

function renderQuickFilterButton(item, attribute) {
  const countCopy = item.count ? `<span>${formatInteger(item.count)}</span>` : ""
  return `
    <button
      class="quick-filter-button${item.active ? " is-active" : ""}"
      type="button"
      data-${attribute}="${escapeHtml(item.value)}"
    >
      ${escapeHtml(item.label)}
      ${countCopy}
    </button>
  `
}

function renderRegionGuide(deals) {
  const grouped = new Map()

  deals.forEach((deal) => {
    if (!deal.country) {
      return
    }

    const countryKey = deal.country
    const regionKey = deal.region || ""
    if (!grouped.has(countryKey)) {
      grouped.set(countryKey, { deals: [], regions: new Map() })
    }

    const countryBucket = grouped.get(countryKey)
    countryBucket.deals.push(deal)
    if (!countryBucket.regions.has(regionKey)) {
      countryBucket.regions.set(regionKey, [])
    }
    countryBucket.regions.get(regionKey).push(deal)
  })

  const countries = Array.from(grouped.entries())
    .map(([country, payload]) => ({ country, deals: payload.deals, regions: payload.regions }))
    .sort((left, right) => right.deals.length - left.deals.length || left.country.localeCompare(right.country))

  if (!countries.length) {
    els.regionGuide.innerHTML = `<div class="empty-state">Origin grouping is still thin for the current filter set.</div>`
    return
  }

  els.regionGuide.innerHTML = countries
    .map(({ country, deals: countryDeals, regions }) => {
      const platinumCheaper = countryDeals.filter((deal) => deal.cheaper_side === "Platinum Cheaper").length
      const regionButtons = Array.from(regions.entries())
        .sort((left, right) => right[1].length - left[1].length || left[0].localeCompare(right[0]))
        .map(([region, regionDeals]) => {
          const regionLabel = region || "Region unknown"
          const isActive = Boolean(region) && state.country === country && state.region === region
          return `
            <button
              class="region-pill${isActive ? " is-active" : ""}${region ? "" : " is-muted"}"
              type="button"
              data-country-pick="${escapeHtml(country)}"
              data-region-pick="${escapeHtml(region)}"
              ${region ? "" : "data-region-disabled=true"}
            >
              ${escapeHtml(regionLabel)}
              <span>${formatInteger(regionDeals.length)}</span>
            </button>
          `
        })
        .join("")

      return `
        <article class="country-card">
          <div class="country-card-head">
            <div>
              <p class="eyebrow">${escapeHtml(country)}</p>
              <h3>${formatInteger(countryDeals.length)} wines in view</h3>
            </div>
            <button
              class="country-jump${state.country === country && !state.region ? " is-active" : ""}"
              type="button"
              data-country-pick="${escapeHtml(country)}"
              data-region-pick=""
            >
              ${state.country === country && !state.region ? "Showing" : "View all"}
            </button>
          </div>
          <div class="family-stats">
            <span class="meta-chip">${formatInteger(countryDeals.length)} wines</span>
            <span class="meta-chip">${formatInteger(platinumCheaper)} cheaper</span>
            <span class="meta-chip">${formatInteger(regions.size)} regions</span>
          </div>
          <div class="region-pill-grid">
            ${regionButtons}
          </div>
        </article>
      `
    })
    .join("")
}

function renderBarList(container, items, mapper) {
  if (!items.length) {
    container.innerHTML = `<div class="empty-state">Nothing to chart for the current filter set.</div>`
    return
  }

  const maxValue = Math.max(...items.map((item) => mapper(item).value), 1)
  container.innerHTML = items
    .slice(0, 6)
    .map((item) => {
      const row = mapper(item)
      const width = Math.max(8, (row.value / maxValue) * 100)
      return `
        <div class="bar-row">
          <div class="bar-copy">
            <strong>${escapeHtml(row.label)}</strong>
            <span>${escapeHtml(row.detail)}</span>
          </div>
          <div class="bar-track">
            <span class="bar-fill ${escapeHtml(row.tone)}" style="width:${width}%"></span>
          </div>
        </div>
      `
    })
    .join("")
}

function renderTopPicks(deals) {
  const picks = groupDealsIntoFamilies(deals)
    .slice()
    .sort((left, right) => right.familyScore - left.familyScore || left.title.localeCompare(right.title))
    .slice(0, 3)

  if (!picks.length) {
    els.topPicks.innerHTML = `<div class="empty-state">No picks match the current filters yet.</div>`
    return
  }

  els.topPicks.innerHTML = picks
    .map((family) => {
      const deal = family.bestOffer
      const verdict = resolveVerdict(deal)
      return `
        <article class="pick-card">
          <div class="pick-card-top">
            <div class="pick-head">
              <span class="verdict-chip ${verdict.tone}">${escapeHtml(compactVerdictLabel(verdict.label))}</span>
              <h3>${escapeHtml(family.title)}</h3>
              <p class="panel-note">${escapeHtml(familyOriginLine(family))}</p>
            </div>
            ${renderRatingBadge(deal)}
          </div>
          <div class="pick-meta">
            <span class="meta-chip">${escapeHtml(family.styleLabel)}</span>
            ${family.vintageLabel ? `<span class="meta-chip">${escapeHtml(family.vintageLabel)}</span>` : ""}
          </div>
          <div class="pick-price-row">
            <div class="pick-meta">
              <span class="meta-chip">Platinum</span>
              <strong class="pick-price">${formatMoney(deal.price_platinum)}</strong>
            </div>
            <span class="pill ${gapTone(deal)}">${escapeHtml(gapShortCopy(deal))}</span>
          </div>
          <div class="pick-actions">
            ${actionLink(deal.platinum_url, "Buy on Platinum", "primary")}
            ${actionLink(deal.grand_cru_url, "Compare Grand Cru")}
            ${actionLink(deal.vivino_url, "See Vivino")}
          </div>
        </article>
      `
    })
    .join("")
}

function renderStyleGroups(deals) {
  const groups = groupDealsIntoStyleGroups(deals)

  if (!groups.length) {
    els.familyBoard.innerHTML = `<div class="empty-state">No style groups match the current filter combination.</div>`
    return
  }

  els.familyBoard.innerHTML = groups
    .map((group) => {
      const best = group.bestOffer
      const topRows = group.topOffers
        .map((offer) => {
          const offerVerdict = resolveVerdict(offer)
          return `
            <article class="offer-variant">
              <div class="offer-variant-head">
                <div>
                  <strong>${escapeHtml(offer.wine_name)}</strong>
                  <div class="cell-subline">${escapeHtml(familyOriginLine(offer))}</div>
                </div>
                ${renderInlineRatingBadge(offer)}
              </div>
              <div class="offer-variant-metrics">
                <span class="pill ghost">${formatMoney(offer.price_platinum)}</span>
                <span class="pill ${gapTone(offer)}">${escapeHtml(gapShortCopy(offer))}</span>
                <span class="meta-chip">${escapeHtml(compactVerdictLabel(offerVerdict.label))}</span>
              </div>
              <div class="offer-variant-links">
                ${actionLink(offer.platinum_url, "Buy on Platinum", "primary")}
                ${actionLink(offer.grand_cru_url, "Compare Grand Cru")}
                ${actionLink(offer.vivino_url, "See Vivino")}
              </div>
            </article>
          `
        })
        .join("")

      const regionChips = group.topRegions
        .map((item) => `<span class="meta-chip">${escapeHtml(item.label)} ${formatInteger(item.count)}</span>`)
        .join("")

      const actionButton = state.styleFamily === group.styleLabel
        ? `<button class="link-chip secondary is-compact" type="button" data-style-group-clear="true">Show all styles</button>`
        : `<button class="link-chip primary is-compact" type="button" data-style-group-filter="${escapeHtml(group.styleLabel)}">Filter ${escapeHtml(group.styleLabel)}</button>`

      return `
        <details class="family-card">
          <summary class="family-summary">
            <div class="family-copy">
              <div class="pick-meta">
                <span class="verdict-chip calm">${escapeHtml(group.styleLabel)}</span>
                <span class="meta-chip">${formatInteger(group.wineCount)} wines</span>
                <span class="meta-chip">${formatInteger(group.platinumCheaperCount)} cheaper</span>
              </div>
              <h3>${escapeHtml(group.styleLabel)}</h3>
              <p class="panel-note">${escapeHtml(group.topCountries.join(" · ") || "Mixed origins")}</p>
            </div>
            <div class="family-aside">
              <div class="style-lead">
                <span class="style-lead-label">Top pick</span>
                <strong class="style-lead-name">${escapeHtml(best.wine_name)}</strong>
              </div>
              <span class="family-toggle meta-chip">
                <span class="toggle-closed">${escapeHtml(`Show ${Math.min(group.topOffers.length, 3)} picks`)}</span>
                <span class="toggle-open">Hide picks</span>
              </span>
              ${actionButton}
            </div>
          </summary>
          <div class="family-body">
            <div class="family-stats">
              ${regionChips}
            </div>
            <div class="offer-variant-grid">
              ${topRows}
            </div>
          </div>
        </details>
      `
    })
    .join("")
}

function renderMap(points) {
  const map = ensureOriginMap()
  const groupedPoints = groupMapPointsByCountry(points)
  const selectedCountry = state.country || mapFocusCountry
  const selected = groupedPoints.find((point) => point.country === selectedCountry) || null

  if (!map) {
    els.mapSelection.textContent = "The map is unavailable right now."
    return
  }

  latestMapPoints = points
  els.clearMapFocus.hidden = !mapFocusCountry
  originMarkerLayer.clearLayers()

  if (!groupedPoints.length) {
    els.mapSelection.textContent = "No mapped origins are available for the current filter set."
    fitCountryMap(map, groupedPoints)
    return
  }

  const largestCount = Math.max(...groupedPoints.map((point) => point.wineCount), 1)

  groupedPoints.forEach((point) => {
    const isSelected = selected ? selected.country === point.country : false
    const radius = Math.max(10, 9 + (point.wineCount / largestCount) * 16)
    const marker = window.L.circleMarker([point.originLatitude, point.originLongitude], {
      radius,
      fillColor: point.isEuropean ? "#60a5fa" : "#d6b066",
      fillOpacity: isSelected ? 0.98 : 0.86,
      color: isSelected ? "#dbeafe" : "#060608",
      weight: isSelected ? 3 : 2,
    })

    marker.bindTooltip(
      `${point.country} · ${formatInteger(point.wineCount)} wine${point.wineCount === 1 ? "" : "s"}`,
      {
        direction: "top",
        offset: [0, -10],
        className: "country-tooltip",
      }
    )

    marker.on("click", () => {
      mapFocusCountry = mapFocusCountry === point.country ? "" : point.country
      renderMap(points)
    })

    marker.addTo(originMarkerLayer)
  })

  fitCountryMap(map, groupedPoints)
  renderMapSelection(selected)
}

function renderMapSelection(point) {
  if (!point) {
    els.mapSelection.textContent = "Click a country bubble to inspect it."
    return
  }

  const avgGapCopy = point.average_price_diff_pct === null ? null : `${formatSignedPct(point.average_price_diff_pct, 1)} avg gap`
  const isCountryFiltered = state.country === point.country && !state.region
  const hasOriginFilter = Boolean(state.country || state.region)

  els.mapSelection.innerHTML = `
    <div class="map-selection-head">
      <h3>${escapeHtml(point.country)}</h3>
      ${
        hasOriginFilter
          ? `
            <div class="map-selection-actions">
              ${isCountryFiltered ? `<span class="meta-chip">Filtered</span>` : ""}
              <button class="link-chip secondary is-compact" type="button" data-map-clear-filter="true">Show all countries</button>
            </div>
          `
          : `<button class="link-chip primary is-compact" type="button" data-map-country-filter="${escapeHtml(point.country)}">Filter country</button>`
      }
    </div>
    <div class="map-selection-stats">
      <span class="meta-chip">${formatInteger(point.wineCount)} wines</span>
      <span class="meta-chip">${formatInteger(point.platinumCheaperCount)} cheaper on Platinum</span>
      <span class="meta-chip">${formatInteger(point.regionLabels.length)} regions</span>
      ${avgGapCopy ? `<span class="meta-chip">${escapeHtml(avgGapCopy)}</span>` : ""}
    </div>
  `
}

function ensureOriginMap() {
  if (!els.originMap || !window.L) {
    return null
  }

  if (!originMap) {
    originMap = window.L.map(els.originMap, {
      zoomControl: true,
      scrollWheelZoom: false,
      worldCopyJump: true,
      zoomSnap: 0.1,
      zoomDelta: 0.25,
      minZoom: 0.6,
      maxZoom: 5.5,
    }).setView([18, 18], 0.9)

    window.L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
      maxZoom: 19,
      subdomains: "abcd",
    }).addTo(originMap)

    originMarkerLayer = window.L.layerGroup().addTo(originMap)
  }

  window.setTimeout(() => originMap.invalidateSize(), 0)
  return originMap
}

function groupMapPointsByCountry(points) {
  const grouped = new Map()

  points.forEach((point) => {
    const country = (point.country || "").trim()
    if (!country) {
      return
    }

    if (!grouped.has(country)) {
      grouped.set(country, {
        country,
        totalWeight: 0,
        latitudeWeight: 0,
        longitudeWeight: 0,
        wineCount: 0,
        platinumCheaperCount: 0,
        avgGapTotal: 0,
        avgGapWeight: 0,
        sampleWines: [],
        regionLabels: new Set(),
      })
    }

    const bucket = grouped.get(country)
    const weight = Math.max(Number(point.wine_count) || 0, 1)
    bucket.totalWeight += weight
    bucket.latitudeWeight += point.origin_latitude * weight
    bucket.longitudeWeight += point.origin_longitude * weight
    bucket.wineCount += Number(point.wine_count) || 0
    bucket.platinumCheaperCount += Number(point.platinum_cheaper_count) || 0
    if (typeof point.average_price_diff_pct === "number") {
      bucket.avgGapTotal += point.average_price_diff_pct * weight
      bucket.avgGapWeight += weight
    }
    ;(point.sample_wines || []).forEach((wine) => {
      if (bucket.sampleWines.length < 5 && !bucket.sampleWines.includes(wine)) {
        bucket.sampleWines.push(wine)
      }
    })
    if (point.region) {
      bucket.regionLabels.add(point.region)
    }
  })

  return Array.from(grouped.values())
    .map((bucket) => ({
      country: bucket.country,
      originLatitude: bucket.latitudeWeight / bucket.totalWeight,
      originLongitude: bucket.longitudeWeight / bucket.totalWeight,
      wineCount: bucket.wineCount,
      platinumCheaperCount: bucket.platinumCheaperCount,
      average_price_diff_pct: bucket.avgGapWeight ? bucket.avgGapTotal / bucket.avgGapWeight : null,
      sampleWines: bucket.sampleWines,
      regionLabels: Array.from(bucket.regionLabels).sort(),
      isEuropean: isEuropeanCountry(bucket.country),
    }))
    .sort((left, right) => right.wineCount - left.wineCount || left.country.localeCompare(right.country))
}

function fitCountryMap(map, points) {
  if (!points.length) {
    map.setView([18, 18], 0.9)
    return
  }

  const bounds = points.map((point) => [point.originLatitude, point.originLongitude])

  if (!state.country && !state.region) {
    map.fitBounds(bounds, {
      padding: [28, 28],
      maxZoom: 0.9,
    })
    return
  }

  if (bounds.length === 1) {
    map.setView(bounds[0], 2.9)
    return
  }

  map.fitBounds(bounds, {
    padding: [44, 44],
    maxZoom: 2.8,
  })
}

function renderTable(deals) {
  if (!deals.length) {
    els.dealTableBody.innerHTML = `
      <tr>
        <td colspan="8">
          <div class="empty-state">No wines match the current filter combination.</div>
        </td>
      </tr>
    `
    return
  }

  els.dealTableBody.innerHTML = deals
    .map((deal) => {
      const verdict = resolveVerdict(deal)
      const styleLabel = deal.style_family || deal.wine_type || "Unclassified"
      const subtypeLabel = deal.wine_type && deal.wine_type !== styleLabel ? ` · ${deal.wine_type}` : ""
      return `
        <tr class="deal-row">
          <td>
            <div class="wine-title">${escapeHtml(deal.wine_name)}</div>
            <div class="wine-subline">${escapeHtml(deal.producer || "Producer unknown")}</div>
            <div class="wine-links">
              ${actionLink(deal.platinum_url, "Buy on Platinum", "primary")}
              ${actionLink(deal.grand_cru_url, "Compare Grand Cru")}
              ${actionLink(deal.vivino_url, "See Vivino")}
            </div>
          </td>
          <td>
            <span class="verdict-chip ${verdict.tone}">${escapeHtml(verdict.label)}</span>
            <div class="cell-subline">${escapeHtml(verdict.detail)}</div>
            ${deal.has_competitor_match ? "" : `<div class="cell-subline">No direct retailer comp yet.</div>`}
          </td>
          <td>
            <div class="cell-subline">
              <strong>${escapeHtml(deal.region || "Unknown region")}</strong><br>
              ${escapeHtml(deal.country || "Unknown country")}<br>
              ${escapeHtml(styleLabel)}${escapeHtml(subtypeLabel)} · ${escapeHtml(deal.grapes || "Grape blend unknown")}
            </div>
            <div class="cell-subline">${escapeHtml(metadataNarrative(deal))}</div>
          </td>
          <td>
            <div class="price-stack">
              <span class="money">${formatMoney(deal.price_platinum)}</span>
              <span class="muted">Grand Cru ${formatMoney(deal.price_grand_cru)}</span>
            </div>
            <div class="trend-list">
              ${renderTrendChip("P 7d", deal.price_platinum_change_7d, deal.platinum_trend_7d)}
              ${renderTrendChip("G 7d", deal.price_grand_cru_change_7d, deal.grand_cru_trend_7d)}
            </div>
          </td>
          <td>
            <div class="gap-figure ${gapTone(deal)}">${escapeHtml(gapDisplay(deal))}</div>
            <div class="cell-subline">${escapeHtml(gapNarrative(deal))}</div>
          </td>
          <td>
            <div class="rating-stack">
              <span class="money">${deal.vivino_rating != null ? deal.vivino_rating.toFixed(1) : "-"}</span>
              <span class="muted">${deal.vivino_num_ratings ? `${formatInteger(deal.vivino_num_ratings)} ratings` : "low confidence"}</span>
            </div>
            <div class="cell-subline">${escapeHtml(qualityNarrative(deal))}</div>
          </td>
          <td>
            <div class="score-stack">
              <span class="money">${deal.deal_score.toFixed(1)}</span>
              <div class="score-bar"><span style="width:${Math.max(6, Math.min(100, deal.deal_score))}%"></span></div>
            </div>
          </td>
          <td>
            <div class="offer-stack">
              <span class="pill ghost">${escapeHtml(deal.offering_type || "Unknown offer")}</span>
              <span class="muted">${escapeHtml(deal.volume || "-")} · ${escapeHtml(String(deal.quantity || 1))} bottle${deal.quantity > 1 ? "s" : ""}</span>
            </div>
          </td>
        </tr>
      `
    })
    .join("")
}

function groupDealsIntoFamilies(deals) {
  const families = new Map()

  deals.forEach((deal) => {
    const descriptor = describeWineFamily(deal)
    if (!families.has(descriptor.key)) {
      families.set(descriptor.key, {
        ...descriptor,
        offers: [],
      })
    }
    families.get(descriptor.key).offers.push(deal)
  })

  return Array.from(families.values())
    .map((family) => {
      const offers = family.offers
        .slice()
        .sort((left, right) => {
          return (
            recommendationScore(right) - recommendationScore(left) ||
            compareNumbersAsc(left.price_platinum, right.price_platinum) ||
            compareNumbersDesc(left.price_diff_pct_abs, right.price_diff_pct_abs) ||
            (left.offering_type || "").localeCompare(right.offering_type || "")
          )
        })
      const bestOffer = offers[0]
      const platinumCheaperCount = offers.filter((offer) => offer.cheaper_side === "Platinum Cheaper").length
      const comparableCount = offers.filter((offer) => offer.has_competitor_match).length
      const prices = offers.map((offer) => offer.price_platinum).filter((value) => typeof value === "number")
      const vintages = Array.from(new Set(offers.map((offer) => formatVintage(offer.vintage)))).filter(Boolean)
      const priceBand = prices.length
        ? `${formatMoney(Math.min(...prices))}${prices.length > 1 && Math.min(...prices) !== Math.max(...prices) ? ` to ${formatMoney(Math.max(...prices))}` : ""}`
        : null

      return {
        ...family,
        offers,
        bestOffer,
        platinumCheaperCount,
        comparableCount,
        priceBand,
        vintageLabel: vintages.join(", "),
        familyScore: recommendationScore(bestOffer) + Math.min(offers.length, 4),
      }
    })
    .sort((left, right) => right.familyScore - left.familyScore || left.title.localeCompare(right.title))
}

function groupDealsIntoStyleGroups(deals) {
  const preferredOrder = ["Red", "White", "Champagne", "Sparkling", "Rose", "Sweet / Dessert", "Orange", "Unclassified"]
  const order = new Map(preferredOrder.map((value, index) => [value, index]))
  const groups = new Map()

  deals.forEach((deal) => {
    const styleLabel = deal.style_family || deal.wine_type || "Unclassified"
    if (!groups.has(styleLabel)) {
      groups.set(styleLabel, {
        styleLabel,
        offers: [],
        countries: new Map(),
        regions: new Map(),
      })
    }

    const bucket = groups.get(styleLabel)
    bucket.offers.push(deal)

    const country = deal.country || "Unknown"
    bucket.countries.set(country, (bucket.countries.get(country) || 0) + 1)

    const region = deal.region || "Unknown region"
    bucket.regions.set(region, (bucket.regions.get(region) || 0) + 1)
  })

  return Array.from(groups.values())
    .map((group) => {
      const offers = group.offers
        .slice()
        .sort((left, right) => {
          return (
            recommendationScore(right) - recommendationScore(left) ||
            compareNumbersAsc(left.price_platinum, right.price_platinum) ||
            compareNumbersDesc(left.vivino_rating, right.vivino_rating) ||
            left.wine_name.localeCompare(right.wine_name)
          )
        })

      const wineFamilies = groupDealsIntoFamilies(offers)
      const bestOffer = wineFamilies[0]?.bestOffer || offers[0]
      const sortedCountries = Array.from(group.countries.entries())
        .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
      const sortedRegions = Array.from(group.regions.entries())
        .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))

      return {
        styleLabel: group.styleLabel,
        offers,
        bestOffer,
        wineCount: wineFamilies.length,
        topOffers: wineFamilies.slice(0, 3).map((family) => family.bestOffer),
        platinumCheaperCount: wineFamilies.filter((family) => family.offers.some((offer) => offer.cheaper_side === "Platinum Cheaper")).length,
        countryCount: sortedCountries.length,
        topCountries: sortedCountries.slice(0, 3).map(([label]) => label),
        topRegions: sortedRegions.slice(0, 4).map(([label, count]) => ({ label, count })),
      }
    })
    .sort((left, right) => {
      const leftRank = order.has(left.styleLabel) ? order.get(left.styleLabel) : preferredOrder.length
      const rightRank = order.has(right.styleLabel) ? order.get(right.styleLabel) : preferredOrder.length
      if (leftRank !== rightRank) {
        return leftRank - rightRank
      }
      return right.wineCount - left.wineCount || left.styleLabel.localeCompare(right.styleLabel)
    })
}

function describeWineFamily(deal) {
  const producer = (deal.producer || "").trim()
  const labelName = (deal.label_name || fallbackLabelName(deal)).trim()
  const country = deal.country || ""
  const region = deal.region || ""
  const styleLabel = deal.style_family || deal.wine_type || "Unclassified"
  const title = [producer, labelName].filter(Boolean).join(" ").trim() || deal.wine_name
  const key = [
    producer.toLowerCase(),
    labelName.toLowerCase(),
    region.toLowerCase(),
    country.toLowerCase(),
  ].join("|")

  return {
    key,
    title,
    producer,
    labelName,
    country,
    region,
    styleLabel,
    grapes: deal.grapes || null,
  }
}

function fallbackLabelName(deal) {
  const parts = String(deal.wine_name || "")
    .split(" - ")
    .map((part) => part.trim())
    .filter(Boolean)
  const colorIndex = parts.findIndex((part) => ["red", "white", "rose", "orange"].includes(part.toLowerCase()))
  const body = colorIndex >= 0 ? parts.slice(0, colorIndex) : parts
  if (!body.length) {
    return deal.wine_name || "Wine"
  }

  const trimmedFirst = body[0].replace(/^(?:19|20)\d{2}\s+|^nv\s+/i, "").trim()
  const normalized = [trimmedFirst, ...body.slice(1)]
  if (normalized.length >= 2) {
    return normalized[normalized.length - 1]
  }
  if (deal.producer && normalized[0].toLowerCase() === String(deal.producer).trim().toLowerCase()) {
    return deal.wine_name || "Wine"
  }
  return normalized[0]
}

function volumeQuantityCopy(offer) {
  const quantity = Number(offer.quantity || 1)
  return `${formatVintage(offer.vintage)} · ${offer.volume || "-"} · ${quantity} bottle${quantity > 1 ? "s" : ""}`
}

function formatVintage(vintage) {
  return vintage || "NV"
}

function compareNumbersAsc(left, right) {
  const leftIsNumber = typeof left === "number"
  const rightIsNumber = typeof right === "number"
  if (leftIsNumber && rightIsNumber) {
    if (left === right) {
      return 0
    }
    return left < right ? -1 : 1
  }
  if (leftIsNumber) {
    return -1
  }
  if (rightIsNumber) {
    return 1
  }
  return 0
}

function compareNumbersDesc(left, right) {
  const result = compareNumbersAsc(right, left)
  if (result === 0) {
    return 0
  }
  return result
}

function renderActiveFilters() {
  const chips = []

  if (state.search) chips.push({ label: `Search: ${state.search}`, reset: "search" })
  if (state.country) chips.push({ label: `Country: ${state.country}`, reset: "country" })
  if (state.region) chips.push({ label: `Region: ${state.region}`, reset: "region" })
  if (state.wineType) chips.push({ label: `Type: ${state.wineType}`, reset: "wineType" })
  if (state.styleFamily) chips.push({ label: `Style: ${state.styleFamily}`, reset: "styleFamily" })
  if (state.grape) chips.push({ label: `Grape: ${state.grape}`, reset: "grape" })
  if (state.offeringType) chips.push({ label: `Offering: ${state.offeringType}`, reset: "offeringType" })
  if (state.producer) chips.push({ label: `Producer: ${state.producer}`, reset: "producer" })
  if (state.comparableOnly) chips.push({ label: "Comparable only", reset: "comparableOnly" })
  if (state.onlyPlatinumCheaper) chips.push({ label: "Platinum cheaper only", reset: "onlyPlatinumCheaper" })
  if (state.minVivinoRating) chips.push({ label: `Rating ${state.minVivinoRating}+`, reset: "minVivinoRating" })
  if (state.minVivinoNumRatings) chips.push({ label: `${state.minVivinoNumRatings}+ ratings`, reset: "minVivinoNumRatings" })

  if (!chips.length) {
    els.activeFilters.innerHTML = ""
    return
  }

  els.activeFilters.innerHTML = chips
    .map(
      (chip) => `
        <span class="filter-chip">
          ${escapeHtml(chip.label)}
          <button type="button" data-reset="${escapeHtml(chip.reset)}" aria-label="Remove filter">x</button>
        </span>
      `
    )
    .join("")
}

function renderErrorState(error) {
  const message = error instanceof Error ? error.message : "The dashboard could not load."
  els.resultsMeta.textContent = "Something broke while loading the board."
  els.mapSelection.textContent = message
  els.topPicks.innerHTML = `<div class="empty-state">${escapeHtml(message)}</div>`
  els.familyBoard.innerHTML = `<div class="empty-state">${escapeHtml(message)}</div>`
  els.regionGuide.innerHTML = `<div class="empty-state">${escapeHtml(message)}</div>`
  els.dealTableBody.innerHTML = `
    <tr>
      <td colspan="8">
        <div class="empty-state">${escapeHtml(message)}</div>
      </td>
    </tr>
  `
}

function resetFilter(key) {
  switch (key) {
    case "search":
      state.search = ""
      break
    case "country":
      state.country = ""
      state.region = ""
      break
    case "region":
      state.region = ""
      break
    case "wineType":
      state.wineType = ""
      break
    case "styleFamily":
      state.styleFamily = ""
      break
    case "grape":
      state.grape = ""
      break
    case "offeringType":
      state.offeringType = ""
      break
    case "producer":
      state.producer = ""
      break
    case "comparableOnly":
      state.comparableOnly = false
      break
    case "onlyPlatinumCheaper":
      state.onlyPlatinumCheaper = false
      break
    case "minVivinoRating":
      state.minVivinoRating = ""
      break
    case "minVivinoNumRatings":
      state.minVivinoNumRatings = ""
      break
    default:
      break
  }
}

function resolveVerdict(deal) {
  if (deal.value_verdict && deal.value_verdict_tone) {
    return {
      label: deal.value_verdict,
      tone: deal.value_verdict_tone,
      detail: deal.value_verdict_reason || "",
    }
  }

  if (deal.cheaper_side === "Platinum Cheaper" && (deal.vivino_rating || 0) >= 4 && (deal.vivino_num_ratings || 0) >= 100) {
    return {
      label: "Strong Credit Spend",
      tone: "good",
      detail: "Good wine, healthy rating count, and no obvious Platinum markup.",
    }
  }
  if (deal.cheaper_side === "Platinum Cheaper") {
    return {
      label: "Solid Value",
      tone: "good",
      detail: "Platinum currently beats Grand Cru on price.",
    }
  }
  if (deal.cheaper_side === "Same Price" && (deal.vivino_rating || 0) >= 4.1) {
    return {
      label: "Quality Buy",
      tone: "calm",
      detail: "Not cheaper, but still appealing if you want the bottle on Platinum.",
    }
  }
  if (deal.cheaper_side === "Grand Cru Cheaper") {
    return {
      label: "Platinum Markup",
      tone: "warn",
      detail: "Grand Cru is the better pure price play right now.",
    }
  }
  if (deal.cheaper_side === "No Match") {
    return {
      label: "Quality Only",
      tone: "ghost",
      detail: "Interesting wine, but there is no retailer comparison yet.",
    }
  }
  return {
    label: "Needs Review",
    tone: "ghost",
    detail: "Worth a manual look before spending credits.",
  }
}

function compactVerdictLabel(label) {
  switch (label) {
    case "Strong Credit Spend":
      return "Top Value"
    case "Solid Value":
      return "Good Value"
    case "Quality Buy":
      return "Worth A Look"
    case "Platinum Markup":
      return "Markup"
    case "Quality Only":
      return "No Comp"
    case "Needs Review":
      return "Review"
    default:
      return label
  }
}

function recommendationScore(deal) {
  let score = deal.deal_score || 0
  if (deal.cheaper_side === "Platinum Cheaper") score += 20
  if (deal.cheaper_side === "Grand Cru Cheaper") score -= 18
  if ((deal.vivino_rating || 0) >= 4.2) score += 8
  if ((deal.vivino_num_ratings || 0) >= 500) score += 4
  if (deal.offering_type === "Magnum") score += 2
  return score
}

function gapDisplay(deal) {
  if (deal.price_diff_pct == null) {
    return "No match"
  }
  return `${formatPct(deal.price_diff_pct_abs, 1)}`
}

function gapNarrative(deal) {
  if (deal.price_diff_pct == null) {
    return "No Grand Cru comparison available."
  }
  if (deal.cheaper_side === "Platinum Cheaper") {
    return `Platinum is cheaper by ${formatPct(deal.price_diff_pct_abs, 1)}.`
  }
  if (deal.cheaper_side === "Grand Cru Cheaper") {
    return `Grand Cru is cheaper by ${formatPct(deal.price_diff_pct_abs, 1)}.`
  }
  return "Both retailers are currently at the same price."
}

function grapeNarrative(deal) {
  const ratingCopy = deal.vivino_rating != null
    ? `${deal.vivino_rating.toFixed(1)} on Vivino${deal.vivino_num_ratings ? ` from ${formatInteger(deal.vivino_num_ratings)} ratings` : ""}`
    : "Vivino rating still thin"
  return `${deal.grapes || "Grape identity still fuzzy"} - ${ratingCopy}.`
}

function qualityNarrative(deal) {
  if (deal.is_good_wine && deal.is_high_confidence) {
    return "Good wine signal with a strong rating sample."
  }
  if (deal.is_good_wine) {
    return "Looks promising, but the rating sample is still light."
  }
  if (deal.vivino_rating != null) {
    return "Quality signal is present, but this is not an obvious standout."
  }
  return "Quality signal is still thin."
}

function metadataNarrative(deal) {
  const originCopy = deal.origin_confidence ? `Origin ${deal.origin_confidence}` : "Origin unknown"
  const grapeCopy = deal.grape_confidence ? `grapes ${deal.grape_confidence}` : "grapes unknown"
  return `${originCopy} via ${humanizeCode(deal.origin_source || "unknown source")} - ${grapeCopy}.`
}

function actionLink(url, label, tone = "secondary") {
  if (!url) {
    return ""
  }
  return `<a class="link-chip ${escapeHtml(tone)}" href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`
}

function gapTone(deal) {
  if (deal.cheaper_side === "Platinum Cheaper") return "gain"
  if (deal.cheaper_side === "Grand Cru Cheaper") return "loss"
  return "flat"
}

function renderTrendChip(label, change, direction) {
  if (direction === "unknown") {
    return `<span class="meta-chip"> ${escapeHtml(label)} no data </span>`
  }
  const tone = direction === "down" ? "good" : direction === "up" ? "warn" : "calm"
  const copy = direction === "flat"
    ? `${label} flat`
    : `${label} ${direction} ${formatMoney(Math.abs(change || 0))}`
  return `<span class="pill ${tone}">${escapeHtml(copy)}</span>`
}

function toneForCheaperSide(label) {
  if (label === "Platinum Cheaper") return "gain"
  if (label === "Grand Cru Cheaper") return "loss"
  return "flat"
}

function toneForAverage(value) {
  if (value == null) return "flat"
  if (value <= -3) return "gain"
  if (value >= 3) return "loss"
  return "flat"
}

async function fetchJson(path) {
  const response = await fetch(path, {
    headers: {
      Accept: "application/json",
    },
  })

  if (!response.ok) {
    throw new Error(`Request failed for ${path}: ${response.status}`)
  }

  return response.json()
}

function setLoading(isLoading) {
  ;[els.heroStats, els.dealMixChart, els.offeringChart, els.topPicks, els.familyBoard, els.regionGuide].forEach((element) => {
    element.classList.toggle("loading-sheen", isLoading)
  })
  if (els.originMap) {
    els.originMap.classList.toggle("loading-sheen", isLoading)
  }
  els.dealTableBody.classList.toggle("loading-sheen", isLoading)
}

function formatMoney(value) {
  if (value == null) {
    return "-"
  }
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(value)
}

function formatPct(value, digits = 1) {
  if (value == null) {
    return "-"
  }
  return `${Number(value).toFixed(digits)}%`
}

function formatSignedPct(value, digits = 1) {
  if (value == null) {
    return "-"
  }
  const formatted = Number(value).toFixed(digits)
  return `${value > 0 ? "+" : ""}${formatted}%`
}

function formatInteger(value) {
  return new Intl.NumberFormat("en-US").format(value)
}

function formatCompactInteger(value) {
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value)
}

function formatFormatCount(count) {
  return `${count} ${count === 1 ? "format" : "formats"}`
}

function familyOriginLine(family) {
  return [family.region || "Region unknown", family.country || "Country unknown"].join(", ")
}

function gapShortCopy(deal) {
  if (deal.price_diff_pct == null) {
    return "No direct comparison"
  }
  if (deal.cheaper_side === "Platinum Cheaper") {
    return `Platinum -${formatPct(deal.price_diff_pct_abs, 1)}`
  }
  if (deal.cheaper_side === "Grand Cru Cheaper") {
    return `Grand Cru -${formatPct(deal.price_diff_pct_abs, 1)}`
  }
  return "Same price"
}

function renderRatingBadge(deal) {
  if (deal.vivino_rating == null) {
    return `<div class="rating-badge is-empty"><span class="rating-value">-</span><span class="rating-caption">No rating</span></div>`
  }
  const countCopy = deal.vivino_num_ratings ? `${formatCompactInteger(deal.vivino_num_ratings)} ratings` : "Vivino"
  return `
    <div class="rating-badge">
      <span class="rating-value">${escapeHtml(deal.vivino_rating.toFixed(1))}</span>
      <span class="rating-caption">${escapeHtml(countCopy)}</span>
    </div>
  `
}

function renderInlineRatingBadge(deal) {
  if (deal.vivino_rating == null) {
    return `<span class="meta-chip">No rating</span>`
  }
  return `<span class="meta-chip">Vivino ${escapeHtml(deal.vivino_rating.toFixed(1))}</span>`
}

function humanizeCode(value) {
  return String(value)
    .replaceAll("_", " ")
    .replaceAll("-", " ")
    .replace(/\s+/g, " ")
    .trim()
}

function average(values) {
  if (!values.length) {
    return 0
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length
}

function isEuropeanCountry(country) {
  return EUROPEAN_COUNTRIES.has(country)
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;")
}
