// Origin map: one bubble per country from /deals/map, on OpenStreetMap tiles (Leaflet, served from /static).
// Picking a country, on the map or in the chips below, filters the offer list and lists its regions.
// The map and its tiles load only when the section comes near the screen.
const ORIGIN_TILES = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"

const origins = { map: null, layer: null, countries: [], serial: 0, fitted: false }

function originSlug(country) {
  return country.toLowerCase().replace(/[^a-z0-9]+/g, "-")
}

// The map ignores the country and region filters so every country stays pickable; the rest still apply.
function originParams() {
  const params = filterParams()
  params.delete("country")
  params.delete("region")
  return params
}

function groupOriginsByCountry(points) {
  const grouped = new Map()
  points.forEach((point) => {
    const country = (point.country || "").trim()
    if (!country) return
    if (!grouped.has(country)) grouped.set(country, { country, count: 0, cheaper: 0, lat: 0, lng: 0, weight: 0, regions: new Map() })
    const bucket = grouped.get(country)
    const count = Number(point.wine_count) || 0
    bucket.count += count
    bucket.cheaper += Number(point.platinum_cheaper_count) || 0
    const weight = Math.max(count, 1)
    bucket.lat += point.origin_latitude * weight
    bucket.lng += point.origin_longitude * weight
    bucket.weight += weight
    if (point.region) bucket.regions.set(point.region, (bucket.regions.get(point.region) || 0) + count)
  })
  return Array.from(grouped.values())
    .map((bucket) => ({
      country: bucket.country,
      count: bucket.count,
      cheaper: bucket.cheaper,
      lat: bucket.lat / bucket.weight,
      lng: bucket.lng / bucket.weight,
      regions: Array.from(bucket.regions, ([region, count]) => ({ region, count }))
        .sort((a, b) => b.count - a.count || a.region.localeCompare(b.region)),
    }))
    .sort((a, b) => b.count - a.count || a.country.localeCompare(b.country))
}

function setupOriginMap() {
  const element = document.getElementById("originMap")
  const panel = document.getElementById("originPanel")
  if (!element || !panel) return

  panel.addEventListener("click", (event) => {
    const country = event.target.closest("[data-country]")
    const region = event.target.closest("[data-region]")
    if (country) pickOrigin(country.dataset.country === state.country ? "" : country.dataset.country)
    else if (region) pickOrigin(state.country, region.dataset.region === state.region ? "" : region.dataset.region)
    else if (event.target.closest("[data-origin-clear]")) pickOrigin("")
    else if (event.target.closest("[data-origin-offers]")) {
      document.getElementById("offers").scrollIntoView({ behavior: prefersReducedMotion() ? "auto" : "smooth" })
    }
  })

  const observer = new IntersectionObserver((entries) => {
    if (!entries.some((entry) => entry.isIntersecting)) return
    observer.disconnect()
    if (!window.L) {
      panel.innerHTML = `<p class="origin-hint">The map could not load. Use the country filter above the offers instead.</p>`
      return
    }
    origins.map = window.L.map(element, {
      scrollWheelZoom: false,
      // On phones one finger scrolls the page; the zoom buttons and chips still work.
      dragging: !window.L.Browser.mobile,
      zoomSnap: 0.1,
      zoomDelta: 0.5,
      minZoom: 0.4,
      maxZoom: 6,
      maxBounds: [[-70, -180], [85, 180]],
      maxBoundsViscosity: 1,
    }).setView([25, 10], 1.2)
    window.L.tileLayer(ORIGIN_TILES, {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      maxZoom: 19,
      className: "muted-basemap",
    }).addTo(origins.map)
    origins.layer = window.L.layerGroup().addTo(origins.map)
    refreshOriginMap()
  }, { rootMargin: "300px" })
  observer.observe(element)
}

function pickOrigin(country, region = "") {
  state.country = country
  state.region = region
  syncControlsFromState()
  loadOffers()
}

async function refreshOriginMap() {
  if (!origins.map) return
  const id = ++origins.serial
  try {
    const points = await fetchJson(`/deals/map?${originParams()}`)
    if (id !== origins.serial) return
    origins.countries = groupOriginsByCountry(points)
    renderOriginMap()
  } catch (error) {
    if (id !== origins.serial) return
    console.error(error)
    document.getElementById("originPanel").innerHTML = `<p class="origin-hint">The map could not load. Use the country filter above the offers instead.</p>`
  }
}

function renderOriginMap() {
  const tokens = getComputedStyle(document.documentElement)
  const accent = tokens.getPropertyValue("--accent").trim()
  const ink = tokens.getPropertyValue("--text-2").trim()
  const paper = tokens.getPropertyValue("--surface").trim()
  const largest = Math.max(...origins.countries.map((c) => c.count), 1)

  origins.layer.clearLayers()
  origins.countries.forEach((c) => {
    const selected = c.country === state.country
    const marker = window.L.circleMarker([c.lat, c.lng], {
      radius: 8 + Math.sqrt(c.count / largest) * 16,
      className: `origin-marker origin-marker-${originSlug(c.country)}`,
      fillColor: selected ? accent : ink,
      fillOpacity: selected ? 0.9 : 0.65,
      color: paper,
      weight: selected ? 3 : 1.5,
    })
    marker.bindTooltip(`${c.country} · ${formatInteger(c.count)} ${plural(c.count, "offer", "offers")}`, {
      direction: "top",
      offset: [0, -8],
      className: "origin-tooltip",
    })
    marker.on("click", () => pickOrigin(c.country === state.country ? "" : c.country))
    marker.addTo(origins.layer)
    if (selected) marker.bringToFront()
  })
  if (!origins.fitted && origins.countries.length) {
    origins.map.fitBounds(window.L.latLngBounds(origins.countries.map((c) => [c.lat, c.lng])).pad(0.3), { maxZoom: 4 })
    origins.fitted = true
  }
  renderOriginPanel()
}

function renderOriginPanel() {
  const panel = document.getElementById("originPanel")
  if (!origins.countries.length) {
    panel.innerHTML = `<p class="origin-hint">No offers with a known origin match these filters.</p>`
    return
  }
  const chip = (attr, value, label, count, pressed) => `
    <button type="button" class="quick" ${attr}="${escapeHtml(value)}" aria-pressed="${pressed}">${escapeHtml(label)} <span class="origin-count">${formatInteger(count)}</span></button>`
  const countries = origins.countries.map((c) => chip("data-country", c.country, c.country, c.count, c.country === state.country)).join("")
  const selected = origins.countries.find((c) => c.country === state.country)

  panel.innerHTML = `
    <div class="origin-chips" role="group" aria-label="Countries">${countries}</div>
    ${selected ? `
      <div class="origin-selected">
        <h3>${escapeHtml(selected.country)}</h3>
        <p>${formatInteger(selected.count)} ${plural(selected.count, "offer", "offers")}${selected.cheaper ? `, ${formatInteger(selected.cheaper)} cheaper on Platinum than at Grand Cru` : ""}.</p>
        ${selected.regions.length ? `<div class="origin-chips" role="group" aria-label="Regions in ${escapeHtml(selected.country)}">
          ${selected.regions.map((r) => chip("data-region", r.region, r.region, r.count, r.region === state.region)).join("")}
        </div>` : ""}
        <div class="origin-actions">
          <button type="button" class="button" data-origin-offers>Show the offers</button>
          <button type="button" class="text-button" data-origin-clear>All countries</button>
        </div>
      </div>` : ""}
  `
}

document.addEventListener("DOMContentLoaded", setupOriginMap)
