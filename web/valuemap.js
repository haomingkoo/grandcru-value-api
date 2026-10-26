// Value map: every offer plotted by Platinum price per 750 ml (x, log) against Vivino rating (y).
// The frontier is the highest rating at or below each price among the offers in view.
const VM_NS = "http://www.w3.org/2000/svg"

const STYLE_GROUPS = [
  { key: "red", label: "Red", members: ["Red"], color: "var(--style-red)" },
  { key: "white", label: "White", members: ["White", "Orange"], color: "var(--style-white)" },
  { key: "rose", label: "Rosé", members: ["Rose"], color: "var(--style-rose)" },
  { key: "sparkling", label: "Sparkling", members: ["Sparkling", "Champagne"], color: "var(--style-sparkling)" },
]
const OTHER_GROUP = { key: "other", label: "Other", members: [], color: "var(--text-2)" }

function styleGroup(deal) {
  const style = deal.style_family || deal.wine_type || ""
  return STYLE_GROUPS.find((g) => g.members.includes(style)) || OTHER_GROUP
}

function vmEl(name, attrs = {}, parent) {
  const node = document.createElementNS(VM_NS, name)
  for (const [key, value] of Object.entries(attrs)) {
    if (value !== "" && value != null) node.setAttribute(key, value)
  }
  if (parent) parent.appendChild(node)
  return node
}

function vmEscape(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]))
}

function dotRadius(deal) {
  if (!deal.vivino_num_ratings) return 5.5
  return Math.min(11, Math.max(4, 3 + 1.7 * Math.log10(deal.vivino_num_ratings)))
}

// Highest rating at or below each price: a dot is on the frontier when nothing cheaper rates as high.
function computeFrontier(points) {
  const sorted = points.filter((p) => p.rating != null).sort((a, b) => a.price - b.price || b.rating - a.rating)
  const frontier = []
  let best = -Infinity
  for (const p of sorted) {
    if (p.rating > best) {
      frontier.push(p)
      best = p.rating
    }
  }
  return frontier
}

function fullName(deal) {
  const label = deal.label_name || deal.wine_name || ""
  const producer = deal.producer || ""
  return producer && !label.toLowerCase().startsWith(producer.toLowerCase()) ? `${producer} ${label}` : label
}

function shortName(deal) {
  const text = fullName(deal)
  return text.length > 28 ? `${text.slice(0, 26).trim()}…` : text
}

function sizeLabel(deal) {
  const qty = Math.max(deal.quantity || 1, 1)
  const ml = deal.volume_ml
  if (!ml) return deal.volume || ""
  const size = ml >= 1000 ? `${ml / 1000} L` : `${ml} ml`
  return qty > 1 ? `${qty} × ${size}` : size
}

class ValueMap {
  constructor(root, { onShowInList, onStyleToggle }) {
    this.root = root
    this.canvas = root.querySelector(".map-canvas")
    this.card = root.querySelector(".map-card")
    this.live = root.querySelector(".map-live")
    this.legend = root.querySelector(".map-legend")
    this.onShowInList = onShowInList
    this.onStyleToggle = onStyleToggle
    this.points = []
    this.visible = new Set()
    this.activeStyles = []
    this.current = null
    this.pinned = false
    this.drawnOnce = false
    this.canvas.addEventListener("pointermove", (e) => this.hoverAt(e))
    this.canvas.addEventListener("pointerleave", () => { if (!this.pinned && document.activeElement !== this.canvas) this.hideCard() })
    this.canvas.addEventListener("click", (e) => this.clickAt(e))
    this.canvas.addEventListener("keydown", (e) => this.onKey(e))
    this.canvas.addEventListener("focus", () => { if (!this.current) this.focusFirst(); else this.showCard(this.current, false) })
    this.canvas.addEventListener("blur", (e) => { if (!this.root.contains(e.relatedTarget)) { this.pinned = false; this.hideCard() } })
    this.card.addEventListener("click", (e) => {
      if (e.target.closest("[data-map-close]")) { this.pinned = false; this.hideCard(); this.canvas.focus() }
      if (e.target.closest("[data-map-show]") && this.current) this.onShowInList(this.current.deal)
    })
    this.card.addEventListener("keydown", (e) => { if (e.key === "Escape") { this.pinned = false; this.hideCard(); this.canvas.focus() } })
    this.legend.addEventListener("click", (e) => {
      const button = e.target.closest("[data-style-group]")
      if (button) this.onStyleToggle(STYLE_GROUPS.find((g) => g.key === button.dataset.styleGroup))
    })
    let resizeTimer = 0
    new ResizeObserver(() => {
      window.clearTimeout(resizeTimer)
      resizeTimer = window.setTimeout(() => this.draw(false), 120)
    }).observe(this.canvas)
  }

  setDeals(deals) {
    this.points = deals
      .filter((d) => d.price_platinum_per_750 != null)
      .map((d) => ({ deal: d, id: d.id, price: d.price_platinum_per_750, rating: d.vivino_rating, group: styleGroup(d), r: dotRadius(d), badgeOnly: d.vivino_rating != null && !d.vivino_num_ratings }))
    this.visible = new Set(this.points.map((p) => p.id))
    this.renderLegend()
    this.draw(true)
  }

  setVisible(ids, activeStyles) {
    this.visible = new Set(ids)
    this.activeStyles = activeStyles || []
    this.renderLegend()
    this.draw(false)
  }

  renderLegend() {
    const counts = new Map()
    this.points.forEach((p) => counts.set(p.group.key, (counts.get(p.group.key) || 0) + 1))
    const styles = STYLE_GROUPS.filter((g) => counts.get(g.key)).map((g) => {
      const pressed = this.activeStyles.length ? g.members.every((m) => this.activeStyles.includes(m)) : false
      return `<li><button type="button" class="legend-style" data-style-group="${g.key}" aria-pressed="${pressed}" style="--dot:${g.color}"><span class="legend-dot" aria-hidden="true"></span>${g.label}<span class="legend-count">${counts.get(g.key)}</span></button></li>`
    }).join("")
    this.legend.innerHTML = `
      <ul class="legend-styles" aria-label="Show one style">${styles}</ul>
      <ul class="legend-keys">
        <li><span class="key-size" aria-hidden="true"><i></i><i></i></span>Bigger dot, more Vivino ratings</li>
        <li><span class="key-hollow" aria-hidden="true"></span>Rating as shown on Platinum, count not given</li>
        <li><span class="key-frontier" aria-hidden="true"></span>Best rating at or below each price</li>
      </ul>
    `
  }

  layout() {
    const width = Math.max(300, Math.round(this.canvas.clientWidth))
    const narrow = width < 560
    const rated = this.points.filter((p) => p.rating != null)
    const unrated = this.points.filter((p) => p.rating == null)
    const height = narrow ? 380 : Math.min(520, Math.max(420, Math.round(width * 0.58)))
    const margin = { top: 14, right: narrow ? 14 : 24, bottom: 46, left: narrow ? 40 : 52 }
    const strip = unrated.length ? 58 : 0
    const plotBottom = height - margin.bottom - strip
    const prices = this.points.map((p) => p.price)
    const lo = Math.log(Math.min(...prices) * 0.82)
    const hi = Math.log(Math.max(...prices) * 1.18)
    const ratings = rated.map((p) => p.rating)
    // Vivino ratings cluster between 3.4 and 4.6; the axis never starts at zero.
    const yMin = Math.min(3.4, Math.floor((Math.min(...ratings) - 0.1) * 10) / 10)
    const yMax = Math.max(4.6, Math.ceil((Math.max(...ratings) + 0.1) * 10) / 10)
    const x = (v) => margin.left + ((Math.log(v) - lo) / (hi - lo)) * (width - margin.left - margin.right)
    const y = (v) => margin.top + (1 - (v - yMin) / (yMax - yMin)) * (plotBottom - margin.top)
    return { width, height, narrow, margin, strip, plotBottom, x, y, lo, hi, yMin, yMax, unrated }
  }

  draw(animate) {
    if (!this.points.length) return
    const L = this.layout()
    this.L = L
    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches
    const svg = vmEl("svg", { viewBox: `0 0 ${L.width} ${L.height}`, width: L.width, height: L.height, class: "map-svg", "aria-hidden": "true", focusable: "false" })

    const grid = vmEl("g", { class: "map-grid" }, svg)
    const yStep = L.narrow ? 0.4 : 0.2
    for (let v = Math.ceil(L.yMin / yStep) * yStep; v <= L.yMax + 1e-9; v += yStep) {
      const val = Math.round(v * 10) / 10
      vmEl("line", { x1: L.margin.left, x2: L.width - L.margin.right, y1: L.y(val), y2: L.y(val) }, grid)
      const t = vmEl("text", { x: L.margin.left - 8, y: L.y(val) + 4, "text-anchor": "end", class: "map-axis" }, grid)
      t.textContent = val.toFixed(1)
    }
    const ticks = [10, 15, 25, 50, 100, 200, 400, 800, 1600].filter((v) => Math.log(v) > L.lo && Math.log(v) < L.hi)
    ticks.forEach((v) => {
      vmEl("line", { x1: L.x(v), x2: L.x(v), y1: L.margin.top, y2: L.plotBottom, class: "map-vgrid" }, grid)
      const t = vmEl("text", { x: L.x(v), y: L.height - L.margin.bottom + 18, "text-anchor": "middle", class: "map-axis" }, grid)
      t.textContent = `S$${v}`
    })
    const xTitle = vmEl("text", { x: L.width - L.margin.right, y: L.height - 6, "text-anchor": "end", class: "map-title-axis" }, grid)
    xTitle.textContent = L.narrow ? "Platinum price per 750 ml" : "Platinum price per 750 ml (each step doubles)"
    const yTitle = vmEl("text", { x: L.margin.left, y: L.margin.top - 2, class: "map-title-axis", "text-anchor": "start" }, grid)
    yTitle.textContent = ""
    if (L.strip) {
      const top = L.plotBottom + 10
      vmEl("rect", { x: L.margin.left, y: top, width: L.width - L.margin.left - L.margin.right, height: L.strip - 14, rx: 6, class: "map-strip" }, grid)
      const label = vmEl("text", { x: L.margin.left + 8, y: top + 15, class: "map-strip-label" }, grid)
      label.textContent = `Not rated yet (${L.unrated.length})`
    }

    // Frontier over the offers currently in view.
    const inView = this.points.filter((p) => this.visible.has(p.id))
    const frontier = computeFrontier(inView)
    this.frontierIds = new Set(frontier.map((p) => p.id))
    if (frontier.length) {
      let d = `M${L.x(frontier[0].price).toFixed(1)},${L.y(frontier[0].rating).toFixed(1)}`
      for (let i = 1; i < frontier.length; i++) {
        d += `H${L.x(frontier[i].price).toFixed(1)}V${L.y(frontier[i].rating).toFixed(1)}`
      }
      d += `H${(L.width - L.margin.right).toFixed(1)}`
      const path = vmEl("path", { d, class: "map-frontier" }, svg)
      if (animate && !reduce) {
        const length = path.getTotalLength ? 2000 : 0
        path.style.strokeDasharray = `${length}`
        path.style.strokeDashoffset = `${length}`
        path.style.animation = "frontier-draw 900ms cubic-bezier(0.22, 1, 0.36, 1) 450ms forwards"
      }
    }

    const dots = vmEl("g", { class: "map-dots" }, svg)
    const order = this.points.slice().sort((a, b) => b.r - a.r)
    const prices = this.points.map((p) => p.price)
    const minPrice = Math.min(...prices)
    const maxPrice = Math.max(...prices)
    order.forEach((p) => {
      const cy = p.rating != null ? L.y(p.rating) : L.plotBottom + 10 + L.strip - 14 - 13
      p.cx = L.x(p.price)
      p.cy = cy
      const shown = this.visible.has(p.id)
      const onFrontier = this.frontierIds.has(p.id)
      const g = vmEl("g", { class: `map-dot${shown ? "" : " is-dim"}${onFrontier ? " is-frontier" : ""}`, "data-id": p.id }, dots)
      if (animate && !reduce) {
        const delay = Math.round(((Math.log(p.price) - Math.log(minPrice)) / (Math.log(maxPrice) - Math.log(minPrice) || 1)) * 380)
        g.style.animation = `dot-in 420ms cubic-bezier(0.22, 1, 0.36, 1) ${delay}ms both`
      }
      if (onFrontier) vmEl("circle", { cx: p.cx, cy, r: p.r + 3.5, class: "frontier-ring" }, g)
      if (p.badgeOnly || p.rating == null) {
        vmEl("circle", { cx: p.cx, cy, r: p.r - 1, fill: "var(--surface)", stroke: p.group.color, "stroke-width": 2 }, g)
      } else {
        vmEl("circle", { cx: p.cx, cy, r: p.r, fill: p.group.color, stroke: "var(--surface)", "stroke-width": 1.5 }, g)
      }
    })

    // Direct labels on frontier wines, skipped where they would collide.
    if (!L.narrow) {
      const labels = vmEl("g", { class: "map-labels" }, svg)
      const boxes = []
      frontier.forEach((p) => {
        const text = shortName(p.deal)
        const w = text.length * 6.6 + 4
        const candidates = [
          { x: p.cx + p.r + 6, y: p.cy - p.r - 4, anchor: "start" },
          { x: p.cx - p.r - 6, y: p.cy - p.r - 4, anchor: "end" },
          { x: p.cx + p.r + 6, y: p.cy + p.r + 14, anchor: "start" },
          { x: p.cx - p.r - 6, y: p.cy + p.r + 14, anchor: "end" },
        ]
        for (const c of candidates) {
          const left = c.anchor === "start" ? c.x : c.x - w
          const box = { l: left, r: left + w, t: c.y - 12, b: c.y + 3 }
          const fits = box.l > L.margin.left && box.r < L.width - L.margin.right && box.t > 0
          const clear = !boxes.some((o) => box.l < o.r && box.r > o.l && box.t < o.b && box.b > o.t)
          const overFrontier = frontier.some((q) => q !== p && q.cx > box.l - q.r && q.cx < box.r + q.r && q.cy > box.t - q.r && q.cy < box.b + q.r)
          if (fits && clear && !overFrontier) {
            boxes.push(box)
            const t = vmEl("text", { x: c.x, y: c.y, "text-anchor": c.anchor, class: "map-label" }, labels)
            t.textContent = text
            break
          }
        }
      })
    }

    this.cursorRing = vmEl("circle", { class: "map-cursor", r: 0, cx: -20, cy: -20 }, svg)
    this.canvas.querySelector("svg")?.remove()
    this.canvas.prepend(svg)
    this.svg = svg
    this.canvas.setAttribute("aria-label", `Value map of ${inView.length} offers. ${frontier.length} sit on the best-rating-for-price line. Use the arrow keys to move between offers and Enter to open one.`)
    if (this.current) {
      const again = this.points.find((p) => p.id === this.current.id)
      if (again && this.visible.has(again.id)) this.showCard(again, this.pinned)
      else { this.pinned = false; this.hideCard() }
    }
    this.drawnOnce = true
  }

  nearest(e, radius) {
    const rect = this.svg.getBoundingClientRect()
    const px = ((e.clientX - rect.left) / rect.width) * this.L.width
    const py = ((e.clientY - rect.top) / rect.height) * this.L.height
    let best = null
    let bestDist = radius
    for (const p of this.points) {
      if (!this.visible.has(p.id)) continue
      const dist = Math.hypot(p.cx - px, p.cy - py) - p.r
      if (dist < bestDist) { best = p; bestDist = dist }
    }
    return best
  }

  hoverAt(e) {
    if (this.pinned) return
    const p = this.nearest(e, e.pointerType === "touch" ? 28 : 18)
    if (p) this.showCard(p, false)
    else this.hideCard()
  }

  clickAt(e) {
    if (e.target.closest(".map-card")) return
    const p = this.nearest(e, 28)
    if (p) { this.showCard(p, true); this.card.querySelector("[data-map-show]")?.focus({ preventScroll: true }) }
    else if (this.pinned) { this.pinned = false; this.hideCard() }
  }

  ordered() {
    return this.points.filter((p) => this.visible.has(p.id)).sort((a, b) => a.price - b.price || (b.rating || 0) - (a.rating || 0))
  }

  focusFirst() {
    const list = this.ordered()
    const start = list.find((p) => this.frontierIds.has(p.id)) || list[0]
    if (start) this.showCard(start, false)
  }

  onKey(e) {
    const list = this.ordered()
    if (!list.length) return
    const index = Math.max(0, list.findIndex((p) => this.current && p.id === this.current.id))
    let next = null
    if (e.key === "ArrowRight" || e.key === "ArrowDown") next = list[Math.min(list.length - 1, index + 1)]
    else if (e.key === "ArrowLeft" || e.key === "ArrowUp") next = list[Math.max(0, index - 1)]
    else if (e.key === "Home") next = list[0]
    else if (e.key === "End") next = list[list.length - 1]
    else if (e.key === "Enter" || e.key === " ") {
      if (this.current) { this.showCard(this.current, true); this.card.querySelector("[data-map-show]")?.focus({ preventScroll: true }) }
      e.preventDefault()
      return
    } else if (e.key === "Escape") { this.pinned = false; this.hideCard(); return }
    else return
    e.preventDefault()
    this.pinned = false
    this.showCard(next, false)
  }

  showCard(p, pin) {
    this.current = p
    this.pinned = pin
    const d = p.deal
    const money = WineCharts.money
    const sized = !(d.volume_ml === 750 && Math.max(d.quantity || 1, 1) === 1)
    const per100 = d.volume_ml ? money((d.price_platinum / (d.volume_ml * Math.max(d.quantity || 1, 1))) * 100) : null
    const rating = d.vivino_rating == null
      ? "Not rated on Vivino yet"
      : `Vivino ${d.vivino_rating.toFixed(1)} · ${d.vivino_num_ratings ? `${new Intl.NumberFormat("en-SG").format(d.vivino_num_ratings)} ratings` : "as shown on Platinum"}`
    const frontierLine = this.frontierIds.has(p.id) ? `<p class="card-frontier">Nothing cheaper in view rates as high.</p>` : ""
    this.card.innerHTML = `
      <p class="card-name">${vmEscape(fullName(d))}</p>
      <p class="card-meta"><span class="card-dot" style="--dot:${p.group.color}"></span>${vmEscape([d.vintage || "NV", p.group.label, d.region || d.country].filter(Boolean).join(" · "))}</p>
      <p class="card-price"><strong>${money(d.price_platinum_per_750)}</strong> per 750 ml</p>
      <p class="card-sub">${sized ? `${money(d.price_platinum)} for ${vmEscape(sizeLabel(d))} · ` : ""}${per100 ? `${per100}/100 ml` : ""}</p>
      <p class="card-rating">${vmEscape(rating)}</p>
      ${frontierLine}
      ${pin ? `
        <div class="card-actions">
          <button type="button" class="button button-primary" data-map-show>Show in list</button>
          ${d.platinum_url ? `<a class="text-link" href="${vmEscape(d.platinum_url)}" target="_blank" rel="noopener noreferrer">Platinum<span class="visually-hidden"> (opens in a new tab)</span></a>` : ""}
          ${d.vivino_url ? `<a class="text-link" href="${vmEscape(d.vivino_url)}" target="_blank" rel="noopener noreferrer">Vivino<span class="visually-hidden"> (opens in a new tab)</span></a>` : ""}
          <button type="button" class="card-close" data-map-close aria-label="Close">×</button>
        </div>` : `<p class="card-hint">${matchMedia("(pointer: coarse)").matches ? "Tap" : "Click"} for links</p>`}
    `
    this.card.hidden = false
    this.card.classList.toggle("is-pinned", pin)
    this.cursorRing.setAttribute("cx", p.cx)
    this.cursorRing.setAttribute("cy", p.cy)
    this.cursorRing.setAttribute("r", p.r + 5)
    if (!this.L.narrow) {
      const cw = this.card.offsetWidth
      const ch = this.card.offsetHeight
      const scale = this.svg.getBoundingClientRect().width / this.L.width || 1
      let left = p.cx * scale + 18
      if (left + cw > this.canvas.clientWidth) left = p.cx * scale - 18 - cw
      let top = p.cy * scale - ch / 2
      top = Math.max(0, Math.min(top, this.canvas.clientHeight - ch))
      this.card.style.left = `${Math.max(0, left)}px`
      this.card.style.top = `${top}px`
    } else {
      this.card.style.left = ""
      this.card.style.top = ""
    }
    this.live.textContent = `${fullName(d)}, ${money(d.price_platinum_per_750)} per 750 ml, ${rating}.`
  }

  hideCard() {
    this.card.hidden = true
    if (this.cursorRing) this.cursorRing.setAttribute("r", 0)
  }
}

window.ValueMap = ValueMap
window.STYLE_GROUPS = STYLE_GROUPS
window.computeFrontier = computeFrontier
