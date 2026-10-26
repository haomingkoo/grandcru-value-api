// Price history drawings. Plain SVG, no library. Every value comes from /deals/price-history.
const DAY_MS = 86400000
const SVG_NS = "http://www.w3.org/2000/svg"
const SOURCE_STYLE = {
  platinum: { color: "var(--src-platinum)", dash: "" },
  grand_cru: { color: "var(--src-grand-cru)", dash: "5 4" },
  vivino: { color: "var(--src-vivino)", dash: "" },
}
const OTHER_SOURCE = { color: "var(--text)", dash: "2 3" }

function sourceStyle(source) {
  return SOURCE_STYLE[source] || OTHER_SOURCE
}

function dayNumber(iso) {
  return Math.round(Date.parse(`${iso}T00:00:00Z`) / DAY_MS)
}

function dayLabel(day, withYear) {
  const date = new Date(day * DAY_MS)
  return date.toLocaleDateString("en-SG", { day: "numeric", month: "short", ...(withYear ? { year: "numeric" } : {}), timeZone: "UTC" })
}

function weekdayLabel(day) {
  return new Date(day * DAY_MS).toLocaleDateString("en-SG", { weekday: "short", day: "numeric", month: "short", year: "numeric", timeZone: "UTC" })
}

function money(value) {
  if (value == null) return ""
  const cents = Math.abs(value - Math.round(value)) > 0.004
  return `S$${new Intl.NumberFormat("en-SG", { minimumFractionDigits: cents ? 2 : 0, maximumFractionDigits: cents ? 2 : 0 }).format(value)}`
}

// Each source becomes a list of drawable runs: { start, end, value, saved } in day numbers, end inclusive.
function sourceRuns(source) {
  if (source.kind === "checks") {
    return (source.points || []).map((p) => ({ start: dayNumber(p.date), end: dayNumber(p.date), value: p.price_per_750, saved: p.origin === "saved_list", check: true }))
  }
  return (source.segments || []).map((s) => ({ start: dayNumber(s.start), end: dayNumber(s.end), value: s.price_per_750, saved: s.origin === "saved_list" }))
}

function niceTicks(min, max, count) {
  const span = max - min || Math.abs(max) || 1
  const raw = span / count
  const magnitude = 10 ** Math.floor(Math.log10(raw))
  const step = [1, 2, 2.5, 5, 10].map((m) => m * magnitude).find((s) => span / s <= count) || 10 * magnitude
  const ticks = []
  for (let v = Math.ceil(min / step) * step; v <= max + 1e-9; v += step) ticks.push(Math.round(v * 100) / 100)
  return ticks
}

function valueDomain(allRuns) {
  const values = allRuns.flat().map((r) => r.value).filter((v) => v != null)
  let min = Math.min(...values)
  let max = Math.max(...values)
  const pad = Math.max((max - min) * 0.15, max * 0.06, 1)
  min = Math.max(0, min - pad)
  max += pad
  return [min, max]
}

function el(name, attrs = {}, parent) {
  const node = document.createElementNS(SVG_NS, name)
  for (const [key, value] of Object.entries(attrs)) {
    if (value !== "" && value != null) node.setAttribute(key, value)
  }
  if (parent) parent.appendChild(node)
  return node
}

// A step line: horizontal for each run, vertical only where two runs touch. Gaps stay gaps.
function drawRuns(group, runs, x, y, style, { width, dotRadius }) {
  const daily = runs.filter((r) => !r.saved && !r.check)
  daily.forEach((run, i) => {
    const next = daily[i + 1]
    const x0 = x(run.start)
    const x1 = x(run.end + 1)
    const isolated = !next || next.start !== run.end + 1
    const prev = daily[i - 1]
    const joinedBefore = prev && prev.end + 1 === run.start
    if (run.start === run.end && isolated && !joinedBefore) {
      el("circle", { cx: (x0 + x1) / 2, cy: y(run.value), r: dotRadius, fill: style.color }, group)
      return
    }
    let d = `M${x0.toFixed(1)},${y(run.value).toFixed(1)}H${x1.toFixed(1)}`
    if (!isolated) d += `V${y(next.value).toFixed(1)}`
    el("path", { d, fill: "none", stroke: style.color, "stroke-width": width, "stroke-dasharray": style.dash, "stroke-linejoin": "round", "stroke-linecap": "butt" }, group)
  })
  runs.filter((r) => r.saved || r.check).forEach((run) => {
    el("circle", {
      cx: x(run.start + 0.5),
      cy: y(run.value),
      r: dotRadius + (run.check ? 1 : 0),
      fill: run.saved ? "var(--surface)" : style.color,
      stroke: style.color,
      "stroke-width": run.saved ? 1.5 : 0,
    }, group)
  })
}

const WineCharts = {
  // Small Platinum vs Grand Cru line for the offer row. Returns "" when the history is too short.
  sparkline(offer, today, { days = 90, width = 112, height = 28 } = {}) {
    if (!offer) return ""
    const end = dayNumber(today) + 1
    const start = end - days
    const pick = (name) => offer.sources.find((s) => s.source === name)
    const platinum = pick("platinum")
    if (!platinum) return ""
    const clip = (runs) => runs.filter((r) => r.end >= start).map((r) => ({ ...r, start: Math.max(r.start, start) }))
    const platRuns = clip(sourceRuns(platinum)).filter((r) => !r.saved)
    const platDays = platRuns.reduce((sum, r) => sum + (r.end - r.start + 1), 0)
    if (platDays < 7) return ""
    const gc = pick("grand_cru")
    const gcRuns = gc ? clip(sourceRuns(gc)).filter((r) => !r.saved) : []
    const [min, max] = valueDomain([platRuns, gcRuns])
    const x = (d) => 2 + ((d - start) / days) * (width - 6)
    const y = (v) => height - 3 - ((v - min) / (max - min)) * (height - 6)
    const svg = el("svg", { viewBox: `0 0 ${width} ${height}`, width, height, class: "sparkline", "aria-hidden": "true", focusable: "false" })
    if (gcRuns.length) drawRuns(el("g", {}, svg), gcRuns, x, y, sourceStyle("grand_cru"), { width: 1.5, dotRadius: 1.5 })
    drawRuns(el("g", {}, svg), platRuns, x, y, sourceStyle("platinum"), { width: 1.75, dotRadius: 1.75 })
    const last = platRuns[platRuns.length - 1]
    if (last && last.end >= end - 2) el("circle", { cx: x(last.end + 1), cy: y(last.value), r: 2.5, fill: "var(--src-platinum)" }, svg)
    return svg.outerHTML
  },

  // Full interactive history for one offer: every source, every recorded day, per 750 ml.
  history(container, offer, today) {
    container.innerHTML = ""
    const sources = offer.sources.map((s) => ({ ...s, runs: sourceRuns(s), style: sourceStyle(s.source) }))
    if (!sources.length) return
    const first = Math.min(...sources.flatMap((s) => s.runs.map((r) => r.start)))
    const last = dayNumber(today)
    const end = Math.max(last + 1, first + 7)
    const width = Math.max(280, Math.round(container.clientWidth || 640))
    const narrow = width < 520
    const height = narrow ? 200 : 240
    const margin = { top: 12, right: narrow ? 12 : 20, bottom: 28, left: narrow ? 52 : 60 }
    const [min, max] = valueDomain(sources.map((s) => s.runs))
    const x = (d) => margin.left + ((d - first) / (end - first)) * (width - margin.left - margin.right)
    const y = (v) => margin.top + (1 - (v - min) / (max - min)) * (height - margin.top - margin.bottom)
    const withYear = new Date(first * DAY_MS).getUTCFullYear() !== new Date(last * DAY_MS).getUTCFullYear()

    const svg = el("svg", { viewBox: `0 0 ${width} ${height}`, width, height, class: "history-svg", "aria-hidden": "true", focusable: "false" })
    const grid = el("g", { class: "chart-grid" }, svg)
    niceTicks(min, max, narrow ? 3 : 4).forEach((v) => {
      el("line", { x1: margin.left, x2: width - margin.right, y1: y(v), y2: y(v) }, grid)
      const label = el("text", { x: margin.left - 8, y: y(v) + 4, "text-anchor": "end", class: "chart-axis" }, grid)
      label.textContent = money(v)
    })
    const spanDays = end - first
    const monthStep = spanDays > 300 ? 3 : spanDays > 150 ? 2 : 1
    const startDate = new Date(first * DAY_MS)
    for (let m = 0; m < 40; m += monthStep) {
      const d = Date.UTC(startDate.getUTCFullYear(), startDate.getUTCMonth() + m + 1, 1) / DAY_MS
      if (d >= end) break
      if (x(d) - margin.left < 18) continue
      el("line", { x1: x(d), x2: x(d), y1: height - margin.bottom, y2: height - margin.bottom + 4, class: "chart-tick" }, grid)
      const label = el("text", { x: x(d), y: height - 8, "text-anchor": "middle", class: "chart-axis" }, grid)
      label.textContent = new Date(d * DAY_MS).toLocaleDateString("en-SG", { month: "short", timeZone: "UTC" })
    }
    el("line", { x1: margin.left, x2: width - margin.right, y1: height - margin.bottom, y2: height - margin.bottom, class: "chart-baseline" }, grid)

    // Platinum underneath, Grand Cru dashed on top, so equal prices show both colours.
    const drawOrder = sources.slice().sort((a, b) => (a.source === "platinum" ? -1 : b.source === "platinum" ? 1 : 0))
    drawOrder.forEach((s) => drawRuns(el("g", { class: `series series-${s.source}` }, svg), s.runs, x, y, s.style, { width: 2.25, dotRadius: 3 }))

    const cursor = el("line", { class: "chart-cursor", y1: margin.top, y2: height - margin.bottom, x1: -10, x2: -10 }, svg)
    const frame = document.createElement("div")
    frame.className = "history-frame"
    frame.tabIndex = 0
    frame.setAttribute("role", "group")
    frame.setAttribute("aria-label", `Price history chart from ${dayLabel(first, true)} to ${dayLabel(last, true)}. Use the left and right arrow keys to read each day.`)
    frame.appendChild(svg)
    const tip = document.createElement("div")
    tip.className = narrow ? "chart-tip is-below" : "chart-tip"
    tip.hidden = true
    frame.appendChild(tip)
    const live = document.createElement("p")
    live.className = "visually-hidden"
    live.setAttribute("aria-live", "polite")
    container.appendChild(frame)
    container.appendChild(live)

    const valueOn = (s, day) => {
      if (s.kind === "checks") {
        const check = s.runs.filter((r) => r.start <= day && day - r.start <= 30).pop()
        return check ? { value: check.value, note: `checked ${dayLabel(check.start)}` } : null
      }
      const run = s.runs.find((r) => r.start <= day && day <= r.end)
      return run ? { value: run.value, note: run.saved ? "saved price list" : "" } : null
    }
    let current = last
    const show = (day) => {
      current = Math.min(Math.max(day, first), last)
      const cx = x(current + 0.5)
      cursor.setAttribute("x1", cx)
      cursor.setAttribute("x2", cx)
      const rows = sources.map((s) => {
        const v = valueOn(s, current)
        return `<li><span class="tip-swatch" style="--swatch:${s.style.color}"></span><span>${s.label}</span><strong>${v ? money(v.value) : "No price"}</strong>${v && v.note ? `<em>${v.note}</em>` : ""}</li>`
      }).join("")
      tip.innerHTML = `<p class="tip-date">${weekdayLabel(current)}</p><ul>${rows}</ul><p class="tip-unit">Per 750 ml</p>`
      live.textContent = `${weekdayLabel(current)}: ${sources.map((s) => { const v = valueOn(s, current); return `${s.label} ${v ? money(v.value) : "no price"}` }).join(", ")}`
      tip.hidden = false
      if (narrow) return
      const tipWidth = tip.offsetWidth
      const left = cx + 12 + tipWidth > width ? cx - 12 - tipWidth : cx + 12
      tip.style.left = `${Math.max(0, left)}px`
    }
    const hide = () => {
      tip.hidden = true
      cursor.setAttribute("x1", -10)
      cursor.setAttribute("x2", -10)
    }
    const dayAt = (clientX) => {
      const rect = svg.getBoundingClientRect()
      const px = ((clientX - rect.left) / rect.width) * width
      return Math.floor(first + ((px - margin.left) / (width - margin.left - margin.right)) * (end - first))
    }
    frame.addEventListener("pointermove", (e) => show(dayAt(e.clientX)))
    frame.addEventListener("pointerdown", (e) => show(dayAt(e.clientX)))
    frame.addEventListener("pointerleave", () => { if (document.activeElement !== frame) hide() })
    frame.addEventListener("focus", () => show(current))
    frame.addEventListener("blur", hide)
    frame.addEventListener("keydown", (e) => {
      const moves = { ArrowLeft: -1, ArrowRight: 1, PageUp: -7, PageDown: 7 }
      if (e.key in moves) show(current + moves[e.key])
      else if (e.key === "Home") show(first)
      else if (e.key === "End") show(last)
      else if (e.key === "Escape") { hide(); return }
      else return
      e.preventDefault()
    })
  },

  dayNumber,
  dayLabel,
  money,
}

window.WineCharts = WineCharts
