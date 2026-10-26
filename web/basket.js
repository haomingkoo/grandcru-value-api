// Basket builder: in-stock wines that reach a spend level with the least overspend (GET /basket).
const BASKET_TITLES = {
  closest: (b) => (b.strict ? `Just over ${WineCharts.money(b.target)}` : `Closest to ${WineCharts.money(b.target)}`),
  best_value: () => "Most rating for the money",
  best_rated: () => "Highest rated",
}

function basketEscape(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]))
}

function basketName(name) {
  return String(name || "").replace(/ - (Red|White|Rose|Rosé|Sparkling|Orange)( - .+)?$/, "")
}

function setupBasket() {
  const form = document.getElementById("basketForm")
  const results = document.getElementById("basketResults")
  const rules = document.getElementById("basketRules")
  if (!form) return
  let serial = 0
  let timer = 0

  const params = () => {
    const data = new FormData(form)
    const p = new URLSearchParams()
    const mode = data.get("mode")
    p.set("mode", mode)
    if (mode === "custom") p.set("target", data.get("target") || "300")
    const styles = data.getAll("style")
    if (styles.length) p.set("styles", styles.join(","))
    if (data.get("max_bottles")) p.set("max_bottles", data.get("max_bottles"))
    p.set("prefer", data.get("prefer") || "value")
    if (data.get("include_unrated")) p.set("include_unrated", "true")
    return p
  }

  const render = (data) => {
    const money = WineCharts.money
    if (!data.baskets.length) {
      results.innerHTML = `<div class="basket-empty"><p class="state-title">No mix of in-stock wines reaches ${money(data.target)} with these choices.</p><p>Allow more styles or more bottles, or include wines not rated yet.</p></div>`
      return
    }
    results.innerHTML = data.baskets.map((b) => `
      <article class="basket-card">
        <h3>${basketEscape(BASKET_TITLES[b.kind](b))}</h3>
        <p class="basket-total"><strong>${money(b.total)}</strong> for ${b.bottles} ${b.bottles === 1 ? "bottle" : "bottles"}${b.average_rating ? ` · Vivino average ${b.average_rating.toFixed(2)}` : ""}</p>
        <p class="basket-over">${b.over_target < 0.005 ? `Exactly ${money(b.target)}` : `${money(b.over_target)} over ${money(b.target)}`}</p>
        <ul class="basket-items">
          ${b.items.map((i) => `
            <li>
              <button type="button" class="basket-wine" data-deal-id="${i.deal_id}">${basketEscape(basketName(i.name))}</button>
              <span class="basket-price">${money(i.price)}</span>
              <span class="basket-reason">${basketEscape(i.reason)}</span>
            </li>`).join("")}
        </ul>
        <dl class="basket-money">
          <div><dt>Pay at checkout</dt><dd>${money(b.pay_at_checkout)}${b.free_delivery ? "" : ` <span>incl. ${money(b.delivery_fee)} delivery</span>`}</dd></div>
          <div><dt>Back on your Amex statement</dt><dd>${b.credit_back ? `${money(b.credit_back)} <span>after payment</span>` : "None (under S$300)"}</dd></div>
          <div><dt>Cost after the credit</dt><dd>${money(b.net_after_credit)}</dd></div>
          <div><dt>Delivery</dt><dd>${b.free_delivery ? "Free" : `${money(b.delivery_fee)}`}</dd></div>
          ${b.promo_eligible ? `<div><dt>Shop promotion</dt><dd>S$50 off may apply <span>check at checkout</span></dd></div>` : ""}
          <div><dt>Against Grand Cru</dt><dd>${b.grand_cru_known
            ? `${b.grand_cru_difference >= 0.5 ? `${money(b.grand_cru_difference)} less here` : b.grand_cru_difference <= -0.5 ? `${money(-b.grand_cru_difference)} more here` : "Same total"} <span>for the ${b.grand_cru_known} of ${b.items.length} also sold there</span>`
            : "None of these are sold there"}</dd></div>
        </dl>
      </article>
    `).join("")
  }

  const load = async () => {
    const id = ++serial
    results.setAttribute("aria-busy", "true")
    results.classList.add("is-loading")
    try {
      const response = await fetch(`/basket?${params()}`, { headers: { Accept: "application/json" } })
      if (!response.ok) throw new Error(String(response.status))
      const data = await response.json()
      if (id !== serial) return
      render(data)
      if (!rules.dataset.filled) {
        const r = data.rules
        rules.innerHTML = `
          ${basketEscape(r.credit.text)} ${basketEscape(r.credit.conditions)} <a href="${basketEscape(r.credit.source_url)}" target="_blank" rel="noopener noreferrer">${basketEscape(r.credit.source_label)}</a>.
          ${basketEscape(r.delivery.text)} <a href="${basketEscape(r.delivery.source_url)}" target="_blank" rel="noopener noreferrer">${basketEscape(r.delivery.source_label)}</a>.
          ${basketEscape(r.promo.text)} <a href="${basketEscape(r.promo.source_url)}" target="_blank" rel="noopener noreferrer">${basketEscape(r.promo.source_label)}</a>.
          Prices change; baskets use today&rsquo;s listed prices for in-stock wines only.`
        rules.dataset.filled = "true"
      }
    } catch (_) {
      if (id === serial) results.innerHTML = `<div class="basket-empty"><p class="state-title">The baskets could not load.</p><button class="button" type="button" data-basket-retry>Try again</button></div>`
    } finally {
      if (id === serial) {
        results.setAttribute("aria-busy", "false")
        results.classList.remove("is-loading")
      }
    }
  }

  const schedule = () => {
    window.clearTimeout(timer)
    timer = window.setTimeout(load, 250)
  }
  form.addEventListener("change", (event) => {
    if (event.target.name === "target") form.querySelector("input[value='custom']").checked = true
    schedule()
  })
  form.addEventListener("input", (event) => { if (event.target.name === "target") { form.querySelector("input[value='custom']").checked = true; schedule() } })
  form.addEventListener("submit", (event) => { event.preventDefault(); load() })
  results.addEventListener("click", (event) => {
    if (event.target.closest("[data-basket-retry]")) load()
    const wine = event.target.closest(".basket-wine")
    if (wine) {
      const id = Number(wine.dataset.dealId)
      if (document.querySelector(`[data-offer-ids~="${id}"]`)) {
        showDealInList({ id })
      } else {
        // The table is filtered; clear it, then point at the wine.
        resetFilter("all")
        syncControlsFromState()
        pendingHighlight = id
        loadOffers()
      }
    }
  })
  // Load when the section comes near the screen, so the first view stays fast.
  const observer = new IntersectionObserver((entries) => {
    if (entries.some((e) => e.isIntersecting)) {
      observer.disconnect()
      load()
    }
  }, { rootMargin: "300px" })
  observer.observe(results)
}

document.addEventListener("DOMContentLoaded", setupBasket)
