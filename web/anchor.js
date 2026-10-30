// A link to a section (/#offers) jumps before the basket, map and offers have loaded, and what
// loads above then pushes the section down. Hold the section in place while the page grows,
// and let go as soon as the visitor scrolls, taps or types.
const anchorHold = { target: null }

const releaseAnchor = () => { anchorHold.target = null }

function holdAnchor() {
  const id = decodeURIComponent(window.location.hash.slice(1))
  anchorHold.target = id ? document.getElementById(id) : null
  if (anchorHold.target) anchorHold.target.scrollIntoView({ block: "start", behavior: "instant" })
}

new ResizeObserver(() => {
  if (anchorHold.target) anchorHold.target.scrollIntoView({ block: "start", behavior: "instant" })
}).observe(document.documentElement)

;["wheel", "touchstart", "pointerdown", "keydown"].forEach((type) => {
  window.addEventListener(type, releaseAnchor, { capture: true, passive: true })
})
window.addEventListener("hashchange", holdAnchor)
holdAnchor()
