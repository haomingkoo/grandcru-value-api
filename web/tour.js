;(() => {
  const dialog = document.getElementById("tourDialog")
  const trigger = document.getElementById("tourOpen")
  if (!dialog || !trigger || typeof dialog.showModal !== "function") return
  const video = dialog.querySelector("video")
  let returnFocus = trigger

  function open() {
    returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : trigger
    if (!dialog.open) dialog.showModal()
  }

  trigger.addEventListener("click", open)
  document.getElementById("tourClose").addEventListener("click", () => dialog.close())
  // Clicks on the backdrop land on the dialog element itself.
  dialog.addEventListener("click", (event) => {
    if (event.target === dialog) dialog.close()
  })
  dialog.addEventListener("close", () => {
    video.pause()
    if (returnFocus === document.body) returnFocus = trigger
    returnFocus.focus()
  })

  // Read before app.js rewrites the query string.
  if (new URLSearchParams(window.location.search).get("tour") === "1" || window.location.hash === "#tour") {
    returnFocus = trigger
    dialog.showModal()
  }
})()
