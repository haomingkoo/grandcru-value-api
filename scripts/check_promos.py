"""Read the Platinum Wine Club home page and record which basket promotions it still advertises.

Runs inside refresh_pipeline.py (daily and weekly ingest). The result lives in app_state under
promo_check; app/promos.py turns it into rule states for /basket and a status for /health.
A failed read is recorded and reported, and keeps the last good reading.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.durable_state import load_state, save_state  # noqa: E402
from app.promos import HOME_URL, PROMO_CHECK_KEY, read_home_page  # noqa: E402

USER_AGENT = (
    "Mozilla/5.0 (compatible; MinMaxWine/1.0; +https://wine.kooexperience.com/privacy; "
    "privacy@kooexperience.com)"
)


def fetch_home() -> str:
    response = httpx.get(HOME_URL, headers={"User-Agent": USER_AGENT}, timeout=20, follow_redirects=True)
    response.raise_for_status()
    return response.text


def run(fetch: Callable[[], str] = fetch_home, now: datetime | None = None) -> dict:
    now = now or datetime.now(UTC)
    previous = load_state(PROMO_CHECK_KEY, {})
    try:
        found = read_home_page(fetch())
    except httpx.HTTPError as exc:
        state = {**previous, "checked_at": now.isoformat(), "ok": False, "error": f"{type(exc).__name__}: {exc}"[:200]}
        print(f"[promos] FAILED to read {HOME_URL}: {state['error']}; keeping the last good reading", flush=True)
    else:
        state = {"checked_at": now.isoformat(), "ok": True, "error": None, "found": found, "found_at": now.isoformat()}
        missing = [key for key, seen in found.items() if not seen]
        print(f"[promos] home page read; missing: {', '.join(missing) or 'none'}", flush=True)
    save_state(PROMO_CHECK_KEY, state)
    return state


if __name__ == "__main__":
    run()
