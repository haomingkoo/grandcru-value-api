"""Turn the persisted Vivino fetch state into a loud, human-readable status."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

STALE_AFTER = timedelta(days=10)
MIN_ATTEMPTS_FOR_RATE = 5
MIN_SUCCESS_RATE = 0.5


def _parse(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def summarize_vivino_status(state: dict[str, Any] | None, now: datetime | None = None) -> dict[str, Any]:
    state = state or {}
    now = now or datetime.now(UTC)
    last_success = _parse(state.get("last_success_at"))
    last_run = state.get("last_run") or {}
    result: dict[str, Any] = {
        "vivino_status": "ok",
        "vivino_ok": True,
        "vivino_last_success_at": last_success,
    }

    if state.get("blocked_since"):
        result["vivino_status"] = f"blocked since {state['blocked_since']}"
    elif (
        int(last_run.get("attempted") or 0) >= MIN_ATTEMPTS_FOR_RATE
        and int(last_run.get("ok") or 0) / int(last_run["attempted"]) < MIN_SUCCESS_RATE
    ):
        result["vivino_status"] = (
            f"degraded since {last_run.get('at')}: {last_run.get('ok')} of {last_run.get('attempted')} pages read"
        )
    elif last_success and now - last_success > STALE_AFTER:
        result["vivino_status"] = f"stale since {last_success.isoformat()}"
    elif not last_success:
        result["vivino_status"] = "not checked yet"
        return result
    else:
        return result

    result["vivino_ok"] = False
    return result
