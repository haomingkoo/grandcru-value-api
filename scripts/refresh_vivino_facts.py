"""Refresh Vivino price, rating and rating count for current offers, politely.

Reads each offer's Vivino wine page at most once a week (or when its Platinum
vintage changes) within the shared daily budget in scripts/vivino_fetch.py.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.durable_state import load_state, save_state  # noqa: E402
from app.vivino_status import summarize_vivino_status  # noqa: E402
from scripts.import_wine_data import (  # noqa: E402
    build_vivino_lookup,
    match_vivino_row,
    read_csv_rows,
    read_optional_csv_rows,
)
from scripts.vivino_fetch import (  # noqa: E402
    FACTS_KEY,
    identity_drift,
    PoliteFetcher,
    VivinoBlocked,
    canonical_wine_url,
    fact_key,
    fetch_facts,
)
from scripts.vivino_overrides import with_durable_overrides  # noqa: E402

DRIFT_KEY = "vivino_match_audit"


def build_targets(
    comparison_rows: list[dict[str, str]],
    vivino_rows: list[dict[str, str]],
    names: dict[str, str] | None = None,
) -> list[tuple[str, str]]:
    """(Vivino wine URL, Platinum vintage) per offer, one entry per wine/vintage. Fills names[key] with the offer name."""
    lookup = build_vivino_lookup(vivino_rows)
    targets: dict[str, tuple[str, str]] = {}
    for row in comparison_rows:
        name = (row.get("name_plat") or "").strip()
        if not name:
            continue
        matched, method = match_vivino_row(name, lookup)
        url = canonical_wine_url(matched.get("vivino_url")) if method != "none" else None
        url = url or canonical_wine_url(row.get("platinum_vivino_url"))
        vintage = (row.get("year_plat") or "").strip()
        key = fact_key(url, vintage)
        if url and key and key not in targets:
            targets[key] = (url, vintage)
            if names is not None:
                names[key] = name
    return list(targets.values())


def oldest_first(targets: list[tuple[str, str]], facts: dict[str, dict]) -> list[tuple[str, str]]:
    def checked(target: tuple[str, str]) -> str:
        return (facts.get(fact_key(*target) or "") or {}).get("checked_at") or ""

    return sorted(targets, key=checked)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparison", type=Path, default=ROOT / "seed" / "comparison_summary.csv")
    parser.add_argument("--vivino", type=Path, default=ROOT / "seed" / "vivino_results.csv")
    parser.add_argument("--vivino-overrides", type=Path, default=ROOT / "seed" / "vivino_overrides.csv")
    args = parser.parse_args()

    comparison_rows = read_csv_rows(args.comparison)
    vivino_rows = read_csv_rows(args.vivino) + with_durable_overrides(read_optional_csv_rows(args.vivino_overrides))
    facts: dict[str, dict] = load_state(FACTS_KEY, {}) or {}
    names: dict[str, str] = {}
    targets = oldest_first(build_targets(comparison_rows, vivino_rows, names), facts)

    fetcher = PoliteFetcher()
    print(
        f"[vivino] {len(targets)} offers with a Vivino page; "
        f"budget left today {fetcher.remaining_budget()}/{fetcher.daily_cap}",
        flush=True,
    )
    try:
        for url, vintage in targets:
            if not fetcher.can_fetch_more():
                break
            fetch_facts(fetcher, facts, url, vintage)
        # Self-audit every stored match: a page that no longer looks like the offer loses its price.
        drift = []
        for url, vintage in targets:
            key = fact_key(url, vintage)
            fact = facts.get(key or "")
            reason = identity_drift(names.get(key or "", ""), fact)
            if fact is not None:
                fact["drift"] = reason
            if reason:
                drift.append({"offer": names.get(key or ""), "url": url, "page": fact.get("name"), "reason": reason})
        save_state(DRIFT_KEY, {"checked_at": datetime.now(UTC).isoformat(), "checked": len(targets), "drift": drift})
        if drift:
            print(f"[vivino] WARNING: {len(drift)} matches look wrong now; their prices are withheld: {drift[:5]}", flush=True)
    except VivinoBlocked as exc:
        print(f"[vivino] WARNING: Vivino refused a page ({exc}); backing off.", flush=True)
    finally:
        save_state(FACTS_KEY, facts)
        summary = fetcher.finish()

    now = datetime.now(UTC)
    fresh = sum(1 for url, vintage in targets if (facts.get(fact_key(url, vintage) or "") or {}).get("price"))
    status = summarize_vivino_status(fetcher.state, now)
    print(
        "[vivino] run:",
        summary,
        f"offers_with_checked_price={fresh}/{len(targets)}",
        f"status={status['vivino_status']}",
        flush=True,
    )
    if not status["vivino_ok"]:
        print(f"[vivino] WARNING: market prices paused: {status['vivino_status']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
