"""Explain remaining Platinum rows without a Grand Cru match.

This is a no-network diagnostic. It uses the same deterministic candidate
filter as the Gemini Grand Cru resolver, then records why each no-match row
still has no Grand Cru URL.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.scoring import parse_float  # noqa: E402
from scripts.build_comparison_summary import prepare_rows  # noqa: E402
from scripts.llm_grandcru_resolver import _candidate_by_url, _candidate_pool, read_csv_rows  # noqa: E402
from scripts.llm_utils import cache_key  # noqa: E402

FIELDNAMES = [
    "name_plat",
    "year_plat",
    "quantity_plat",
    "volume_plat",
    "price_plat",
    "url_plat",
    "reason",
    "candidate_count",
    "top_score",
    "top_name",
    "top_price",
    "top_quantity",
    "top_volume",
    "top_package_type",
    "top_url",
    "llm_decision",
    "llm_confidence",
    "llm_match_url",
]


def load_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def classify_no_match(
    row: dict[str, str],
    grandcru_rows: list[dict[str, object]],
    cache: dict[str, object],
    *,
    max_candidates: int,
    min_confidence: float,
) -> dict[str, str]:
    wine_name = (row.get("name_plat") or "").strip()
    candidates = _candidate_pool(row, grandcru_rows, max_candidates=max_candidates)
    top = candidates[0] if candidates else None
    cache_entry = cache.get(cache_key(wine_name))
    suggestion = cache_entry.get("suggestion") if isinstance(cache_entry, dict) else None
    suggestion = suggestion if isinstance(suggestion, dict) else {}

    llm_decision = str(suggestion.get("decision") or "").strip().lower()
    llm_match_url = str(suggestion.get("match_url") or "").strip()
    llm_confidence_value = parse_float(suggestion.get("confidence"))
    llm_confidence = f"{llm_confidence_value:.3f}" if llm_confidence_value is not None else ""

    if not candidates:
        reason = "no_same_year_volume_candidate"
    elif not suggestion:
        reason = "llm_not_run_or_cache_missing"
    elif llm_decision != "match":
        reason = "llm_declined"
    elif _candidate_by_url(candidates, llm_match_url) is None:
        reason = "llm_invalid_candidate"
    elif llm_confidence_value is None or llm_confidence_value < min_confidence:
        reason = "llm_low_confidence"
    else:
        reason = "llm_match_not_applied"

    return {
        "name_plat": wine_name,
        "year_plat": row.get("year_plat") or "",
        "quantity_plat": row.get("quantity_plat") or "",
        "volume_plat": row.get("volume_plat") or "",
        "price_plat": row.get("price_plat") or "",
        "url_plat": row.get("url_plat") or "",
        "reason": reason,
        "candidate_count": str(len(candidates)),
        "top_score": f"{top.score:.4f}" if top else "",
        "top_name": top.name if top else "",
        "top_price": top.price if top else "",
        "top_quantity": str(top.quantity) if top else "",
        "top_volume": top.volume if top else "",
        "top_package_type": top.package_type if top else "",
        "top_url": top.url if top else "",
        "llm_decision": llm_decision,
        "llm_confidence": llm_confidence,
        "llm_match_url": llm_match_url,
    }


def diagnose_rows(
    comparison_rows: list[dict[str, str]],
    grandcru_rows: list[dict[str, object]],
    cache: dict[str, object],
    *,
    max_candidates: int,
    min_confidence: float,
) -> list[dict[str, str]]:
    report_rows: list[dict[str, str]] = []
    for row in comparison_rows:
        if (row.get("url_main") or "").strip():
            continue
        report_rows.append(
            classify_no_match(
                row,
                grandcru_rows,
                cache,
                max_candidates=max_candidates,
                min_confidence=min_confidence,
            )
        )
    return report_rows


def write_report(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Explain remaining Grand Cru no-match rows")
    parser.add_argument("--comparison", type=Path, default=Path("seed/comparison_summary.csv"))
    parser.add_argument("--grandcru", type=Path, default=Path("seed/latest_refresh/grandcru_wines.csv"))
    parser.add_argument("--llm-cache", type=Path, default=Path("data/grandcru_llm_cache.json"))
    parser.add_argument("--output", type=Path, default=Path("data/grandcru_match_diagnostics.csv"))
    parser.add_argument("--max-candidates", type=int, default=8)
    parser.add_argument("--min-confidence", type=float, default=0.75)
    args = parser.parse_args()

    comparison_rows = read_csv_rows(args.comparison)
    grandcru_rows = prepare_rows(read_csv_rows(args.grandcru), enforce_in_stock=False)
    report_rows = diagnose_rows(
        comparison_rows,
        grandcru_rows,
        load_json(args.llm_cache),
        max_candidates=args.max_candidates,
        min_confidence=args.min_confidence,
    )
    write_report(args.output, report_rows)

    reason_counts = Counter(row["reason"] for row in report_rows)
    reason_text = ",".join(f"{reason}={count}" for reason, count in sorted(reason_counts.items())) or "none"
    print(
        "[diagnose-grandcru]",
        f"total={len(comparison_rows)}",
        f"matched={len(comparison_rows) - len(report_rows)}",
        f"no_match={len(report_rows)}",
        f"reasons={reason_text}",
        f"report={args.output}",
        flush=True,
    )


if __name__ == "__main__":
    main()
