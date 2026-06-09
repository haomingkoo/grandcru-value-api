"""LLM-assisted resolver for unmatched Grand Cru comparison rows.

This script takes the current comparison summary plus the raw Grand Cru
catalog and tries to resolve rows whose `url_main` is still blank.

The LLM is only used to pick among already-filtered candidate rows. A
deterministic validation step still enforces that the final match exists
in the candidate set and meets a minimum confidence threshold.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.scoring import parse_float, parse_int  # noqa: E402
from scripts.build_comparison_summary import (  # noqa: E402
    match_similarity,
    prepare_rows,
    year_matches,
)
from scripts.llm_utils import cache_key, call_gemini, is_cache_fresh, load_cache, save_cache, _parse_grounding_json  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("grandcru.llm_grandcru_resolver")

CACHE_TTL_DAYS = 30
DEFAULT_COMPARISON_FIELDS = [
    "name_plat",
    "year_plat",
    "quantity_plat",
    "volume_plat",
    "quantity_main",
    "price_plat",
    "price_main",
    "price_diff",
    "price_diff_pct",
    "cheaper_side",
    "url_plat",
    "url_main",
    "platinum_in_stock",
    "grand_cru_in_stock",
    "image_url",
    "platinum_vivino_rating",
    "platinum_vivino_num_ratings",
    "platinum_vivino_url",
]


@dataclass(frozen=True)
class Candidate:
    name: str
    price: str
    url: str
    year: int | None
    quantity: int
    volume: str
    package_type: str
    score: float
    in_stock: str = ""


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def comparison_fieldnames(rows: list[dict[str, str]]) -> list[str]:
    fieldnames = list(rows[0].keys()) if rows else []
    for field in DEFAULT_COMPARISON_FIELDS:
        if field not in fieldnames:
            fieldnames.append(field)
    return fieldnames


def _build_platinum_source_row(row: dict[str, str]) -> dict[str, object]:
    prepared = prepare_rows(
        [
            {
                "name": row.get("name_plat") or "",
                "price": row.get("price_plat") or "",
                "url": row.get("url_plat") or "",
                "in_stock": row.get("platinum_in_stock") or "",
            }
        ],
        enforce_in_stock=False,
    )
    return prepared[0] if prepared else {}


def _candidate_pool(
    platinum_row: dict[str, str],
    grandcru_rows: list[dict[str, object]],
    *,
    max_candidates: int,
) -> list[Candidate]:
    plat_source = _build_platinum_source_row(platinum_row)
    if not plat_source:
        return []

    strict: list[Candidate] = []
    relaxed: list[Candidate] = []
    target_year = plat_source.get("year")
    target_volume = plat_source.get("volume")
    target_package = plat_source.get("package_type")

    for grandcru in grandcru_rows:
        if not grandcru.get("name") or not grandcru.get("url"):
            continue
        if not year_matches(target_year, grandcru.get("year")):
            continue
        if target_volume != grandcru.get("volume"):
            continue

        score = match_similarity(plat_source, grandcru)
        candidate = Candidate(
            name=str(grandcru["name"]),
            price=str(grandcru.get("price") or ""),
            url=str(grandcru["url"]),
            year=grandcru.get("year") if isinstance(grandcru.get("year"), int) else None,
            quantity=int(grandcru.get("quantity") or 1),
            volume=str(grandcru.get("volume") or ""),
            package_type=str(grandcru.get("package_type") or ""),
            score=round(score, 4),
            in_stock=str(grandcru.get("in_stock") or ""),
        )

        if candidate.package_type == target_package:
            strict.append(candidate)
        else:
            relaxed.append(candidate)

    pool = strict if strict else relaxed
    pool.sort(key=lambda item: (-item.score, item.name.lower()))
    return pool[:max_candidates]


def _build_prompt(wine_name: str, candidates: list[Candidate]) -> str:
    candidate_lines = "\n".join(
        f"{idx}. score={candidate.score:.4f} | name={candidate.name} | price={candidate.price} | "
        f"url={candidate.url} | year={candidate.year or 'unknown'} | quantity={candidate.quantity} | "
        f"volume={candidate.volume} | package={candidate.package_type}"
        for idx, candidate in enumerate(candidates, 1)
    )

    return (
        "You are matching a Platinum wine to the correct Grand Cru catalog row.\n"
        "Use only the candidates below. Do not invent a row.\n"
        "Return ONLY JSON with these keys:\n"
        "{"
        "\"decision\": \"match\" or \"no_match\", "
        "\"match_name\": string, "
        "\"match_url\": string, "
        "\"confidence\": number from 0 to 1, "
        "\"reason\": string"
        "}\n"
        "Rules:\n"
        "- Prefer the same producer, cuvée, vintage, and format.\n"
        "- If none are a true equivalent, return no_match.\n"
        "- The match_url must exactly match one of the candidate URLs.\n"
        f'Platinum wine: "{wine_name}"\n'
        "Candidate rows:\n"
        f"{candidate_lines}"
    )


def _apply_match(row: dict[str, str], candidate: Candidate) -> dict[str, str]:
    updated = dict(row)
    quantity_plat = parse_int(updated.get("quantity_plat")) or 1
    quantity_main = candidate.quantity or 1
    price_plat = parse_float(updated.get("price_plat"))
    price_main_total = parse_float(candidate.price)

    if price_plat is None or price_main_total is None:
        return updated

    price_main = round((price_main_total / max(quantity_main, 1)) * max(quantity_plat, 1), 2)
    price_diff = round(price_plat - price_main, 2)
    price_diff_pct = round((price_diff / price_main) * 100.0, 2) if price_main else None

    if price_diff is None or price_diff == 0:
        cheaper_side = "Same Price"
    elif price_diff < 0:
        cheaper_side = "Platinum Cheaper"
    else:
        cheaper_side = "Grand Cru Cheaper"

    updated["quantity_main"] = str(quantity_main)
    updated["price_main"] = f"{price_main:.2f}"
    updated["price_diff"] = f"{price_diff:.2f}"
    updated["price_diff_pct"] = f"{price_diff_pct:.2f}" if price_diff_pct is not None else ""
    updated["cheaper_side"] = cheaper_side
    updated["url_main"] = candidate.url
    updated["grand_cru_in_stock"] = getattr(candidate, "in_stock", "") or ""
    return updated


def _candidate_by_url(candidates: list[Candidate], url: str) -> Candidate | None:
    for candidate in candidates:
        if candidate.url == url:
            return candidate
    return None


def resolve_rows(
    comparison_rows: list[dict[str, str]],
    grandcru_rows: list[dict[str, object]],
    *,
    api_key: str,
    max_candidates: int,
    min_confidence: float,
    cache: dict[str, dict],
    force: bool,
    limit: int = 0,
) -> tuple[list[dict[str, str]], int]:
    updated_rows: list[dict[str, str]] = []
    matched_count = 0
    unresolved_seen = 0

    for row in comparison_rows:
        if (row.get("url_main") or "").strip():
            updated_rows.append(row)
            continue

        wine_name = (row.get("name_plat") or "").strip()
        if not wine_name:
            updated_rows.append(row)
            continue

        unresolved_seen += 1
        if limit > 0 and unresolved_seen > limit:
            updated_rows.append(row)
            continue

        candidates = _candidate_pool(row, grandcru_rows, max_candidates=max_candidates)
        if not candidates:
            updated_rows.append(row)
            continue

        cache_entry = cache.get(cache_key(wine_name))
        suggestion: dict[str, object] | None = None
        if not force and cache_entry and is_cache_fresh(cache_entry, CACHE_TTL_DAYS):
            maybe_suggestion = cache_entry.get("suggestion")
            if isinstance(maybe_suggestion, dict):
                suggestion = maybe_suggestion

        if suggestion is None:
            prompt = _build_prompt(wine_name, candidates)
            raw = call_gemini(prompt, api_key)
            suggestion = _parse_grounding_json(raw)
            cache[cache_key(wine_name)] = {
                "resolved_at": time.time(),
                "suggestion": suggestion,
            }

        decision = str(suggestion.get("decision") or "").strip().lower()
        match_url = str(suggestion.get("match_url") or "").strip()
        confidence = parse_float(suggestion.get("confidence"))
        candidate = _candidate_by_url(candidates, match_url) if match_url else None

        if decision != "match" or candidate is None or confidence is None or confidence < min_confidence:
            updated_rows.append(row)
            continue

        updated_rows.append(_apply_match(row, candidate))
        matched_count += 1

    return updated_rows, matched_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve unmatched Grand Cru comparison rows with Gemini")
    parser.add_argument("--comparison", type=Path, default=Path("seed/comparison_summary.csv"))
    parser.add_argument("--grandcru", type=Path, default=Path("seed/latest_refresh/grandcru_wines.csv"))
    parser.add_argument("--output", type=Path, default=None, help="Optional output CSV path. Defaults to overwriting --comparison.")
    parser.add_argument("--cache", type=Path, default=Path("data/grandcru_llm_cache.json"))
    parser.add_argument("--gemini-api-key", default="")
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of unresolved rows to process (0 = all).")
    parser.add_argument("--max-candidates", type=int, default=8)
    parser.add_argument("--min-confidence", type=float, default=0.75)
    parser.add_argument("--force", action="store_true", help="Bypass the cache and call Gemini for every row.")
    args = parser.parse_args()

    api_key = args.gemini_api_key.strip() or os.getenv("GEMINI_API_KEY", "").strip() or os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key:
        print("[llm-grandcru] skipped: no GEMINI_API_KEY or GOOGLE_API_KEY set")
        return

    comparison_rows = read_csv_rows(args.comparison)
    grandcru_rows = prepare_rows(read_csv_rows(args.grandcru), enforce_in_stock=False)

    cache = load_cache(args.cache)
    updated_rows, matched_count = resolve_rows(
        comparison_rows,
        grandcru_rows,
        api_key=api_key,
        max_candidates=args.max_candidates,
        min_confidence=args.min_confidence,
        cache=cache,
        force=args.force,
        limit=args.limit,
    )

    output_path = args.output or args.comparison
    write_csv_rows(
        output_path,
        updated_rows,
        comparison_fieldnames(comparison_rows),
    )
    save_cache(args.cache, cache)

    remaining = sum(1 for row in updated_rows if not (row.get("url_main") or "").strip())
    print(
        f"[llm-grandcru] matched={matched_count} remaining_no_match={remaining} "
        f"output={output_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
