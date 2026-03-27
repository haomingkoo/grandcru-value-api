"""Validate market_prices.csv — check Wine-Searcher URLs match the wine.

Flags real mismatches: wrong classification level, wrong producer,
wrong cuvée, wrong grape. Tolerates fuzzy spelling differences.

Usage:
    python scripts/validate_market_prices.py
    python scripts/validate_market_prices.py --fix  # remove flagged rows
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import unquote_plus, urlparse

ROOT = Path(__file__).resolve().parents[1]
MARKET_PATH = ROOT / "seed" / "market_prices.csv"

COLOURS = {"red", "white", "rose", "rosé"}


def _url_search_text(url: str) -> str:
    """Extract the search terms from a Wine-Searcher /find/ URL as a string."""
    parsed = urlparse(url)
    path = unquote_plus(parsed.path).lower()
    match = re.search(r"/find/(.+?)(?:/\d{4})?$", path)
    if not match:
        return ""
    return match.group(1).replace("+", " ").replace("-", " ")


def _parse_wine_name(name: str) -> dict:
    """Parse structured wine name into producer, label, colour, etc.

    Format: "2021 Domaine Xavier Monnot - Meursault Charmes - White - 750 ml - ..."
    Challenge: some producers have dashes (Hudelot - Baillet).
    Strategy: find the colour token to anchor the split.
    """
    # Find colour position to split reliably
    parts = [p.strip() for p in name.split(" - ")]
    colour_idx = None
    for i, p in enumerate(parts):
        if p.lower() in COLOURS:
            colour_idx = i
            break

    if colour_idx and colour_idx >= 2:
        # Everything before colour_idx-1 is producer, colour_idx-1 is label
        producer_parts = parts[0:colour_idx - 1]
        producer_raw = " ".join(producer_parts)
        label = parts[colour_idx - 1]
    elif len(parts) >= 2:
        producer_raw = parts[0]
        label = parts[1] if parts[1].lower() not in COLOURS else None
    else:
        producer_raw = parts[0]
        label = None

    # Strip vintage year from producer
    producer = re.sub(r"^(NV|\d{4})\s+", "", producer_raw).lower().strip()
    label_lower = label.lower().strip() if label else None

    lower = name.lower()
    return {
        "producer": producer,
        "label": label_lower,
        "has_premier_cru": "premier cru" in lower or "1er cru" in lower,
        "has_grand_cru": "grand cru" in lower,
    }


def _fuzzy_in(needle: str, haystack: str, threshold: float = 0.75) -> bool:
    """Check if needle appears in haystack, allowing fuzzy spelling."""
    if needle in haystack:
        return True
    # Check each word in haystack for fuzzy match
    for word in haystack.split():
        if SequenceMatcher(None, needle, word).ratio() >= threshold:
            return True
    return False


def _extract_key_terms(text: str) -> list[str]:
    """Extract meaningful terms, dropping generic wine words."""
    skip = {
        "les", "le", "la", "de", "du", "des", "en", "aux", "et",
        "clos", "cru", "premier", "grand", "1er", "domaine", "dom",
        "chateau", "château", "maison", "tenuta", "cave",
    }
    tokens = re.split(r"[\s\-]+", text.lower())
    return [t for t in tokens if t not in skip and len(t) > 2]


def validate_row(name: str, url: str) -> list[str]:
    """Return list of issues. Empty = passed."""
    issues = []
    url_text = _url_search_text(url)
    if not url_text:
        return ["could not parse URL"]

    info = _parse_wine_name(name)

    # 1. Classification mismatch (most reliable check)
    url_lower = url.lower()
    if not info["has_premier_cru"] and "premier" in url_text and "cru" in url_text:
        issues.append("URL has 'premier cru' but wine name does not")
    if not info["has_grand_cru"] and "grand" in url_text and "cru" in url_text:
        if "champagne" not in url_text:
            issues.append("URL has 'grand cru' but wine name does not")

    # 2. Producer check: key surnames should fuzzy-match something in URL
    producer_terms = _extract_key_terms(info["producer"])
    if producer_terms:
        matched = sum(1 for t in producer_terms if _fuzzy_in(t, url_text))
        if matched == 0:
            issues.append(
                f"producer '{info['producer']}' — no terms match URL"
            )

    # 3. Label / cuvée check
    if info["label"]:
        label_terms = _extract_key_terms(info["label"])
        if label_terms:
            matched = sum(1 for t in label_terms if _fuzzy_in(t, url_text))
            if matched == 0:
                issues.append(
                    f"label '{info['label']}' — no terms match URL"
                )

    return issues


def validate_price_sanity(
    name: str,
    price_sgd: float | None,
    comparison_lookup: dict[str, dict],
) -> list[str]:
    """Check if market price is reasonable vs Platinum/Grand Cru prices."""
    if price_sgd is None or price_sgd <= 0:
        return []

    comp = comparison_lookup.get(name)
    if not comp:
        return []

    issues = []
    plat_per = float(comp.get("price_plat") or 0)
    gc_per = float(comp.get("price_main") or 0)
    ref = plat_per or gc_per  # Use whichever is available

    if ref <= 0:
        return []

    ratio = price_sgd / ref
    if ratio > 3.0:
        issues.append(
            f"market S${price_sgd:.0f} is {ratio:.1f}× retailer "
            f"S${ref:.0f}/btl — likely wrong wine"
        )
    elif ratio < 0.15:
        issues.append(
            f"market S${price_sgd:.0f} is only {ratio:.1%} of retailer "
            f"S${ref:.0f}/btl — suspiciously cheap"
        )

    return issues


def _load_comparison_lookup(path: Path) -> dict[str, dict]:
    """Load comparison CSV keyed by wine name."""
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    return {r.get("name_plat", ""): r for r in rows if r.get("name_plat")}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fix", action="store_true", help="Remove flagged rows")
    parser.add_argument("--path", type=Path, default=MARKET_PATH)
    parser.add_argument(
        "--comparison", type=Path,
        default=ROOT / "seed" / "comparison_summary.csv",
        help="Comparison CSV for price sanity check",
    )
    args = parser.parse_args()

    if not args.path.exists():
        print(f"File not found: {args.path}")
        sys.exit(1)

    with args.path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    comp_lookup = _load_comparison_lookup(args.comparison)

    clean_rows = []
    flagged = 0

    for row in rows:
        name = row.get("match_name", "")
        url = row.get("retailer_url", "")
        price_str = row.get("price_sgd", "")

        # Check 1: URL validation (producer, label, classification)
        issues = validate_row(name, url)

        # Check 2: Price sanity vs retailer prices
        try:
            price_val = float(price_str) if price_str else None
        except ValueError:
            price_val = None
        issues.extend(validate_price_sanity(name, price_val, comp_lookup))

        if issues:
            flagged += 1
            print(f"\n  FLAGGED: {name[:72]}")
            for issue in issues:
                print(f"    - {issue}")
            print(f"    URL: {url}")
        else:
            clean_rows.append(row)

    print(f"\n{'='*60}")
    print(f"Total: {len(rows)} | Passed: {len(rows) - flagged} | Flagged: {flagged}")

    if args.fix and flagged > 0:
        with args.path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(clean_rows)
        print(f"Removed {flagged} rows. {len(clean_rows)} remain.")


if __name__ == "__main__":
    main()
