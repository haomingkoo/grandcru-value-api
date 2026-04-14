"""Post-import wine data completeness validator.

Runs after import_wine_data.py to catch missing fields before the data
reaches the web interface. Exits with code 1 if any critical gaps are
found (use --strict to block deploys on warnings too).

Usage:
    python scripts/validate_wine_completeness.py
    python scripts/validate_wine_completeness.py --strict
    python scripts/validate_wine_completeness.py --json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.database import SessionLocal  # noqa: E402
from app.models import WineDeal  # noqa: E402
from sqlalchemy import select  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
)
logger = logging.getLogger("grandcru.validate")

# ── Known gaps (mirrors tests/test_data_completeness.py) ─────────────────────
# These are documented limitations — not silently ignored.
WINES_MISSING_VIVINO_URL: set[str] = {
    "2022 La Croix de Brully - Puligny-Montrachet Les Enseignères - White - 750 ml - Standard Bottle",
}

WINES_MISSING_VIVINO_PRICE: set[str] = {
    "2014 Rhys - Chardonnay Bearwallow Vineyard - White - 1.5 L - Magnum",
    "2017 Paul Hobbs - Chardonnay Dinner Vineyard Cuvee Agustina Sonoma Mountain - White - 750 ml - Standard Bottle",
    "2017 Rhys - Chardonnay Bearwallow Vineyard - White - 1.5 L - Magnum",
    "2017 Rhys - Chardonnay Mt. Pajaro Vineyard - White - 1.5 L - Magnum",
    "2021 Chateau Tahbilk - Shiraz Tower Release - Red - 750 ml - Standard Bottle",
    "2021 Hudelot - Baillet - Bonnes Mares - Red - 750 ml - Standard Bottle (Bundle of 3)",
    "2021 Pierre Boisson - Auxey Duresses - White - 750 ml - Standard Bottle (Bundle of 6)",
    "2022 00 Wines - Freya Hermann Cuvee Chardonnay - White - 750 ml - Standard Bottle",
    "2022 Domaine Claude Dugat - La Gibryotte Charmes Chambertin Grand Cru - Red - 750 ml - Standard Bottle (Bundle of 3)",
    "2022 La Croix de Brully - Puligny-Montrachet Les Enseignères - White - 750 ml - Standard Bottle",
    "2023 Hudelot - Baillet - Chambolle Musigny Charmes - Red - 750 ml - Standard Bottle (Bundle of 3)",
    "NV Adrien Renoir - Grand Cru Le Terroir - White - 750 ml - Standard Bottle",
    "NV Botter - Brilla Asolo Prosecco Superiore DOCG - White - 750 ml - Standard Bottle",
    "NV Charles Heidsieck - Brut Reserve - White - 750 ml - Standard Bottle",
    "NV Coutier - Tradition Brut - White - 750 ml - Standard Bottle",
    "NV Dhondt-Grellet - Extra Brut Blanc de Blancs Premier Cru Les Terres Fines (Base 2022) - White - 750 ml - Standard Bottle",
    "NV Miraval - Fleur De Miraval Exclusivement Rose 3 - Rose - 750 ml - Standard Bottle",
}


def _pct(count: int, total: int) -> str:
    if total == 0:
        return "N/A"
    return f"{count / total * 100:.1f}%"


def run_validation(*, strict: bool = False) -> dict:
    """Run completeness checks and return a structured report.

    Returns a dict with keys: total, errors, warnings, details.
    errors are critical gaps; warnings are known-gap allowlist items.
    """
    session = SessionLocal()
    try:
        wines: list[WineDeal] = session.execute(select(WineDeal)).scalars().all()
    finally:
        session.close()

    total = len(wines)
    errors: list[str] = []
    warnings: list[str] = []
    details: dict[str, list[str]] = {}

    # ── Critical fields (always required) ────────────────────────────────────

    missing_pt_price = [w.wine_name for w in wines if w.price_platinum is None]
    if missing_pt_price:
        errors.append(f"{len(missing_pt_price)} wine(s) missing price_platinum")
        details["missing_price_platinum"] = missing_pt_price

    missing_wine_type = [w.wine_name for w in wines if not w.wine_type]
    if missing_wine_type:
        errors.append(f"{len(missing_wine_type)} wine(s) missing wine_type")
        details["missing_wine_type"] = missing_wine_type

    missing_country = [w.wine_name for w in wines if not w.country]
    if missing_country:
        errors.append(f"{len(missing_country)} wine(s) missing country")
        details["missing_country"] = missing_country

    missing_grapes = [w.wine_name for w in wines if not w.grapes]
    if missing_grapes:
        errors.append(f"{len(missing_grapes)} wine(s) missing grapes")
        details["missing_grapes"] = missing_grapes

    missing_match = [
        w.wine_name for w in wines if not w.vivino_match_method
    ]
    if missing_match:
        errors.append(f"{len(missing_match)} wine(s) missing vivino_match_method (pipeline bug)")
        details["missing_match_method"] = missing_match

    # ── Allowlist-gated fields (warn if unexpected) ───────────────────────────

    missing_url = [w.wine_name for w in wines if not w.vivino_url]
    unexpected_no_url = [n for n in missing_url if n not in WINES_MISSING_VIVINO_URL]
    known_no_url = [n for n in missing_url if n in WINES_MISSING_VIVINO_URL]
    if unexpected_no_url:
        errors.append(
            f"{len(unexpected_no_url)} wine(s) missing Vivino URL (not in allowlist)"
        )
        details["missing_vivino_url_unexpected"] = unexpected_no_url
    if known_no_url:
        warnings.append(
            f"{len(known_no_url)} wine(s) in known-gap allowlist with no Vivino URL"
        )
        details["missing_vivino_url_known"] = known_no_url

    missing_rating = [w.wine_name for w in wines if w.vivino_rating is None]
    unexpected_no_rating = [n for n in missing_rating if n not in WINES_MISSING_VIVINO_URL]
    known_no_rating = [n for n in missing_rating if n in WINES_MISSING_VIVINO_URL]
    if unexpected_no_rating:
        errors.append(
            f"{len(unexpected_no_rating)} wine(s) missing Vivino rating (not in allowlist)"
        )
        details["missing_vivino_rating_unexpected"] = unexpected_no_rating
    if known_no_rating:
        warnings.append(
            f"{len(known_no_rating)} wine(s) in known-gap allowlist with no Vivino rating"
        )
        details["missing_vivino_rating_known"] = known_no_rating

    missing_price = [w.wine_name for w in wines if w.vivino_price is None]
    unexpected_no_price = [n for n in missing_price if n not in WINES_MISSING_VIVINO_PRICE]
    known_no_price = [n for n in missing_price if n in WINES_MISSING_VIVINO_PRICE]
    if unexpected_no_price:
        errors.append(
            f"{len(unexpected_no_price)} wine(s) missing Vivino price (not in allowlist)"
        )
        details["missing_vivino_price_unexpected"] = unexpected_no_price
    if known_no_price:
        warnings.append(
            f"{len(known_no_price)} wine(s) in known-gap allowlist with no Vivino price "
            "(requires manual SGD price entry)"
        )

    return {
        "total": total,
        "errors": errors,
        "warnings": warnings,
        "details": details,
        "coverage": {
            "vivino_rating": _pct(total - len(missing_rating), total),
            "vivino_url": _pct(total - len(missing_url), total),
            "vivino_price": _pct(total - len(missing_price), total),
            "country": _pct(total - len(missing_country), total),
            "grapes": _pct(total - len(missing_grapes), total),
        },
    }


def _log_report(report: dict) -> None:
    total = report["total"]
    cov = report["coverage"]
    logger.info(
        "completeness total=%d rating=%s url=%s price=%s country=%s grapes=%s",
        total,
        cov["vivino_rating"],
        cov["vivino_url"],
        cov["vivino_price"],
        cov["country"],
        cov["grapes"],
    )
    for warning in report["warnings"]:
        logger.warning("known gap: %s", warning)
    for error in report["errors"]:
        logger.error("CRITICAL: %s", error)
    for field, names in report["details"].items():
        if "unexpected" in field:
            for name in names:
                logger.error("  missing %s: %s", field.replace("missing_", ""), name)
        elif field in ("missing_country", "missing_grapes", "missing_wine_type",
                       "missing_price_platinum", "missing_match_method"):
            for name in names:
                logger.error("  %s: %s", field, name)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate wine data completeness after import")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with code 1 on warnings as well as errors",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output report as JSON to stdout (for CI consumption)",
    )
    args = parser.parse_args()

    report = run_validation(strict=args.strict)

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        _log_report(report)

    has_errors = bool(report["errors"])
    has_warnings = bool(report["warnings"])

    if has_errors:
        sys.exit(1)
    if args.strict and has_warnings:
        logger.error("Exiting with error due to --strict mode and warnings present")
        sys.exit(1)

    logger.info("Validation passed")


if __name__ == "__main__":
    main()
