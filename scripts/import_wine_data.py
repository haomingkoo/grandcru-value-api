import argparse
import csv
import logging
import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import quote_plus, urlparse, urlunparse

from sqlalchemy import delete, select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import settings  # noqa: E402
from app.database import Base, SessionLocal, engine  # noqa: E402
from app.models import IngestionRun, WineDeal, WineDealSnapshot  # noqa: E402
from app.scoring import compute_deal_score, parse_float, parse_int  # noqa: E402
from app.wine_metadata import derive_wine_metadata  # noqa: E402


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("grandcru.import")

DEAL_EXTRA_COLUMNS = (
    ("producer", "VARCHAR(255)"),
    ("label_name", "VARCHAR(255)"),
    ("country", "VARCHAR(128)"),
    ("region", "VARCHAR(128)"),
    ("wine_type", "VARCHAR(64)"),
    ("style_family", "VARCHAR(64)"),
    ("grapes", "VARCHAR(255)"),
    ("offering_type", "VARCHAR(64)"),
    ("origin_label", "VARCHAR(255)"),
    ("origin_latitude", "FLOAT"),
    ("origin_longitude", "FLOAT"),
    ("origin_precision", "VARCHAR(32)"),
)

SNAPSHOT_FIELDS = {
    "wine_name",
    "vintage",
    "quantity",
    "volume",
    "price_platinum",
    "price_grand_cru",
    "price_diff",
    "price_diff_pct",
    "cheaper_side",
    "platinum_url",
    "grand_cru_url",
    "vivino_url",
    "vivino_rating",
    "vivino_num_ratings",
    "vivino_match_method",
    "deal_score",
}

PLATINUM_LEGACY_HOSTS = (
    "https://platinum.grandcruwines.com",
    "http://platinum.grandcruwines.com",
)
PLATINUM_BASE_URL_OVERRIDE = os.getenv("PLATINUM_BASE_URL_OVERRIDE", "").strip()

_NON_ALNUM_RE = re.compile(r"[^a-z0-9\s]")
_SPACE_RE = re.compile(r"\s+")
_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
_DROP_TOKENS = {
    "and",
    "the",
    "de",
    "la",
    "le",
    "du",
    "des",
    "di",
    "standard",
    "bottle",
    "magnum",
    "jeroboam",
    "double",
    "half",
    "red",
    "white",
    "rose",
    "blanc",
    "rouge",
    "ml",
    "l",
    "igt",
    "doc",
    "docg",
    "aoc",
    "aop",
    "vdt",
}


def normalize_key(value: str | None) -> str:
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
    text = text.replace("&", " and ")
    text = text.replace("’", "'")
    text = text.replace("'", "")
    text = _NON_ALNUM_RE.sub(" ", text)
    return _SPACE_RE.sub(" ", text).strip()


def extract_year(value: str | None) -> int | None:
    if not value:
        return None
    match = _YEAR_RE.search(str(value))
    if not match:
        return None
    return int(match.group(0))


def canonicalize_key(value: str | None) -> str:
    base = normalize_key(value)
    if not base:
        return ""

    tokens: list[str] = []
    for token in base.split():
        if token in _DROP_TOKENS:
            continue
        if token.endswith("ml") and token[:-2].isdigit():
            continue
        if token.endswith("l") and token[:-1].isdigit():
            continue
        if token.isdigit() and len(token) <= 3:
            continue
        tokens.append(token)
    return " ".join(tokens)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def read_optional_csv_rows(path: Path | None) -> list[dict[str, str]]:
    if path is None or not path.exists():
        return []
    return read_csv_rows(path)


def _annotate_vivino_rows(rows: list[dict[str, str]], source: str) -> list[dict[str, str]]:
    annotated: list[dict[str, str]] = []
    for row in rows:
        tagged = dict(row)
        tagged["__source"] = source
        annotated.append(tagged)
    return annotated


@dataclass(frozen=True)
class VivinoLookup:
    exact: dict[str, dict[str, str]]
    canonical: dict[str, list[dict[str, str]]]
    by_year: dict[int, list[dict[str, str]]]
    rows: list[dict[str, str]]


def _has_value(value: str | None) -> bool:
    return bool((value or "").strip())


def _vivino_row_quality(row: dict[str, str]) -> tuple[int, int, int, int]:
    # Prefer rows that carry concrete rating data, then URL and price.
    return (
        1 if _has_value(row.get("vivino_rating")) else 0,
        1 if _has_value(row.get("vivino_num_ratings") or row.get("vivino_raters")) else 0,
        1 if _has_value(row.get("vivino_url")) else 0,
        1 if _has_value(row.get("vivino_price")) else 0,
    )


def _pick_better_vivino_row(current: dict[str, str], candidate: dict[str, str]) -> dict[str, str]:
    if _vivino_row_quality(candidate) >= _vivino_row_quality(current):
        return candidate
    return current


def build_vivino_lookup(rows: list[dict[str, str]]) -> VivinoLookup:
    exact: dict[str, dict[str, str]] = {}
    canonical: dict[str, list[dict[str, str]]] = {}
    by_year: dict[int, list[dict[str, str]]] = {}

    for row in rows:
        candidate_names = [row.get("wine_name"), row.get("match_name")]
        for candidate_name in candidate_names:
            key = normalize_key(candidate_name)
            if key:
                existing = exact.get(key)
                if existing is None:
                    exact[key] = row
                else:
                    exact[key] = _pick_better_vivino_row(existing, row)

            canonical_key = canonicalize_key(candidate_name)
            if canonical_key:
                canonical.setdefault(canonical_key, []).append(row)

            year = extract_year(candidate_name)
            if year is not None:
                by_year.setdefault(year, []).append(row)

    return VivinoLookup(exact=exact, canonical=canonical, by_year=by_year, rows=rows)


def _token_set_ratio(target_tokens: set[str], candidate_tokens: set[str]) -> float:
    intersection = sorted(target_tokens & candidate_tokens)
    if not intersection:
        return 0.0

    intersection_text = " ".join(intersection)
    target_text = " ".join(sorted(target_tokens))
    candidate_text = " ".join(sorted(candidate_tokens))
    ratio_to_target = SequenceMatcher(None, intersection_text, target_text).ratio()
    ratio_to_candidate = SequenceMatcher(None, intersection_text, candidate_text).ratio()
    return max(ratio_to_target, ratio_to_candidate)


def _score_name_similarity(target: str, candidate: str) -> tuple[float, float, float, float, int]:
    target_tokens = set(target.split())
    candidate_tokens = set(candidate.split())
    if not target_tokens or not candidate_tokens:
        return (0.0, 0.0, 0.0, 0.0, 0)

    overlap = len(target_tokens & candidate_tokens)
    token_ratio = overlap / max(len(target_tokens), len(candidate_tokens))
    seq_ratio = SequenceMatcher(None, target, candidate).ratio()
    set_ratio = _token_set_ratio(target_tokens, candidate_tokens)
    combined = (token_ratio * 0.45) + (seq_ratio * 0.20) + (set_ratio * 0.35)
    return (combined, token_ratio, seq_ratio, set_ratio, overlap)


def match_vivino_row(wine_name: str, lookup: VivinoLookup) -> tuple[dict[str, str], str]:
    exact_key = normalize_key(wine_name)
    if exact_key and exact_key in lookup.exact:
        return lookup.exact[exact_key], "exact"

    canonical_key = canonicalize_key(wine_name)
    if canonical_key and canonical_key in lookup.canonical:
        candidates = lookup.canonical[canonical_key]
        target_year = extract_year(wine_name)
        if target_year is not None:
            for candidate in candidates:
                candidate_name = candidate.get("match_name") or candidate.get("wine_name")
                if extract_year(candidate_name) == target_year:
                    return candidate, "canonical"
        return candidates[0], "canonical"

    if not canonical_key:
        return {}, "none"

    target_year = extract_year(wine_name)
    candidate_rows = lookup.by_year.get(target_year, []) if target_year is not None else []
    if not candidate_rows:
        candidate_rows = lookup.rows

    scored_candidates: list[tuple[float, float, float, float, int, dict[str, str]]] = []
    for candidate in candidate_rows:
        candidate_name = candidate.get("match_name") or candidate.get("wine_name")
        candidate_key = canonicalize_key(candidate_name)
        if not candidate_key:
            continue

        combined, token_ratio, seq_ratio, set_ratio, overlap = _score_name_similarity(canonical_key, candidate_key)
        if combined <= 0:
            continue
        scored_candidates.append((combined, token_ratio, seq_ratio, set_ratio, overlap, candidate))

    if not scored_candidates:
        return {}, "none"

    scored_candidates.sort(key=lambda item: item[0], reverse=True)
    best = scored_candidates[0]
    second_best = scored_candidates[1] if len(scored_candidates) > 1 else None

    # Gate to avoid linking the wrong wine — relaxed to improve coverage.
    if best[4] < 2:
        return {}, "none"
    if best[0] < 0.60 and best[3] < 0.85:
        return {}, "none"
    if best[1] < 0.35 and best[3] < 0.85:
        return {}, "none"
    if second_best is not None and (best[0] - second_best[0]) < 0.02 and best[3] < 0.88:
        return {}, "none"

    return best[5], "fuzzy"




def normalize_platinum_url(url: str | None) -> str | None:
    if not url:
        return None
    normalized = url.strip()
    if not PLATINUM_BASE_URL_OVERRIDE:
        return normalized or None

    for legacy_host in PLATINUM_LEGACY_HOSTS:
        if normalized.startswith(legacy_host):
            suffix = normalized[len(legacy_host) :]
            normalized = f"{PLATINUM_BASE_URL_OVERRIDE.rstrip('/')}{suffix}"
            break
    return normalized


def normalize_url(url: str | None) -> str | None:
    if not url:
        return None
    return url.strip() or None


def normalize_vivino_url(url: str | None) -> str | None:
    if not url:
        return None
    cleaned = url.strip()
    parsed = urlparse(cleaned)
    if "vivino.com" not in (parsed.netloc or ""):
        return cleaned
    if "/w/" not in (parsed.path or ""):
        return cleaned
    normalized_path = (parsed.path or "").rstrip("/")
    return urlunparse((parsed.scheme or "https", parsed.netloc, normalized_path, "", "", ""))


def build_vivino_url_index(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    index: dict[str, dict[str, str]] = {}
    for row in rows:
        normalized = normalize_vivino_url(row.get("vivino_url"))
        if not normalized:
            continue
        existing = index.get(normalized)
        if existing is None:
            index[normalized] = row
        else:
            index[normalized] = _pick_better_vivino_row(existing, row)
    return index


def build_vivino_search_url(query: str | None) -> str | None:
    cleaned = (query or "").strip()
    if not cleaned:
        return None
    return f"https://www.vivino.com/en/search/wines?q={quote_plus(cleaned)}"


def _build_market_lookup(path: Path | None) -> dict[str, dict[str, str]]:
    """Build a lookup from wine name → market price data."""
    if not path or not path.exists():
        return {}
    rows = read_optional_csv_rows(path)
    lookup: dict[str, dict[str, str]] = {}
    for row in rows:
        name = (row.get("match_name") or "").strip()
        price = row.get("price_sgd", "")
        if name and price:
            key = canonicalize_key(name)
            if key:
                lookup[key] = row
    return lookup


def _resolve_market_price(
    wine_name: str,
    quantity: int,
    volume: str,
    lookup: dict[str, dict[str, str]],
) -> float | None:
    """Look up market price and scale to match Platinum's listing.

    Wine-Searcher prices are per standard 750ml bottle (USD → SGD).
    Scale for magnums (×2), double magnums (×4), then by bundle quantity
    so comparison is apples-to-apples with Platinum's total listing price.
    """
    key = canonicalize_key(wine_name)
    if not key or key not in lookup:
        return None
    raw = parse_float(lookup[key].get("price_sgd"))
    if raw is None or raw <= 0:
        return None
    # Scale for non-standard volumes (WS is always per 750ml)
    vol_lower = (volume or "").lower()
    if vol_lower in ("1.5l", "1500ml", "magnum"):
        raw = raw * 2
    elif vol_lower in ("3l", "3000ml", "double magnum", "jeroboam"):
        raw = raw * 4
    # Scale to Platinum's bundle quantity
    return round(raw * max(quantity, 1), 2)


def _resolve_market_field(
    wine_name: str, lookup: dict[str, dict[str, str]], field: str,
) -> str | None:
    """Look up a string field from the market price lookup."""
    key = canonicalize_key(wine_name)
    if not key or key not in lookup:
        return None
    return lookup[key].get(field) or None


def _scale_vivino_price_to_listing(
    raw_price: float | None,
    quantity: int | None,
    volume: str | None,
) -> float | None:
    if raw_price is None or raw_price <= 0:
        return None

    adjusted = raw_price
    volume_lower = (volume or "").lower()
    if volume_lower in ("1.5l", "1500ml", "magnum"):
        adjusted *= 2
    elif volume_lower in ("3l", "3000ml", "double magnum", "jeroboam"):
        adjusted *= 4

    if quantity and quantity > 1:
        adjusted *= quantity

    return round(adjusted, 2)


def _price_anchor(price_platinum: float | None, price_grand_cru: float | None) -> float | None:
    anchors = [price for price in (price_platinum, price_grand_cru) if price is not None and price > 0]
    if not anchors:
        return None
    return sum(anchors) / len(anchors)


def _is_override_price_outlier(price: float, anchor: float) -> bool:
    if price <= 0 or anchor <= 0:
        return False
    ratio = price / anchor
    return ratio < 0.40 or ratio > 2.0


def _resolve_vivino_price_to_listing(
    raw_price: float | None,
    quantity: int | None,
    volume: str | None,
    *,
    price_platinum: float | None,
    price_grand_cru: float | None,
    source: str,
    vivino_url: str | None = None,
) -> float | None:
    if not vivino_url:
        return None
    adjusted = _scale_vivino_price_to_listing(raw_price, quantity, volume)
    if adjusted is None:
        return None

    if source != "override":
        return adjusted

    anchor = _price_anchor(price_platinum, price_grand_cru)
    chosen = adjusted
    if anchor is not None and raw_price is not None and round(raw_price, 2) != adjusted:
        raw_candidate = round(raw_price, 2)
        chosen = raw_candidate if abs(raw_candidate - anchor) <= abs(adjusted - anchor) else adjusted

    if anchor is None:
        return chosen
    if _is_override_price_outlier(chosen, anchor):
        return None
    return chosen


def import_data(
    comparison_path: Path,
    vivino_path: Path,
    vivino_overrides_path: Path | None = None,
    *,
    market_prices_path: Path | None = None,
) -> None:
    if not comparison_path.exists():
        raise FileNotFoundError(f"comparison_summary missing: {comparison_path}")
    if not vivino_path.exists():
        raise FileNotFoundError(f"vivino_results missing: {vivino_path}")

    Base.metadata.create_all(bind=engine)

    from app.database import ensure_column

    ensure_column("wine_deals", "vivino_match_method", "VARCHAR(32)")
    ensure_column("wine_deal_snapshots", "vivino_match_method", "VARCHAR(32)")
    ensure_column("wine_deals", "vivino_price", "FLOAT")
    ensure_column("wine_deals", "vivino_description", "VARCHAR(512)")
    for column, col_type in DEAL_EXTRA_COLUMNS:
        ensure_column("wine_deals", column, col_type)
    ensure_column("wine_deals", "price_market", "FLOAT")
    ensure_column("wine_deals", "market_retailer_name", "VARCHAR(128)")
    ensure_column("wine_deals", "market_retailer_url", "VARCHAR(512)")

    comparison_rows = read_csv_rows(comparison_path)
    vivino_rows_base = _annotate_vivino_rows(read_csv_rows(vivino_path), "base")
    vivino_rows_override = _annotate_vivino_rows(read_optional_csv_rows(vivino_overrides_path), "override")
    vivino_rows = vivino_rows_base + vivino_rows_override
    vivino_lookup = build_vivino_lookup(vivino_rows)
    vivino_by_url = build_vivino_url_index(vivino_rows)
    market_lookup = _build_market_lookup(market_prices_path)

    logger.info(
        "import_start comparison=%s vivino=%s overrides=%s",
        comparison_path,
        vivino_path,
        vivino_overrides_path,
    )

    session = SessionLocal()
    run = IngestionRun(status="running", comparison_rows=len(comparison_rows), vivino_rows=len(vivino_rows))
    session.add(run)
    session.commit()
    session.refresh(run)

    try:
        merged_records: list[WineDeal] = []
        snapshot_records: list[WineDealSnapshot] = []
        snapshot_time = datetime.now(UTC)

        match_counts: dict[str, int] = {
            "exact": 0, "canonical": 0, "fuzzy": 0, "platinum": 0, "none": 0,
        }

        for row in comparison_rows:
            wine_name = (row.get("name_plat") or "").strip()
            if not wine_name:
                continue

            # --- Source 1: vivino_results.csv + overrides (richest: rating + count + URL) ---
            vivino, match_method = match_vivino_row(wine_name, vivino_lookup)
            vivino_rating = parse_float(vivino.get("vivino_rating"))
            vivino_num_ratings = (
                parse_int(vivino.get("vivino_num_ratings"))
                or parse_int(vivino.get("vivino_raters"))
            )
            vivino_url = normalize_vivino_url(vivino.get("vivino_url"))

            # If a matched row has URL but blank metrics, hydrate from URL index.
            if vivino_url and vivino_rating is None and vivino_num_ratings is None:
                url_row = vivino_by_url.get(vivino_url)
                if url_row is not None:
                    url_rating = parse_float(url_row.get("vivino_rating"))
                    if url_rating is not None:
                        vivino_rating = url_rating
                    url_num_ratings = (
                        parse_int(url_row.get("vivino_num_ratings"))
                        or parse_int(url_row.get("vivino_raters"))
                    )
                    if url_num_ratings is not None:
                        vivino_num_ratings = url_num_ratings

            # --- Source 2: Platinum-embedded Vivino data (explicit, named) ---
            # Only used when CSV matching found nothing. Not a hidden fallback —
            # stored as match_method="platinum" and visible in the API/UI.
            if match_method == "none":
                plat_rating = parse_float(row.get("platinum_vivino_rating"))
                if plat_rating is not None:
                    vivino_rating = plat_rating
                    vivino_num_ratings = parse_int(row.get("platinum_vivino_num_ratings"))
                    vivino_url = normalize_url(row.get("platinum_vivino_url"))
                    match_method = "platinum"

            # Generate a Vivino search URL for wines that have a rating but no direct link.
            if vivino_url is None and vivino_rating is not None:
                vivino_url = build_vivino_search_url(
                    vivino.get("wine_name") or vivino.get("match_name") or wine_name
                )

            # Preserve Vivino URLs even without metrics — users can click through to verify.
            # Only generate a search URL when we have a rating but no direct link.

            match_counts[match_method] = match_counts.get(match_method, 0) + 1

            # The comparison CSV stores total listing prices (post build_summary):
            # - price_plat: Platinum's listing total (e.g. $600 for 3 bottles)
            # - price_main: Grand Cru scaled to match Platinum's quantity
            # Use as-is — no further scaling needed.
            price_platinum = parse_float(row.get("price_plat"))
            price_grand_cru = parse_float(row.get("price_main"))
            price_diff = parse_float(row.get("price_diff"))
            price_diff_pct = parse_float(row.get("price_diff_pct"))
            grand_cru_url = normalize_url(row.get("url_main")) if price_grand_cru is not None else None

            quantity = parse_int(row.get("quantity_plat"))
            volume = (row.get("volume_plat") or "").strip() or None

            if price_diff is None and price_platinum is not None and price_grand_cru is not None:
                price_diff = round(price_platinum - price_grand_cru, 2)
            if price_diff_pct is None and price_diff is not None and price_grand_cru not in (None, 0):
                price_diff_pct = round((price_diff / price_grand_cru) * 100.0, 2)

            metadata = derive_wine_metadata(
                wine_name=wine_name,
                quantity=quantity,
                volume=volume,
            )

            # --- Volume-aware Vivino price ---
            # Base Vivino results are raw bottle prices and must be scaled to
            # match the Platinum listing. Override rows are trickier: some are
            # stored as bottle prices, others already reflect the reviewed
            # listing total. For overrides, keep the interpretation that stays
            # closest to the retailer anchors instead of blindly multiplying.
            raw_vivino_price = parse_float(vivino.get("vivino_price"))
            vivino_price_adjusted = _resolve_vivino_price_to_listing(
                raw_vivino_price,
                quantity,
                volume,
                price_platinum=price_platinum,
                price_grand_cru=price_grand_cru,
                source=(vivino.get("__source") or "base").strip().lower(),
                vivino_url=vivino_url,
            )

            # --- Gift set detection ---
            gc_url_lower = (grand_cru_url or "").lower()
            gc_name_lower = wine_name.lower()
            _GIFT_TOKENS = ("gift-box", "gift box", "gift-set", "gift set", "2-glasses", "2 glasses")
            is_gift_set = any(tok in gc_url_lower or tok in gc_name_lower for tok in _GIFT_TOKENS)
            if is_gift_set:
                logger.info("gift_set_detected wine=%s gc_url=%s", wine_name, grand_cru_url)

            # --- Vivino metadata for gap-fill (Phase 5) ---
            # Fill metadata gaps with Vivino-extracted data (grapes, region).
            vivino_grapes = (vivino.get("vivino_grapes") or "").strip()
            vivino_region_raw = (vivino.get("vivino_region") or "").strip()

            gap_fill: dict[str, str] = {}
            if not metadata.grapes and vivino_grapes:
                gap_fill["grapes"] = vivino_grapes
                gap_fill["grape_source"] = "vivino"
            if vivino_region_raw and "/" in vivino_region_raw:
                parts = [p.strip() for p in vivino_region_raw.split("/")]
                if not metadata.country and len(parts) >= 1:
                    gap_fill["country"] = parts[0]
                if not metadata.region and len(parts) >= 2:
                    gap_fill["region"] = parts[1]
            if gap_fill:
                from dataclasses import replace as _dc_replace
                metadata = _dc_replace(metadata, **gap_fill)

            vivino_desc = (vivino.get("vivino_description") or "").strip() or None

            deal_payload = {
                "wine_name": wine_name,
                "vintage": parse_int(row.get("year_plat")),
                "quantity": quantity,
                "volume": volume,
                "price_platinum": price_platinum,
                "price_grand_cru": price_grand_cru,
                "price_diff": price_diff,
                "price_diff_pct": price_diff_pct,
                "cheaper_side": (row.get("cheaper_side") or "").strip() or None,
                "platinum_url": normalize_platinum_url(row.get("url_plat")),
                "grand_cru_url": grand_cru_url,
                "vivino_url": vivino_url,
                "vivino_rating": vivino_rating,
                "vivino_num_ratings": vivino_num_ratings,
                "vivino_price": vivino_price_adjusted,
                "vivino_description": vivino_desc,
                "vivino_match_method": match_method,
                "price_market": _resolve_market_price(
                    wine_name, quantity, volume, market_lookup,
                ),
                "market_retailer_name": _resolve_market_field(
                    wine_name, market_lookup, "retailer_name",
                ),
                "market_retailer_url": _resolve_market_field(
                    wine_name, market_lookup, "retailer_url",
                ),
                "producer": metadata.producer,
                "label_name": metadata.label_name,
                "country": metadata.country,
                "region": metadata.region,
                "wine_type": metadata.wine_type,
                "style_family": metadata.style_family,
                "grapes": metadata.grapes,
                "offering_type": metadata.offering_type,
                "origin_label": metadata.origin_label,
                "origin_latitude": metadata.origin_latitude,
                "origin_longitude": metadata.origin_longitude,
                "origin_precision": metadata.origin_precision,
                "deal_score": compute_deal_score(
                    price_diff_pct,
                    vivino_rating,
                    vivino_num_ratings,
                    price_platinum=price_platinum,
                    vivino_price=vivino_price_adjusted,
                ),
            }
            merged_records.append(WineDeal(**deal_payload))
            snapshot_payload = {key: value for key, value in deal_payload.items() if key in SNAPSHOT_FIELDS}
            snapshot_records.append(
                WineDealSnapshot(
                    ingestion_run_id=run.id,
                    captured_at=snapshot_time,
                    **snapshot_payload,
                )
            )

        session.execute(delete(WineDeal))
        session.add_all(merged_records)
        session.add_all(snapshot_records)

        cutoff = snapshot_time - timedelta(days=settings.history_retention_days)
        prune_result = session.execute(delete(WineDealSnapshot).where(WineDealSnapshot.captured_at < cutoff))
        deleted_snapshots = int(prune_result.rowcount or 0)

        run.status = "success"
        run.finished_at = datetime.now(UTC)
        run.merged_rows = len(merged_records)
        match_summary = ", ".join(f"{k}={v}" for k, v in sorted(match_counts.items()))
        run.details = (
            f"Loaded {len(comparison_rows)} comparison rows and {len(vivino_rows_base)} vivino rows "
            f"(+{len(vivino_rows_override)} overrides) into {len(merged_records)} current deals and "
            f"{len(snapshot_records)} snapshots (vivino: {match_summary}); "
            f"pruned {deleted_snapshots} snapshots older than {settings.history_retention_days} days."
        )
        session.commit()
        logger.info("import_success %s", run.details)
    except Exception as exc:
        session.rollback()
        run.status = "failed"
        run.finished_at = datetime.now(UTC)
        run.details = f"Import failed: {exc}"
        session.add(run)
        session.commit()
        logger.exception("import_failed %s", run.details)
        raise
    finally:
        session.close()


def _db_has_fresh_data(max_age_hours: float = 2.0) -> bool:
    """Check if the database already has recent data from a cron run."""
    from datetime import datetime, timedelta, UTC
    try:
        Base.metadata.create_all(bind=engine)
        with Session(engine) as session:
            from app.service import get_latest_ingestion
            latest = get_latest_ingestion(session)
            if latest is None or latest.finished_at is None:
                return False
            finished = latest.finished_at
            if finished.tzinfo is None:
                finished = finished.replace(tzinfo=UTC)
            age = datetime.now(UTC) - finished
            if age < timedelta(hours=max_age_hours):
                logger.info(
                    "import_skipped reason=fresh_data age_hours=%.1f last_ingestion=%s",
                    age.total_seconds() / 3600,
                    finished.isoformat(),
                )
                return True
    except Exception:
        pass
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Import wine deal data into database")
    parser.add_argument(
        "--comparison",
        type=Path,
        default=Path("seed/comparison_summary.csv"),
        help="Path to comparison_summary.csv",
    )
    parser.add_argument(
        "--vivino",
        type=Path,
        default=Path("seed/vivino_results.csv"),
        help="Path to vivino_results.csv",
    )
    parser.add_argument(
        "--vivino-overrides",
        type=Path,
        default=Path("seed/vivino_overrides.csv"),
        help="Optional path to manual vivino overrides CSV.",
    )
    parser.add_argument(
        "--market-prices",
        type=Path,
        default=Path("seed/market_prices.csv"),
        help="Path to market_prices.csv from llm_market_resolver.",
    )
    parser.add_argument(
        "--skip-if-fresh",
        type=float,
        default=0,
        metavar="HOURS",
        help="Skip import if DB has data newer than HOURS (0 = always import).",
    )
    args = parser.parse_args()
    if args.skip_if_fresh > 0 and _db_has_fresh_data(args.skip_if_fresh):
        return
    import_data(
        args.comparison, args.vivino, args.vivino_overrides,
        market_prices_path=args.market_prices,
    )


if __name__ == "__main__":
    main()
