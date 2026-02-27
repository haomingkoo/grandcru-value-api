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
from urllib.parse import quote_plus

from sqlalchemy import delete, select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import settings  # noqa: E402
from app.database import Base, SessionLocal, engine  # noqa: E402
from app.models import IngestionRun, WineDeal, WineDealSnapshot  # noqa: E402
from app.scoring import compute_deal_score, parse_float, parse_int  # noqa: E402


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("grandcru.import")

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
    "standard",
    "bottle",
    "magnum",
    "jeroboam",
    "double",
    "red",
    "white",
    "rose",
    "blanc",
    "rouge",
    "ml",
    "l",
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


@dataclass(frozen=True)
class VivinoLookup:
    exact: dict[str, dict[str, str]]
    canonical: dict[str, list[dict[str, str]]]
    by_year: dict[int, list[dict[str, str]]]
    rows: list[dict[str, str]]


def build_vivino_lookup(rows: list[dict[str, str]]) -> VivinoLookup:
    exact: dict[str, dict[str, str]] = {}
    canonical: dict[str, list[dict[str, str]]] = {}
    by_year: dict[int, list[dict[str, str]]] = {}

    for row in rows:
        candidate_names = [row.get("wine_name"), row.get("match_name")]
        for candidate_name in candidate_names:
            key = normalize_key(candidate_name)
            if key:
                # Latest row wins, so overrides can replace stale base records.
                exact[key] = row

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

    # Conservative gate to avoid linking the wrong wine.
    if best[4] < 3:
        return {}, "none"
    if best[0] < 0.68 and best[3] < 0.90:
        return {}, "none"
    if best[1] < 0.45 and best[3] < 0.90:
        return {}, "none"
    if second_best is not None and (best[0] - second_best[0]) < 0.03 and best[3] < 0.92:
        return {}, "none"

    return best[5], "fuzzy"


def to_optional_int(value: str | None) -> int | None:
    return parse_int(value)


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


def build_vivino_search_url(query: str | None) -> str | None:
    cleaned = (query or "").strip()
    if not cleaned:
        return None
    return f"https://www.vivino.com/en/search/wines?q={quote_plus(cleaned)}"


def import_data(comparison_path: Path, vivino_path: Path, vivino_overrides_path: Path | None = None) -> None:
    if not comparison_path.exists():
        raise FileNotFoundError(f"comparison_summary missing: {comparison_path}")
    if not vivino_path.exists():
        raise FileNotFoundError(f"vivino_results missing: {vivino_path}")

    Base.metadata.create_all(bind=engine)

    comparison_rows = read_csv_rows(comparison_path)
    vivino_rows_base = read_csv_rows(vivino_path)
    vivino_rows_override = read_optional_csv_rows(vivino_overrides_path)
    vivino_rows = vivino_rows_base + vivino_rows_override
    vivino_lookup = build_vivino_lookup(vivino_rows)

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
        existing_deals = list(session.scalars(select(WineDeal)).all())
        existing_vivino_by_name = {normalize_key(deal.wine_name): deal for deal in existing_deals if deal.wine_name}

        merged_records: list[WineDeal] = []
        snapshot_records: list[WineDealSnapshot] = []
        snapshot_time = datetime.now(UTC)

        exact_matches = 0
        canonical_matches = 0
        fuzzy_matches = 0
        unmatched = 0
        db_fallback_matches = 0

        for row in comparison_rows:
            wine_name = (row.get("name_plat") or "").strip()
            if not wine_name:
                continue

            vivino, match_method = match_vivino_row(wine_name, vivino_lookup)
            if match_method == "exact":
                exact_matches += 1
            elif match_method == "canonical":
                canonical_matches += 1
            elif match_method == "fuzzy":
                fuzzy_matches += 1
            else:
                unmatched += 1

            vivino_rating = parse_float(vivino.get("vivino_rating"))
            vivino_num_ratings = parse_int(vivino.get("vivino_num_ratings")) or parse_int(vivino.get("vivino_raters"))
            vivino_url = normalize_url(vivino.get("vivino_url"))

            if vivino_url is None and (vivino_rating is not None or vivino_num_ratings is not None):
                vivino_url = build_vivino_search_url(
                    vivino.get("wine_name") or vivino.get("match_name") or wine_name
                )

            if match_method == "none":
                prior = existing_vivino_by_name.get(normalize_key(wine_name))
                if prior is not None and (
                    prior.vivino_url is not None
                    or prior.vivino_rating is not None
                    or prior.vivino_num_ratings is not None
                ):
                    vivino_url = prior.vivino_url
                    vivino_rating = prior.vivino_rating
                    vivino_num_ratings = prior.vivino_num_ratings
                    db_fallback_matches += 1

            # Avoid exposing orphaned Vivino links with no usable quality metrics.
            if vivino_rating is None and vivino_num_ratings is None:
                vivino_url = None

            price_platinum = parse_float(row.get("price_plat"))
            price_grand_cru = parse_float(row.get("price_main"))
            price_diff = parse_float(row.get("price_diff"))
            price_diff_pct = parse_float(row.get("price_diff_pct"))
            grand_cru_url = normalize_url(row.get("url_main")) if price_grand_cru is not None else None

            if price_diff is None and price_platinum is not None and price_grand_cru is not None:
                price_diff = round(price_platinum - price_grand_cru, 2)
            if price_diff_pct is None and price_diff is not None and price_grand_cru not in (None, 0):
                price_diff_pct = round((price_diff / price_grand_cru) * 100.0, 2)

            deal_payload = {
                "wine_name": wine_name,
                "vintage": to_optional_int(row.get("year_plat")),
                "quantity": to_optional_int(row.get("quantity_plat")),
                "volume": (row.get("volume_plat") or "").strip() or None,
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
                "deal_score": compute_deal_score(price_diff_pct, vivino_rating, vivino_num_ratings),
            }
            merged_records.append(WineDeal(**deal_payload))
            snapshot_records.append(
                WineDealSnapshot(
                    ingestion_run_id=run.id,
                    captured_at=snapshot_time,
                    **deal_payload,
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
        run.details = (
            f"Loaded {len(comparison_rows)} comparison rows and {len(vivino_rows_base)} vivino rows "
            f"(+{len(vivino_rows_override)} overrides) into {len(merged_records)} current deals and "
            f"{len(snapshot_records)} snapshots (vivino matched: exact={exact_matches}, "
            f"canonical={canonical_matches}, fuzzy={fuzzy_matches}, unmatched={unmatched}, "
            f"db_fallback={db_fallback_matches}); "
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
    args = parser.parse_args()
    import_data(args.comparison, args.vivino, args.vivino_overrides)


if __name__ == "__main__":
    main()
