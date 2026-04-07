from datetime import UTC, datetime, timedelta

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.config import settings
from app.deal_insights import compute_deal_insights
from app.models import IngestionRun, WineDeal, WineDealSnapshot

VIVINO_UNRESOLVED_EXPORT_FIELDS = (
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
    "platinum_vivino_rating",
    "platinum_vivino_num_ratings",
    "platinum_vivino_url",
)


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _find_snapshot_before(
    snapshots_desc: list[WineDealSnapshot],
    cutoff: datetime,
) -> WineDealSnapshot | None:
    for snapshot in snapshots_desc:
        if _as_utc(snapshot.captured_at) <= cutoff:
            return snapshot
    return None


def _safe_diff(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None:
        return None
    return round(current - previous, 2)


def _normalize_csv_filter(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _apply_exact_match_filter(stmt, column, raw_value: str | None):
    values = _normalize_csv_filter(raw_value)
    if not values:
        return stmt

    normalized = [value.lower() for value in values]
    return stmt.where(func.lower(column).in_(normalized))


def _apply_contains_filter(stmt, column, raw_value: str | None):
    values = _normalize_csv_filter(raw_value)
    if not values:
        return stmt

    clauses = []
    for value in values:
        escaped = value.replace("%", r"\%").replace("_", r"\_")
        clauses.append(column.ilike(f"%{escaped}%", escape="\\"))
    return stmt.where(or_(*clauses))


def _sort_with_direction(column, direction: str):
    return column.asc().nullslast() if direction == "asc" else column.desc().nullslast()


def _normalize_sort_by(sort_by: str) -> str:
    normalized = (sort_by or "deal_score").lower()
    aliases = {
        "score": "deal_score",
        "price_diff_percent": "price_diff_pct",
        "price_difference_pct": "price_diff_pct",
        "difference_pct": "price_diff_pct",
        "price_gap_pct": "price_diff_pct_abs",
        "percent_difference": "price_diff_pct_abs",
        "percent_gap": "price_diff_pct_abs",
        "price_diff_pct_abs": "price_diff_pct_abs",
        "rating": "vivino_rating",
        "ratings": "vivino_num_ratings",
        "price": "price_platinum",
        "name": "wine_name",
    }
    return aliases.get(normalized, normalized)


def _deal_sort_expressions(sort_by: str, sort_order: str) -> list:
    direction = "asc" if (sort_order or "").lower() == "asc" else "desc"
    normalized_sort_by = _normalize_sort_by(sort_by)

    sort_map = {
        "deal_score": WineDeal.deal_score,
        "price_diff_pct": WineDeal.price_diff_pct,
        "price_diff_pct_abs": func.abs(WineDeal.price_diff_pct),
        "vivino_rating": WineDeal.vivino_rating,
        "vivino_num_ratings": WineDeal.vivino_num_ratings,
        "price_platinum": WineDeal.price_platinum,
        "wine_name": WineDeal.wine_name,
    }
    primary_column = sort_map.get(normalized_sort_by, WineDeal.deal_score)
    expressions = [_sort_with_direction(primary_column, direction)]

    # Give ties a domain-aware order so equal rounded values still feel intentional in the UI.
    secondary_map = {
        "deal_score": [
            WineDeal.price_diff_pct.asc().nullslast(),
            WineDeal.vivino_rating.desc().nullslast(),
            WineDeal.vivino_num_ratings.desc().nullslast(),
            WineDeal.wine_name.asc(),
        ],
        "price_diff_pct": [
            WineDeal.deal_score.desc().nullslast(),
            WineDeal.vivino_rating.desc().nullslast(),
            WineDeal.vivino_num_ratings.desc().nullslast(),
            WineDeal.wine_name.asc(),
        ],
        "price_diff_pct_abs": [
            WineDeal.deal_score.desc().nullslast(),
            WineDeal.price_diff_pct.asc().nullslast(),
            WineDeal.vivino_rating.desc().nullslast(),
            WineDeal.wine_name.asc(),
        ],
        "vivino_rating": [
            WineDeal.vivino_num_ratings.desc().nullslast(),
            WineDeal.deal_score.desc().nullslast(),
            WineDeal.price_diff_pct.asc().nullslast(),
            WineDeal.wine_name.asc(),
        ],
        "vivino_num_ratings": [
            WineDeal.vivino_rating.desc().nullslast(),
            WineDeal.deal_score.desc().nullslast(),
            WineDeal.price_diff_pct.asc().nullslast(),
            WineDeal.wine_name.asc(),
        ],
        "price_platinum": [
            WineDeal.deal_score.desc().nullslast(),
            WineDeal.vivino_rating.desc().nullslast(),
            WineDeal.wine_name.asc(),
        ],
        "wine_name": [
            WineDeal.deal_score.desc().nullslast(),
            WineDeal.vivino_rating.desc().nullslast(),
            WineDeal.id.asc(),
        ],
    }

    expressions.extend(secondary_map.get(normalized_sort_by, secondary_map["deal_score"]))
    if normalized_sort_by != "wine_name":
        expressions.append(WineDeal.id.asc())
    return expressions


def _apply_response_fields(deals: list[WineDeal]) -> None:
    for deal in deals:
        pct = getattr(deal, "price_diff_pct", None)
        setattr(deal, "price_diff_pct_abs", round(abs(pct), 2) if pct is not None else None)
        insights = compute_deal_insights(deal)
        setattr(deal, "has_competitor_match", insights.has_competitor_match)
        setattr(deal, "is_platinum_cheaper", insights.is_platinum_cheaper)
        setattr(deal, "is_good_wine", insights.is_good_wine)
        setattr(deal, "is_high_confidence", insights.is_high_confidence)
        setattr(deal, "value_verdict", insights.value_verdict)
        setattr(deal, "value_verdict_tone", insights.value_verdict_tone)
        setattr(deal, "value_verdict_reason", insights.value_verdict_reason)
        setattr(deal, "origin_source", insights.origin_source)
        setattr(deal, "origin_confidence", insights.origin_confidence)
        setattr(deal, "grape_source", insights.grape_source)
        setattr(deal, "grape_confidence", insights.grape_confidence)
        setattr(deal, "metadata_confidence", insights.metadata_confidence)
        setattr(deal, "platinum_trend_7d", insights.platinum_trend_7d)
        setattr(deal, "grand_cru_trend_7d", insights.grand_cru_trend_7d)
        setattr(deal, "platinum_trend_30d", insights.platinum_trend_30d)
        setattr(deal, "grand_cru_trend_30d", insights.grand_cru_trend_30d)


def _apply_price_change_fields(session: Session, deals: list[WineDeal]) -> None:
    if not deals:
        return

    wine_names = sorted({deal.wine_name for deal in deals if deal.wine_name})
    if not wine_names:
        return

    now_utc = datetime.now(UTC)
    cutoff = now_utc - timedelta(days=45)
    snapshots = list(
        session.scalars(
            select(WineDealSnapshot)
            .where(
                WineDealSnapshot.wine_name.in_(wine_names),
                WineDealSnapshot.captured_at >= cutoff,
            )
            .order_by(WineDealSnapshot.wine_name.asc(), WineDealSnapshot.captured_at.desc())
        ).all()
    )

    by_name: dict[str, list[WineDealSnapshot]] = {}
    for snapshot in snapshots:
        by_name.setdefault(snapshot.wine_name, []).append(snapshot)

    cut7 = now_utc - timedelta(days=7)
    cut30 = now_utc - timedelta(days=30)

    for deal in deals:
        setattr(deal, "price_platinum_7d_ago", None)
        setattr(deal, "price_platinum_change_7d", None)
        setattr(deal, "price_grand_cru_7d_ago", None)
        setattr(deal, "price_grand_cru_change_7d", None)
        setattr(deal, "price_platinum_30d_ago", None)
        setattr(deal, "price_platinum_change_30d", None)
        setattr(deal, "price_grand_cru_30d_ago", None)
        setattr(deal, "price_grand_cru_change_30d", None)

        history = by_name.get(deal.wine_name, [])
        if not history:
            continue

        snap7 = _find_snapshot_before(history, cut7)
        snap30 = _find_snapshot_before(history, cut30)

        if snap7 is not None:
            setattr(deal, "price_platinum_7d_ago", snap7.price_platinum)
            setattr(deal, "price_grand_cru_7d_ago", snap7.price_grand_cru)
            setattr(deal, "price_platinum_change_7d", _safe_diff(deal.price_platinum, snap7.price_platinum))
            setattr(deal, "price_grand_cru_change_7d", _safe_diff(deal.price_grand_cru, snap7.price_grand_cru))

        if snap30 is not None:
            setattr(deal, "price_platinum_30d_ago", snap30.price_platinum)
            setattr(deal, "price_grand_cru_30d_ago", snap30.price_grand_cru)
            setattr(deal, "price_platinum_change_30d", _safe_diff(deal.price_platinum, snap30.price_platinum))
            setattr(deal, "price_grand_cru_change_30d", _safe_diff(deal.price_grand_cru, snap30.price_grand_cru))


def _build_deals_stmt(
    *,
    min_score: float = 0.0,
    only_platinum_cheaper: bool = False,
    comparable_only: bool = False,
    search: str | None = None,
    cheaper_side: str | None = None,
    min_vivino_rating: float | None = None,
    min_vivino_num_ratings: int | None = None,
    max_platinum_price: float | None = None,
    country: str | None = None,
    region: str | None = None,
    wine_type: str | None = None,
    style_family: str | None = None,
    grape: str | None = None,
    offering_type: str | None = None,
    producer: str | None = None,
):
    stmt = select(WineDeal).where(WineDeal.deal_score >= min_score)

    effective_cheaper_side = cheaper_side
    if only_platinum_cheaper and not effective_cheaper_side:
        effective_cheaper_side = "Platinum Cheaper"
    if effective_cheaper_side and effective_cheaper_side.lower() != "all":
        stmt = stmt.where(WineDeal.cheaper_side == effective_cheaper_side)
    if comparable_only:
        stmt = stmt.where(WineDeal.price_diff_pct.is_not(None))
        stmt = stmt.where(WineDeal.vivino_rating.is_not(None))

    if search:
        escaped = search.strip().replace("%", r"\%").replace("_", r"\_")
        pattern = f"%{escaped}%"
        stmt = stmt.where(
            WineDeal.wine_name.ilike(pattern, escape="\\")
            | WineDeal.vivino_description.ilike(pattern, escape="\\")
            | WineDeal.grapes.ilike(pattern, escape="\\")
            | WineDeal.region.ilike(pattern, escape="\\")
            | WineDeal.producer.ilike(pattern, escape="\\")
        )

    if min_vivino_rating is not None:
        stmt = stmt.where(WineDeal.vivino_rating >= min_vivino_rating)

    if min_vivino_num_ratings is not None:
        stmt = stmt.where(WineDeal.vivino_num_ratings >= min_vivino_num_ratings)

    if max_platinum_price is not None:
        stmt = stmt.where(WineDeal.price_platinum <= max_platinum_price)

    stmt = _apply_exact_match_filter(stmt, WineDeal.country, country)
    stmt = _apply_exact_match_filter(stmt, WineDeal.region, region)
    stmt = _apply_exact_match_filter(stmt, WineDeal.wine_type, wine_type)
    stmt = _apply_exact_match_filter(stmt, WineDeal.style_family, style_family)
    stmt = _apply_exact_match_filter(stmt, WineDeal.offering_type, offering_type)
    stmt = _apply_exact_match_filter(stmt, WineDeal.producer, producer)
    stmt = _apply_contains_filter(stmt, WineDeal.grapes, grape)
    return stmt


def list_deals(
    session: Session,
    *,
    limit: int = 50,
    offset: int = 0,
    min_score: float = 0.0,
    only_platinum_cheaper: bool = False,
    comparable_only: bool = False,
    search: str | None = None,
    cheaper_side: str | None = None,
    min_vivino_rating: float | None = None,
    min_vivino_num_ratings: int | None = None,
    max_platinum_price: float | None = None,
    sort_by: str = "deal_score",
    sort_order: str = "desc",
    country: str | None = None,
    region: str | None = None,
    wine_type: str | None = None,
    style_family: str | None = None,
    grape: str | None = None,
    offering_type: str | None = None,
    producer: str | None = None,
) -> list[WineDeal]:
    stmt = _build_deals_stmt(
        min_score=min_score,
        only_platinum_cheaper=only_platinum_cheaper,
        comparable_only=comparable_only,
        search=search,
        cheaper_side=cheaper_side,
        min_vivino_rating=min_vivino_rating,
        min_vivino_num_ratings=min_vivino_num_ratings,
        max_platinum_price=max_platinum_price,
        country=country,
        region=region,
        wine_type=wine_type,
        style_family=style_family,
        grape=grape,
        offering_type=offering_type,
        producer=producer,
    )
    stmt = stmt.order_by(*_deal_sort_expressions(sort_by, sort_order))
    stmt = stmt.offset(offset).limit(min(limit, 500))
    deals = list(session.scalars(stmt).all())
    _apply_price_change_fields(session, deals)
    _apply_response_fields(deals)
    return deals


def list_vivino_unresolved_export_rows(
    session: Session,
    *,
    limit: int = 500,
    include_locked: bool = False,
    locked_wine_names: set[str] | None = None,
) -> list[dict[str, str]]:
    stmt = (
        select(WineDeal)
        .where(
            WineDeal.vivino_rating.is_(None),
            WineDeal.vivino_num_ratings.is_(None),
        )
        .order_by(WineDeal.deal_score.desc().nullslast(), WineDeal.wine_name.asc(), WineDeal.id.asc())
        .limit(limit)
    )

    locked_names = {name for name in (locked_wine_names or set()) if name}
    if locked_names and not include_locked:
        stmt = stmt.where(WineDeal.wine_name.not_in(sorted(locked_names)))

    deals = list(session.scalars(stmt).all())
    rows: list[dict[str, str]] = []
    for deal in deals:
        rows.append(
            {
                "name_plat": deal.wine_name or "",
                "year_plat": str(deal.vintage or ""),
                "quantity_plat": str(deal.quantity or ""),
                "volume_plat": deal.volume or "",
                "quantity_main": str(deal.quantity or ""),
                "price_plat": f"{deal.price_platinum:.2f}" if deal.price_platinum is not None else "",
                "price_main": f"{deal.price_grand_cru:.2f}" if deal.price_grand_cru is not None else "",
                "price_diff": f"{deal.price_diff:.2f}" if deal.price_diff is not None else "",
                "price_diff_pct": f"{deal.price_diff_pct:.2f}" if deal.price_diff_pct is not None else "",
                "cheaper_side": deal.cheaper_side or "",
                "url_plat": deal.platinum_url or "",
                "url_main": deal.grand_cru_url or "",
                "platinum_vivino_rating": "",
                "platinum_vivino_num_ratings": "",
                "platinum_vivino_url": "",
            }
        )
    return rows


def get_deal_by_id(session: Session, deal_id: int) -> WineDeal | None:
    deal = session.get(WineDeal, deal_id)
    if deal is not None:
        _apply_price_change_fields(session, [deal])
        _apply_response_fields([deal])
    return deal


def count_deals(session: Session) -> int:
    return int(session.scalar(select(func.count(WineDeal.id))) or 0)


def count_snapshots(session: Session) -> int:
    return int(session.scalar(select(func.count(WineDealSnapshot.id))) or 0)


def get_latest_ingestion(session: Session) -> IngestionRun | None:
    stmt = select(IngestionRun).order_by(IngestionRun.started_at.desc()).limit(1)
    return session.scalars(stmt).first()


def is_ingestion_stale(run: IngestionRun | None) -> bool | None:
    if run is None or run.finished_at is None:
        return True
    finished_at = run.finished_at
    if finished_at.tzinfo is None:
        finished_at = finished_at.replace(tzinfo=UTC)
    age = datetime.now(UTC) - finished_at
    return age > timedelta(hours=settings.ingestion_stale_hours)


def get_deal_history(
    session: Session,
    *,
    wine_name: str,
    limit: int = 30,
    days: int = 90,
    sort_order: str = "asc",
) -> list[WineDealSnapshot]:
    now_utc = datetime.now(UTC)
    cutoff = now_utc - timedelta(days=max(days, 1))

    stmt = select(WineDealSnapshot).where(
        WineDealSnapshot.wine_name == wine_name,
        WineDealSnapshot.captured_at >= cutoff,
    )
    if (sort_order or "").lower() == "desc":
        stmt = stmt.order_by(WineDealSnapshot.captured_at.desc())
    else:
        stmt = stmt.order_by(WineDealSnapshot.captured_at.asc())
    stmt = stmt.limit(min(limit, 3650))
    return list(session.scalars(stmt).all())


def _sorted_label_counts(counter: dict[str, int]) -> list[dict[str, int | str]]:
    return [
        {"value": value, "count": count}
        for value, count in sorted(counter.items(), key=lambda item: (-item[1], item[0].lower()))
    ]


def _load_filtered_deals(
    session: Session,
    **filters,
) -> list[WineDeal]:
    stmt = _build_deals_stmt(**filters).order_by(WineDeal.wine_name.asc(), WineDeal.id.asc())
    deals = list(session.scalars(stmt).all())
    _apply_response_fields(deals)
    return deals


def get_deal_filters(session: Session, **filters) -> dict[str, list[dict[str, int | str]]]:
    deals = _load_filtered_deals(session, **filters)

    countries: dict[str, int] = {}
    regions: dict[str, int] = {}
    wine_types: dict[str, int] = {}
    style_families: dict[str, int] = {}
    grapes: dict[str, int] = {}
    offering_types: dict[str, int] = {}
    producers: dict[str, int] = {}

    for deal in deals:
        for bucket, value in (
            (countries, deal.country),
            (regions, deal.region),
            (wine_types, deal.wine_type),
            (style_families, deal.style_family),
            (offering_types, deal.offering_type),
            (producers, deal.producer),
        ):
            if value:
                bucket[value] = bucket.get(value, 0) + 1

        if deal.grapes:
            for grape in [part.strip() for part in deal.grapes.split(",") if part.strip()]:
                grapes[grape] = grapes.get(grape, 0) + 1

    return {
        "countries": _sorted_label_counts(countries),
        "regions": _sorted_label_counts(regions),
        "wine_types": _sorted_label_counts(wine_types),
        "style_families": _sorted_label_counts(style_families),
        "grapes": _sorted_label_counts(grapes),
        "offering_types": _sorted_label_counts(offering_types),
        "producers": _sorted_label_counts(producers),
    }


def _average(values: list[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    if not present:
        return None
    return round(sum(present) / len(present), 2)


def get_deal_stats(session: Session, **filters) -> dict:
    deals = _load_filtered_deals(session, **filters)

    cheaper_sides: dict[str, int] = {}
    wine_types: dict[str, int] = {}
    style_families: dict[str, int] = {}
    countries: dict[str, int] = {}
    offering_buckets: dict[str, list[WineDeal]] = {}

    for deal in deals:
        if deal.cheaper_side:
            cheaper_sides[deal.cheaper_side] = cheaper_sides.get(deal.cheaper_side, 0) + 1
        if deal.wine_type:
            wine_types[deal.wine_type] = wine_types.get(deal.wine_type, 0) + 1
        if deal.style_family:
            style_families[deal.style_family] = style_families.get(deal.style_family, 0) + 1
        if deal.country:
            countries[deal.country] = countries.get(deal.country, 0) + 1
        key = deal.offering_type or "Unknown"
        offering_buckets.setdefault(key, []).append(deal)

    offering_types = []
    for value, bucket in sorted(offering_buckets.items(), key=lambda item: (-len(item[1]), item[0].lower())):
        offering_types.append(
            {
                "value": value,
                "count": len(bucket),
                "platinum_cheaper_count": sum(1 for deal in bucket if deal.cheaper_side == "Platinum Cheaper"),
                "grand_cru_cheaper_count": sum(1 for deal in bucket if deal.cheaper_side == "Grand Cru Cheaper"),
                "average_price_platinum": _average([deal.price_platinum for deal in bucket]),
                "average_price_diff_pct": _average([deal.price_diff_pct for deal in bucket]),
            }
        )

    return {
        "total_deals": len(deals),
        "cheaper_sides": _sorted_label_counts(cheaper_sides),
        "wine_types": _sorted_label_counts(wine_types),
        "style_families": _sorted_label_counts(style_families),
        "countries": _sorted_label_counts(countries),
        "offering_types": offering_types,
    }


def get_deal_map_points(session: Session, **filters) -> list[dict]:
    deals = _load_filtered_deals(session, **filters)
    grouped: dict[tuple[str, float, float], list[WineDeal]] = {}

    for deal in deals:
        if not deal.origin_label or deal.origin_latitude is None or deal.origin_longitude is None:
            continue
        key = (deal.origin_label, deal.origin_latitude, deal.origin_longitude)
        grouped.setdefault(key, []).append(deal)

    points = []
    for (origin_label, latitude, longitude), bucket in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0][0].lower())):
        points.append(
            {
                "origin_label": origin_label,
                "country": bucket[0].country,
                "region": bucket[0].region,
                "origin_latitude": latitude,
                "origin_longitude": longitude,
                "origin_precision": bucket[0].origin_precision,
                "wine_count": len(bucket),
                "platinum_cheaper_count": sum(1 for deal in bucket if deal.cheaper_side == "Platinum Cheaper"),
                "average_price_diff_pct": _average([deal.price_diff_pct for deal in bucket]),
                "sample_wines": [deal.wine_name for deal in bucket[:5]],
            }
        )
    return points
