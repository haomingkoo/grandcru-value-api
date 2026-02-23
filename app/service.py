from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import IngestionRun, WineDeal, WineDealSnapshot


def list_deals(
    session: Session,
    *,
    limit: int = 50,
    offset: int = 0,
    min_score: float = 0.0,
    only_platinum_cheaper: bool = True,
    search: str | None = None,
    cheaper_side: str | None = None,
    min_vivino_rating: float | None = None,
    min_vivino_num_ratings: int | None = None,
    max_platinum_price: float | None = None,
    sort_by: str = "deal_score",
    sort_order: str = "desc",
) -> list[WineDeal]:
    stmt = select(WineDeal).where(WineDeal.deal_score >= min_score)

    effective_cheaper_side = cheaper_side
    if only_platinum_cheaper and not effective_cheaper_side:
        effective_cheaper_side = "Platinum Cheaper"
    if effective_cheaper_side and effective_cheaper_side.lower() != "all":
        stmt = stmt.where(WineDeal.cheaper_side == effective_cheaper_side)

    if search:
        stmt = stmt.where(WineDeal.wine_name.ilike(f"%{search.strip()}%"))

    if min_vivino_rating is not None:
        stmt = stmt.where(WineDeal.vivino_rating >= min_vivino_rating)

    if min_vivino_num_ratings is not None:
        stmt = stmt.where(WineDeal.vivino_num_ratings >= min_vivino_num_ratings)

    if max_platinum_price is not None:
        stmt = stmt.where(WineDeal.price_platinum <= max_platinum_price)

    sort_map = {
        "deal_score": WineDeal.deal_score,
        "price_diff_pct": WineDeal.price_diff_pct,
        "vivino_rating": WineDeal.vivino_rating,
        "vivino_num_ratings": WineDeal.vivino_num_ratings,
        "price_platinum": WineDeal.price_platinum,
        "wine_name": WineDeal.wine_name,
    }
    sort_column = sort_map.get(sort_by, WineDeal.deal_score)
    sort_expression = sort_column.asc().nullslast() if sort_order == "asc" else sort_column.desc().nullslast()
    stmt = stmt.order_by(sort_expression, WineDeal.id.asc())
    stmt = stmt.offset(offset).limit(min(limit, 500))
    return list(session.scalars(stmt).all())


def get_deal_by_id(session: Session, deal_id: int) -> WineDeal | None:
    return session.get(WineDeal, deal_id)


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
) -> list[WineDealSnapshot]:
    stmt = (
        select(WineDealSnapshot)
        .where(WineDealSnapshot.wine_name == wine_name)
        .order_by(WineDealSnapshot.captured_at.desc())
        .limit(min(limit, 365))
    )
    return list(session.scalars(stmt).all())
