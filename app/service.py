from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import IngestionRun, WineDeal


def list_deals(
    session: Session,
    *,
    limit: int = 50,
    offset: int = 0,
    min_score: float = 0.0,
    only_platinum_cheaper: bool = True,
    search: str | None = None,
) -> list[WineDeal]:
    stmt = select(WineDeal).where(WineDeal.deal_score >= min_score)

    if only_platinum_cheaper:
        stmt = stmt.where(WineDeal.cheaper_side == "Platinum Cheaper")

    if search:
        stmt = stmt.where(WineDeal.wine_name.ilike(f"%{search.strip()}%"))

    stmt = stmt.order_by(WineDeal.deal_score.desc(), WineDeal.price_diff_pct.asc())
    stmt = stmt.offset(offset).limit(min(limit, 200))
    return list(session.scalars(stmt).all())


def get_deal_by_id(session: Session, deal_id: int) -> WineDeal | None:
    return session.get(WineDeal, deal_id)


def count_deals(session: Session) -> int:
    return int(session.scalar(select(func.count(WineDeal.id))) or 0)


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
