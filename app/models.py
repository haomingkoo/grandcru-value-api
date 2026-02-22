from datetime import UTC, datetime

from sqlalchemy import DateTime, Float, Index, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class WineDeal(Base):
    __tablename__ = "wine_deals"
    __table_args__ = (
        Index("ix_wine_deals_name_url", "wine_name", "platinum_url", unique=True),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    wine_name: Mapped[str] = mapped_column(String(255), index=True)
    vintage: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    volume: Mapped[str | None] = mapped_column(String(50), nullable=True)

    price_platinum: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_grand_cru: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_diff: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_diff_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    cheaper_side: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)

    platinum_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    grand_cru_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    vivino_url: Mapped[str | None] = mapped_column(String(512), nullable=True)

    vivino_rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    vivino_num_ratings: Mapped[int | None] = mapped_column(Integer, nullable=True)
    deal_score: Mapped[float] = mapped_column(Float, default=0.0, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        index=True,
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    comparison_rows: Mapped[int] = mapped_column(Integer, default=0)
    vivino_rows: Mapped[int] = mapped_column(Integer, default=0)
    merged_rows: Mapped[int] = mapped_column(Integer, default=0)
    details: Mapped[str | None] = mapped_column(Text, nullable=True)
