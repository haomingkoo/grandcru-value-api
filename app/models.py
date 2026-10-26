from datetime import UTC, date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, Index, Integer, String, Text, func
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
    platinum_in_stock: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    grand_cru_in_stock: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    platinum_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    grand_cru_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    vivino_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    image_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)

    vivino_rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    vivino_num_ratings: Mapped[int | None] = mapped_column(Integer, nullable=True)
    vivino_match_method: Mapped[str | None] = mapped_column(String(32), nullable=True)
    vivino_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    vivino_description: Mapped[str | None] = mapped_column(String(512), nullable=True)
    vivino_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    price_market: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_retailer_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    market_retailer_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    producer: Mapped[str | None] = mapped_column(String(255), nullable=True)
    label_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    country: Mapped[str | None] = mapped_column(String(128), nullable=True)
    region: Mapped[str | None] = mapped_column(String(128), nullable=True)
    wine_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    style_family: Mapped[str | None] = mapped_column(String(64), nullable=True)
    grapes: Mapped[str | None] = mapped_column(String(255), nullable=True)
    offering_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    origin_label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    origin_latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    origin_longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    origin_precision: Mapped[str | None] = mapped_column(String(32), nullable=True)
    deal_score: Mapped[float | None] = mapped_column(Float, nullable=True, index=True)

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


class WineDealSnapshot(Base):
    __tablename__ = "wine_deal_snapshots"
    __table_args__ = (
        Index("ix_wine_deal_snapshots_name_captured", "wine_name", "captured_at"),
        Index("ix_wine_deal_snapshots_run", "ingestion_run_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ingestion_run_id: Mapped[int] = mapped_column(Integer, index=True)
    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        index=True,
    )

    wine_name: Mapped[str] = mapped_column(String(255), index=True)
    vintage: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    volume: Mapped[str | None] = mapped_column(String(50), nullable=True)

    price_platinum: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_grand_cru: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_diff: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_diff_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    cheaper_side: Mapped[str | None] = mapped_column(String(64), nullable=True)
    platinum_in_stock: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    grand_cru_in_stock: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    platinum_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    grand_cru_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    vivino_url: Mapped[str | None] = mapped_column(String(512), nullable=True)

    vivino_rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    vivino_num_ratings: Mapped[int | None] = mapped_column(Integer, nullable=True)
    vivino_match_method: Mapped[str | None] = mapped_column(String(32), nullable=True)
    deal_score: Mapped[float | None] = mapped_column(Float, nullable=True, index=True)


class PriceObservation(Base):
    """One price for one offer from one source on one Singapore day. Never pruned."""

    __tablename__ = "price_observations"
    __table_args__ = (
        Index("ix_price_observations_offer_source_day", "wine_name", "source", "observed_on", unique=True),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    wine_name: Mapped[str] = mapped_column(String(255), index=True)
    source: Mapped[str] = mapped_column(String(32))
    price: Mapped[float] = mapped_column(Float)
    currency: Mapped[str] = mapped_column(String(3), default="SGD")
    volume_ml: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    observed_on: Mapped[date] = mapped_column(Date)
    url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    origin: Mapped[str] = mapped_column(String(32), default="daily_check")
    ingestion_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)


class AppState(Base):
    """Small JSON documents that must survive redeploys (Railway disk is ephemeral)."""

    __tablename__ = "app_state"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(Text, default="{}")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )
