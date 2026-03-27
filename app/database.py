from __future__ import annotations

import logging
from collections.abc import Generator
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import settings

logger = logging.getLogger("grandcru.database")


class Base(DeclarativeBase):
    pass


connect_args = {}
if settings.database_url.startswith("sqlite"):
    connect_args = {"check_same_thread": False}
    sqlite_path = settings.database_url.removeprefix("sqlite:///")
    if sqlite_path and sqlite_path != ":memory:":
        db_file = Path(sqlite_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)

engine = create_engine(settings.database_url, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def get_session() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


_ALLOWED_TABLES = {"wine_deals", "wine_deal_snapshots", "ingestion_runs"}
_ALLOWED_COLUMNS = {
    "vivino_match_method",
    "vivino_price",
    "vivino_description",
    "producer",
    "label_name",
    "country",
    "region",
    "wine_type",
    "style_family",
    "grapes",
    "offering_type",
    "origin_label",
    "origin_latitude",
    "origin_longitude",
    "origin_precision",
    "price_market",
    "market_retailer_name",
    "market_retailer_url",
}


def ensure_column(table: str, column: str, col_type: str) -> None:
    """Add a column to an existing table if missing. Postgres + SQLite safe."""
    if table not in _ALLOWED_TABLES or column not in _ALLOWED_COLUMNS:
        raise ValueError(f"Unrecognized migration target: {table}.{column}")

    with engine.connect() as conn:
        dialect = conn.dialect.name
        if dialect == "postgresql":
            result = conn.execute(text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name = :table AND column_name = :column"
            ), {"table": table, "column": column})
            if result.fetchone() is None:
                conn.execute(text(
                    f'ALTER TABLE "{table}" ADD COLUMN "{column}" {col_type}'
                ))
                logger.info("migration: added %s.%s (%s)", table, column, col_type)
        else:
            try:
                conn.execute(text(f"SELECT {column} FROM {table} LIMIT 0"))
            except Exception:
                conn.execute(text(
                    f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"
                ))
                logger.info("migration: added %s.%s (%s)", table, column, col_type)
        conn.commit()
