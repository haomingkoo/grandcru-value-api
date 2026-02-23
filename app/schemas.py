from datetime import datetime

from pydantic import BaseModel, ConfigDict


class DealOut(BaseModel):
    id: int
    wine_name: str
    vintage: int | None = None
    quantity: int | None = None
    volume: str | None = None
    price_platinum: float | None = None
    price_grand_cru: float | None = None
    price_diff: float | None = None
    price_diff_pct: float | None = None
    cheaper_side: str | None = None
    platinum_url: str | None = None
    grand_cru_url: str | None = None
    vivino_url: str | None = None
    vivino_rating: float | None = None
    vivino_num_ratings: int | None = None
    deal_score: float

    model_config = ConfigDict(from_attributes=True)


class IngestionRunOut(BaseModel):
    id: int
    started_at: datetime
    finished_at: datetime | None = None
    status: str
    comparison_rows: int
    vivino_rows: int
    merged_rows: int
    details: str | None = None

    model_config = ConfigDict(from_attributes=True)


class HealthOut(BaseModel):
    status: str
    db_ok: bool
    total_deals: int
    total_snapshots: int = 0
    history_retention_days: int = 0
    ingestion_stale: bool | None = None
    latest_ingestion: IngestionRunOut | None = None


class LegalOut(BaseModel):
    title: str
    content: str


class DealHistoryOut(BaseModel):
    id: int
    ingestion_run_id: int
    captured_at: datetime
    wine_name: str
    price_platinum: float | None = None
    price_grand_cru: float | None = None
    price_diff: float | None = None
    price_diff_pct: float | None = None
    deal_score: float
    vivino_rating: float | None = None
    vivino_num_ratings: int | None = None

    model_config = ConfigDict(from_attributes=True)
