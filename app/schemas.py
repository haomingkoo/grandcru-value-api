from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class DealOut(BaseModel):
    id: int
    wine_name: str
    vintage: int | None = None
    quantity: int | None = None
    volume: str | None = None
    producer: str | None = None
    label_name: str | None = None
    country: str | None = None
    region: str | None = None
    wine_type: str | None = None
    style_family: str | None = None
    grapes: str | None = None
    offering_type: str | None = None
    origin_label: str | None = None
    origin_latitude: float | None = None
    origin_longitude: float | None = None
    origin_precision: str | None = None
    price_platinum: float | None = None
    price_grand_cru: float | None = None
    price_diff: float | None = None
    price_diff_pct: float | None = Field(
        default=None,
        description="Signed percentage: (Platinum - Grand Cru) / Grand Cru * 100. Negative means Platinum cheaper; positive means Grand Cru cheaper.",
    )
    price_diff_pct_abs: float | None = Field(
        default=None,
        description="Absolute retailer price gap in percent, regardless of which side is cheaper.",
    )
    cheaper_side: str | None = Field(
        default=None,
        description="Retailer comparison outcome: Platinum Cheaper, Grand Cru Cheaper, Same Price, or No Match.",
    )
    platinum_url: str | None = None
    grand_cru_url: str | None = None
    vivino_url: str | None = None
    vivino_rating: float | None = None
    vivino_num_ratings: int | None = None
    vivino_price: float | None = None
    vivino_description: str | None = None
    vivino_match_method: str | None = Field(
        default=None,
        description="How Vivino metadata was attached: exact, canonical, fuzzy, platinum, or none.",
    )
    deal_score: float = Field(
        description="0-100 ranking score. Discount contributes up to 60 points, Vivino rating up to 30 points, and rating-count confidence up to 10 points.",
    )
    price_platinum_7d_ago: float | None = None
    price_platinum_change_7d: float | None = None
    price_grand_cru_7d_ago: float | None = None
    price_grand_cru_change_7d: float | None = None
    price_platinum_30d_ago: float | None = None
    price_platinum_change_30d: float | None = None
    price_grand_cru_30d_ago: float | None = None
    price_grand_cru_change_30d: float | None = None
    has_competitor_match: bool = False
    is_platinum_cheaper: bool = False
    is_good_wine: bool = False
    is_high_confidence: bool = False
    value_verdict: str = ""
    value_verdict_tone: str = ""
    value_verdict_reason: str = ""
    origin_source: str | None = None
    origin_confidence: str | None = None
    grape_source: str | None = None
    grape_confidence: str | None = None
    metadata_confidence: str | None = None
    platinum_trend_7d: str = "unknown"
    grand_cru_trend_7d: str = "unknown"
    platinum_trend_30d: str = "unknown"
    grand_cru_trend_30d: str = "unknown"

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
    vintage: int | None = None
    quantity: int | None = None
    volume: str | None = None
    price_platinum: float | None = None
    price_grand_cru: float | None = None
    price_diff: float | None = None
    price_diff_pct: float | None = Field(
        default=None,
        description="Signed percentage: (Platinum - Grand Cru) / Grand Cru * 100. Negative means Platinum cheaper; positive means Grand Cru cheaper.",
    )
    deal_score: float = Field(
        description="0-100 ranking score using discount, Vivino rating, and rating-count confidence.",
    )
    vivino_rating: float | None = None
    vivino_num_ratings: int | None = None
    vivino_match_method: str | None = Field(
        default=None,
        description="How Vivino metadata was attached: exact, canonical, fuzzy, platinum, or none.",
    )

    model_config = ConfigDict(from_attributes=True)


class OpsRefreshTriggerIn(BaseModel):
    mode: str = Field(default="daily", description="daily | weekly | import_only")
    health_url: str | None = None
    strict_health: bool = False


class OpsRefreshStatusOut(BaseModel):
    run_id: str | None = None
    status: str
    mode: str | None = None
    triggered_by: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    exit_code: int | None = None
    command: list[str] | None = None
    log_path: str | None = None
    pid: int | None = None


class OpsRefreshLogOut(BaseModel):
    run_id: str | None = None
    log_tail: str


class OpsDiagnosticsOut(BaseModel):
    timestamp: str
    app_name: str
    hostname: str
    python_version: str
    git_commit: str
    railway_service: str
    railway_env: str
    database_scheme: str
    brave_api_key_set: bool
    ops_api_key_set: bool
    total_deals: int
    total_snapshots: int
    refresh_status: dict
    files: list[dict]


class LabelCountOut(BaseModel):
    value: str
    count: int


class OfferingStatOut(BaseModel):
    value: str
    count: int
    platinum_cheaper_count: int
    grand_cru_cheaper_count: int
    average_price_platinum: float | None = None
    average_price_diff_pct: float | None = None


class DealFiltersOut(BaseModel):
    countries: list[LabelCountOut]
    regions: list[LabelCountOut]
    wine_types: list[LabelCountOut]
    style_families: list[LabelCountOut]
    grapes: list[LabelCountOut]
    offering_types: list[LabelCountOut]
    producers: list[LabelCountOut]


class DealStatsOut(BaseModel):
    total_deals: int
    cheaper_sides: list[LabelCountOut]
    wine_types: list[LabelCountOut]
    style_families: list[LabelCountOut]
    countries: list[LabelCountOut]
    offering_types: list[OfferingStatOut]


class DealMapPointOut(BaseModel):
    origin_label: str
    country: str | None = None
    region: str | None = None
    origin_latitude: float
    origin_longitude: float
    origin_precision: str | None = None
    wine_count: int
    platinum_cheaper_count: int
    average_price_diff_pct: float | None = None
    sample_wines: list[str]
