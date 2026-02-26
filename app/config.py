import os
from dataclasses import dataclass


def _as_bool(value: str, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "GrandCru Value API")
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./data/wines.db")
    legal_notice_path: str = os.getenv("LEGAL_NOTICE_PATH", "LEGAL_NOTICE.md")
    ingestion_stale_hours: int = int(os.getenv("INGESTION_STALE_HOURS", "24"))
    cors_origins: str = os.getenv("CORS_ORIGINS", "*")
    history_retention_days: int = int(os.getenv("HISTORY_RETENTION_DAYS", "90"))
    rate_limit_enabled: bool = _as_bool(os.getenv("RATE_LIMIT_ENABLED"), True)
    rate_limit_requests_per_minute: int = int(os.getenv("RATE_LIMIT_REQUESTS_PER_MINUTE", "120"))
    rate_limit_exempt_paths: str = os.getenv(
        "RATE_LIMIT_EXEMPT_PATHS",
        "/,/health,/legal,/docs,/openapi.json,/redoc",
    )
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()
    access_log_enabled: bool = _as_bool(os.getenv("ACCESS_LOG_ENABLED"), True)


settings = Settings()
