import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "GrandCru Value API")
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./data/wines.db")
    legal_notice_path: str = os.getenv("LEGAL_NOTICE_PATH", "LEGAL_NOTICE.md")
    ingestion_stale_hours: int = int(os.getenv("INGESTION_STALE_HOURS", "24"))
    cors_origins: str = os.getenv("CORS_ORIGINS", "*")


settings = Settings()
