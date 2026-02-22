from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from app.config import settings
from app.database import Base, engine, get_session
from app.schemas import DealOut, HealthOut, LegalOut
from app.service import count_deals, get_deal_by_id, get_latest_ingestion, is_ingestion_stale, list_deals


@asynccontextmanager
async def lifespan(_: FastAPI):
    if settings.database_url.startswith("sqlite:///./"):
        Path("data").mkdir(exist_ok=True)
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title=settings.app_name,
    description="API for ranking GrandCru Platinum wine deals against comparable listings.",
    version="0.1.0",
    lifespan=lifespan,
)

cors_origins = [origin.strip() for origin in settings.cors_origins.split(",") if origin.strip()]
if not cors_origins:
    cors_origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/")
def root() -> dict[str, str]:
    return {"service": settings.app_name, "docs": "/docs", "health": "/health", "deals": "/deals"}


@app.get("/health", response_model=HealthOut)
def health(session: Session = Depends(get_session)) -> HealthOut:
    total = count_deals(session)
    latest = get_latest_ingestion(session)
    stale = is_ingestion_stale(latest)
    return HealthOut(
        status="ok",
        db_ok=True,
        total_deals=total,
        ingestion_stale=stale,
        latest_ingestion=latest,
    )


@app.get("/deals", response_model=list[DealOut])
def get_deals(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    min_score: float = Query(default=0.0, ge=0.0, le=100.0),
    only_platinum_cheaper: bool = Query(default=True),
    search: str | None = Query(default=None),
    session: Session = Depends(get_session),
) -> list[DealOut]:
    return list_deals(
        session,
        limit=limit,
        offset=offset,
        min_score=min_score,
        only_platinum_cheaper=only_platinum_cheaper,
        search=search,
    )


@app.get("/deals/{deal_id}", response_model=DealOut)
def get_deal(deal_id: int, session: Session = Depends(get_session)) -> DealOut:
    deal = get_deal_by_id(session, deal_id)
    if deal is None:
        raise HTTPException(status_code=404, detail="Deal not found")
    return deal


@app.get("/legal", response_model=LegalOut)
def legal() -> LegalOut:
    notice_path = Path(settings.legal_notice_path)
    if not notice_path.exists():
        raise HTTPException(status_code=404, detail=f"Missing legal notice file: {notice_path}")
    return LegalOut(title="Responsible Data Use Notice", content=notice_path.read_text(encoding="utf-8"))
