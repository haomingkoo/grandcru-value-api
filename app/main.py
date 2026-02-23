from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.config import settings
from app.database import Base, engine, get_session
from app.schemas import DealHistoryOut, DealOut, HealthOut, LegalOut
from app.security import InMemoryRateLimiter, is_exempt_path, parse_exempt_paths, resolve_client_ip
from app.service import (
    count_deals,
    count_snapshots,
    get_deal_by_id,
    get_deal_history,
    get_latest_ingestion,
    is_ingestion_stale,
    list_deals,
)


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

_exempt_paths = parse_exempt_paths(settings.rate_limit_exempt_paths)
_rate_limiter = None
if settings.rate_limit_enabled:
    _rate_limiter = InMemoryRateLimiter(settings.rate_limit_requests_per_minute)


@app.middleware("http")
async def rate_limit_middleware(request, call_next):
    if _rate_limiter is None or is_exempt_path(request.url.path, _exempt_paths):
        return await call_next(request)

    client_ip = resolve_client_ip(
        request.client.host if request.client else None,
        request.headers.get("x-forwarded-for"),
        request.headers.get("x-real-ip"),
    )
    limit_result = _rate_limiter.check(client_ip)
    if not limit_result.allowed:
        return JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded. Please retry shortly."},
            headers={
                "Retry-After": str(limit_result.reset_seconds),
                "X-RateLimit-Limit": str(_rate_limiter.limit),
                "X-RateLimit-Remaining": "0",
            },
        )

    response = await call_next(request)
    response.headers["X-RateLimit-Limit"] = str(_rate_limiter.limit)
    response.headers["X-RateLimit-Remaining"] = str(limit_result.remaining)
    return response


@app.get("/")
def root() -> dict[str, str]:
    return {"service": settings.app_name, "docs": "/docs", "health": "/health", "deals": "/deals"}


@app.get("/health", response_model=HealthOut)
def health(session: Session = Depends(get_session)) -> HealthOut:
    total = count_deals(session)
    total_snapshots = count_snapshots(session)
    latest = get_latest_ingestion(session)
    stale = is_ingestion_stale(latest)
    return HealthOut(
        status="ok",
        db_ok=True,
        total_deals=total,
        total_snapshots=total_snapshots,
        history_retention_days=settings.history_retention_days,
        ingestion_stale=stale,
        latest_ingestion=latest,
    )


@app.get("/deals", response_model=list[DealOut])
def get_deals(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    min_score: float = Query(default=0.0, ge=0.0, le=100.0),
    only_platinum_cheaper: bool = Query(default=True),
    cheaper_side: str | None = Query(default=None),
    min_vivino_rating: float | None = Query(default=None, ge=0.0, le=5.0),
    min_vivino_num_ratings: int | None = Query(default=None, ge=0),
    max_platinum_price: float | None = Query(default=None, ge=0.0),
    sort_by: str = Query(default="deal_score"),
    sort_order: str = Query(default="desc"),
    search: str | None = Query(default=None),
    session: Session = Depends(get_session),
) -> list[DealOut]:
    return list_deals(
        session,
        limit=limit,
        offset=offset,
        min_score=min_score,
        only_platinum_cheaper=only_platinum_cheaper,
        cheaper_side=cheaper_side,
        min_vivino_rating=min_vivino_rating,
        min_vivino_num_ratings=min_vivino_num_ratings,
        max_platinum_price=max_platinum_price,
        sort_by=sort_by,
        sort_order=sort_order,
        search=search,
    )


@app.get("/deals/{deal_id}", response_model=DealOut)
def get_deal(deal_id: int, session: Session = Depends(get_session)) -> DealOut:
    deal = get_deal_by_id(session, deal_id)
    if deal is None:
        raise HTTPException(status_code=404, detail="Deal not found")
    return deal


@app.get("/deals/{deal_id}/history", response_model=list[DealHistoryOut])
def deal_history(
    deal_id: int,
    limit: int = Query(default=30, ge=1, le=365),
    session: Session = Depends(get_session),
) -> list[DealHistoryOut]:
    deal = get_deal_by_id(session, deal_id)
    if deal is None:
        raise HTTPException(status_code=404, detail="Deal not found")
    return get_deal_history(session, wine_name=deal.wine_name, limit=limit)


@app.get("/legal", response_model=LegalOut)
def legal() -> LegalOut:
    notice_path = Path(settings.legal_notice_path)
    if not notice_path.exists():
        raise HTTPException(status_code=404, detail=f"Missing legal notice file: {notice_path}")
    return LegalOut(title="Responsible Data Use Notice", content=notice_path.read_text(encoding="utf-8"))
