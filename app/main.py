import csv
from contextlib import asynccontextmanager
import hmac
from io import StringIO
import logging
from pathlib import Path
import time
import uuid

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.config import settings
from app.database import Base, engine, ensure_column, get_session
from app.ops import RefreshRunner, diagnostics_payload, locked_vivino_override_names
from app.schemas import (
    DealHistoryOut,
    DealFiltersOut,
    DealMapPointOut,
    DealOut,
    DealStatsOut,
    HealthOut,
    LegalOut,
    OpsDiagnosticsOut,
    OpsRefreshLogOut,
    OpsRefreshStatusOut,
    OpsRefreshTriggerIn,
)
from app.security import InMemoryRateLimiter, is_exempt_path, parse_exempt_paths, resolve_client_ip
from app.service import (
    count_deals,
    count_snapshots,
    get_deal_filters,
    get_deal_by_id,
    get_deal_history,
    get_deal_map_points,
    get_deal_stats,
    get_latest_ingestion,
    is_ingestion_stale,
    VIVINO_UNRESOLVED_EXPORT_FIELDS,
    list_vivino_unresolved_export_rows,
    list_deals,
)


_level = getattr(logging, settings.log_level, logging.INFO)
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
else:
    logging.getLogger().setLevel(_level)
logger = logging.getLogger("grandcru.api")
refresh_runner = RefreshRunner()
ROOT_DIR = Path(__file__).resolve().parents[1]
WEB_DIR = ROOT_DIR / "web"
FRONTEND_ASSET_PATHS = (
    WEB_DIR / "index.html",
    WEB_DIR / "styles.css",
    WEB_DIR / "app.js",
)
CANONICAL_BASE_URL = "https://wine.kooexperience.com"
CONTENT_SECURITY_POLICY = (
    "default-src 'self'; "
    "base-uri 'self'; "
    "object-src 'none'; "
    "frame-ancestors 'none'; "
    "form-action 'self'; "
    "img-src 'self' data: https://*.tile.openstreetmap.org https://*.basemaps.cartocdn.com https://unpkg.com https://*.app.netsuite.com; "
    "script-src 'self' 'unsafe-inline' https://unpkg.com https://static.cloudflareinsights.com; "
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://unpkg.com; "
    "font-src 'self' https://fonts.gstatic.com; "
    "connect-src 'self' https://cloudflareinsights.com https://unpkg.com; "
    "upgrade-insecure-requests"
)

DEAL_EXTRA_COLUMNS = (
    ("producer", "VARCHAR(255)"),
    ("label_name", "VARCHAR(255)"),
    ("country", "VARCHAR(128)"),
    ("region", "VARCHAR(128)"),
    ("wine_type", "VARCHAR(64)"),
    ("style_family", "VARCHAR(64)"),
    ("grapes", "VARCHAR(255)"),
    ("offering_type", "VARCHAR(64)"),
    ("origin_label", "VARCHAR(255)"),
    ("origin_latitude", "FLOAT"),
    ("origin_longitude", "FLOAT"),
    ("origin_precision", "VARCHAR(32)"),
)


def _ensure_runtime_columns() -> None:
    ensure_column("wine_deals", "vivino_match_method", "VARCHAR(32)")
    ensure_column("wine_deal_snapshots", "vivino_match_method", "VARCHAR(32)")
    ensure_column("wine_deals", "platinum_in_stock", "BOOLEAN")
    ensure_column("wine_deal_snapshots", "platinum_in_stock", "BOOLEAN")
    ensure_column("wine_deals", "grand_cru_in_stock", "BOOLEAN")
    ensure_column("wine_deal_snapshots", "grand_cru_in_stock", "BOOLEAN")
    ensure_column("wine_deals", "image_url", "VARCHAR(1024)")
    ensure_column("wine_deals", "vivino_price", "FLOAT")
    ensure_column("wine_deals", "vivino_description", "VARCHAR(512)")
    for column, col_type in DEAL_EXTRA_COLUMNS:
        ensure_column("wine_deals", column, col_type)
    ensure_column("wine_deals", "price_market", "FLOAT")
    ensure_column("wine_deals", "market_retailer_name", "VARCHAR(128)")
    ensure_column("wine_deals", "market_retailer_url", "VARCHAR(512)")


def deal_filter_params(
    min_score: float = Query(
        default=0.0,
        ge=0.0,
        le=100.0,
        description="Minimum deal_score threshold.",
    ),
    only_platinum_cheaper: bool = Query(
        default=False,
        description="Shortcut for cheaper_side=Platinum Cheaper.",
    ),
    comparable_only: bool = Query(
        default=False,
        description="Only return wines with a comparable Grand Cru price match.",
    ),
    cheaper_side: str | None = Query(
        default=None,
        description="Retailer comparison outcome: Platinum Cheaper, Grand Cru Cheaper, Same Price, or No Match.",
    ),
    min_vivino_rating: float | None = Query(
        default=None,
        ge=0.0,
        le=5.0,
        description="Minimum Vivino rating.",
    ),
    min_vivino_num_ratings: int | None = Query(
        default=None,
        ge=0,
        description="Minimum Vivino rating count.",
    ),
    max_platinum_price: float | None = Query(
        default=None,
        ge=0.0,
        description="Maximum Platinum price.",
    ),
    search: str | None = Query(
        default=None,
        description="Case-insensitive search on wine_name.",
    ),
    country: str | None = Query(
        default=None,
        description="Country filter. Accepts a single value or comma-separated list.",
    ),
    region: str | None = Query(
        default=None,
        description="Region filter. Accepts a single value or comma-separated list.",
    ),
    wine_type: str | None = Query(
        default=None,
        description="Wine type filter such as Red, White, Rose, Sparkling, or Sparkling Rose.",
    ),
    style_family: str | None = Query(
        default=None,
        description="Browse-style filter such as Red, White, Sparkling, Champagne, or Sweet / Dessert.",
    ),
    grape: str | None = Query(
        default=None,
        description="Grape filter. Matches partial text and accepts comma-separated values.",
    ),
    offering_type: str | None = Query(
        default=None,
        description="Offering filter such as Single Bottle, Magnum, Bundle, or Case.",
    ),
    producer: str | None = Query(
        default=None,
        description="Producer filter. Accepts a single value or comma-separated list.",
    ),
) -> dict:
    return {
        "min_score": min_score,
        "only_platinum_cheaper": only_platinum_cheaper,
        "comparable_only": comparable_only,
        "cheaper_side": cheaper_side,
        "min_vivino_rating": min_vivino_rating,
        "min_vivino_num_ratings": min_vivino_num_ratings,
        "max_platinum_price": max_platinum_price,
        "search": search,
        "country": country,
        "region": region,
        "wine_type": wine_type,
        "style_family": style_family,
        "grape": grape,
        "offering_type": offering_type,
        "producer": producer,
    }


@asynccontextmanager
async def lifespan(_: FastAPI):
    if settings.database_url.startswith("sqlite:///./"):
        Path("data").mkdir(exist_ok=True)
    Base.metadata.create_all(bind=engine)
    _ensure_runtime_columns()
    yield


app = FastAPI(
    title=settings.app_name,
    description="API for ranking GrandCru Platinum wine deals against comparable listings.",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs" if settings.api_docs_enabled else None,
    redoc_url="/redoc" if settings.api_docs_enabled else None,
    openapi_url="/openapi.json" if settings.api_docs_enabled else None,
)
app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")

cors_origins = [origin.strip() for origin in settings.cors_origins.split(",") if origin.strip()]
if not cors_origins:
    logger.warning("CORS_ORIGINS is empty; falling back to DEFAULT_CORS_ORIGINS")
    from app.config import DEFAULT_CORS_ORIGINS
    cors_origins = [o.strip() for o in DEFAULT_CORS_ORIGINS.split(",") if o.strip()]

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
async def security_headers_middleware(request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Content-Security-Policy"] = CONTENT_SECURITY_POLICY
    response.headers["Permissions-Policy"] = (
        "camera=(), microphone=(), geolocation=(), payment=(), usb=(), interest-cohort=()"
    )
    return response


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
        logger.warning(
            "rate_limited path=%s method=%s ip=%s retry_after=%s",
            request.url.path,
            request.method,
            client_ip,
            limit_result.reset_seconds,
        )
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


@app.middleware("http")
async def access_log_middleware(request, call_next):
    if not settings.access_log_enabled:
        return await call_next(request)

    start = time.perf_counter()
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    client_ip = resolve_client_ip(
        request.client.host if request.client else None,
        request.headers.get("x-forwarded-for"),
        request.headers.get("x-real-ip"),
    )

    try:
        response = await call_next(request)
    except Exception:
        duration_ms = (time.perf_counter() - start) * 1000.0
        logger.exception(
            "request_failed request_id=%s method=%s path=%s ip=%s duration_ms=%.2f",
            request_id,
            request.method,
            request.url.path,
            client_ip,
            duration_ms,
        )
        raise

    duration_ms = (time.perf_counter() - start) * 1000.0
    response.headers["X-Request-ID"] = request_id
    logger.info(
        "request request_id=%s method=%s path=%s status=%s ip=%s duration_ms=%.2f",
        request_id,
        request.method,
        request.url.path,
        response.status_code,
        client_ip,
        duration_ms,
    )
    return response


@app.get("/", include_in_schema=False)
def frontend() -> HTMLResponse:
    asset_version = str(
        max((path.stat().st_mtime_ns for path in FRONTEND_ASSET_PATHS if path.exists()), default=int(time.time_ns()))
    )
    html = (WEB_DIR / "index.html").read_text(encoding="utf-8").replace("__ASSET_VERSION__", asset_version)
    return HTMLResponse(html, headers={"Cache-Control": "no-store, max-age=0"})


@app.get("/robots.txt", response_class=PlainTextResponse, include_in_schema=False)
def robots_txt() -> PlainTextResponse:
    return PlainTextResponse(
        "\n".join(
            [
                "User-agent: *",
                "Allow: /",
                "Disallow: /ops/",
                "Disallow: /docs",
                "Disallow: /redoc",
                "Disallow: /openapi.json",
                f"LLMs: {CANONICAL_BASE_URL}/llms.txt",
                f"Sitemap: {CANONICAL_BASE_URL}/sitemap.xml",
                "",
            ]
        ),
        headers={"Cache-Control": "public, max-age=86400"},
    )


@app.get("/sitemap.xml", include_in_schema=False)
def sitemap_xml() -> Response:
    lastmod = time.strftime("%Y-%m-%d", time.gmtime())
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>{CANONICAL_BASE_URL}/</loc>
    <lastmod>{lastmod}</lastmod>
    <changefreq>daily</changefreq>
    <priority>1.0</priority>
  </url>
</urlset>
"""
    return Response(
        content=xml,
        media_type="application/xml",
        headers={"Cache-Control": "public, max-age=86400"},
    )


@app.get("/social-card.svg", include_in_schema=False)
def social_card_svg() -> Response:
    svg = """<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630">
  <rect width="1200" height="630" fill="#f2e2d5"/>
  <rect y="0" width="1200" height="84" fill="#651c32"/>
  <circle cx="955" cy="326" r="178" fill="#fef6ee" stroke="#651c32" stroke-width="4"/>
  <circle cx="955" cy="326" r="112" fill="#651c32"/>
  <text x="72" y="68" fill="#fef6ee" font-family="Arial, sans-serif" font-size="34" font-weight="700">MINMAX WINE</text>
  <text x="72" y="216" fill="#651c32" font-family="Arial Narrow, Arial, sans-serif" font-size="82" font-weight="700">THE CREDIT CELLAR</text>
  <text x="76" y="294" fill="#651c32" font-family="Georgia, serif" font-size="34">Platinum Wine Club, Grand Cru, and Vivino market checks.</text>
  <text x="76" y="352" fill="#651c32" font-family="Georgia, serif" font-size="30">Live Singapore wine value tracker by Haoming Koo.</text>
  <rect x="76" y="420" width="278" height="56" rx="28" fill="#ef6079"/>
  <text x="112" y="457" fill="#fef6ee" font-family="Georgia, serif" font-size="24" font-weight="700">Compare live offers</text>
  <text x="895" y="342" fill="#fef6ee" font-family="Arial Narrow, Arial, sans-serif" font-size="76" font-weight="700">SGD</text>
</svg>
"""
    return Response(
        content=svg,
        media_type="image/svg+xml",
        headers={"Cache-Control": "public, max-age=86400"},
    )


@app.get("/llms.txt", response_class=PlainTextResponse, include_in_schema=False)
def llms_txt(session: Session = Depends(get_session)) -> PlainTextResponse:
    try:
        total_deals = count_deals(session)
    except SQLAlchemyError:
        session.rollback()
        total_deals = 0
    text = f"""# MinMax Wine

MinMax Wine is a Singapore wine price comparison and deal-scoring web app by Haoming Koo.

URL: {CANONICAL_BASE_URL}/
Source: https://github.com/haomingkoo/grandcru-value-api
Purpose: compare Platinum Wine Club offers against Grand Cru Wines and Vivino market context.
Current coverage: {total_deals} live Platinum offers in the production database.
Data sources: Platinum Wine Club inventory and prices, Grand Cru Wines comparison prices, Vivino ratings and market prices.
Important note: prices and availability can change; users should verify on the merchant site before purchase.

Useful endpoints:
- {CANONICAL_BASE_URL}/
- {CANONICAL_BASE_URL}/deals
- {CANONICAL_BASE_URL}/deals/stats
- {CANONICAL_BASE_URL}/llms-full.txt
"""
    return PlainTextResponse(text, headers={"Cache-Control": "public, max-age=3600"})


def _money(value: float | None) -> str:
    return "-" if value is None else f"SGD {value:,.2f}"


@app.get("/llms-full.txt", response_class=PlainTextResponse, include_in_schema=False)
def llms_full_txt(session: Session = Depends(get_session)) -> PlainTextResponse:
    try:
        deals = list_deals(session, limit=5, offset=0, sort_by="deal_score", sort_order="desc")
        stats = get_deal_stats(session)
    except SQLAlchemyError:
        session.rollback()
        deals = []
        stats = {"total_deals": 0, "cheaper_sides": []}
    cheaper_sides = {item["value"]: item["count"] for item in stats.get("cheaper_sides", [])}
    comparable_count = sum(
        cheaper_sides.get(label, 0)
        for label in ("Platinum Cheaper", "Grand Cru Cheaper", "Same Price")
    )
    lines = [
        "# MinMax Wine - full discovery notes",
        "",
        "MinMax Wine helps Singapore wine buyers decide whether a Platinum Wine Club offer is good value.",
        "It compares Platinum prices with strict Grand Cru Wines matches and Vivino market prices where available.",
        "The app also tracks seven-day and thirty-day price movement, source coverage, Vivino ratings, and offer formats such as single bottles, magnums, bundles, and cases.",
        "",
        f"Canonical URL: {CANONICAL_BASE_URL}/",
        "Creator: Haoming Koo",
        "Source code: https://github.com/haomingkoo/grandcru-value-api",
        "Data caveat: prices and availability are not guaranteed and should be verified on merchant sites before purchase.",
        "",
        "Current production stats:",
        f"- Live offers: {stats.get('total_deals', 0)}",
        f"- Grand Cru matched offers: {comparable_count}",
        f"- Platinum cheaper offers: {cheaper_sides.get('Platinum Cheaper', 0)}",
        "",
        "Top current offers by deal score:",
    ]
    for deal in deals:
        lines.append(
            "- "
            f"{deal.wine_name}: Platinum {_money(deal.price_platinum)}, "
            f"Grand Cru {_money(deal.price_grand_cru)}, "
            f"Vivino rating {deal.vivino_rating or '-'}, score {deal.deal_score:.1f}."
        )
    lines.append("")
    return PlainTextResponse("\n".join(lines), headers={"Cache-Control": "public, max-age=3600"})


@app.get("/api")
def root() -> dict[str, str]:
    payload = {"service": settings.app_name, "health": "/health", "deals": "/deals"}
    if settings.api_docs_enabled:
        payload["docs"] = "/docs"
    return payload


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
    limit: int = Query(default=100, ge=1, le=500, description="Max number of rows to return."),
    offset: int = Query(default=0, ge=0, description="Pagination offset."),
    sort_by: str = Query(
        default="deal_score",
        description="Sort field: deal_score, price_diff_pct, price_diff_pct_abs, vivino_rating, vivino_num_ratings, price_platinum, or wine_name.",
    ),
    sort_order: str = Query(
        default="desc",
        description="Sort direction. For price_diff_pct, asc means Platinum-cheaper-first. Use price_diff_pct_abs for largest-gap sorting regardless of side.",
    ),
    filters: dict = Depends(deal_filter_params),
    session: Session = Depends(get_session),
) -> list[DealOut]:
    """List ranked wine deals with stable tie-break ordering for equal-looking UI values."""
    return list_deals(
        session,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_order=sort_order,
        **filters,
    )


@app.get("/deals/filters", response_model=DealFiltersOut)
def get_deal_filter_options(
    filters: dict = Depends(deal_filter_params),
    session: Session = Depends(get_session),
) -> DealFiltersOut:
    return DealFiltersOut(**get_deal_filters(session, **filters))


@app.get("/deals/stats", response_model=DealStatsOut)
def get_deal_stats_summary(
    filters: dict = Depends(deal_filter_params),
    session: Session = Depends(get_session),
) -> DealStatsOut:
    return DealStatsOut(**get_deal_stats(session, **filters))


@app.get("/deals/map", response_model=list[DealMapPointOut])
def get_deal_map(
    filters: dict = Depends(deal_filter_params),
    session: Session = Depends(get_session),
) -> list[DealMapPointOut]:
    return [DealMapPointOut(**point) for point in get_deal_map_points(session, **filters)]


@app.get("/deals/{deal_id}", response_model=DealOut)
def get_deal(deal_id: int, session: Session = Depends(get_session)) -> DealOut:
    deal = get_deal_by_id(session, deal_id)
    if deal is None:
        raise HTTPException(status_code=404, detail="Deal not found")
    return deal


@app.get("/deals/{deal_id}/history", response_model=list[DealHistoryOut])
def deal_history(
    deal_id: int,
    limit: int = Query(default=30, ge=1, le=365, description="Max number of history points to return."),
    days: int = Query(default=90, ge=1, le=3650, description="Lookback window in days."),
    sort_order: str = Query(
        default="asc",
        description="History sort direction. asc is typically best for charting.",
    ),
    session: Session = Depends(get_session),
) -> list[DealHistoryOut]:
    deal = get_deal_by_id(session, deal_id)
    if deal is None:
        raise HTTPException(status_code=404, detail="Deal not found")
    if sort_order not in {"asc", "desc"}:
        raise HTTPException(status_code=400, detail="sort_order must be 'asc' or 'desc'")
    return get_deal_history(
        session,
        wine_name=deal.wine_name,
        limit=limit,
        days=days,
        sort_order=sort_order,
    )


@app.get("/legal", response_model=LegalOut)
def legal() -> LegalOut:
    notice_path = Path(settings.legal_notice_path)
    if not notice_path.exists():
        raise HTTPException(status_code=404, detail="Legal notice not available")
    return LegalOut(title="Responsible Data Use Notice", content=notice_path.read_text(encoding="utf-8"))


def require_ops_key(x_ops_key: str | None = Header(default=None, alias="X-Ops-Key")) -> None:
    if not settings.ops_api_key:
        raise HTTPException(
            status_code=503,
            detail="Ops endpoints are disabled. Set OPS_API_KEY to enable them.",
        )
    if not x_ops_key or not hmac.compare_digest(x_ops_key, settings.ops_api_key):
        raise HTTPException(status_code=403, detail="Invalid X-Ops-Key")


@app.get("/ops/diagnostics", response_model=OpsDiagnosticsOut)
def ops_diagnostics(
    _: None = Depends(require_ops_key),
    session: Session = Depends(get_session),
) -> OpsDiagnosticsOut:
    payload = diagnostics_payload(
        refresh_runner=refresh_runner,
        total_deals=count_deals(session),
        total_snapshots=count_snapshots(session),
    )
    return OpsDiagnosticsOut(**payload)


@app.get("/ops/refresh/status", response_model=OpsRefreshStatusOut)
def ops_refresh_status(_: None = Depends(require_ops_key)) -> OpsRefreshStatusOut:
    return OpsRefreshStatusOut(**refresh_runner.get_status())


@app.get("/ops/refresh/log", response_model=OpsRefreshLogOut)
def ops_refresh_log(
    lines: int = Query(default=200, ge=20, le=5000),
    _: None = Depends(require_ops_key),
) -> OpsRefreshLogOut:
    return OpsRefreshLogOut(**refresh_runner.tail_log(lines=lines))


@app.get("/ops/vivino/unresolved.csv", response_class=PlainTextResponse)
def ops_vivino_unresolved_csv(
    limit: int = Query(default=500, ge=1, le=5000),
    include_locked: bool = Query(default=False),
    _: None = Depends(require_ops_key),
    session: Session = Depends(get_session),
) -> PlainTextResponse:
    rows = list_vivino_unresolved_export_rows(
        session,
        limit=limit,
        include_locked=include_locked,
        locked_wine_names=locked_vivino_override_names(),
    )
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(VIVINO_UNRESOLVED_EXPORT_FIELDS))
    writer.writeheader()
    writer.writerows(rows)
    return PlainTextResponse(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="vivino_unresolved.csv"'},
    )


@app.post("/ops/refresh/trigger", response_model=OpsRefreshStatusOut, status_code=202)
def ops_refresh_trigger(
    request: OpsRefreshTriggerIn,
    _: None = Depends(require_ops_key),
) -> OpsRefreshStatusOut:
    mode = (request.mode or "").strip().lower()
    if mode not in {"daily", "weekly", "import_only"}:
        raise HTTPException(status_code=400, detail="mode must be one of: daily, weekly, import_only")
    if refresh_runner.is_running():
        raise HTTPException(status_code=409, detail="A refresh run is already in progress.")

    status = refresh_runner.start(
        mode=mode,
        health_url=request.health_url,
        strict_health=request.strict_health,
        triggered_by="ops_api",
    )
    return OpsRefreshStatusOut(**status)
