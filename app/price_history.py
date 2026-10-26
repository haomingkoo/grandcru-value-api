"""Daily price history per offer and source, and the plain-language lines built from it."""

from __future__ import annotations

import csv
import logging
import re
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AppState, PriceObservation, WineDeal, WineDealSnapshot

logger = logging.getLogger("grandcru.price_history")

SGT = timezone(timedelta(hours=8))
SOURCE_LABELS = {"platinum": "Platinum", "grand_cru": "Grand Cru", "vivino": "Vivino"}
# Vivino is read about once a week, so it is drawn as dated checks, not a daily line.
CHECK_SOURCES = {"vivino"}
FRESH_DAYS = {"platinum": 2, "grand_cru": 3, "vivino": 30}
DEFAULT_FRESH_DAYS = 7
MIN_SPAN_DAYS = 14
BACKFILL_STATE_KEY = "price_history_backfill"


def source_label(source: str) -> str:
    return SOURCE_LABELS.get(source) or source.replace("_", " ").title()


def volume_ml(volume: str | None) -> int | None:
    text = re.sub(r"\s+", "", (volume or "").lower())
    match = re.fullmatch(r"(\d+(?:\.\d+)?)(ml|cl|l)", text)
    if not match:
        return None
    amount = float(match.group(1))
    factor = {"ml": 1, "cl": 10, "l": 1000}[match.group(2)]
    return int(round(amount * factor))


def per_750(price: float | None, quantity: int | None, ml: int | None) -> float | None:
    if price is None or not ml:
        return None
    bottles = max(quantity or 1, 1)
    return round(price / (bottles * ml / 750), 2)


def sg_day(moment: datetime) -> date:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(SGT).date()


def observations_for_deal(payload: dict[str, Any], *, checked_at: datetime, run_id: int | None) -> list[dict]:
    """The prices one import read for one offer, one row per source."""
    ml = volume_ml(payload.get("volume"))
    base = {
        "wine_name": payload["wine_name"],
        "volume_ml": ml,
        "quantity": payload.get("quantity"),
        "currency": "SGD",
        "origin": "daily_check",
        "ingestion_run_id": run_id,
    }
    rows = []
    if payload.get("price_platinum") is not None:
        rows.append({**base, "source": "platinum", "price": payload["price_platinum"],
                     "url": payload.get("platinum_url"), "checked_at": checked_at,
                     "in_stock": payload.get("platinum_in_stock")})
    if payload.get("price_grand_cru") is not None:
        rows.append({**base, "source": "grand_cru", "price": payload["price_grand_cru"],
                     "url": payload.get("grand_cru_url"), "checked_at": checked_at})
    vivino_checked = payload.get("vivino_checked_at")
    if payload.get("vivino_price") is not None and vivino_checked is not None:
        rows.append({**base, "source": "vivino", "price": payload["vivino_price"],
                     "url": payload.get("vivino_url"), "checked_at": vivino_checked})
    return rows


def record_observations(session: Session, rows: Iterable[dict]) -> int:
    """Upsert one row per offer, source and Singapore day; the latest check of the day wins."""
    latest: dict[tuple[str, str, date], dict] = {}
    for row in rows:
        if row.get("price") is None or row["price"] <= 0:
            continue
        key = (row["wine_name"], row["source"], sg_day(row["checked_at"]))
        if key not in latest or _as_utc(row["checked_at"]) >= _as_utc(latest[key]["checked_at"]):
            latest[key] = row
    if not latest:
        return 0

    names = {key[0] for key in latest}
    days = {key[2] for key in latest}
    existing = {
        (obs.wine_name, obs.source, obs.observed_on): obs
        for obs in session.scalars(
            select(PriceObservation).where(
                PriceObservation.wine_name.in_(names),
                PriceObservation.observed_on.in_(days),
            )
        )
    }
    written = 0
    for key, row in latest.items():
        obs = existing.get(key)
        if obs is not None and _as_utc(obs.checked_at) > _as_utc(row["checked_at"]):
            continue
        if obs is None:
            obs = PriceObservation(wine_name=key[0], source=key[1], observed_on=key[2])
            session.add(obs)
        obs.price = float(row["price"])
        obs.currency = row.get("currency") or "SGD"
        obs.volume_ml = row.get("volume_ml")
        obs.quantity = row.get("quantity")
        obs.checked_at = row["checked_at"]
        obs.url = row.get("url")
        obs.in_stock = row.get("in_stock")
        obs.origin = row.get("origin") or "daily_check"
        obs.ingestion_run_id = row.get("ingestion_run_id")
        written += 1
    return written


def _as_utc(moment: datetime) -> datetime:
    return moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment


# --- Backfill ---------------------------------------------------------------

def _seed_pairs(seed_comparison: Path) -> set[tuple[str, float]]:
    if not seed_comparison.exists():
        return set()
    pairs = set()
    with seed_comparison.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            price = _parse_price(row.get("price_plat"))
            if row.get("name_plat") and price is not None:
                pairs.add((row["name_plat"].strip(), price))
    return pairs


def _parse_price(raw: str | None) -> float | None:
    cleaned = re.sub(r"[^0-9.]", "", raw or "")
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def is_seed_replay(run_pairs: set[tuple[str, float]], seed_pairs: set[tuple[str, float]]) -> bool:
    """A web restart re-imports the committed seed file; its prices are old, not that day's."""
    return bool(seed_pairs) and run_pairs == seed_pairs


def backfill_from_snapshots(session: Session, seed_comparison: Path) -> int:
    snapshots = list(session.scalars(select(WineDealSnapshot).order_by(WineDealSnapshot.captured_at)))
    by_run: dict[int, list[WineDealSnapshot]] = defaultdict(list)
    for snap in snapshots:
        by_run[snap.ingestion_run_id].append(snap)
    seed = _seed_pairs(seed_comparison)
    rows: list[dict] = []
    skipped_runs = []
    for run_id, run_snaps in by_run.items():
        pairs = {(s.wine_name, s.price_platinum) for s in run_snaps if s.price_platinum is not None}
        if is_seed_replay(pairs, seed):
            skipped_runs.append(run_id)
            continue
        for snap in run_snaps:
            rows.extend(observations_for_deal(
                {
                    "wine_name": snap.wine_name,
                    "volume": snap.volume,
                    "quantity": snap.quantity,
                    "price_platinum": snap.price_platinum,
                    "platinum_url": snap.platinum_url,
                    "price_grand_cru": snap.price_grand_cru,
                    "grand_cru_url": snap.grand_cru_url,
                },
                checked_at=snap.captured_at,
                run_id=run_id,
            ))
    if skipped_runs:
        logger.info("backfill_skipped_seed_replays runs=%s", skipped_runs)
    return record_observations(session, rows)


def backfill_from_seed_commits(session: Session, history_csv: Path) -> int:
    """Prices from committed seed files, dated by commit (see scripts/build_seed_price_history.py)."""
    if not history_csv.exists():
        return 0
    rows = []
    with history_csv.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            price = _parse_price(row.get("price"))
            if price is None:
                continue
            rows.append({
                "wine_name": row["wine_name"],
                "source": row["source"],
                "price": price,
                "currency": row.get("currency") or "SGD",
                "volume_ml": int(row["volume_ml"]) if row.get("volume_ml") else None,
                "quantity": int(row["quantity"]) if row.get("quantity") else None,
                "checked_at": datetime.fromisoformat(row["committed_at"]),
                "url": row.get("url") or None,
                "origin": "saved_list",
                "ingestion_run_id": None,
            })
    # Only fill days with no daily check; a real check always outranks a saved list.
    days_taken = {
        (obs.wine_name, obs.source, obs.observed_on)
        for obs in session.scalars(select(PriceObservation))
    }
    rows = [r for r in rows if (r["wine_name"], r["source"], sg_day(r["checked_at"])) not in days_taken]
    return record_observations(session, rows)


def backfill_current_vivino(session: Session) -> int:
    """Dated Vivino checks already on the current offers (checks started before history did)."""
    rows = []
    for deal in session.scalars(select(WineDeal).where(WineDeal.vivino_checked_at.is_not(None))):
        rows.extend(r for r in observations_for_deal(
            {c.name: getattr(deal, c.name) for c in WineDeal.__table__.columns},
            checked_at=deal.vivino_checked_at,
            run_id=None,
        ) if r["source"] == "vivino")
    return record_observations(session, rows)


def run_backfills(session: Session, *, seed_comparison: Path, seed_history: Path) -> dict[str, int]:
    """Idempotent: each backfill runs once, recorded in app_state."""
    import json

    state_row = session.get(AppState, BACKFILL_STATE_KEY)
    done = json.loads(state_row.value) if state_row else {}
    result: dict[str, int] = {}
    if "snapshots" not in done:
        result["snapshots"] = backfill_from_snapshots(session, seed_comparison)
        session.flush()
        done["snapshots"] = datetime.now(UTC).isoformat()
    if "vivino_current" not in done:
        result["vivino_current"] = backfill_current_vivino(session)
        session.flush()
        done["vivino_current"] = datetime.now(UTC).isoformat()
    if "seed_commits" not in done:
        result["seed_commits"] = backfill_from_seed_commits(session, seed_history)
        done["seed_commits"] = datetime.now(UTC).isoformat()
    if result:
        payload = json.dumps(done, sort_keys=True)
        if state_row is None:
            session.add(AppState(key=BACKFILL_STATE_KEY, value=payload))
        else:
            state_row.value = payload
        session.commit()
    return result


# --- Read side --------------------------------------------------------------

def format_money(value: float) -> str:
    rounded = round(value, 2)
    if abs(rounded - round(rounded)) < 0.005:
        return f"S${round(rounded):,.0f}"
    return f"S${rounded:,.2f}"


def format_day(day: date, today: date) -> str:
    text = f"{day.day} {day.strftime('%b')}"
    return text if day.year == today.year else f"{text} {day.year}"


def _segments(points: list[dict]) -> list[dict]:
    """Merge runs of consecutive days at the same price. A missing day starts a new segment."""
    segments: list[dict] = []
    for point in points:
        last = segments[-1] if segments else None
        if (
            last is not None
            and last["price"] == point["price"]
            and date.fromisoformat(last["end"]) + timedelta(days=1) == point["day"]
        ):
            last["end"] = point["day"].isoformat()
            last["days"] += 1
            continue
        segments.append({
            "start": point["day"].isoformat(),
            "end": point["day"].isoformat(),
            "price": point["price"],
            "price_per_750": point["price_per_750"],
            "days": 1,
            "origin": point["origin"],
        })
    return segments


def _deal_series(deal, observations: list[PriceObservation]) -> dict[str, list[dict]]:
    by_source: dict[str, list[PriceObservation]] = defaultdict(list)
    for obs in observations:
        by_source[obs.source].append(obs)

    # Grand Cru history only counts for the same Grand Cru product page as now.
    gc = by_source.get("grand_cru", [])
    if gc:
        reference = deal.grand_cru_url or max(gc, key=lambda o: o.observed_on).url
        by_source["grand_cru"] = [o for o in gc if o.url == reference]

    series = {}
    for source, items in by_source.items():
        items = sorted(items, key=lambda o: o.observed_on)
        points = [
            {
                "day": o.observed_on,
                "price": round(o.price, 2),
                "price_per_750": per_750(o.price, o.quantity, o.volume_ml),
                "bottles_750": max(o.quantity or 1, 1) * o.volume_ml / 750 if o.volume_ml else None,
                "origin": o.origin,
            }
            for o in items
        ]
        points = [p for p in points if p["price_per_750"] is not None]
        if points:
            series[source] = points
    return series


def _history_verdict(current: float, points: list[dict], today: date) -> dict | None:
    if not points:
        return None
    first_day = points[0]["day"]
    prices = [p["price"] for p in points]
    previous = [p for p in points if p["day"] < today] or points[:-1]

    # A change is a fact on its own, whatever the span.
    changed = [p for p in previous if abs(p["price"] - current) >= 0.5]
    if changed:
        last_diff = changed[-1]
        delta = current - last_diff["price"]
        since = format_day(last_diff["day"], today)
        lowest = min(prices + [current])
        if (today - first_day).days >= MIN_SPAN_DAYS and current <= lowest and current < min(p["price"] for p in previous):
            return {"kind": "lowest", "tone": "good",
                    "text": f"Lowest Platinum price we have recorded since {format_day(first_day, today)}"}
        if delta > 0:
            return {"kind": "up", "tone": "bad", "text": f"Up {format_money(delta)} since {since}"}
        return {"kind": "down", "tone": "good", "text": f"Down {format_money(-delta)} since {since}"}

    if (today - first_day).days >= MIN_SPAN_DAYS:
        return {"kind": "unchanged", "tone": "calm",
                "text": f"Same Platinum price at every check since {format_day(first_day, today)}"}
    return None


def _cross_source_verdict(deal, series: dict[str, list[dict]], today: date) -> dict | None:
    platinum = series.get("platinum")
    if not platinum or deal.price_platinum is None:
        return None
    deal_ml = volume_ml(deal.volume)
    if not deal_ml:
        return None
    bottles_750 = max(deal.quantity or 1, 1) * deal_ml / 750

    others = []
    for source, points in series.items():
        if source == "platinum":
            continue
        last = points[-1]
        if (today - last["day"]).days > FRESH_DAYS.get(source, DEFAULT_FRESH_DAYS):
            continue
        # The other price scaled to this listing's size, unrounded.
        others.append((last["price"] * bottles_750 / last["bottles_750"], source))
    if not others:
        return None

    cheapest, source = min(others)
    amount = round(deal.price_platinum - cheapest, 2)
    name = "the Vivino market price" if source == "vivino" else source_label(source)
    if abs(amount) < 0.5:
        return {"kind": "cross_same", "tone": "calm", "text": f"Same as {name}"}
    money = format_money(abs(amount))
    if amount < 0:
        if len(others) == 1:
            return {"kind": "cross_below", "tone": "good", "text": f"{money} below {name}"}
        return {"kind": "cross_below", "tone": "good",
                "text": f"{money} below the cheapest other price we track ({source_label(source)})"}
    if source == "vivino":
        return {"kind": "cross_above", "tone": "bad", "text": f"The Vivino market price is {money} lower"}
    return {"kind": "cross_above", "tone": "bad", "text": f"{money} cheaper at {name}"}


def stock_dates(session: Session, wine_names: Iterable[str]) -> dict[str, dict]:
    """When each offer was first seen, last seen in stock, and first seen sold out after that."""
    names = set(wine_names)
    out: dict[str, dict] = {}
    if not names:
        return out
    rows = session.execute(
        select(PriceObservation.wine_name, PriceObservation.observed_on, PriceObservation.in_stock)
        .where(PriceObservation.wine_name.in_(names), PriceObservation.source == "platinum",
               PriceObservation.origin == "daily_check")
        .order_by(PriceObservation.observed_on)
    ).all()
    for name, day, in_stock in rows:
        info = out.setdefault(name, {"first_seen": day, "last_in_stock": None, "sold_out_since": None})
        if in_stock is False:
            if info["sold_out_since"] is None:
                info["sold_out_since"] = day
        else:  # null rows predate sold-out scraping, so they were in stock
            info["last_in_stock"] = day
            info["sold_out_since"] = None
    return out


def build_price_history(session: Session, deals: list, *, now: datetime | None = None) -> dict:
    today = sg_day(now or datetime.now(UTC))
    names = {d.wine_name for d in deals}
    observations: dict[str, list[PriceObservation]] = defaultdict(list)
    if names:
        for obs in session.scalars(select(PriceObservation).where(PriceObservation.wine_name.in_(names))):
            observations[obs.wine_name].append(obs)

    offers = []
    earliest: date | None = None
    for deal in deals:
        series = _deal_series(deal, observations.get(deal.wine_name, []))
        sources = []
        for source in sorted(series, key=lambda s: (list(SOURCE_LABELS).index(s) if s in SOURCE_LABELS else 99, s)):
            points = series[source]
            entry = {
                "source": source,
                "label": source_label(source),
                "kind": "checks" if source in CHECK_SOURCES else "daily",
                "first_date": points[0]["day"].isoformat(),
                "last_date": points[-1]["day"].isoformat(),
            }
            if entry["kind"] == "checks":
                entry["points"] = [
                    {"date": p["day"].isoformat(), "price": p["price"],
                     "price_per_750": p["price_per_750"], "origin": p["origin"]}
                    for p in points
                ]
            else:
                entry["segments"] = _segments(points)
            sources.append(entry)
            if earliest is None or points[0]["day"] < earliest:
                earliest = points[0]["day"]

        verdicts = []
        if deal.price_platinum is not None and "platinum" in series:
            history = _history_verdict(round(deal.price_platinum, 2), series["platinum"], today)
            if history:
                verdicts.append(history)
        cross = _cross_source_verdict(deal, series, today)
        if cross:
            verdicts.append(cross)

        saved = sorted({p["day"].isoformat() for pts in series.values() for p in pts if p["origin"] == "saved_list"})
        offers.append({
            "deal_id": deal.id,
            "volume_ml": volume_ml(deal.volume),
            "quantity": deal.quantity,
            "first_date": min((s["first_date"] for s in sources), default=None),
            "sources": sources,
            "saved_list_dates": saved,
            "verdicts": verdicts,
        })
    return {
        "today": today.isoformat(),
        "first_date": earliest.isoformat() if earliest else None,
        "offers": offers,
    }
