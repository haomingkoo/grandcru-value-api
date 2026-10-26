"""JSON key/value state kept in the database so it survives redeploys."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy.orm import Session

from app.database import SessionLocal, engine
from app.models import AppState


def _ensure_table(bind=None) -> None:
    AppState.__table__.create(bind=bind or engine, checkfirst=True)


def _decode(row: AppState | None, default: Any) -> Any:
    if row is None:
        return default
    try:
        return json.loads(row.value)
    except (TypeError, json.JSONDecodeError):
        return default


def load_state(key: str, default: Any = None, session: Session | None = None) -> Any:
    if session is not None:
        _ensure_table(session.get_bind())
        return _decode(session.get(AppState, key), default)
    _ensure_table()
    with SessionLocal() as own:
        return _decode(own.get(AppState, key), default)


def save_state(key: str, value: Any) -> None:
    _ensure_table()
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True)
    with SessionLocal() as session:
        row = session.get(AppState, key)
        if row is None:
            session.add(AppState(key=key, value=payload))
        else:
            row.value = payload
        session.commit()
