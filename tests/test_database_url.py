from __future__ import annotations

import pytest

from app.config import normalize_database_url


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("postgresql://u:p@h:5432/d", "postgresql+psycopg://u:p@h:5432/d"),
        ("postgres://u:p@h/d", "postgresql+psycopg://u:p@h/d"),
        ("postgresql+psycopg://u@h/d", "postgresql+psycopg://u@h/d"),
        ("postgresql+psycopg2://u@h/d", "postgresql+psycopg2://u@h/d"),
        ("sqlite:///./data/wines.db", "sqlite:///./data/wines.db"),
    ],
)
def test_normalize_database_url_pins_psycopg3(url: str, expected: str) -> None:
    assert normalize_database_url(url) == expected


def test_normalized_url_uses_psycopg3_driver() -> None:
    from sqlalchemy import create_engine

    engine = create_engine(normalize_database_url("postgresql://u@h/d"))

    assert engine.dialect.driver == "psycopg"
