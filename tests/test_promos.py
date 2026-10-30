import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app import main as main_module
from app.basket import RULES, candidate_items, suggest
from app.database import Base, get_session
from app.main import app
from app.models import AppState, WineDeal
from app.promos import PROMO_CHECK_KEY, read_home_page, rule_states, summarize
from app.security import InMemoryRateLimiter

# Wording from the shop's home page on 30 Oct 2026, with the markup around it.
HOME_HTML = """
<div class="banner"><p>Receive <strong>S$200 back</strong> in Platinum Wine Credits with min. spend $300
(credited by American Express)</p>
<p>Enjoy an additional $50 off instantly when you spend $600 or more (Wine Portal exclusive)</p></div>
<script>var price = "$50 off $600";</script>
"""
NOW = datetime(2026, 11, 2, 3, 0, tzinfo=UTC)


def check(found, *, ok=True, age=timedelta(hours=2), error=None):
    stamp = (NOW - age).isoformat()
    return {"checked_at": stamp, "ok": ok, "error": error, "found": found, "found_at": stamp}


def test_reads_both_lines_from_the_home_page():
    assert read_home_page(HOME_HTML) == {"credit": True, "promo": True}


def test_wording_drift_still_matches():
    html = "<p>Get S$ 200 back with a minimum spend of S$300.</p><p>Extra S$50 OFF on orders of S$600 and above</p>"
    assert read_home_page(html) == {"credit": True, "promo": True}


def test_promo_text_only_inside_scripts_does_not_count():
    html = "<p>Welcome</p><script>var promo = 'Extra $50 off at $600';</script>"
    assert read_home_page(html)["promo"] is False


def test_promo_missing_from_the_home_page_turns_the_promo_off():
    states = rule_states(RULES, check({"credit": True, "promo": False}), NOW)
    assert states["promo"]["active"] is False
    assert "no longer advertised" in states["promo"]["note"].lower()
    assert states["credit"]["active"] is True


def test_credit_missing_from_the_home_page_only_warns():
    states = rule_states(RULES, check({"credit": False, "promo": True}), NOW)
    assert states["credit"]["active"] is True
    summary = summarize(RULES, check({"credit": False, "promo": True}), NOW)
    assert summary["promo_ok"] is False
    assert "credit" in summary["promo_status"]


def test_failed_fetch_keeps_the_last_good_reading():
    last = check({"credit": True, "promo": False})
    failed = {**last, "checked_at": NOW.isoformat(), "ok": False, "error": "timed out"}
    assert rule_states(RULES, failed, NOW)["promo"]["active"] is False
    summary = summarize(RULES, failed, NOW)
    assert summary["promo_ok"] is False
    assert "timed out" in summary["promo_status"]


def test_credit_ends_after_its_end_date():
    after = datetime(2027, 1, 1, 1, 0, tzinfo=UTC)
    states = rule_states(RULES, check({"credit": True, "promo": True}, age=timedelta(0)) | {"found_at": after.isoformat()}, after)
    assert states["credit"]["active"] is False
    assert "31 Dec 2026" in states["credit"]["note"]


def test_credit_end_date_close_shows_in_health():
    soon = datetime(2026, 12, 15, tzinfo=UTC)
    summary = summarize(RULES, check({"credit": True, "promo": True}, age=timedelta(0)) | {"found_at": soon.isoformat()}, soon)
    assert summary["promo_ok"] is False
    assert "ends 31 Dec 2026" in summary["promo_status"]


def test_never_checked_and_stale_checks_are_reported():
    assert summarize(RULES, {}, NOW)["promo_status"] == "not checked yet"
    stale = summarize(RULES, check({"credit": True, "promo": True}, age=timedelta(days=4)), NOW)
    assert stale["promo_ok"] is False and "4 days" in stale["promo_status"]


def test_all_clear():
    summary = summarize(RULES, check({"credit": True, "promo": True}), NOW)
    assert summary == {"promo_ok": True, "promo_status": "ok", "promo_checked_at": NOW - timedelta(hours=2)}


def _deal(i, price):
    return SimpleNamespace(id=i, wine_name=f"Wine {i}", price_platinum=price, vivino_rating=4.0, vivino_num_ratings=100,
                           style_family="Red", wine_type="Red", quantity=1, volume="750ml", volume_ml=None,
                           platinum_in_stock=True, price_grand_cru=None)


def test_basket_without_the_credit_pays_full_price():
    items = candidate_items([_deal(1, 400), _deal(2, 210)], styles=None, include_unrated=False)
    out = suggest(items, target=600, strict=False, credit_active=False, promo_active=False)
    assert out[0]["credit_back"] == 0.0
    assert out[0]["net_after_credit"] == out[0]["pay_at_checkout"]
    assert out[0]["promo_eligible"] is False


@pytest.fixture()
def client(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'promos.db'}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    with Session() as session:
        session.add_all([
            WineDeal(wine_name=f"2022 Promo Test {i}", platinum_url=f"https://example.com/{i}", price_platinum=price,
                     platinum_in_stock=True, vivino_rating=4.1, vivino_num_ratings=300, style_family="Red",
                     wine_type="Red", volume="750ml", quantity=1)
            for i, price in enumerate([320.0, 300.0, 150.0, 160.0], start=1)
        ])
        session.commit()

    def override():
        with Session() as session:
            yield session

    monkeypatch.setattr(main_module, "_rate_limiter", InMemoryRateLimiter(1000))
    app.dependency_overrides[get_session] = override
    try:
        yield TestClient(app), Session
    finally:
        app.dependency_overrides.pop(get_session, None)
        engine.dispose()


def _save(Session, value):
    with Session() as session:
        session.merge(AppState(key=PROMO_CHECK_KEY, value=json.dumps(value)))
        session.commit()


def test_basket_api_reports_rule_states_and_blocks_an_ended_promo(client):
    api, Session = client
    now = datetime.now(UTC).isoformat()
    _save(Session, {"checked_at": now, "ok": True, "error": None, "found": {"credit": True, "promo": False}, "found_at": now})

    data = api.get("/basket?mode=promo").json()
    assert data["baskets"] == []
    assert "no longer advertised" in data["unavailable"].lower()
    assert data["rules"]["promo"]["active"] is False
    assert data["rules"]["credit"]["active"] is True

    credit = api.get("/basket?mode=credit").json()
    assert credit["baskets"] and credit["unavailable"] is None
    assert all(not b["promo_eligible"] for b in credit["baskets"])

    rules = api.get("/basket/rules").json()
    assert rules["promo"]["active"] is False


def test_health_reports_promo_check(client):
    api, Session = client
    assert api.get("/health").json()["promo_status"] == "not checked yet"
    now = datetime.now(UTC).isoformat()
    _save(Session, {"checked_at": now, "ok": True, "error": None, "found": {"credit": True, "promo": True}, "found_at": now})
    health = api.get("/health").json()
    assert health["promo_ok"] is (datetime.now(UTC).date() < datetime(2026, 12, 1).date())


def test_check_script_records_a_reading_and_keeps_it_when_the_fetch_fails(monkeypatch):
    import httpx

    from scripts import check_promos

    store = {}
    monkeypatch.setattr(check_promos, "load_state", lambda key, default=None: store.get(key, default))
    monkeypatch.setattr(check_promos, "save_state", lambda key, value: store.__setitem__(key, value))

    good = check_promos.run(fetch=lambda: HOME_HTML.replace("$50 off", "gift"), now=NOW)
    assert good["found"] == {"credit": True, "promo": False} and good["ok"] is True

    def down():
        raise httpx.ConnectTimeout("timed out")

    later = NOW + timedelta(days=1)
    failed = check_promos.run(fetch=down, now=later)
    assert failed["ok"] is False and "timed out" in failed["error"]
    assert failed["found"] == good["found"] and failed["found_at"] == good["found_at"]
    assert store[PROMO_CHECK_KEY]["checked_at"] == later.isoformat()
