"""Every S$ amount the basket shows comes from app.basket.RULES, so new terms are a one-place change."""

import re
from pathlib import Path

from fastapi.testclient import TestClient

from app import basket
from app.main import app
from app.promos import read_home_page

WEB = Path(__file__).resolve().parents[1] / "web"
RULE_AMOUNTS = {
    basket.RULES["credit"]["amount"],
    basket.RULES["credit"]["min_spend"],
    basket.RULES["delivery"]["free_over"],
    basket.RULES["promo"]["amount"],
    basket.RULES["promo"]["min_spend"],
}


def test_page_source_names_no_rule_amount():
    # Price presets ("Under S$50") are filters, not rules; they carry data-min/data-max.
    for name in ("index.html", "basket.js"):
        text = "\n".join(line for line in (WEB / name).read_text().splitlines() if "data-max" not in line)
        for amount in RULE_AMOUNTS:
            assert not re.search(rf"S\$\s?{amount:.0f}\b", text), f"{name} hard-codes S${amount:.0f}"


def test_page_shows_the_rule_amounts(monkeypatch):
    monkeypatch.setitem(basket.RULES["credit"], "amount", 250.0)
    monkeypatch.setitem(basket.RULES["promo"], "min_spend", 700.0)
    page = TestClient(app).get("/").text
    assert "S$250 back" in page
    assert "S$700 or more" in page
    assert not re.search(r"__[A-Z_]+__", page)


def test_preset_modes_follow_the_rules():
    assert basket.modes()["credit"]["target"] == basket.RULES["credit"]["min_spend"]
    assert basket.modes()["delivery"]["target"] == basket.RULES["delivery"]["free_over"]
    assert basket.modes()["promo"]["target"] == basket.RULES["promo"]["min_spend"]


def test_home_page_check_follows_the_rules(monkeypatch):
    html = "<p>S$200 back with min. spend $300</p><p>$80 off when you spend $900</p>"
    assert read_home_page(html)["promo"] is False
    monkeypatch.setitem(basket.RULES["promo"], "amount", 80.0)
    monkeypatch.setitem(basket.RULES["promo"], "min_spend", 900.0)
    assert read_home_page(html)["promo"] is True
