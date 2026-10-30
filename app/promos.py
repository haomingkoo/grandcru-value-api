"""Whether each basket rule is still running.

Two signals: the end date in the rule's own terms, and a daily read of the shop's home page
(scripts/check_promos.py, stored in app_state). The shop's S$50 off exists only as a home-page
banner, so it switches off when the banner goes. The Amex credit follows Amex's terms, so a
missing home-page line only shows as a warning in /health. A failed read changes nothing.
"""

from __future__ import annotations

import html as html_lib
import re
from datetime import UTC, date, datetime, timedelta
from typing import Any

from app.basket import RULES

PROMO_CHECK_KEY = "promo_check"
HOME_URL = "https://platwineclub.wineportal.com/"
STALE_AFTER = timedelta(days=3)
ENDING_SOON = timedelta(days=30)

# Wording seen on 30 Oct 2026: "S$200 back ... $300", "$50 off ... $600". The amounts come from the rules;
# allows "S$" or "$", spacing and up to 80 characters between the two amounts.
HOME_WORDS = {"credit": "back", "promo": "off"}


def _home_pattern(amount: float, word: str, min_spend: float) -> re.Pattern[str]:
    return re.compile(rf"S?\$\s?{amount:.0f}\s+{word}\b.{{0,80}}?S?\$\s?{min_spend:.0f}\b", re.I | re.S)
# Rules the home page decides. The credit is set by Amex's terms, so it is only watched.
HOME_DECIDES = {"promo"}


def page_text(html: str) -> str:
    text = re.sub(r"<(script|style)\b.*?</\1>", " ", html, flags=re.I | re.S)
    text = html_lib.unescape(re.sub(r"<[^>]+>", " ", text))
    return re.sub(r"\s+", " ", text)


def read_home_page(html: str) -> dict[str, bool]:
    text = page_text(html)
    return {
        key: bool(_home_pattern(RULES[key]["amount"], word, RULES[key]["min_spend"]).search(text))
        for key, word in HOME_WORDS.items()
    }


def _day(value: date) -> str:
    return f"{value.day} {value:%b %Y}"


def _when(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def rule_states(rules: dict[str, dict], check: dict[str, Any], now: datetime) -> dict[str, dict]:
    found = check.get("found") or {}
    found_at = _when(check.get("found_at"))
    states = {}
    for key, rule in rules.items():
        ends = date.fromisoformat(rule["ends_on"]) if rule.get("ends_on") else None
        if ends and now.date() > ends:
            states[key] = {"active": False, "note": f"Ended {_day(ends)}. Check the latest terms before relying on it."}
        elif key in HOME_DECIDES and found_at and found.get(key) is False:
            states[key] = {"active": False, "note": f"No longer advertised on the shop's home page (checked {_day(found_at.date())})."}
        else:
            states[key] = {"active": True, "note": None}
    return states


def summarize(rules: dict[str, dict], check: dict[str, Any], now: datetime) -> dict[str, Any]:
    """Health fields: promo_ok is false whenever a person should look at the rules."""
    if not check:
        return {"promo_ok": False, "promo_status": "not checked yet", "promo_checked_at": None}
    problems = []
    if check.get("ok") is False:
        problems.append(f"last check failed: {check.get('error') or 'unknown error'}")
    found_at = _when(check.get("found_at"))
    if found_at is None:
        problems.append("no successful check yet")
    elif now - found_at > STALE_AFTER:
        problems.append(f"last successful check is {(now - found_at).days} days old")
    for key, seen in (check.get("found") or {}).items():
        if seen is False:
            problems.append(f"{key} line missing from the shop's home page")
    for key, rule in rules.items():
        if rule.get("ends_on"):
            ends = date.fromisoformat(rule["ends_on"])
            if now.date() > ends:
                problems.append(f"{key} ended {_day(ends)}")
            elif ends - now.date() <= ENDING_SOON:
                problems.append(f"{key} ends {_day(ends)}; check for new terms")
    return {
        "promo_ok": not problems,
        "promo_status": "; ".join(problems) if problems else "ok",
        "promo_checked_at": found_at,
    }
