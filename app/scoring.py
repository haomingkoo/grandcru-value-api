import math
import re


_FLOAT_RE = re.compile(r"-?\d+(?:\.\d+)?")
_INT_RE = re.compile(r"\d[\d,]*")


def parse_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text or text.lower() in {"n/a", "none", "nan"}:
        return None

    match = _FLOAT_RE.search(text.replace(",", ""))
    if not match:
        return None
    return float(match.group(0))


def parse_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)

    text = str(value).strip()
    if not text or text.lower() in {"n/a", "none", "nan"}:
        return None

    match = _INT_RE.search(text)
    if not match:
        return None
    return int(match.group(0).replace(",", ""))


def compute_deal_score(
    price_diff_pct: float | None,
    vivino_rating: float | None,
    vivino_num_ratings: int | None,
    *,
    price_platinum: float | None = None,
    vivino_price: float | None = None,
) -> float:
    """Score a wine deal from 0-100.

    Components:
    - Retailer discount (Platinum vs Grand Cru): up to 30 points
    - Market discount (Platinum vs Vivino market): up to 30 points
    - Vivino rating quality: up to 25 points
    - Rating confidence (sample size): up to 10 points
    - Bonus for beating both Grand Cru AND market: up to 5 points
    """
    # Retailer discount: Platinum vs Grand Cru (up to 30 pts)
    gc_discount_pct = max(-(price_diff_pct or 0.0), 0.0)
    retailer_component = min(gc_discount_pct, 30.0)

    # Market discount: Platinum vs Vivino price (up to 30 pts)
    market_component = 0.0
    if price_platinum and vivino_price and vivino_price > 0:
        market_discount_pct = ((vivino_price - price_platinum) / vivino_price) * 100.0
        if market_discount_pct > 0:
            market_component = min(market_discount_pct, 30.0)

    # Rating quality (up to 25 pts)
    rating = max(min(vivino_rating or 0.0, 5.0), 0.0)
    rating_component = (rating / 5.0) * 25.0

    # Confidence from sample size (up to 10 pts)
    rating_count = max(vivino_num_ratings or 0, 0)
    confidence_component = min(math.log10(rating_count + 1) / 3.0, 1.0) * 10.0

    # Bonus: beating both retailers AND market (up to 5 pts)
    bonus = 0.0
    if retailer_component > 5 and market_component > 5:
        bonus = 5.0

    return round(
        retailer_component + market_component + rating_component + confidence_component + bonus,
        2,
    )
