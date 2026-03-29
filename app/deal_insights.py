from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.wine_metadata import derive_wine_metadata


GOOD_WINE_RATING = 4.0
HIGH_CONFIDENCE_RATINGS = 100
TREND_EPSILON = 0.01


@dataclass(frozen=True)
class DealInsights:
    has_competitor_match: bool
    is_platinum_cheaper: bool
    is_good_wine: bool
    is_high_confidence: bool
    value_verdict: str
    value_verdict_tone: str
    value_verdict_reason: str
    origin_source: str | None
    origin_confidence: str | None
    grape_source: str | None
    grape_confidence: str | None
    metadata_confidence: str | None
    platinum_trend_7d: str
    grand_cru_trend_7d: str
    platinum_trend_30d: str
    grand_cru_trend_30d: str


def classify_price_trend(change: float | None) -> str:
    if change is None:
        return "unknown"
    if change <= -TREND_EPSILON:
        return "down"
    if change >= TREND_EPSILON:
        return "up"
    return "flat"


def compute_deal_insights(deal: Any) -> DealInsights:
    has_competitor_match = getattr(deal, "price_diff_pct", None) is not None
    is_platinum_cheaper = getattr(deal, "cheaper_side", None) == "Platinum Cheaper"
    rating = getattr(deal, "vivino_rating", None) or 0.0
    rating_count = getattr(deal, "vivino_num_ratings", None) or 0
    is_good_wine = rating >= GOOD_WINE_RATING
    is_high_confidence = rating_count >= HIGH_CONFIDENCE_RATINGS

    metadata = derive_wine_metadata(
        wine_name=getattr(deal, "wine_name", None),
        quantity=getattr(deal, "quantity", None),
        volume=getattr(deal, "volume", None),
    )

    if is_platinum_cheaper and is_good_wine and is_high_confidence:
        verdict = ("Strong Credit Spend", "good", "Good wine, healthy rating count, and no obvious Platinum markup.")
    elif is_platinum_cheaper:
        verdict = ("Solid Value", "good", "Platinum currently beats Grand Cru on price.")
    elif getattr(deal, "cheaper_side", None) == "Same Price" and rating >= 4.1:
        verdict = ("Quality Buy", "calm", "Not cheaper, but still appealing if you want the bottle on Platinum.")
    elif getattr(deal, "cheaper_side", None) == "Same Price":
        verdict = ("Retail Match", "calm", "Platinum matches Grand Cru on price, so this is more about convenience than edge.")
    elif getattr(deal, "cheaper_side", None) == "Grand Cru Cheaper":
        verdict = ("Platinum Markup", "warn", "Grand Cru is the better pure price play right now.")
    elif getattr(deal, "cheaper_side", None) == "No Match":
        verdict = ("Quality Only", "ghost", "Interesting wine, but there is no retailer comparison yet.")
    else:
        verdict = ("Needs Review", "ghost", "Worth a manual look before spending credits.")

    return DealInsights(
        has_competitor_match=has_competitor_match,
        is_platinum_cheaper=is_platinum_cheaper,
        is_good_wine=is_good_wine,
        is_high_confidence=is_high_confidence,
        value_verdict=verdict[0],
        value_verdict_tone=verdict[1],
        value_verdict_reason=verdict[2],
        origin_source=metadata.origin_source,
        origin_confidence=metadata.origin_confidence,
        grape_source=metadata.grape_source,
        grape_confidence=metadata.grape_confidence,
        metadata_confidence=metadata.metadata_confidence,
        platinum_trend_7d=classify_price_trend(getattr(deal, "price_platinum_change_7d", None)),
        grand_cru_trend_7d=classify_price_trend(getattr(deal, "price_grand_cru_change_7d", None)),
        platinum_trend_30d=classify_price_trend(getattr(deal, "price_platinum_change_30d", None)),
        grand_cru_trend_30d=classify_price_trend(getattr(deal, "price_grand_cru_change_30d", None)),
    )
