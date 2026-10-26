"""Basket suggestions that reach a Platinum spend threshold with the least overspend.

0/1 knapsack over cents: every reachable total up to the threshold plus a margin is found
exactly, keeping the best few baskets (by rating quality) at each total. Prices are the live
Platinum listing prices; nothing is estimated.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from app.price_history import per_750, volume_ml

# Facts checked on 26 Oct 2026; each is quoted with its source on the page.
RULES = {
    "credit": {
        "amount": 200.0,
        "min_spend": 300.0,
        "text": "S$200 back on your Amex statement after you pay, for a single purchase of S$300 or more.",
        "conditions": "Save the benefit to your Platinum Card first. Basic Card Members only. Once per half-year (January to June, July to December). Ends 31 Dec 2026.",
        "source_label": "American Express Platinum terms (2026), Platinum Wine Credit",
        "source_url": "https://www.americanexpress.com/content/dam/amex/en-sg/benefits/the-platinum-card/2026_Platinum_TermsandConditions.pdf",
    },
    "delivery": {
        "free_over": 350.0,
        "fee": 30.0,
        "text": "Free delivery in Singapore on orders over S$350; otherwise S$30 per trip.",
        "source_label": "Platinum Wine Club terms and conditions",
        "source_url": "https://platwineclub.wineportal.com/terms-and-conditions",
    },
    "promo": {
        "amount": 50.0,
        "min_spend": 600.0,
        "text": "The shop advertises an extra S$50 off at S$600 or more. Its terms say promotions cannot be combined with other offers, and it is unclear whether that affects the Amex credit, so check at checkout.",
        "source_label": "Platinum Wine Club home page",
        "source_url": "https://platwineclub.wineportal.com/",
    },
}

MODES = {
    "credit": {"target": 300.0, "strict": False, "label": "S$300 or more: S$200 back"},
    "delivery": {"target": 350.0, "strict": True, "label": "Over S$350: S$200 back and free delivery"},
    "promo": {"target": 600.0, "strict": False, "label": "S$600 or more: shop's extra S$50 off"},
}

STYLE_GROUPS = {
    "red": {"Red"},
    "white": {"White", "Orange"},
    "rose": {"Rose"},
    "sparkling": {"Sparkling", "Champagne"},
}

KEEP_PER_TOTAL = 4
MARGIN = 40.0  # how far above the threshold baskets are searched


@dataclass(frozen=True)
class Item:
    deal_id: int
    name: str
    cents: int
    bottles: int
    bottles_750: float
    rating: float | None
    num_ratings: int | None
    per_750: float
    price: float
    grand_cru: float | None
    on_frontier: bool = False

    @property
    def quality(self) -> float:
        """Rating above 3.0, per 750 ml bottle: the sum is what a basket delivers for its spend."""
        if self.rating is None:
            return 0.0
        return self.bottles_750 * (self.rating - 3.0) + (0.15 * self.bottles_750 if self.on_frontier else 0.0)


def candidate_items(deals: Iterable, *, styles: set[str] | None, include_unrated: bool) -> list[Item]:
    items = []
    for d in deals:
        if d.platinum_in_stock is False or not d.price_platinum or d.price_platinum <= 0:
            continue
        if styles and (d.style_family or d.wine_type) not in styles:
            continue
        if d.vivino_rating is None and not include_unrated:
            continue
        ml = getattr(d, "volume_ml", None) or volume_ml(d.volume)
        p750 = per_750(d.price_platinum, d.quantity, ml)
        if p750 is None:
            continue
        bottles = max(d.quantity or 1, 1)
        items.append(Item(
            deal_id=d.id,
            name=d.wine_name,
            cents=int(round(d.price_platinum * 100)),
            bottles=bottles,
            bottles_750=bottles * ml / 750,
            rating=d.vivino_rating,
            num_ratings=d.vivino_num_ratings,
            per_750=p750,
            price=d.price_platinum,
            grand_cru=d.price_grand_cru,
        ))
    # Mark the value frontier among the candidates: nothing cheaper per 750 ml rates as high.
    best = -1.0
    marked = []
    for item in sorted(items, key=lambda i: (i.per_750, -(i.rating or 0))):
        on = item.rating is not None and item.rating > best
        if on:
            best = item.rating
        marked.append(Item(**{**item.__dict__, "on_frontier": on}))
    return marked


def search(items: list[Item], *, limit_cents: int, max_bottles: int | None) -> dict[int, list[tuple[float, int, tuple[int, ...]]]]:
    """For every reachable total up to limit_cents, the best few baskets as (quality, bottles, item indexes)."""
    states: dict[int, list[tuple[float, int, tuple[int, ...]]]] = {0: [(0.0, 0, ())]}
    for index, item in enumerate(items):
        if item.cents > limit_cents:
            continue
        additions: list[tuple[int, tuple[float, int, tuple[int, ...]]]] = []
        for total, baskets in states.items():
            new_total = total + item.cents
            if new_total > limit_cents:
                continue
            for quality, bottles, chosen in baskets:
                if max_bottles and bottles + item.bottles > max_bottles:
                    continue
                additions.append((new_total, (quality + item.quality, bottles + item.bottles, chosen + (index,))))
        for total, basket in additions:
            bucket = states.setdefault(total, [])
            bucket.append(basket)
            if len(bucket) > KEEP_PER_TOTAL:
                bucket.sort(key=lambda b: (-b[0], b[1]))
                del bucket[KEEP_PER_TOTAL:]
    return states


def _avg_rating(items: list[Item], chosen: tuple[int, ...]) -> float:
    rated = [(items[i].rating, items[i].bottles_750) for i in chosen if items[i].rating is not None]
    weight = sum(w for _, w in rated)
    return sum(r * w for r, w in rated) / weight if weight else 0.0


def suggest(items: list[Item], *, target: float, strict: bool, max_bottles: int | None = None, prefer: str = "value") -> list[dict]:
    """Up to three different baskets at or above the target, closest first."""
    floor = int(round(target * 100)) + (1 if strict else 0)
    states = search(items, limit_cents=floor + int(MARGIN * 100), max_bottles=max_bottles)
    options = [(total, b) for total, baskets in states.items() if total >= floor for b in baskets if b[2]]
    if not options:
        return []
    closest_total = min(total for total, _ in options)
    picks: list[tuple[str, int, tuple]] = []

    def add(kind: str, pool: list[tuple[int, tuple]], key) -> None:
        seen = {tuple(sorted(p[2][2])) for p in picks}
        for total, basket in sorted(pool, key=key):
            if tuple(sorted(basket[2])) not in seen:
                picks.append((kind, total, basket))
                return

    add("closest", [o for o in options if o[0] == closest_total], key=lambda o: (-o[1][0], o[1][1]))
    near = [o for o in options if o[0] <= closest_total + 1500]
    wider = [o for o in options if o[0] <= closest_total + 3000]
    if prefer == "rating":
        add("best_rated", [o for o in wider if o[1][1] >= 2], key=lambda o: (-_avg_rating(items, o[1][2]), o[0]))
        add("best_value", near, key=lambda o: (-o[1][0], o[0]))
    else:
        add("best_value", near, key=lambda o: (-o[1][0], o[0]))
        add("best_rated", [o for o in wider if o[1][1] >= 2], key=lambda o: (-_avg_rating(items, o[1][2]), o[0]))
    return [describe(kind, total, basket, items, target=target, strict=strict) for kind, total, basket in picks]


def describe(kind: str, total_cents: int, basket: tuple, items: list[Item], *, target: float, strict: bool) -> dict:
    chosen = [items[i] for i in basket[2]]
    total = total_cents / 100
    credit = RULES["credit"]
    delivery = RULES["delivery"]
    promo = RULES["promo"]
    gets_credit = total >= credit["min_spend"]
    free_delivery = total > delivery["free_over"]
    gc_known = [i for i in chosen if i.grand_cru]
    return {
        "kind": kind,
        "total": round(total, 2),
        "over_target": round(total - target, 2),
        "target": target,
        "strict": strict,
        "bottles": sum(i.bottles for i in chosen),
        "average_rating": round(_avg_rating(items, basket[2]), 2) or None,
        "credit_back": credit["amount"] if gets_credit else 0.0,
        "delivery_fee": 0.0 if free_delivery else delivery["fee"],
        "free_delivery": free_delivery,
        "promo_eligible": total >= promo["min_spend"],
        "pay_at_checkout": round(total + (0.0 if free_delivery else delivery["fee"]), 2),
        "net_after_credit": round(total + (0.0 if free_delivery else delivery["fee"]) - (credit["amount"] if gets_credit else 0.0), 2),
        "grand_cru_difference": round(sum(i.grand_cru - i.price for i in gc_known), 2) if gc_known else None,
        "grand_cru_known": len(gc_known),
        "items": [
            {
                "deal_id": i.deal_id,
                "name": i.name,
                "price": i.price,
                "price_per_750": i.per_750,
                "rating": i.rating,
                "num_ratings": i.num_ratings,
                "grand_cru": i.grand_cru,
                "reason": reason(i),
            }
            for i in sorted(chosen, key=lambda i: -i.price)
        ],
    }


def _money(value: float) -> str:
    return f"S${value:,.0f}" if abs(value - round(value)) < 0.005 else f"S${value:,.2f}"


def reason(item: Item) -> str:
    per = f"{_money(item.per_750)} per 750 ml"
    if item.rating is None:
        return f"Not rated on Vivino yet; here to reach the total. {per}."
    if item.on_frontier:
        return f"Nothing you can buy for less per 750 ml rates higher than its {item.rating:.1f}."
    if item.grand_cru and item.grand_cru - item.price >= 0.5:
        return f"{_money(item.grand_cru - item.price)} less than at Grand Cru. Vivino {item.rating:.1f}."
    count = f" from {item.num_ratings:,} ratings" if item.num_ratings else ""
    return f"Vivino {item.rating:.1f}{count} at {per}."
