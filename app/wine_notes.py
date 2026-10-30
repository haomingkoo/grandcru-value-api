"""One-line wine notes: the grape, and how that grape usually tastes.

Notes are written per grape, not per wine, and say what is typical. Nothing comes from
Vivino reviews or retailer copy. A grape that does not fit the wine's colour is treated
as a wrong guess and gets no line; a guessed grape reads "Likely".
"""

from __future__ import annotations

from typing import Any

# grape: (colour, typical style of a still wine from it)
GRAPE_NOTES = {
    "Aglianico": ("red", "Firm tannins, high acidity and dark fruit; southern Italy's grape for ageing."),
    "Albarino": ("white", "Crisp and aromatic, with stone fruit and a salty edge."),
    "Barbera": ("red", "Juicy and high in acidity, low in tannin, with dark cherry."),
    "Cabernet Franc": ("red", "Medium body, red fruit and a leafy, herbal note."),
    "Cabernet Sauvignon": ("red", "Full body, blackcurrant and firm tannins; often ages well."),
    "Carmenere": ("red", "Dark fruit with a green-pepper note; Chile's signature grape."),
    "Chardonnay": ("white", "Crisp and citrusy, or rich and buttery when aged in oak."),
    "Chenin Blanc": ("white", "High acidity, apple and honey; made from dry to sweet."),
    "Dolcetto": ("red", "Soft, dark-fruited and low in acidity, with a bitter-almond finish."),
    "Freisa": ("red", "Piedmont red with raspberry and firm tannins."),
    "Gewurztraminer": ("white", "Very aromatic, with lychee, rose and spice; low acidity."),
    "Grenache": ("red", "Juicy red fruit and spice, with gentle tannins."),
    "Gruner Veltliner": ("white", "Crisp, with citrus and a white-pepper note; Austria's grape."),
    "Merlot": ("red", "Soft and plummy, rounder than Cabernet."),
    "Montepulciano": ("red", "Soft, plummy dark fruit and easy tannins."),
    "Muscat": ("white", "Grapey and floral; often off-dry or sweet."),
    "Nascetta": ("white", "Rare Piedmont white, aromatic with citrus and herbs."),
    "Nebbiolo": ("red", "Pale but high in tannin and acidity; Barolo's grape, built to age."),
    "Negroamaro": ("red", "Dark, rustic fruit with a bitter-cherry edge; from Puglia."),
    "Pinot Grigio": ("white", "Light and crisp, with pear and citrus."),
    "Pinot Noir": ("red", "Light to medium body, red fruit and soft tannins."),
    "Primitivo": ("red", "Ripe, jammy dark fruit and soft tannins; the same grape as Zinfandel."),
    "Riesling": ("white", "High acidity, citrus and stone fruit; made from dry to sweet."),
    "Sangiovese": ("red", "Bright acidity, sour cherry and a savoury finish; Chianti's grape."),
    "Sauvignon Blanc": ("white", "Zesty and aromatic, with citrus and green notes."),
    "Shiraz": ("red", "Dark fruit and spice, usually riper and fuller than Syrah."),
    "Syrah": ("red", "Dark fruit, black pepper and a firm structure."),
    "Tempranillo": ("red", "Medium body, cherry and leather; Rioja's main grape."),
    "Timorasso": ("white", "Full-bodied Piedmont white with a mineral edge; ages well."),
    "Vermentino": ("white", "Fresh and zesty, with citrus and a bitter-herb finish."),
    "Viognier": ("white", "Full and perfumed, with apricot, peach and flowers."),
    "Zinfandel": ("red", "Ripe, jammy dark fruit, often high in alcohol; the same grape as Primitivo."),
}

# Colour of every grape we know, including ones without a note, for the colour check.
GRAPE_COLOURS = {grape: colour for grape, (colour, _) in GRAPE_NOTES.items()} | {
    "Alicante Bouschet": "red", "Baga": "red", "Malbec": "red", "Mourvedre": "red", "Nero d'Avola": "red",
    "Petit Verdot": "red", "Pinot Meunier": "red", "Touriga Nacional": "red",
    "Glera": "white", "Pinot Bianco": "white",
}

# Sparkling wines, by grape mix: (label shown, note).
SPARKLING_NOTES = {
    ("Chardonnay",): "Blanc de blancs: all Chardonnay, usually crisp and citrusy.",
    ("Glera",): "Prosecco's grape: light, fruity bubbles with pear and apple.",
    ("Pinot Meunier", "Pinot Noir"): "Blanc de noirs: white bubbles from black grapes, usually fuller.",
    ("Chardonnay", "Pinot Meunier", "Pinot Noir"): "The classic Champagne blend.",
}

# Catch-all labels from the metadata guesses; they add nothing to the row.
GENERIC_BLENDS = {"Red Blend", "White Blend", "Rose Blend", "Sparkling Rose Blend", "Rhone White Blend"}

FITS = {"red": {"Red", "Rose", "Sparkling", "Champagne"}, "white": {"White", "Sparkling", "Champagne", "Orange"}}
STILL = {"red": "Red", "white": "White"}
SPARKLING = {"Sparkling", "Champagne"}


def _join(names: list[str]) -> str:
    return names[0] if len(names) == 1 else f"{', '.join(names[:-1])} and {names[-1]}"


def wine_note(deal: Any) -> str | None:
    grapes = (getattr(deal, "grapes", None) or "").strip()
    style = getattr(deal, "style_family", None)
    if not grapes or not style or grapes in GENERIC_BLENDS:
        return None
    confidence = getattr(deal, "grape_confidence", None)
    if confidence == "low":
        return None

    blend = grapes.endswith(" Blend")
    names = [grapes.removesuffix(" Blend")] if blend else [g.strip() for g in grapes.split(",") if g.strip()]
    if any(name in GRAPE_COLOURS and style not in FITS[GRAPE_COLOURS[name]] for name in names):
        return None

    label = f"{names[0]} blend" if blend else _join(names)
    if style in SPARKLING:
        text = SPARKLING_NOTES.get(tuple(sorted(names))) if not blend else None
    elif len(names) == 1 and names[0] in GRAPE_NOTES and STILL[GRAPE_NOTES[names[0]][0]] == style:
        text = GRAPE_NOTES[names[0]][1]
    else:
        text = None

    guessed = (getattr(deal, "grape_source", None) or "").endswith("inference")
    return f"{'Likely ' if guessed else ''}{label}.{f' {text}' if text else ''}"
