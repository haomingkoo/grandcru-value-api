from types import SimpleNamespace

from app.wine_metadata import GRAPE_COLOURS
from app.wine_notes import GRAPE_NOTES, wine_note


def note(grapes, style, source=None, confidence=None):
    return wine_note(SimpleNamespace(grapes=grapes, style_family=style, grape_source=source, grape_confidence=confidence))


def test_single_grape_gets_its_style_note():
    assert note("Pinot Noir", "Red", "listing_keyword", "high") == f"Pinot Noir. {GRAPE_NOTES['Pinot Noir']}"


def test_grape_without_a_written_note_still_names_the_grape():
    assert note("Baga", "Red", "vivino", "high") == "Baga."


def test_inferred_grape_says_likely():
    assert note("Pinot Noir", "Red", "regional_inference", "medium").startswith("Likely Pinot Noir. ")


def test_low_confidence_grape_gets_no_line():
    assert note("Pinot Noir, Chardonnay, Pinot Meunier", "Champagne", "style_inference", "low") is None


def test_grape_that_does_not_match_the_wine_colour_gets_no_line():
    # Seen live: red Chassagne-Montrachet tagged Chardonnay by the regional guess.
    assert note("Chardonnay", "Red", "regional_inference", "medium") is None
    assert note("Nebbiolo", "White", "vivino", "medium") is None


def test_rose_and_sparkling_name_the_grape_without_the_still_wine_note():
    assert note("Pinot Noir", "Rose", "vivino", "high") == "Pinot Noir."
    assert note("Sauvignon Blanc", "Sparkling", "vivino", "high") == "Sauvignon Blanc."


def test_sparkling_styles_have_their_own_notes():
    assert note("Chardonnay", "Champagne", "style_inference", "medium").startswith("Likely Chardonnay. Blanc de blancs")
    assert note("Glera", "Sparkling", "listing_keyword", "high").startswith("Glera. Prosecco")
    assert note("Pinot Noir, Chardonnay, Pinot Meunier", "Champagne", "listing_keyword", "high").startswith(
        "Pinot Noir, Chardonnay and Pinot Meunier. The classic Champagne blend"
    )


def test_blends_list_their_grapes_without_a_note():
    assert note("Cabernet Sauvignon, Merlot", "Red", "vivino", "high") == "Cabernet Sauvignon and Merlot."
    assert note("Grenache, Shiraz, Mourvedre", "Red", "vivino", "high") == "Grenache, Shiraz and Mourvedre."


def test_blend_with_a_grape_of_the_wrong_colour_gets_no_line():
    assert note("Baga, Alicante Bouschet, Touriga Nacional", "White", "vivino", "medium") is None


def test_generic_blend_labels_get_no_line():
    for label in ["Red Blend", "Rose Blend", "Sparkling Rose Blend", "Rhone White Blend"]:
        assert note(label, "Red", "regional_inference", "medium") is None


def test_named_blend_uses_the_grape_note():
    assert note("Grenache Blend", "Red", "listing_keyword", "high") == f"Grenache blend. {GRAPE_NOTES['Grenache']}"


def test_missing_grape_or_style():
    assert note(None, "Red") is None
    assert note("Pinot Noir", None, "vivino", "high") is None


def test_every_note_is_short_and_has_a_known_colour():
    for grape, text in GRAPE_NOTES.items():
        assert GRAPE_COLOURS.get(grape) in {"red", "white"}, grape
        assert len(text) <= 90, grape
        assert "—" not in text, grape


def test_deals_api_returns_the_note():
    from fastapi.testclient import TestClient
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import StaticPool

    from app.database import Base, get_session
    from app.main import app
    from app.models import WineDeal

    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    with Session() as session:
        session.add(WineDeal(wine_name="2019 Nervi - Gattinara Nebbiolo - Red - 750 ml - Standard Bottle", grapes="Nebbiolo",
                             style_family="Red", wine_type="Red", price_platinum=80.0, platinum_url="https://example.com/n"))
        session.commit()

    def override():
        with Session() as session:
            yield session

    app.dependency_overrides[get_session] = override
    try:
        deals = TestClient(app).get("/deals").json()
    finally:
        app.dependency_overrides.pop(get_session, None)
    assert "Nebbiolo. Pale but high in tannin" in deals[0]["wine_note"]
