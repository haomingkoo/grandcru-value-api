"""Data completeness tests.

These tests read the REAL seed CSVs and the real wines.db to verify data
quality before any deployment. They act as a deployment gate: if a wine
slips through with missing critical data, these tests must fail.

Design principles:
- CSV-level tests run in CI without a database (fast, catch issues early)
- DB-level tests skip if wines.db is absent (for CI without an import step)
- Named regression tests lock in specific wines that were previously broken
- Use allowlists for *known, documented* gaps; never expand them silently

Adding a wine to an allowlist requires a comment explaining WHY the gap
exists and what would be needed to close it.
"""

import csv
import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SEED = ROOT / "seed"
DB_PATH = ROOT / "data" / "wines.db"

# ── Known gaps (documented, not silent) ──────────────────────────────────────
# Each entry must have a comment explaining the gap and how to close it.
WINES_MISSING_VIVINO_URL = {
    # No Vivino SG page found for this wine yet.
    # The override row intentionally has no URL to block a wrong fuzzy match
    # to Philippe Girard Puligny-Montrachet (different producer).
    # To close: find the correct La Croix de Brully Vivino URL and add a full override.
    "2022 La Croix de Brully - Puligny-Montrachet Les Enseignères - White - 750 ml - Standard Bottle",
}

# Suppression overrides: rows in vivino_overrides.csv with no URL or rating
# that intentionally block a wrong fuzzy match. These entries get downgraded
# to "none" by the import guard added in import_wine_data.py.
# Format: match_name → reason
SUPPRESSION_OVERRIDES = {
    "2022 La Croix de Brully - Puligny-Montrachet Les Enseignères - White - 750 ml - Standard Bottle":
        "Blocks wrong fuzzy match to Philippe Girard; add correct URL when found",
    "2023 Hudelot - Baillet - Bourgogne Hautes Cotes de Nuits Rouge - Red - 750 ml - Standard Bottle":
        "Pre-emptive suppressor: not currently in comparison, prevents wrong Blanc fuzzy match",
}

# Vivino prices are fetched from the SG page, which blocks Railway datacenter
# IPs. Prices for these wines must be entered manually in vivino_overrides.csv.
# Wine names must match wine_deals.wine_name exactly (including Bundle suffixes).
# To close: manually look up the SGD price on Vivino and add to the override.
WINES_MISSING_VIVINO_PRICE = {
    "2014 Rhys - Chardonnay Bearwallow Vineyard - White - 1.5 L - Magnum",
    "2017 Paul Hobbs - Chardonnay Dinner Vineyard Cuvee Agustina Sonoma Mountain - White - 750 ml - Standard Bottle",
    "2017 Rhys - Chardonnay Bearwallow Vineyard - White - 1.5 L - Magnum",
    "2017 Rhys - Chardonnay Mt. Pajaro Vineyard - White - 1.5 L - Magnum",
    "2021 Chateau Tahbilk - Shiraz Tower Release - Red - 750 ml - Standard Bottle",
    "2021 Hudelot - Baillet - Bonnes Mares - Red - 750 ml - Standard Bottle (Bundle of 3)",
    "2021 Pierre Boisson - Auxey Duresses - White - 750 ml - Standard Bottle (Bundle of 6)",
    "2022 00 Wines - Freya Hermann Cuvee Chardonnay - White - 750 ml - Standard Bottle",
    "2022 Domaine Claude Dugat - La Gibryotte Charmes Chambertin Grand Cru - Red - 750 ml - Standard Bottle (Bundle of 3)",
    "2022 La Croix de Brully - Puligny-Montrachet Les Enseignères - White - 750 ml - Standard Bottle",
    "2023 Hudelot - Baillet - Chambolle Musigny Charmes - Red - 750 ml - Standard Bottle (Bundle of 3)",
    "NV Adrien Renoir - Grand Cru Le Terroir - White - 750 ml - Standard Bottle",
    "NV Botter - Brilla Asolo Prosecco Superiore DOCG - White - 750 ml - Standard Bottle",
    "NV Charles Heidsieck - Brut Reserve - White - 750 ml - Standard Bottle",
    "NV Coutier - Tradition Brut - White - 750 ml - Standard Bottle",
    "NV Dhondt-Grellet - Extra Brut Blanc de Blancs Premier Cru Les Terres Fines (Base 2022) - White - 750 ml - Standard Bottle",
    "NV Miraval - Fleur De Miraval Exclusivement Rose 3 - Rose - 750 ml - Standard Bottle",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _has_value(row: dict, field: str) -> bool:
    return bool((row.get(field) or "").strip())


def _db_connection():
    """Return a sqlite3 connection to wines.db, or None if absent."""
    if not DB_PATH.exists():
        return None
    return sqlite3.connect(str(DB_PATH))


# ── Seed CSV integrity (no DB required) ───────────────────────────────────────

class TestSeedCsvIntegrity:
    """Validates seed CSV structure. Runs in CI without a database."""

    def test_comparison_summary_exists_and_is_nonempty(self) -> None:
        rows = _load_csv(SEED / "comparison_summary.csv")
        assert rows, "comparison_summary.csv is missing or empty"

    def test_comparison_summary_has_required_columns(self) -> None:
        rows = _load_csv(SEED / "comparison_summary.csv")
        required = {"name_plat", "price_plat", "price_main", "url_plat"}
        missing_cols = required - set(rows[0].keys())
        assert not missing_cols, f"comparison_summary.csv missing columns: {missing_cols}"

    def test_comparison_all_wines_have_platinum_price(self) -> None:
        rows = _load_csv(SEED / "comparison_summary.csv")
        bad = [r["name_plat"] for r in rows if not _has_value(r, "price_plat")]
        assert not bad, "Wines in comparison_summary.csv missing price_plat:\n" + "\n".join(
            f"  {n}" for n in bad
        )

    def test_vivino_overrides_exists_and_is_nonempty(self) -> None:
        rows = _load_csv(SEED / "vivino_overrides.csv")
        assert rows, "vivino_overrides.csv is missing or empty"

    def test_vivino_overrides_all_have_urls(self) -> None:
        """Every non-suppression override row must have a Vivino URL.

        A no-URL override row silently shadow-matches a wine and blocks the
        platinum fallback. Suppression entries (in SUPPRESSION_OVERRIDES) are
        intentional exceptions that block wrong fuzzy matches — they get
        downgraded to "none" by the import guard in import_wine_data.py.
        """
        rows = _load_csv(SEED / "vivino_overrides.csv")
        bad = [
            r["match_name"]
            for r in rows
            if not _has_value(r, "vivino_url") and r["match_name"] not in SUPPRESSION_OVERRIDES
        ]
        assert not bad, (
            "Override rows missing Vivino URL (add URL, add to SUPPRESSION_OVERRIDES, or remove the row):\n"
            + "\n".join(f"  {n}" for n in bad)
        )

    def test_vivino_overrides_all_have_ratings(self) -> None:
        """Every non-suppression override row must have a vivino_rating.

        A no-rating override row will match a wine by name but contribute no
        useful data, while silently blocking any other matching source.
        Suppression entries are the only valid exception.
        """
        rows = _load_csv(SEED / "vivino_overrides.csv")
        bad = [
            r["match_name"]
            for r in rows
            if not _has_value(r, "vivino_rating") and r["match_name"] not in SUPPRESSION_OVERRIDES
        ]
        assert not bad, (
            "Override rows missing vivino_rating (add rating, add to SUPPRESSION_OVERRIDES, or remove the row):\n"
            + "\n".join(f"  {n}" for n in bad)
        )

    def test_vivino_overrides_urls_are_vivino_domains(self) -> None:
        rows = _load_csv(SEED / "vivino_overrides.csv")
        bad = [
            r["match_name"]
            for r in rows
            if _has_value(r, "vivino_url") and "vivino.com" not in r["vivino_url"]
        ]
        assert not bad, (
            "Override rows with non-Vivino URLs:\n" + "\n".join(f"  {n}" for n in bad)
        )

    def test_vivino_overrides_ratings_are_numeric(self) -> None:
        rows = _load_csv(SEED / "vivino_overrides.csv")
        bad = []
        for r in rows:
            val = (r.get("vivino_rating") or "").strip()
            if val:
                try:
                    float(val)
                except ValueError:
                    bad.append(f"{r['match_name']}: '{val}'")
        assert not bad, "Override rows with non-numeric ratings:\n" + "\n".join(
            f"  {n}" for n in bad
        )

    def test_vivino_overrides_no_duplicate_match_names(self) -> None:
        rows = _load_csv(SEED / "vivino_overrides.csv")
        seen: dict[str, int] = {}
        for r in rows:
            name = r.get("match_name", "").strip()
            seen[name] = seen.get(name, 0) + 1
        dupes = {name: count for name, count in seen.items() if count > 1}
        assert not dupes, (
            "Duplicate match_name entries in vivino_overrides.csv "
            "(ambiguous; only the last row wins):\n"
            + "\n".join(f"  {name} ({count}x)" for name, count in dupes.items())
        )


# ── DB completeness (skipped if no wines.db) ─────────────────────────────────

@pytest.fixture(scope="module")
def db_conn():
    conn = _db_connection()
    if conn is None:
        pytest.skip("wines.db not found — run import_wine_data.py first")
    yield conn
    conn.close()


class TestDbCompleteness:
    """Validates the imported DB. Skips if wines.db is absent."""

    def test_db_has_wines(self, db_conn) -> None:
        (count,) = db_conn.execute("SELECT COUNT(*) FROM wine_deals").fetchone()
        assert count >= 40, f"Expected at least 40 wines in DB, got {count}"

    def test_all_wines_have_platinum_price(self, db_conn) -> None:
        rows = db_conn.execute(
            "SELECT wine_name FROM wine_deals WHERE price_platinum IS NULL"
        ).fetchall()
        assert not rows, "Wines missing price_platinum:\n" + "\n".join(
            f"  {r[0]}" for r in rows
        )

    def test_all_wines_have_wine_type(self, db_conn) -> None:
        rows = db_conn.execute(
            "SELECT wine_name FROM wine_deals WHERE wine_type IS NULL OR wine_type = ''"
        ).fetchall()
        assert not rows, "Wines missing wine_type (derived from listing name — fix wine_metadata.py):\n" + "\n".join(
            f"  {r[0]}" for r in rows
        )

    def test_all_wines_have_country(self, db_conn) -> None:
        rows = db_conn.execute(
            "SELECT wine_name FROM wine_deals WHERE country IS NULL OR country = ''"
        ).fetchall()
        assert not rows, (
            "Wines missing country (add producer/region keyword to wine_metadata.py OriginRule):\n"
            + "\n".join(f"  {r[0]}" for r in rows)
        )

    def test_all_wines_have_grapes(self, db_conn) -> None:
        rows = db_conn.execute(
            "SELECT wine_name FROM wine_deals WHERE grapes IS NULL OR grapes = ''"
        ).fetchall()
        assert not rows, (
            "Wines missing grapes (add GrapeRule or style catch-all in wine_metadata.py):\n"
            + "\n".join(f"  {r[0]}" for r in rows)
        )

    def test_vivino_rating_gaps_within_allowlist(self, db_conn) -> None:
        """Wines with no Vivino rating must be in the explicit allowlist.

        Any wine NOT in WINES_MISSING_VIVINO_URL that lacks a rating is a
        regression — it means something broke in the matching pipeline.
        """
        rows = db_conn.execute(
            "SELECT wine_name FROM wine_deals WHERE vivino_rating IS NULL"
        ).fetchall()
        unexpected = [r[0] for r in rows if r[0] not in WINES_MISSING_VIVINO_URL]
        assert not unexpected, (
            "Wines missing Vivino rating that are NOT in the known-gap allowlist:\n"
            + "\n".join(f"  {n}" for n in unexpected)
            + "\n\nEither add a Vivino override or add the wine to WINES_MISSING_VIVINO_URL "
            "with a comment explaining why."
        )

    def test_vivino_url_gaps_within_allowlist(self, db_conn) -> None:
        rows = db_conn.execute(
            "SELECT wine_name FROM wine_deals WHERE vivino_url IS NULL OR vivino_url = ''"
        ).fetchall()
        unexpected = [r[0] for r in rows if r[0] not in WINES_MISSING_VIVINO_URL]
        assert not unexpected, (
            "Wines missing Vivino URL that are NOT in the known-gap allowlist:\n"
            + "\n".join(f"  {n}" for n in unexpected)
        )

    def test_vivino_price_gaps_within_allowlist(self, db_conn) -> None:
        """Vivino prices require a Vivino URL and an SGD price on the page.

        Railway IPs are blocked, so prices must be entered manually in
        vivino_overrides.csv. Wines in WINES_MISSING_VIVINO_PRICE are
        documented known gaps. New missing prices (not in the allowlist)
        indicate a regression.
        """
        rows = db_conn.execute(
            "SELECT wine_name FROM wine_deals WHERE vivino_price IS NULL"
        ).fetchall()
        unexpected = [r[0] for r in rows if r[0] not in WINES_MISSING_VIVINO_PRICE]
        assert not unexpected, (
            "Wines missing Vivino price that are NOT in the known-gap allowlist:\n"
            + "\n".join(f"  {n}" for n in unexpected)
            + "\n\nLook up the SGD price on Vivino and add to seed/vivino_overrides.csv."
        )

    def test_no_empty_match_method(self, db_conn) -> None:
        rows = db_conn.execute(
            "SELECT wine_name FROM wine_deals WHERE vivino_match_method IS NULL OR vivino_match_method = ''"
        ).fetchall()
        assert not rows, "Wines missing vivino_match_method (pipeline bug):\n" + "\n".join(
            f"  {r[0]}" for r in rows
        )

    def test_comparable_wines_have_both_prices(self, db_conn) -> None:
        """Wines with price_diff_pct set (comparable) must have both retail prices."""
        rows = db_conn.execute(
            """SELECT wine_name FROM wine_deals
               WHERE price_diff_pct IS NOT NULL
               AND (price_platinum IS NULL OR price_grand_cru IS NULL)"""
        ).fetchall()
        assert not rows, "Comparable wines missing a retail price:\n" + "\n".join(
            f"  {r[0]}" for r in rows
        )


# ── Named regression tests ────────────────────────────────────────────────────

class TestNamedRegressions:
    """Lock in specific wines that were previously broken.

    Each test documents a past bug. If the test starts failing again,
    it means that specific regression has reappeared.
    """

    def test_gd_vajra_barolo_albe_has_vivino_data(self, db_conn) -> None:
        """Vajra Barolo was stuck in resolver cache-miss trap (PR #56).
        Must have a Vivino URL and rating after the fix."""
        row = db_conn.execute(
            "SELECT vivino_url, vivino_rating, vivino_match_method FROM wine_deals "
            "WHERE wine_name LIKE '%Vajra%Barolo Albe%'"
        ).fetchone()
        assert row is not None, "GD Vajra Barolo Albe not found in DB"
        vivino_url, vivino_rating, match_method = row
        assert vivino_url, "GD Vajra Barolo Albe: vivino_url is missing"
        assert vivino_rating is not None, "GD Vajra Barolo Albe: vivino_rating is missing"
        assert match_method not in ("none", None), (
            f"GD Vajra Barolo Albe: match_method is '{match_method}' — "
            "wine is not matched (resolver trap may have returned)"
        )

    def test_dhondt_grellet_has_country_region(self, db_conn) -> None:
        """Dhondt-Grellet was missing country/region (producer not in Champagne rule)."""
        row = db_conn.execute(
            "SELECT country, region FROM wine_deals WHERE wine_name LIKE '%Dhondt%'"
        ).fetchone()
        assert row is not None, "Dhondt-Grellet not found in DB"
        country, region = row
        assert country == "France", f"Dhondt-Grellet country: expected 'France', got '{country}'"
        assert region == "Champagne", f"Dhondt-Grellet region: expected 'Champagne', got '{region}'"

    def test_adrien_renoir_has_country_region(self, db_conn) -> None:
        """Adrien Renoir was missing country/region (producer not in Champagne rule)."""
        row = db_conn.execute(
            "SELECT country, region FROM wine_deals WHERE wine_name LIKE '%Adrien Renoir%'"
        ).fetchone()
        assert row is not None, "Adrien Renoir not found in DB"
        country, region = row
        assert country == "France", f"Adrien Renoir country: expected 'France', got '{country}'"
        assert region == "Champagne", f"Adrien Renoir region: expected 'Champagne', got '{region}'"

    def test_domaine_mumm_has_country_region(self, db_conn) -> None:
        """Domaine Mumm was missing country/region (producer not in Champagne rule)."""
        row = db_conn.execute(
            "SELECT country, region FROM wine_deals WHERE wine_name LIKE '%Mumm%'"
        ).fetchone()
        assert row is not None, "Domaine Mumm not found in DB"
        country, region = row
        assert country == "France", f"Domaine Mumm country: expected 'France', got '{country}'"
        assert region == "Champagne", f"Domaine Mumm region: expected 'Champagne', got '{region}'"

    def test_roagna_solea_has_grapes(self, db_conn) -> None:
        """Roagna Solea (Nascetta) was missing grapes — no rule for this cuvée."""
        rows = db_conn.execute(
            "SELECT wine_name, grapes FROM wine_deals WHERE wine_name LIKE '%Roagna%Solea%'"
        ).fetchall()
        assert rows, "Roagna Solea not found in DB"
        for wine_name, grapes in rows:
            assert grapes, f"{wine_name}: grapes is missing"

    def test_vajra_barolo_has_nebbiolo(self, db_conn) -> None:
        """GD Vajra Barolo should be tagged as Nebbiolo."""
        row = db_conn.execute(
            "SELECT grapes FROM wine_deals WHERE wine_name LIKE '%Vajra%Barolo%'"
        ).fetchone()
        assert row is not None, "GD Vajra Barolo not found in DB"
        (grapes,) = row
        assert grapes and "Nebbiolo" in grapes, (
            f"GD Vajra Barolo grapes: expected Nebbiolo, got '{grapes}'"
        )

    def test_roagna_derthona_has_timorasso(self, db_conn) -> None:
        """Roagna Derthona should be tagged as Timorasso."""
        row = db_conn.execute(
            "SELECT grapes FROM wine_deals WHERE wine_name LIKE '%Derthona%'"
        ).fetchone()
        assert row is not None, "Roagna Derthona not found in DB"
        (grapes,) = row
        assert grapes and "Timorasso" in grapes, (
            f"Roagna Derthona grapes: expected Timorasso, got '{grapes}'"
        )

    def test_vivino_description_preserved_across_pipeline_rebuilds(self, db_conn) -> None:
        """Descriptions must survive pipeline rebuilds (regression from PR #55)."""
        # Pick a wine that definitely has a description in overrides
        row = db_conn.execute(
            "SELECT vivino_description FROM wine_deals WHERE wine_name LIKE '%Chateau Beaucastel%'"
        ).fetchone()
        assert row is not None, "Chateau Beaucastel not found in DB"
        (desc,) = row
        assert desc and len(desc) > 20, (
            f"Chateau Beaucastel: vivino_description is missing or too short: '{desc}'"
        )

    def test_miraval_magnum_not_price_divided(self, db_conn) -> None:
        """Platinum price is the anchor — never divide it. Miraval magnum was
        a past outlier where the price appeared halved."""
        row = db_conn.execute(
            "SELECT price_platinum FROM wine_deals WHERE wine_name LIKE '%Miraval%Muse%Magnum%'"
        ).fetchone()
        if row is None:
            pytest.skip("Miraval Muse Magnum not in current listing")
        (pt_price,) = row
        assert pt_price is not None and pt_price >= 300, (
            f"Miraval Muse Magnum price_platinum={pt_price}: looks divided — "
            "Platinum price must never be halved"
        )
