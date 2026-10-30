from __future__ import annotations

from datetime import UTC, datetime, timedelta
import socket
from threading import Thread
import time

import httpx
import pytest
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import uvicorn

from app import main as main_module
from app.database import Base, get_session
from app.main import app
from app.models import IngestionRun, WineDeal, WineDealSnapshot
from app.security import InMemoryRateLimiter


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture()
def extra_deals() -> list[dict]:
    """More offers for one test; parametrize this name to add them."""
    return []


@pytest.fixture()
def browser_app_url(tmp_path, extra_deals, monkeypatch):
    # Each page load makes many requests; a fresh limiter keeps tests from throttling each other.
    monkeypatch.setattr(main_module, "_rate_limiter", InMemoryRateLimiter(main_module.settings.rate_limit_requests_per_minute))
    engine = create_engine(
        f"sqlite:///{tmp_path / 'browser.db'}",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

    with Session() as session:
        run = IngestionRun(
            started_at=datetime.now(UTC),
            finished_at=datetime.now(UTC),
            status="success",
            comparison_rows=3,
            vivino_rows=3,
            merged_rows=3,
        )
        session.add(run)
        session.flush()
        session.add_all(
            [
                WineDeal(
                    wine_name="2023 Visible Price Burgundy",
                    platinum_url="https://example.com/platinum",
                    grand_cru_url="https://example.com/grand-cru",
                    vivino_url="https://example.com/vivino",
                    price_platinum=120.0,
                    price_grand_cru=150.0,
                    price_diff=-30.0,
                    price_diff_pct=-20.0,
                    cheaper_side="Platinum Cheaper",
                    platinum_in_stock=True,
                    grand_cru_in_stock=True,
                    vivino_price=180.0,
                    vivino_rating=4.2,
                    vivino_num_ratings=1234,
                    vivino_checked_at=datetime(2026, 10, 24, 3, 0, tzinfo=UTC),
                    # Legacy review text left in the column must never reach the page.
                    vivino_description="Regions · Burgundy Red · cherry, raspberry, cedar, clean fruit. Give it a pass. Butter-bomb and odd oak.",
                    deal_score=61.0,
                    country="France",
                    region="Burgundy",
                    origin_label="Burgundy, France",
                    origin_latitude=47.05,
                    origin_longitude=4.84,
                    wine_type="Red",
                    style_family="Red",
                    grapes="Pinot Noir",
                    offering_type="Single Bottle",
                    producer="Visible Estate",
                    label_name="Visible Price Burgundy",
                    volume="750ml",
                    quantity=1,
                ),
                WineDealSnapshot(
                    ingestion_run_id=run.id,
                    captured_at=datetime.now(UTC) - timedelta(days=8),
                    wine_name="2023 Visible Price Burgundy",
                    price_platinum=120.0,
                    price_grand_cru=2700.0,
                    price_diff=-2580.0,
                    price_diff_pct=-95.6,
                    cheaper_side="Platinum Cheaper",
                    platinum_in_stock=True,
                    grand_cru_in_stock=True,
                ),
                WineDeal(
                    wine_name="2022 Visible Price Burgundy",
                    platinum_url="https://example.com/platinum-2022",
                    grand_cru_url="https://example.com/grand-cru-2022",
                    vivino_url="https://example.com/vivino-2022",
                    price_platinum=118.0,
                    price_grand_cru=118.0,
                    price_diff=0.0,
                    price_diff_pct=0.0,
                    cheaper_side="Same Price",
                    platinum_in_stock=True,
                    grand_cru_in_stock=True,
                    vivino_price=130.0,
                    vivino_rating=4.1,
                    vivino_num_ratings=400,
                    vivino_description="Regions · Burgundy Red · cherry, raspberry, cedar, clean fruit.",
                    deal_score=44.0,
                    country="France",
                    region="Burgundy",
                    origin_label="Burgundy, France",
                    origin_latitude=47.05,
                    origin_longitude=4.84,
                    wine_type="Red",
                    style_family="Red",
                    grapes="Pinot Noir",
                    offering_type="Single Bottle",
                    producer="Visible Estate",
                    label_name="Visible Price Burgundy",
                    volume="750ml",
                    quantity=1,
                ),
                WineDeal(
                    wine_name="2022 Market Only White",
                    platinum_url="https://example.com/market-only",
                    price_platinum=90.0,
                    price_grand_cru=None,
                    price_diff=None,
                    price_diff_pct=None,
                    cheaper_side="No Match",
                    vivino_price=100.0,
                    vivino_rating=4.0,
                    vivino_num_ratings=250,
                    deal_score=35.0,
                    country="Italy",
                    region="Piedmont",
                    origin_label="Piedmont, Italy",
                    origin_latitude=44.7,
                    origin_longitude=8.0,
                    wine_type="White",
                    style_family="White",
                    grapes="Timorasso",
                    offering_type="Single Bottle",
                    producer="Market Estate",
                    volume="750ml",
                    quantity=1,
                ),
            ]
        )
        session.add_all([WineDeal(**row) for row in extra_deals])
        session.commit()

    def override_session():
        session = Session()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_session] = override_session
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="warning",
            access_log=False,
        )
    )
    thread = Thread(target=server.run, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{port}"

    try:
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                if httpx.get(f"{base_url}/health", timeout=1).status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.1)
        else:
            raise RuntimeError("Timed out waiting for browser test server")

        yield base_url
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        app.dependency_overrides.pop(get_session, None)
        engine.dispose()


@pytest.fixture()
def mobile_browser():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=390,844")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    try:
        yield driver
    finally:
        driver.quit()


@pytest.fixture()
def desktop_browser():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1280,900")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    try:
        yield driver
    finally:
        driver.quit()


def test_mobile_offer_card_shows_prices_before_actions(browser_app_url, mobile_browser) -> None:
    mobile_browser.get(f"{browser_app_url}/?e2e={time.time_ns()}")

    first_row = WebDriverWait(mobile_browser, 10).until(
        lambda driver: driver.find_element(By.CSS_SELECTOR, ".deal-row")
    )
    cells = first_row.find_elements(By.TAG_NAME, "td")
    platinum_cell = cells[1]
    links = first_row.find_element(By.CSS_SELECTOR, ".wine-links")

    assert "S$120.00" in platinum_cell.text
    assert "Grand Cru S$150" in platinum_cell.text
    assert "S$30 (20%) less than Grand Cru" in platinum_cell.text
    row_text = first_row.text
    assert "Pinot Noir. Light to medium body, red fruit and soft tannins." in row_text
    assert "Butter-bomb" not in row_text
    assert "stable" not in row_text
    assert "Buy on Platinum" in links.text

    metrics = mobile_browser.execute_script(
        """
        const [row, price, links] = arguments
        return {
          priceTop: price.getBoundingClientRect().top,
          linksTop: links.getBoundingClientRect().top,
          priceDisplay: getComputedStyle(price).display,
          scrollWidth: document.documentElement.scrollWidth,
          innerWidth: window.innerWidth,
        }
        """,
        first_row,
        platinum_cell,
        links,
    )
    assert metrics["priceDisplay"] != "none"
    assert metrics["priceTop"] < metrics["linksTop"]
    assert metrics["scrollWidth"] <= metrics["innerWidth"]


def test_split_bar_counts_offers_by_cheaper_side(browser_app_url, desktop_browser) -> None:
    desktop_browser.get(f"{browser_app_url}/?e2e={time.time_ns()}")

    WebDriverWait(desktop_browser, 10).until(
        lambda driver: "3 offers in stock today" in driver.find_element(By.ID, "splitBar").text
    )
    split = " ".join(desktop_browser.find_element(By.ID, "splitBar").text.split())
    assert "1 cheaper on Platinum" in split
    assert "1 same price" in split
    assert "1 not at Grand Cru" in split


def test_vivino_numbers_link_to_vivino_and_show_checked_date(browser_app_url, desktop_browser) -> None:
    desktop_browser.get(f"{browser_app_url}/?e2e={time.time_ns()}#offers")

    first_row = WebDriverWait(desktop_browser, 10).until(
        lambda driver: driver.find_element(By.CSS_SELECTOR, ".deal-row")
    )
    cells = first_row.find_elements(By.TAG_NAME, "td")
    compare_cell, rating_cell = cells[1], cells[2]

    # No review text or tasting notes from Vivino.
    assert "cherry" not in first_row.text
    assert "Butter-bomb" not in first_row.text
    assert "review" not in first_row.text.lower()
    assert "this week" not in compare_cell.text

    assert "checked 24 Oct" in compare_cell.text
    link = rating_cell.find_element(By.CSS_SELECTOR, "a.source-link")
    assert link.text.startswith("Vivino")
    assert link.get_attribute("href") == "https://example.com/vivino"

    searcher = first_row.find_element(By.CSS_SELECTOR, "a[title='Compare Singapore prices on Wine-Searcher']")
    assert searcher.text.startswith("Wine-Searcher")
    assert searcher.get_attribute("href").startswith("https://www.wine-searcher.com/find/")
    assert searcher.get_attribute("href").endswith("/1/singapore")
    assert "noopener" in searcher.get_attribute("rel")
    assert not desktop_browser.find_element(By.ID, "marketNotice").is_displayed()


def test_blocked_vivino_shows_market_prices_paused(browser_app_url, desktop_browser) -> None:
    from app.durable_state import save_state
    import app.durable_state as durable_state

    session = next(app.dependency_overrides[get_session]())
    engine = session.get_bind()
    session.close()
    original = durable_state.SessionLocal, durable_state.engine
    durable_state.SessionLocal = sessionmaker(bind=engine)
    durable_state.engine = engine
    try:
        save_state("vivino_fetch", {"blocked_since": "2026-10-25T02:00:00+00:00"})
    finally:
        durable_state.SessionLocal, durable_state.engine = original

    health = httpx.get(f"{browser_app_url}/health").json()
    assert health["vivino_status"] == "blocked since 2026-10-25T02:00:00+00:00"
    assert health["vivino_ok"] is False

    desktop_browser.get(f"{browser_app_url}/?e2e={time.time_ns()}")
    notice = WebDriverWait(desktop_browser, 10).until(
        lambda driver: driver.find_element(By.CSS_SELECTOR, "#marketNotice:not([hidden])")
    )
    assert notice.text.startswith("Market prices paused.")


def test_offer_list_groups_duplicate_wine_families(browser_app_url, desktop_browser) -> None:
    desktop_browser.get(f"{browser_app_url}/?e2e={time.time_ns()}#offers")

    WebDriverWait(desktop_browser, 10).until(
        lambda driver: driver.find_element(By.CSS_SELECTOR, ".offer-group")
    )

    metrics = desktop_browser.execute_script(
        """
        const group = document.querySelector('.offer-group')
        return {
          rowCount: document.querySelectorAll('.deal-row').length,
          groupRowCount: document.querySelectorAll('.deal-group-row').length,
          meta: document.querySelector('#resultsMeta')?.innerText || '',
          groupedCopy: document.querySelector('.group-summary-line')?.innerText || '',
          summary: group?.querySelector('summary')?.innerText || '',
          open: group?.open || false,
        }
        """
    )

    assert metrics["rowCount"] == 2
    assert metrics["groupRowCount"] == 1
    assert "2 wine groups, 3 live offers" in metrics["meta"]
    assert "2 offers for this wine" in metrics["groupedCopy"]
    assert "1 more offer for Visible Estate Visible Price Burgundy" in metrics["summary"]
    assert metrics["open"] is False

    summary = desktop_browser.find_element(By.CSS_SELECTOR, ".offer-group summary")
    desktop_browser.execute_script("arguments[0].click()", summary)
    WebDriverWait(desktop_browser, 10).until(
        lambda driver: driver.find_element(By.CSS_SELECTOR, ".offer-group").get_attribute("open")
    )
    group_text = desktop_browser.find_element(By.CSS_SELECTOR, ".offer-group").text
    assert "2022 Visible Price Burgundy" in group_text


def test_filter_with_no_results_shows_empty_state_and_clears(browser_app_url, desktop_browser) -> None:
    desktop_browser.get(f"{browser_app_url}/?search=zzzz-no-such-wine&e2e={time.time_ns()}")

    state = WebDriverWait(desktop_browser, 10).until(
        lambda driver: driver.find_element(By.CSS_SELECTOR, "#tableState:not([hidden])")
    )
    assert "No offers match these filters." in state.text
    assert desktop_browser.find_elements(By.CSS_SELECTOR, ".deal-row") == []

    desktop_browser.execute_script("arguments[0].click()", state.find_element(By.CSS_SELECTOR, "[data-action='clear']"))
    WebDriverWait(desktop_browser, 10).until(lambda driver: driver.find_elements(By.CSS_SELECTOR, ".deal-row"))
    assert desktop_browser.find_element(By.ID, "searchInput").get_attribute("value") == ""


PAGER_DEALS = [
    {
        "wine_name": f"2021 Pager Estate {index:02d} Red",
        "platinum_url": f"https://example.com/pager-{index}",
        "price_platinum": 50.0 + index,
        "cheaper_side": "No Match",
        "platinum_in_stock": True,
        "vivino_rating": 4.0,
        "vivino_num_ratings": 200,
        "deal_score": 90.0 - index,
        "country": "Spain",
        "region": "Rioja",
        "wine_type": "Red",
        "style_family": "Red",
        "offering_type": "Single Bottle",
        "producer": f"Pager Estate {index:02d}",
        "label_name": "Red",
        "volume": "750ml",
        "quantity": 1,
    }
    for index in range(1, 11)
]


def _pager_view(driver) -> dict:
    return driver.execute_script(
        """
        const pager = document.querySelector('#pager')
        return {
          titles: [...document.querySelectorAll('.deal-row .wine-title')].map((el) => el.innerText),
          hidden: pager.hidden,
          range: pager.querySelector('.pager-range')?.innerText || '',
          prevDisabled: pager.querySelector('[data-page="prev"]')?.disabled,
          nextDisabled: pager.querySelector('[data-page="next"]')?.disabled,
          navHidden: pager.querySelector('.pager-nav')?.hidden,
          perPage: pager.querySelector('#perPageSelect')?.value,
          perPageOptions: [...pager.querySelectorAll('#perPageSelect option')].map((o) => o.value),
          search: window.location.search,
        }
        """
    )


@pytest.mark.parametrize("extra_deals", [PAGER_DEALS])
def test_offer_list_pages_five_wines_and_offers_ten_or_twenty_five(browser_app_url, desktop_browser) -> None:
    desktop_browser.get(f"{browser_app_url}/?e2e={time.time_ns()}#offers")
    WebDriverWait(desktop_browser, 10).until(lambda driver: len(_pager_view(driver)["titles"]) == 5)

    view = _pager_view(desktop_browser)
    assert view["hidden"] is False
    assert view["range"] == "1–5 of 12 wines"
    assert view["titles"][0].endswith("Pager Estate 01 Red")
    assert view["prevDisabled"] is True and view["nextDisabled"] is False
    assert view["perPage"] == "5" and view["perPageOptions"] == ["5", "10", "25"]
    assert "page=" not in view["search"] and "per_page=" not in view["search"]

    desktop_browser.execute_script("document.querySelector('#pager [data-page=\"next\"]').click()")
    WebDriverWait(desktop_browser, 10).until(lambda driver: _pager_view(driver)["range"] == "6–10 of 12 wines")
    view = _pager_view(desktop_browser)
    assert view["titles"][0].endswith("Pager Estate 06 Red")
    assert view["prevDisabled"] is False
    assert "page=2" in view["search"]

    Select(desktop_browser.find_element(By.ID, "perPageSelect")).select_by_value("25")
    WebDriverWait(desktop_browser, 10).until(lambda driver: len(_pager_view(driver)["titles"]) == 12)
    view = _pager_view(desktop_browser)
    assert view["range"] == "1–12 of 12 wines"
    assert view["navHidden"] is True
    assert "per_page=25" in view["search"] and "page=" not in view["search"].replace("per_page=", "")

    desktop_browser.get(f"{browser_app_url}/?per_page=10&page=2&e2e={time.time_ns()}#offers")
    WebDriverWait(desktop_browser, 10).until(lambda driver: _pager_view(driver)["range"] == "11–12 of 12 wines")
    assert len(_pager_view(desktop_browser)["titles"]) == 2
    assert _pager_view(desktop_browser)["nextDisabled"] is True

    Select(desktop_browser.find_element(By.ID, "countrySelect")).select_by_value("Spain")
    WebDriverWait(desktop_browser, 10).until(lambda driver: _pager_view(driver)["range"] == "1–10 of 10 wines")
    assert "page=" not in _pager_view(desktop_browser)["search"].replace("per_page=", "")


def test_pager_hides_when_no_offers_match(browser_app_url, desktop_browser) -> None:
    desktop_browser.get(f"{browser_app_url}/?search=zzzz-no-such-wine&e2e={time.time_ns()}")
    WebDriverWait(desktop_browser, 10).until(
        lambda driver: driver.find_element(By.CSS_SELECTOR, "#tableState:not([hidden])")
    )
    assert desktop_browser.find_element(By.ID, "pager").get_attribute("hidden") is not None


def _block_map_tiles(driver) -> None:
    driver.execute_cdp_cmd("Network.enable", {})
    driver.execute_cdp_cmd("Network.setBlockedURLs", {"urls": ["*tile.openstreetmap.org*"]})


def _origin_view(driver) -> dict:
    return driver.execute_script(
        """
        return {
          markers: [...document.querySelectorAll('#originMap path.origin-marker')].map((el) => el.dataset.country || el.getAttribute('class')),
          countries: [...document.querySelectorAll('#originPanel [data-country]')].map((el) => el.innerText.replace(/\\s+/g, ' ').trim()),
          regions: [...document.querySelectorAll('#originPanel [data-region]')].map((el) => el.innerText.replace(/\\s+/g, ' ').trim()),
          heading: document.querySelector('#originPanel h3')?.innerText || '',
          country: document.querySelector('#countrySelect').value,
          region: document.querySelector('#regionSelect').value,
          titles: [...document.querySelectorAll('.deal-row .wine-title')].map((el) => el.innerText),
          search: window.location.search,
        }
        """
    )


def test_origin_map_filters_offers_by_country_and_region(browser_app_url, desktop_browser) -> None:
    _block_map_tiles(desktop_browser)
    desktop_browser.get(f"{browser_app_url}/?e2e={time.time_ns()}")
    WebDriverWait(desktop_browser, 10).until(lambda driver: driver.find_elements(By.CSS_SELECTOR, ".deal-row"))
    # The map loads when scrolled near, as a visitor would.
    desktop_browser.execute_script("document.getElementById('originMap').scrollIntoView()")
    WebDriverWait(desktop_browser, 10).until(lambda driver: len(_origin_view(driver)["markers"]) == 2)

    view = _origin_view(desktop_browser)
    assert view["countries"] == ["France 2", "Italy 1"]
    assert view["regions"] == []

    france = desktop_browser.find_element(By.CSS_SELECTOR, "#originMap path.origin-marker-france")
    desktop_browser.execute_script("arguments[0].dispatchEvent(new MouseEvent('click', {bubbles: true}))", france)
    WebDriverWait(desktop_browser, 10).until(lambda driver: _origin_view(driver)["heading"] == "France")
    WebDriverWait(desktop_browser, 10).until(lambda driver: len(_origin_view(driver)["titles"]) == 1)
    view = _origin_view(desktop_browser)
    assert view["country"] == "France"
    assert view["regions"] == ["Burgundy 2"]
    assert all("Burgundy" in title for title in view["titles"])

    desktop_browser.execute_script("document.querySelector('#originPanel [data-region=\"Burgundy\"]').click()")
    WebDriverWait(desktop_browser, 10).until(lambda driver: _origin_view(driver)["region"] == "Burgundy")
    assert "region=Burgundy" in _origin_view(desktop_browser)["search"]

    desktop_browser.execute_script("document.querySelector('#originPanel [data-origin-clear]').click()")
    WebDriverWait(desktop_browser, 10).until(lambda driver: len(_origin_view(driver)["titles"]) == 2)
    view = _origin_view(desktop_browser)
    assert (view["country"], view["region"], view["heading"]) == ("", "", "")

    desktop_browser.execute_script("document.querySelector('#originPanel [data-country=\"Italy\"]').click()")
    WebDriverWait(desktop_browser, 10).until(lambda driver: _origin_view(driver)["heading"] == "Italy")
    assert _origin_view(desktop_browser)["country"] == "Italy"
