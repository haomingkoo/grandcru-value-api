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

from app.database import Base, get_session
from app.main import app
from app.models import IngestionRun, WineDeal, WineDealSnapshot


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture()
def browser_app_url(tmp_path):
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
                    # Reproduces a negative community review. The row should show the profile,
                    # not surface the negative review text by default.
                    vivino_description="Regions · Burgundy Red · cherry, raspberry, cedar, clean fruit. Give it a pass. Butter-bomb and odd oak.",
                    deal_score=61.0,
                    country="France",
                    region="Burgundy",
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
    platinum_cell, grand_cru_cell = cells[1], cells[2]
    links = first_row.find_element(By.CSS_SELECTOR, ".wine-links")

    assert "Platinum" in platinum_cell.text
    assert "$120.00" in platinum_cell.text
    assert "Grand Cru" in grand_cru_cell.text
    assert "$150.00" in grand_cru_cell.text
    assert "Platinum 20.0% less" in grand_cru_cell.text
    row_text = first_row.text
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


def test_summary_sentence_counts_offers_by_cheaper_side(browser_app_url, desktop_browser) -> None:
    desktop_browser.get(f"{browser_app_url}/?e2e={time.time_ns()}")

    WebDriverWait(desktop_browser, 10).until(
        lambda driver: "lists 3 offers" in driver.find_element(By.ID, "summary").text
    )
    summary = desktop_browser.find_element(By.ID, "summary").text
    assert summary.startswith("Platinum Wine Club lists 3 offers in stock today.")
    assert "1 is cheaper on Platinum than at Grand Cru" in summary
    assert "3 of 3 have a Vivino market price to compare." in summary
    assert "1 costs the same" in summary
    assert "1 is not sold at Grand Cru" in summary


def test_offer_notes_hide_negative_review_until_expanded(browser_app_url, desktop_browser) -> None:
    desktop_browser.get(f"{browser_app_url}/?e2e={time.time_ns()}#offers")

    first_row = WebDriverWait(desktop_browser, 10).until(
        lambda driver: driver.find_element(By.CSS_SELECTOR, ".deal-row")
    )
    wine_cell = first_row.find_elements(By.TAG_NAME, "td")[0]
    grand_cru_cell = first_row.find_elements(By.TAG_NAME, "td")[2]

    assert "cherry, raspberry, cedar, clean fruit." in wine_cell.text
    assert "Read a Vivino review" in wine_cell.text
    assert "Butter-bomb" not in wine_cell.text
    # A 7-day change far larger than the price is a data correction, not a trend.
    assert "this week" not in grand_cru_cell.text

    desktop_browser.execute_script("arguments[0].click()", wine_cell.find_element(By.TAG_NAME, "summary"))
    assert "Butter-bomb" in wine_cell.text


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
