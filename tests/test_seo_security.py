import json
import re

from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_frontend_sets_security_headers() -> None:
    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["referrer-policy"] == "strict-origin-when-cross-origin"
    csp = response.headers["content-security-policy"]
    assert "frame-ancestors 'none'" in csp
    assert "unpkg" not in csp
    assert "script-src 'self' 'unsafe-inline' https://static.cloudflareinsights.com;" in csp
    assert "connect-src 'self' https://cloudflareinsights.com;" in csp
    assert "img-src 'self' data: https://*.app.netsuite.com;" in csp
    assert "font-src 'self';" in csp
    assert "googleapis" not in csp and "gstatic" not in csp
    assert "camera=()" in response.headers["permissions-policy"]


def test_robots_txt_allows_public_site_and_blocks_ops() -> None:
    response = client.get("/robots.txt")

    assert response.status_code == 200
    body = response.text
    assert "Allow: /" in body
    assert "Disallow: /ops/" in body
    assert "LLMs: https://wine.kooexperience.com/llms.txt" in body
    assert "Sitemap: https://wine.kooexperience.com/sitemap.xml" in body


def test_sitemap_xml_lists_canonical_pages() -> None:
    response = client.get("/sitemap.xml")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/xml")
    body = response.text
    assert "<loc>https://wine.kooexperience.com/</loc>" in body
    assert "<loc>https://wine.kooexperience.com/legal</loc>" not in body
    assert "<loc>https://wine.kooexperience.com/privacy</loc>" in body


def test_frontend_has_social_and_structured_metadata() -> None:
    response = client.get("/")

    assert response.status_code == 200
    body = response.text
    assert '<link rel="canonical" href="https://wine.kooexperience.com/">' in body
    assert 'property="og:image" content="https://wine.kooexperience.com/social-card.png"' in body
    assert 'name="twitter:card" content="summary_large_image"' in body

    match = re.search(r'<script type="application/ld\+json">\s*(.*?)\s*</script>', body, re.S)
    assert match
    schema = json.loads(match.group(1))
    assert schema["applicationCategory"] == "ShoppingApplication"
    assert schema["isAccessibleForFree"] is True
    assert schema["url"] == "https://wine.kooexperience.com/"


def test_social_card_png_and_favicon_are_available() -> None:
    card = client.get("/social-card.png")
    assert card.status_code == 200
    assert card.headers["content-type"] == "image/png"
    assert card.content.startswith(b"\x89PNG")

    icon = client.get("/favicon.ico")
    assert icon.status_code == 200
    assert icon.headers["content-type"] == "image/x-icon"


def test_privacy_and_terms_pages_render_with_shared_footer() -> None:
    for path, heading in (("/privacy", "<h1>Privacy</h1>"), ("/terms", "<h1>Terms</h1>")):
        response = client.get(path)
        assert response.status_code == 200
        assert heading in response.text
        assert "privacy@kooexperience.com" in response.text
        assert "__ASSET_VERSION__" not in response.text

    home = client.get("/").text
    assert '<a href="/privacy">Privacy</a>' in home
    assert '<a href="/terms">Terms</a>' in home
    assert "kooexperience.com</a></p>" in home


def test_llms_txt_discovery_files_are_available() -> None:
    response = client.get("/llms.txt")
    full_response = client.get("/llms-full.txt")

    assert response.status_code == 200
    assert full_response.status_code == 200
    assert "MinMax Wine is a Singapore wine price comparison" in response.text
    assert "Platinum Wine Club" in full_response.text
    assert "/ops/" not in response.text
