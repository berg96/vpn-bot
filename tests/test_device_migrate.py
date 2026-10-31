"""Мост rs_device_id со старого origin после переезда на .net."""

import pathlib
import re

from jinja2 import Environment, FileSystemLoader

from landing import seo

ROOT = pathlib.Path(__file__).resolve().parent.parent
TPL_DIR = ROOT / "landing" / "templates"
APP_SRC = (ROOT / "landing" / "app.py").read_text(encoding="utf-8")
MIGRATE_JS = (ROOT / "landing" / "static" / "device-migrate.js").read_text(
    encoding="utf-8"
)
BRIDGE_TPL = (TPL_DIR / "legacy-device-bridge.html").read_text(encoding="utf-8")
BASE_TPL = (TPL_DIR / "_base.html").read_text(encoding="utf-8")


def test_robots_disallow_bridge():
    robots = seo.robots_txt("https://radarshield.net")
    assert "Disallow: /legacy-device-bridge" in robots


def test_bridge_route_has_head():
    assert re.search(
        r'@app\.api_route\("/legacy-device-bridge", methods=\["GET", "HEAD"\]',
        APP_SRC,
    )


def test_csp_allows_legacy_iframe_and_bridge_ancestors():
    assert "https://{LEGACY_SITE_HOST}" in APP_SRC
    assert 'path == "/legacy-device-bridge"' in APP_SRC
    assert "frame-ancestors https://{SITE_HOST}" in APP_SRC
    assert 'X-Frame-Options"] = "DENY"' in APP_SRC


def test_bridge_template_posts_device_id():
    assert "rs_device_id" in BRIDGE_TPL
    assert "postMessage" in BRIDGE_TPL
    assert "rs-legacy-device" in BRIDGE_TPL
    assert "noindex" in BRIDGE_TPL


def test_migrate_js_validates_origin_and_uuid():
    assert "rs-legacy-device" in MIGRATE_JS
    assert (
        "event.origin !== legacyOrigin" in MIGRATE_JS
        or "ev.origin !== legacyOrigin" in MIGRATE_JS
    )
    assert "rs:device-migrated" in MIGRATE_JS
    assert "rsDeviceReady" in MIGRATE_JS
    assert "/legacy-device-bridge" in MIGRATE_JS


def test_base_loads_migrate_script():
    assert "device-migrate.js" in BASE_TPL
    assert "RS_LEGACY_ORIGIN" in BASE_TPL
    html = (
        Environment(loader=FileSystemLoader(str(TPL_DIR)), autoescape=True)
        .get_template("_base.html")
        .render(
            request=type("R", (), {"url": type("U", (), {"path": "/"})()})(),
            site_url="https://radarshield.net",
            legacy_site_host="radarshield.mooo.com",
            bot_username="b",
            support_username="s",
            support_email="a@b.c",
            support_phone="",
            ip_full_name="IP",
            ip_inn="",
            ip_ogrnip="",
            ip_address="",
            updated_at="x",
            turnstile_site_key="",
        )
    )
    assert "https://radarshield.mooo.com" in html
    assert "/static/device-migrate.js" in html
