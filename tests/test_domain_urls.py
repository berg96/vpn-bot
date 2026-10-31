"""Новые ссылки подписки/бота смотрят на radarshield.net, не на mooo.com."""

from pathlib import Path

import bot
import sub_tokens

ROOT = Path(__file__).resolve().parent.parent


def test_sub_tokens_default_base_is_net():
    url = sub_tokens.sub_url(1_000_001)
    assert url.startswith("https://radarshield.net/sub/")
    assert url.endswith(f"#{sub_tokens.PROFILE_NAME}")
    assert "mooo.com" not in url


def test_bot_landing_urls_use_net():
    assert bot.LANDING_BASE_URL.rstrip("/") == "https://radarshield.net"
    assert bot._stable_sub_url(42).startswith("https://radarshield.net/sub/")
    assert "mooo.com" not in bot._pay_url(42)
    assert "radarshield.net/refund" in bot.REFUND_NOTE


def test_landing_app_defaults_to_net():
    src = (ROOT / "landing" / "app.py").read_text(encoding="utf-8")
    assert 'SITE_URL = os.environ.get("SITE_URL", "https://radarshield.net")' in src
    assert "DOMAIN-SUFFIX,{SITE_HOST},DIRECT" in src
    assert "DOMAIN-SUFFIX,{LEGACY_SITE_HOST},DIRECT" in src
    # Origin-пин ломает доступ через CF у провайдеров с TCP-блоком IP мастера.
    assert "radarshield.mooo.com: 82.38.171.220" not in src
    assert "hosts:\n  radarshield" not in src


def test_domain_move_campaign_in_runner():
    src = (ROOT / "vpn-campaign.py").read_text(encoding="utf-8")
    assert 'name="domain_move"' in src
    assert "DOMAIN_MOVE" in src
    assert "актуальные серверы" in src
    assert "Удалите" in src
    assert "_install_url" in src
    assert "Открыть в приложении" in src
    assert "синхронизация" in src
    assert 'SITE_URL", "https://radarshield.net"' in src


def test_static_seo_files_point_to_net():
    robots = (ROOT / "landing" / "static" / "robots.txt").read_text(encoding="utf-8")
    sitemap = (ROOT / "landing" / "static" / "sitemap.xml").read_text(encoding="utf-8")
    assert "https://radarshield.net/sitemap.xml" in robots
    assert "https://radarshield.net/" in sitemap
    assert "mooo.com" not in robots
    assert "mooo.com" not in sitemap
