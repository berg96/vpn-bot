import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import db


@pytest.fixture
def auth_db(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "vpn.db"))
    db.init_db()
    db.record_user(123, "alice")
    db.set_mz_username(123, "alice")


def test_code_single_use_and_session_expiry(auth_db):
    code = db.issue_browser_code(123)
    session = db.redeem_browser_code(code)
    assert session and db.browser_session_user(session) == 123
    assert db.redeem_browser_code(code) is None
    with db._conn() as c:
        c.execute("UPDATE browser_auth_sessions SET expires_at = ?", ((datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),))
    assert db.browser_session_user(session) is None


def test_code_expires_and_new_code_invalidates_old(auth_db):
    first = db.issue_browser_code(123)
    second = db.issue_browser_code(123)
    assert db.redeem_browser_code(first) is None
    with db._conn() as c:
        c.execute("UPDATE browser_auth_codes SET expires_at = ?", ((datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),))
    assert db.redeem_browser_code(second) is None


def test_code_cannot_be_redeemed_twice_concurrently(auth_db):
    code = db.issue_browser_code(123)
    with ThreadPoolExecutor(max_workers=2) as executor:
        sessions = list(executor.map(db.redeem_browser_code, [code, code]))
    assert sum(session is not None for session in sessions) == 1


def test_payment_link_and_old_confirmation_cannot_read_subscription(auth_db, monkeypatch):
    monkeypatch.setenv("ROBOKASSA_MERCHANT_LOGIN", "test")
    monkeypatch.setenv("ROBOKASSA_PASSWORD_1", "test")
    monkeypatch.setenv("ROBOKASSA_PASSWORD_2", "test")
    import landing.app as landing

    db.link_browser("attacker", 123, source="pay")
    db.set_browser_link_confirmed("attacker", 123, 1)
    request = SimpleNamespace(cookies={})
    assert asyncio.run(landing.my_subscription.__wrapped__(request, browser_id="attacker")) == {"found": False}

    code = db.issue_browser_code(123)
    session = db.redeem_browser_code(code)
    monkeypatch.setattr(landing.panel, "get_user", AsyncMock(return_value={"status": "active"}))
    result = asyncio.run(landing.my_subscription.__wrapped__(SimpleNamespace(cookies={"rs_browser_session": session})))
    assert result["found"] is True
    assert "/sub/" in result["sub_url"]


def test_auth_endpoint_sets_secure_cookie_and_rejects_replay(auth_db, monkeypatch):
    monkeypatch.setenv("ROBOKASSA_MERCHANT_LOGIN", "test")
    monkeypatch.setenv("ROBOKASSA_PASSWORD_1", "test")
    monkeypatch.setenv("ROBOKASSA_PASSWORD_2", "test")
    from fastapi.testclient import TestClient
    import landing.app as landing

    monkeypatch.setattr(landing.panel, "get_user", AsyncMock(return_value={"status": "active"}))
    client = TestClient(landing.app, base_url="https://testserver")
    code = db.issue_browser_code(123)
    db.link_browser("browser1", 124, source="pay")
    assert client.post("/api/browser-auth", content='{"code":"' + code + '"}', headers={"content-type": "text/plain", "origin": "https://attacker.example"}).status_code == 415
    assert client.post("/api/browser-auth", json={"code": code}, headers={"origin": "https://attacker.example"}).status_code == 403
    response = client.post("/api/browser-auth", json={"code": code})
    assert response.json() == {"ok": True}
    assert "HttpOnly" in response.headers["set-cookie"]
    assert "Secure" in response.headers["set-cookie"]
    assert client.get("/api/my-subscription").json()["found"] is True
    import sub_tokens
    client.post("/api/link-browser", json={"browser_id": "browser1", "uid": "124", "sig": sub_tokens.make_pay_sig(124)})
    assert db.get_browser_accounts("browser1")[0]["tg_id"] == 124
    assert client.get("/api/whois?browser_id=browser1").json()["primary_tg_id"] == 123
    anonymous = TestClient(landing.app, base_url="https://testserver")
    assert anonymous.get("/api/whois?browser_id=browser1").json() == {"found": False}
    assert client.post("/api/browser-auth", json={"code": code}).json()["error"] == "invalid_code"


def test_panel_failure_does_not_claim_or_overwrite(auth_db, monkeypatch):
    import bot

    lead = {"device_id": None, "claimed_tg_id": None}
    monkeypatch.setattr(db, "get_landing_lead", lambda _: lead)
    monkeypatch.setattr(bot.panel, "get_user_strict", AsyncMock(side_effect=RuntimeError("unavailable")))
    claim = AsyncMock()
    monkeypatch.setattr(db, "claim_landing_lead", claim)
    extend = AsyncMock()
    monkeypatch.setattr(bot.panel, "extend_user", extend)
    msg = SimpleNamespace(from_user=SimpleNamespace(id=123, username="alice", first_name="Alice"), answer=AsyncMock())
    asyncio.run(bot._handle_landing_deeplink(msg, "lead"))
    claim.assert_not_called()
    extend.assert_not_awaited()
    assert "попробуй" in msg.answer.await_args.args[0].lower()
