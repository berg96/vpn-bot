import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

import bot
from panel.remnawave_backend import RemnawaveBackend

LIVE = "11111111-1111-1111-1111-111111111111"
DEAD = "22222222-2222-2222-2222-222222222222"
BROKEN = "33333333-3333-3333-3333-333333333333"

RAW_NODES = [
    {
        "uuid": LIVE,
        "name": "RS-Live",
        "isConnected": True,
        "isConnecting": False,
        "isDisabled": False,
    },
    {
        "uuid": DEAD,
        "name": "RS-Dead",
        "isConnected": False,
        "isConnecting": False,
        "isDisabled": True,
    },
    {
        "uuid": BROKEN,
        "name": "RS-Broken",
        "isConnected": False,
        "isConnecting": False,
        "isDisabled": False,
    },
]


@pytest.fixture
async def panel_server(monkeypatch):
    """Фейковая панель с валидацией тела restart как в Remnawave 2.8 (zod forceRestart)."""
    calls = []

    async def nodes(request):
        return web.json_response({"response": RAW_NODES})

    async def restart(request):
        body = await request.json() if request.can_read_body else None
        calls.append(
            {
                "uuid": request.match_info["uuid"],
                "body": body,
                "headers": dict(request.headers),
            }
        )
        if not isinstance(body, dict) or not isinstance(body.get("forceRestart"), bool):
            return web.json_response({"message": "Validation failed"}, status=400)
        node = next(n for n in RAW_NODES if n["uuid"] == request.match_info["uuid"])
        if node["isDisabled"]:
            return web.json_response(
                {"message": "Node is disabled", "errorCode": "A149"}, status=400
            )
        return web.json_response({"response": {"eventSent": True}})

    app = web.Application()
    app.router.add_get("/api/nodes", nodes)
    app.router.add_post("/api/nodes/{uuid}/actions/restart", restart)
    server = TestServer(app)
    await server.start_server()
    monkeypatch.setenv("REMNAWAVE_URL", str(server.make_url("")))
    backend = RemnawaveBackend()
    yield backend, calls
    if backend._session:
        await backend._session.close()
    await server.close()


async def test_get_nodes_marks_disabled(panel_server):
    backend, _ = panel_server
    statuses = {n["name"]: n["status"] for n in await backend.get_nodes()}
    assert statuses == {
        "RS-Live": "connected",
        "RS-Dead": "disabled",
        "RS-Broken": "error",
    }


async def test_reconnect_sends_force_restart_body_and_headers(panel_server):
    backend, calls = panel_server
    await backend.reconnect_node(BROKEN)
    assert calls[0]["body"] == {"forceRestart": False}
    assert calls[0]["headers"]["Authorization"] == "Bearer test-token"
    assert calls[0]["headers"]["X-Forwarded-Proto"] == "https"


async def test_health_check_skips_disabled_and_restarts_only_broken(
    panel_server, monkeypatch
):
    backend, calls = panel_server
    alerts, tasks = [], []

    async def fake_alert(session, text):
        alerts.append(text)

    monkeypatch.setattr(bot.panel, "backend", backend)
    monkeypatch.setattr(bot, "NODE_RECONNECT_WAIT", 0)
    monkeypatch.setattr(bot, "_send_alert", fake_alert)
    monkeypatch.setattr(bot, "_enqueue_bergops_task", lambda **kw: tasks.append(kw))
    monkeypatch.setattr(bot, "_node_alerted", {DEAD: True})

    await bot._check_nodes_health(session=None)

    assert [c["uuid"] for c in calls] == [BROKEN]
    assert all("RS-Dead" not in a for a in alerts)
    assert len(alerts) == 1 and "RS-Broken" in alerts[0]
    assert len(tasks) == 1 and "RS-Broken" in tasks[0]["prompt"]
    assert DEAD not in bot._node_alerted
