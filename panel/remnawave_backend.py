"""RemnawaveBackend — реализация PanelBackend поверх Remnawave REST API.

Возвращает данные в MARZBAN-совместимой форме (ключи subscription_url/expire/
status/data_limit), чтобы вызывающий код (bot.py, landing/app.py) не менялся при
переключении `PANEL_BACKEND=marzban→remnawave`.

Отличия Remnawave, которые адаптер прячет:
- идентификатор операций — uuid (не username); lookup по username есть.
- expire = ISO `expireAt` (не epoch); статусы UPPERCASE; трафик `trafficLimitBytes`.
- инбаунды/прокси → `activeInternalSquads` (юзер привязывается к squad).
- бэкенд требует заголовок `X-Forwarded-Proto: https` (ждёт реверс-прокси).
"""

import logging
import os
from datetime import datetime, timezone

import aiohttp

from .base import PanelBackend

logger = logging.getLogger(__name__)

# Marzban status (lower) ↔ Remnawave status (UPPER)
_STATUS_TO_RW = {
    "active": "ACTIVE",
    "disabled": "DISABLED",
    "limited": "LIMITED",
    "expired": "EXPIRED",
}
_STATUS_FROM_RW = {v: k for k, v in _STATUS_TO_RW.items()}

# Для «бессрочных» (Marzban expire=0) Remnawave требует дату — ставим далёкую.
_FOREVER = "2099-01-01T00:00:00.000Z"


def _epoch_to_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _iso_to_epoch(iso: str | None) -> int:
    if not iso:
        return 0
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())


class RemnawaveBackend(PanelBackend):
    def __init__(self) -> None:
        self.url = os.environ["REMNAWAVE_URL"].rstrip("/")
        self.token = os.environ["REMNAWAVE_TOKEN"]
        self.squad = os.environ[
            "REMNAWAVE_SQUAD"
        ]  # internal squad uuid для новых юзеров
        self._session: aiohttp.ClientSession | None = None

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self.token}",
                    # бэкенд Remnawave дропает запрос без признака "пришло по https через прокси"
                    "X-Forwarded-Proto": "https",
                    "X-Forwarded-For": "127.0.0.1",
                }
            )
        return self._session

    # ── нормализация Remnawave-юзера в Marzban-форму ─────────────────────
    @staticmethod
    def _shape(u: dict) -> dict:
        return {
            "username": u.get("username"),
            "status": _STATUS_FROM_RW.get(
                u.get("status"), (u.get("status") or "").lower()
            ),
            "expire": _iso_to_epoch(u.get("expireAt")),
            "data_limit": u.get("trafficLimitBytes") or 0,
            "used_traffic": u.get("usedTrafficBytes") or 0,
            "subscription_url": u.get("subscriptionUrl"),
            # служебное (Remnawave-специфика, вызывающему коду не мешает)
            "uuid": u.get("uuid"),
            "short_uuid": u.get("shortUuid"),
        }

    async def _get_raw(self, username: str) -> dict | None:
        s = await self._sess()
        resp = await s.get(f"{self.url}/api/users/by-username/{username}")
        if resp.status == 404:
            return None
        resp.raise_for_status()
        return (await resp.json())["response"]

    # ── пользователи ─────────────────────────────────────────────────────
    async def get_user(self, tg_id: int, mz_username: str | None = None) -> dict | None:
        username = mz_username or f"tg_{tg_id}"
        try:
            raw = await self._get_raw(username)
            return self._shape(raw) if raw else None
        except Exception as e:
            logger.error(f"get_user error: {e}")
            return None

    async def _create(
        self, username: str, expire_ts: int, data_limit_bytes: int
    ) -> dict:
        s = await self._sess()
        resp = await s.post(
            f"{self.url}/api/users",
            json={
                "username": username,
                "expireAt": _epoch_to_iso(expire_ts) if expire_ts else _FOREVER,
                "trafficLimitBytes": data_limit_bytes,
                "trafficLimitStrategy": "NO_RESET",
                "activeInternalSquads": [self.squad],
            },
        )
        resp.raise_for_status()
        return self._shape((await resp.json())["response"])

    async def _patch(self, uuid: str, fields: dict) -> dict:
        s = await self._sess()
        resp = await s.patch(f"{self.url}/api/users", json={"uuid": uuid, **fields})
        resp.raise_for_status()
        return self._shape((await resp.json())["response"])

    async def create_or_extend_user(
        self, tg_id: int, days: int, mz_username: str | None = None
    ) -> dict:
        username = mz_username or f"tg_{tg_id}"
        raw = await self._get_raw(username)
        now_ts = int(datetime.now(timezone.utc).timestamp())
        expire_ts = now_ts + days * 86400
        if raw:
            current_expire = _iso_to_epoch(raw.get("expireAt"))
            if current_expire > now_ts:
                expire_ts = current_expire + days * 86400
            return await self._patch(
                raw["uuid"],
                {
                    "expireAt": _epoch_to_iso(expire_ts),
                    "status": "ACTIVE",
                    "trafficLimitBytes": 0,
                },
            )
        return await self._create(username, expire_ts, 0)

    async def create_trial_user(
        self,
        tg_id: int,
        days: int = 10,
        data_limit_gb: float = 5.0,
        mz_username: str | None = None,
    ) -> dict:
        username = mz_username or f"tg_{tg_id}"
        expire_ts = int(datetime.now(timezone.utc).timestamp()) + days * 86400
        return await self._create(username, expire_ts, int(data_limit_gb * 1024**3))

    async def create_landing_trial(
        self, mz_username: str, hours: int = 3, data_limit_mb: int = 500
    ) -> dict:
        expire_ts = int(datetime.now(timezone.utc).timestamp()) + hours * 3600
        return await self._create(mz_username, expire_ts, data_limit_mb * 1024 * 1024)

    async def extend_user(
        self, mz_username: str, total_days: int, data_limit_gb: float = 0
    ) -> dict:
        raw = await self._get_raw(mz_username)
        if not raw:
            raise RuntimeError(f"extend_user: {mz_username} not found")
        expire_ts = int(datetime.now(timezone.utc).timestamp()) + total_days * 86400
        data_limit_bytes = int(data_limit_gb * 1024**3) if data_limit_gb > 0 else 0
        return await self._patch(
            raw["uuid"],
            {
                "expireAt": _epoch_to_iso(expire_ts),
                "status": "ACTIVE",
                "trafficLimitBytes": data_limit_bytes,
            },
        )

    async def add_bonus_days(self, mz_username: str, bonus_days: int) -> dict:
        raw = await self._get_raw(mz_username)
        if not raw:
            raise RuntimeError(f"add_bonus_days: {mz_username} not found")
        now_ts = int(datetime.now(timezone.utc).timestamp())
        current_expire = _iso_to_epoch(raw.get("expireAt")) or now_ts
        new_expire = max(current_expire, now_ts) + bonus_days * 86400
        return await self._patch(
            raw["uuid"], {"expireAt": _epoch_to_iso(new_expire), "status": "ACTIVE"}
        )

    async def set_status(self, mz_username: str, status: str) -> dict:
        raw = await self._get_raw(mz_username)
        if not raw:
            raise RuntimeError(f"set_status: {mz_username} not found")
        return await self._patch(
            raw["uuid"], {"status": _STATUS_TO_RW.get(status, status.upper())}
        )

    async def revoke_sub(self, mz_username: str) -> dict:
        raw = await self._get_raw(mz_username)
        if not raw:
            raise RuntimeError(f"revoke_sub: {mz_username} not found")
        s = await self._sess()
        resp = await s.post(f"{self.url}/api/users/{raw['uuid']}/actions/revoke")
        resp.raise_for_status()
        return self._shape((await resp.json())["response"])

    async def delete_user(self, mz_username: str) -> bool:
        raw = await self._get_raw(mz_username)
        if not raw:
            return True
        s = await self._sess()
        resp = await s.delete(f"{self.url}/api/users/{raw['uuid']}")
        if resp.status == 404:
            return True
        resp.raise_for_status()
        return True

    async def get_subscription_url(
        self, tg_id: int, mz_username: str | None = None
    ) -> str | None:
        user = await self.get_user(tg_id, mz_username)
        return user.get("subscription_url") if user else None

    async def list_all_users(self) -> list[dict]:
        result: list[dict] = []
        start = 0
        size = 200
        s = await self._sess()
        while True:
            try:
                resp = await s.get(
                    f"{self.url}/api/users", params={"start": start, "size": size}
                )
                resp.raise_for_status()
                data = (await resp.json())["response"]
            except Exception as e:
                logger.error(f"list_all_users error: {e}")
                break
            users = data.get("users", [])
            if not users:
                break
            result.extend(self._shape(u) for u in users)
            if len(users) < size:
                break
            start += size
        return result

    # ── ядро / ноды ──────────────────────────────────────────────────────
    async def _nodes_raw(self) -> list[dict]:
        s = await self._sess()
        resp = await s.get(f"{self.url}/api/nodes")
        resp.raise_for_status()
        r = (await resp.json())["response"]
        return r if isinstance(r, list) else r.get("nodes", r)

    async def get_nodes(self) -> list[dict]:
        """Нормализует ноды Remnawave в Marzban-форму (status connected/…, id, name)."""
        out = []
        for n in await self._nodes_raw():
            if n.get("isConnected"):
                status = "connected"
            elif n.get("isConnecting"):
                status = "connecting"
            else:
                status = "error"
            out.append(
                {
                    "id": n.get("uuid"),  # bot.py передаёт этот id в reconnect_node
                    "name": n.get("name"),
                    "status": status,
                    "xray_version": (n.get("versions") or {}).get("xrayVersion"),
                    "message": n.get("lastStatusMessage"),
                }
            )
        return out

    async def reconnect_node(self, node_id) -> None:
        s = await self._sess()
        resp = await s.post(f"{self.url}/api/nodes/{node_id}/actions/restart")
        resp.raise_for_status()

    async def core_restart(self) -> bool:
        """Перезапуск ядра во всём флоте нод Remnawave (best-effort)."""
        s = await self._sess()
        resp = await s.post(f"{self.url}/api/nodes/actions/restart-all")
        resp.raise_for_status()
        return True
