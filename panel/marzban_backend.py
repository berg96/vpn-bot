"""MarzbanBackend — реализация PanelBackend поверх Marzban REST API.

Логика перенесена 1:1 из прежнего marzban.py; отличие — session и токен
принадлежат экземпляру (создаются лениво в работающем loop), а не передаются
аргументом. Поведение запросов не менялось.
"""

import logging
import os
from datetime import datetime, timezone

import aiohttp

from .base import PanelBackend

logger = logging.getLogger(__name__)

INBOUNDS = {"vless": ["VLESS_TCP_REALITY", "VLESS_XHTTP_REALITY"]}


class MarzbanBackend(PanelBackend):
    def __init__(self) -> None:
        self.url = os.environ["MARZBAN_URL"]
        self.user = os.environ["MARZBAN_USER"]
        self.password = os.environ["MARZBAN_PASS"]
        self._session: aiohttp.ClientSession | None = None
        self._token: str | None = None
        self._token_expires: float = 0.0

    # ── транспорт / авторизация ──────────────────────────────────────────
    async def _sess(self) -> aiohttp.ClientSession:
        # ClientSession обязан создаваться внутри работающего event loop —
        # поэтому лениво при первом обращении, не в __init__ (импорт-время).
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _get_token(self) -> str:
        now = datetime.now(timezone.utc).timestamp()
        if self._token and now < self._token_expires - 60:
            return self._token
        s = await self._sess()
        resp = await s.post(
            f"{self.url}/api/admin/token",
            data={"username": self.user, "password": self.password},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        data = await resp.json()
        self._token = data["access_token"]
        self._token_expires = now + 23 * 3600
        return self._token

    async def _headers(self) -> dict:
        return {"Authorization": f"Bearer {await self._get_token()}"}

    # ── пользователи ─────────────────────────────────────────────────────
    async def get_user(self, tg_id: int, mz_username: str | None = None) -> dict | None:
        username = mz_username or f"tg_{tg_id}"
        try:
            s = await self._sess()
            resp = await s.get(
                f"{self.url}/api/user/{username}",
                headers=await self._headers(),
            )
            if resp.status == 404:
                return None
            resp.raise_for_status()
            return await resp.json()
        except Exception as e:
            logger.error(f"get_user error: {e}")
            return None

    async def create_or_extend_user(
        self, tg_id: int, days: int, mz_username: str | None = None
    ) -> dict:
        username = mz_username or f"tg_{tg_id}"
        existing = await self.get_user(tg_id, mz_username)

        now_ts = int(datetime.now(timezone.utc).timestamp())
        expire_ts = now_ts + days * 86400

        s = await self._sess()
        if existing:
            current_expire = existing.get("expire") or 0
            if current_expire > now_ts:
                expire_ts = current_expire + days * 86400
            resp = await s.put(
                f"{self.url}/api/user/{username}",
                headers=await self._headers(),
                json={
                    "expire": expire_ts,
                    "status": "active",
                    "data_limit": 0,
                    "inbounds": INBOUNDS,
                },
            )
        else:
            resp = await s.post(
                f"{self.url}/api/user",
                headers=await self._headers(),
                json={
                    "username": username,
                    "proxies": {p: {} for p in INBOUNDS},
                    "inbounds": INBOUNDS,
                    "expire": expire_ts,
                    "data_limit": 0,
                    "data_limit_reset_strategy": "no_reset",
                    "status": "active",
                },
            )

        resp.raise_for_status()
        return await resp.json()

    async def create_trial_user(
        self,
        tg_id: int,
        days: int = 10,
        data_limit_gb: float = 5.0,
        mz_username: str | None = None,
    ) -> dict:
        """Создаёт пробного пользователя с ограничением трафика."""
        username = mz_username or f"tg_{tg_id}"
        expire_ts = int(datetime.now(timezone.utc).timestamp()) + days * 86400
        data_limit_bytes = int(data_limit_gb * 1024**3)

        s = await self._sess()
        resp = await s.post(
            f"{self.url}/api/user",
            headers=await self._headers(),
            json={
                "username": username,
                "proxies": {p: {} for p in INBOUNDS},
                "inbounds": INBOUNDS,
                "expire": expire_ts,
                "data_limit": data_limit_bytes,
                "data_limit_reset_strategy": "no_reset",
                "status": "active",
            },
        )
        resp.raise_for_status()
        return await resp.json()

    async def create_landing_trial(
        self, mz_username: str, hours: int = 3, data_limit_mb: int = 500
    ) -> dict:
        """Короткий триал для лендинга — по mz_username напрямую, без tg_id."""
        expire_ts = int(datetime.now(timezone.utc).timestamp()) + hours * 3600
        data_limit_bytes = data_limit_mb * 1024 * 1024

        s = await self._sess()
        resp = await s.post(
            f"{self.url}/api/user",
            headers=await self._headers(),
            json={
                "username": mz_username,
                "proxies": {p: {} for p in INBOUNDS},
                "inbounds": INBOUNDS,
                "expire": expire_ts,
                "data_limit": data_limit_bytes,
                "data_limit_reset_strategy": "no_reset",
                "status": "active",
            },
        )
        resp.raise_for_status()
        return await resp.json()

    async def extend_user(
        self, mz_username: str, total_days: int, data_limit_gb: float = 0
    ) -> dict:
        """Продлевает existing mz_user: expire = now + total_days, меняет data_limit.

        Важно: sub_url тот же, клиент подхватит новые лимиты при ближайшем refresh.
        Используется при landing → Telegram конверсии (3ч/500MB → 7д/5GB).
        """
        expire_ts = int(datetime.now(timezone.utc).timestamp()) + total_days * 86400
        data_limit_bytes = int(data_limit_gb * 1024**3) if data_limit_gb > 0 else 0

        s = await self._sess()
        resp = await s.put(
            f"{self.url}/api/user/{mz_username}",
            headers=await self._headers(),
            json={
                "expire": expire_ts,
                "status": "active",
                "data_limit": data_limit_bytes,
                "inbounds": INBOUNDS,
            },
        )
        resp.raise_for_status()
        return await resp.json()

    async def add_bonus_days(self, mz_username: str, bonus_days: int) -> dict:
        """Добавляет bonus_days к текущему expire. Не трогает data_limit.

        Если подписка просрочена — считаем от текущего момента.
        Используется для +3 дня в подарок при merge landing-профиля.
        """
        s = await self._sess()
        resp = await s.get(
            f"{self.url}/api/user/{mz_username}",
            headers=await self._headers(),
        )
        resp.raise_for_status()
        user = await resp.json()

        now_ts = int(datetime.now(timezone.utc).timestamp())
        current_expire = user.get("expire") or now_ts
        new_expire = max(current_expire, now_ts) + bonus_days * 86400

        resp = await s.put(
            f"{self.url}/api/user/{mz_username}",
            headers=await self._headers(),
            json={
                "expire": new_expire,
                "status": "active",
                "inbounds": INBOUNDS,
            },
        )
        resp.raise_for_status()
        return await resp.json()

    async def set_status(self, mz_username: str, status: str) -> dict:
        """Меняет только status (active/disabled/expired/limited). Не трогает expire/data_limit."""
        s = await self._sess()
        resp = await s.put(
            f"{self.url}/api/user/{mz_username}",
            headers=await self._headers(),
            json={"status": status},
        )
        resp.raise_for_status()
        return await resp.json()

    async def revoke_sub(self, mz_username: str) -> dict:
        """Перегенерирует UUID юзера — старые credentials в клиентах становятся невалидны."""
        s = await self._sess()
        resp = await s.post(
            f"{self.url}/api/user/{mz_username}/revoke_sub",
            headers=await self._headers(),
        )
        resp.raise_for_status()
        return await resp.json()

    async def delete_user(self, mz_username: str) -> bool:
        """Удаляет mz_user. True если удалён или уже отсутствовал."""
        s = await self._sess()
        resp = await s.delete(
            f"{self.url}/api/user/{mz_username}",
            headers=await self._headers(),
        )
        if resp.status == 404:
            return True
        resp.raise_for_status()
        return True

    async def get_subscription_url(
        self, tg_id: int, mz_username: str | None = None
    ) -> str | None:
        user = await self.get_user(tg_id, mz_username)
        if not user:
            return None
        return user.get("subscription_url")

    async def list_all_users(self) -> list[dict]:
        """Fetch all users from Marzban with pagination. Used by /stats."""
        result: list[dict] = []
        offset = 0
        limit = 200
        s = await self._sess()
        while True:
            try:
                resp = await s.get(
                    f"{self.url}/api/users",
                    headers=await self._headers(),
                    params={"offset": offset, "limit": limit},
                )
                resp.raise_for_status()
                data = await resp.json()
            except Exception as e:
                logger.error(f"list_all_users error: {e}")
                break
            users = data.get("users", [])
            if not users:
                break
            result.extend(users)
            if len(users) < limit:
                break
            offset += limit
        return result

    # ── доставка подписки ────────────────────────────────────────────────
    async def get_subscription_content(
        self, subscription_url: str, user_agent: str
    ) -> tuple[bytes, str, str | None]:
        # публичный URL → внутренний Marzban (иначе nginx /sub/ зациклится:
        # nginx → app → fetch public → nginx → …)
        url = subscription_url
        for prefix in ("https://", "http://"):
            if url.startswith(prefix):
                path = url[len(prefix):].split("/", 1)
                if len(path) == 2:
                    url = f"{self.url}/{path[1]}"
                break
        s = await self._sess()
        resp = await s.get(url, headers={"User-Agent": user_agent})
        content = await resp.read()
        content_type = resp.headers.get("Content-Type", "text/plain; charset=utf-8")
        userinfo = resp.headers.get("subscription-userinfo") or None
        return content, content_type, userinfo

    # ── ядро / ноды ──────────────────────────────────────────────────────
    async def core_restart(self) -> bool:
        """Перезапускает Xray внутри Marzban. Нужно после disabled/revoke чтобы нода
        выкинула юзеров из памяти. Кратковременный обрыв всех подключений (~1с)."""
        s = await self._sess()
        resp = await s.post(
            f"{self.url}/api/core/restart",
            headers=await self._headers(),
        )
        resp.raise_for_status()
        return True

    async def get_nodes(self) -> list[dict]:
        s = await self._sess()
        resp = await s.get(f"{self.url}/api/nodes", headers=await self._headers())
        resp.raise_for_status()
        return await resp.json()

    async def reconnect_node(self, node_id: int) -> None:
        s = await self._sess()
        resp = await s.post(
            f"{self.url}/api/node/{node_id}/reconnect",
            headers=await self._headers(),
        )
        resp.raise_for_status()
