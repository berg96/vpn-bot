import os
import re
import aiohttp
import logging
from datetime import datetime, timezone

MARZBAN_URL = os.environ["MARZBAN_URL"]
MARZBAN_USER = os.environ["MARZBAN_USER"]
MARZBAN_PASS = os.environ["MARZBAN_PASS"]

INBOUNDS = {"vless": ["VLESS_TCP_REALITY", "VLESS_XHTTP_REALITY"]}

logger = logging.getLogger(__name__)

_token: str | None = None
_token_expires: float = 0


async def _get_token(session: aiohttp.ClientSession) -> str:
    global _token, _token_expires
    now = datetime.now(timezone.utc).timestamp()
    if _token and now < _token_expires - 60:
        return _token
    resp = await session.post(
        f"{MARZBAN_URL}/api/admin/token",
        data={"username": MARZBAN_USER, "password": MARZBAN_PASS},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    resp.raise_for_status()
    data = await resp.json()
    _token = data["access_token"]
    _token_expires = now + 23 * 3600
    return _token


async def _headers(session: aiohttp.ClientSession) -> dict:
    token = await _get_token(session)
    return {"Authorization": f"Bearer {token}"}


def build_mz_username(tg_id: int, tg_username: str | None = None, first_name: str | None = None) -> str:
    """Генерирует красивое имя для Marzban: ник > имя+id > tg_id."""
    if tg_username:
        clean = re.sub(r'[^a-zA-Z0-9_]', '', tg_username)[:28]
        if clean:
            return clean.lower()
    if first_name:
        clean = re.sub(r'[^a-zA-Z0-9]', '', first_name)[:20]
        if clean:
            return f"{clean.lower()}_{tg_id % 10000}"
    return f"tg_{tg_id}"


async def get_user(session: aiohttp.ClientSession, tg_id: int, mz_username: str | None = None) -> dict | None:
    username = mz_username or f"tg_{tg_id}"
    try:
        resp = await session.get(
            f"{MARZBAN_URL}/api/user/{username}",
            headers=await _headers(session),
        )
        if resp.status == 404:
            return None
        resp.raise_for_status()
        return await resp.json()
    except Exception as e:
        logger.error(f"get_user error: {e}")
        return None


async def create_or_extend_user(
    session: aiohttp.ClientSession,
    tg_id: int,
    days: int,
    mz_username: str | None = None,
) -> dict:
    username = mz_username or f"tg_{tg_id}"
    existing = await get_user(session, tg_id, mz_username)

    now_ts = int(datetime.now(timezone.utc).timestamp())
    expire_ts = now_ts + days * 86400

    if existing:
        current_expire = existing.get("expire") or 0
        if current_expire > now_ts:
            expire_ts = current_expire + days * 86400
        resp = await session.put(
            f"{MARZBAN_URL}/api/user/{username}",
            headers=await _headers(session),
            json={
                "expire": expire_ts,
                "status": "active",
                "data_limit": 0,
                "inbounds": INBOUNDS,
            },
        )
    else:
        resp = await session.post(
            f"{MARZBAN_URL}/api/user",
            headers=await _headers(session),
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
    session: aiohttp.ClientSession,
    tg_id: int,
    days: int = 10,
    data_limit_gb: float = 5.0,
    mz_username: str | None = None,
) -> dict:
    """Создаёт пробного пользователя с ограничением трафика."""
    username = mz_username or f"tg_{tg_id}"
    expire_ts = int(datetime.now(timezone.utc).timestamp()) + days * 86400
    data_limit_bytes = int(data_limit_gb * 1024 ** 3)

    resp = await session.post(
        f"{MARZBAN_URL}/api/user",
        headers=await _headers(session),
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
    session: aiohttp.ClientSession,
    mz_username: str,
    hours: int = 3,
    data_limit_mb: int = 500,
) -> dict:
    """Короткий триал для лендинга — создаём по mz_username напрямую, без tg_id."""
    expire_ts = int(datetime.now(timezone.utc).timestamp()) + hours * 3600
    data_limit_bytes = data_limit_mb * 1024 * 1024

    resp = await session.post(
        f"{MARZBAN_URL}/api/user",
        headers=await _headers(session),
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
    session: aiohttp.ClientSession,
    mz_username: str,
    total_days: int,
    data_limit_gb: float = 0,
) -> dict:
    """Продлевает existing mz_user: expire = now + total_days, меняет data_limit.

    Важно: sub_url тот же, клиент подхватит новые лимиты при ближайшем refresh.
    Используется при landing → Telegram конверсии (3ч/500MB → 7д/5GB).
    """
    expire_ts = int(datetime.now(timezone.utc).timestamp()) + total_days * 86400
    data_limit_bytes = int(data_limit_gb * 1024 ** 3) if data_limit_gb > 0 else 0

    resp = await session.put(
        f"{MARZBAN_URL}/api/user/{mz_username}",
        headers=await _headers(session),
        json={
            "expire": expire_ts,
            "status": "active",
            "data_limit": data_limit_bytes,
            "inbounds": INBOUNDS,
        },
    )
    resp.raise_for_status()
    return await resp.json()


async def add_bonus_days(
    session: aiohttp.ClientSession,
    mz_username: str,
    bonus_days: int,
) -> dict:
    """Добавляет bonus_days к текущему expire. Не трогает data_limit.

    Если подписка просрочена — считаем от текущего момента.
    Используется для +3 дня в подарок при merge landing-профиля в существующую подписку.
    """
    resp = await session.get(
        f"{MARZBAN_URL}/api/user/{mz_username}",
        headers=await _headers(session),
    )
    resp.raise_for_status()
    user = await resp.json()

    now_ts = int(datetime.now(timezone.utc).timestamp())
    current_expire = user.get("expire") or now_ts
    new_expire = max(current_expire, now_ts) + bonus_days * 86400

    resp = await session.put(
        f"{MARZBAN_URL}/api/user/{mz_username}",
        headers=await _headers(session),
        json={
            "expire": new_expire,
            "status": "active",
            "inbounds": INBOUNDS,
        },
    )
    resp.raise_for_status()
    return await resp.json()


async def set_status(session: aiohttp.ClientSession, mz_username: str, status: str) -> dict:
    """Меняет только status юзера (active/disabled/expired/limited). Не трогает expire/data_limit."""
    resp = await session.put(
        f"{MARZBAN_URL}/api/user/{mz_username}",
        headers=await _headers(session),
        json={"status": status},
    )
    resp.raise_for_status()
    return await resp.json()


async def revoke_sub(session: aiohttp.ClientSession, mz_username: str) -> dict:
    """Перегенерирует UUID юзера — старые credentials в клиентах становятся невалидны."""
    resp = await session.post(
        f"{MARZBAN_URL}/api/user/{mz_username}/revoke_sub",
        headers=await _headers(session),
    )
    resp.raise_for_status()
    return await resp.json()


async def core_restart(session: aiohttp.ClientSession) -> bool:
    """Перезапускает Xray внутри Marzban. Нужно после disabled/revoke чтобы нода
    выкинула юзеров из памяти — Marzban меняет status в БД, но не шлёт ноде remove.
    Кратковременный обрыв всех активных подключений (~1с)."""
    resp = await session.post(
        f"{MARZBAN_URL}/api/core/restart",
        headers=await _headers(session),
    )
    resp.raise_for_status()
    return True


async def delete_user(session: aiohttp.ClientSession, mz_username: str) -> bool:
    """Удаляет mz_user. True если удалён или уже отсутствовал."""
    resp = await session.delete(
        f"{MARZBAN_URL}/api/user/{mz_username}",
        headers=await _headers(session),
    )
    if resp.status == 404:
        return True
    resp.raise_for_status()
    return True


async def get_subscription_url(session: aiohttp.ClientSession, tg_id: int, mz_username: str | None = None) -> str | None:
    user = await get_user(session, tg_id, mz_username)
    if not user:
        return None
    return user.get("subscription_url")


async def list_all_users(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch all users from Marzban with pagination. Used by /stats."""
    result: list[dict] = []
    offset = 0
    limit = 200
    while True:
        try:
            resp = await session.get(
                f"{MARZBAN_URL}/api/users",
                headers=await _headers(session),
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
