"""Фасад панели: вызывающий код работает через panel.*, не зная движка.

Бэкенд выбирается один раз по env PANEL_BACKEND (marzban|marzneshin), хранится
синглтоном `backend`. Все панельные методы (get_user, create_or_extend_user,
extend_user, get_nodes, …) доступны как panel.<method>(...) без передачи session —
модульный __getattr__ делегирует их на backend (DRY: не дублируем сигнатуры).

Пример:
    import panel
    user = await panel.get_user(tg_id, mz_name)     # было marzban.get_user(session, ...)
    name = panel.build_mz_username(tg_id, uname)

Переезд движка = дописать panel/<engine>_backend.py + PANEL_BACKEND=<engine>.
"""

import os
import re
import asyncio
import hashlib
import sqlite3
from contextlib import asynccontextmanager
from functools import wraps

import db

from .base import PanelBackend

PANEL_BACKEND = os.environ.get("PANEL_BACKEND", "marzban").lower()


def _make_backend() -> PanelBackend:
    if PANEL_BACKEND == "marzban":
        from .marzban_backend import MarzbanBackend

        return MarzbanBackend()
    if PANEL_BACKEND == "remnawave":
        from .remnawave_backend import RemnawaveBackend

        return RemnawaveBackend()
    raise RuntimeError(f"Unknown PANEL_BACKEND={PANEL_BACKEND!r}")


backend: PanelBackend = _make_backend()


@asynccontextmanager
async def mutation_lock(username: str):
    key = hashlib.sha256(username.lower().encode()).hexdigest()
    path = db.DB_PATH + ".panel-lock-" + key
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path, timeout=0, isolation_level=None)
    try:
        while True:
            try:
                conn.execute("BEGIN IMMEDIATE")
                break
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower():
                    raise
                await asyncio.sleep(0.1)
        yield
    finally:
        conn.close()


_MUTATIONS = {
    "create_or_extend_user", "create_trial_user", "create_landing_trial",
    "extend_user", "add_bonus_days", "set_status", "revoke_sub", "delete_user",
    "set_telegram_id",
}


def build_mz_username(
    tg_id: int, tg_username: str | None = None, first_name: str | None = None
) -> str:
    """Имя пользователя панели: ник > имя+id > tg_id. Панель-агностично."""
    if tg_username:
        clean = re.sub(r"[^a-zA-Z0-9_]", "", tg_username)[:28]
        if clean:
            return clean.lower()
    if first_name:
        clean = re.sub(r"[^a-zA-Z0-9]", "", first_name)[:20]
        if clean:
            return f"{clean.lower()}_{tg_id % 10000}"
    return f"tg_{tg_id}"


def __getattr__(name: str):
    """Делегируем panel.<method> на выбранный backend (панельные методы)."""
    try:
        method = getattr(backend, name)
        if name not in _MUTATIONS:
            return method

        @wraps(method)
        async def locked(*args, **kwargs):
            if name in ("create_or_extend_user", "create_trial_user"):
                tg_id = kwargs.get("tg_id", args[0] if args else None)
                index = 2 if name == "create_or_extend_user" else 3
                username = kwargs.get("mz_username") or (args[index] if len(args) > index else None) or f"tg_{tg_id}"
            else:
                username = kwargs.get("mz_username") or (args[0] if args else None)
            if not username:
                raise ValueError(f"missing username for {name}")
            async with mutation_lock(username):
                return await method(*args, **kwargs)

        return locked
    except AttributeError:
        raise AttributeError(f"module 'panel' has no attribute {name!r}") from None
