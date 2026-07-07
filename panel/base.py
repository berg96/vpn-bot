"""Абстракция VPN-панели: PanelBackend.

Прячет транспорт и движок панели. Адаптер сам владеет HTTP-session (лениво,
внутри работающего event loop) и токеном — вызывающий код session не таскает.
Вся работа идёт через фасад `panel.*` (см. panel/__init__.py), который
делегирует на выбранный по env `PANEL_BACKEND` синглтон.

Переезд движка (Marzban → Marzneshin) = дописать новый Backend-класс +
`PANEL_BACKEND=marzneshin`; вызывающий код (bot.py, landing/app.py) не трогаем.
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class PanelBackend(ABC):
    """Контракт панели. Методы не принимают session — им владеет адаптер."""

    # ── пользователи ─────────────────────────────────────────────────────
    @abstractmethod
    async def get_user(
        self, tg_id: int, mz_username: str | None = None
    ) -> dict | None: ...

    @abstractmethod
    async def create_or_extend_user(
        self, tg_id: int, days: int, mz_username: str | None = None
    ) -> dict: ...

    @abstractmethod
    async def create_trial_user(
        self,
        tg_id: int,
        days: int = 10,
        data_limit_gb: float = 5.0,
        mz_username: str | None = None,
    ) -> dict: ...

    @abstractmethod
    async def create_landing_trial(
        self, mz_username: str, hours: int = 3, data_limit_mb: int = 500
    ) -> dict: ...

    @abstractmethod
    async def extend_user(
        self, mz_username: str, total_days: int, data_limit_gb: float = 0
    ) -> dict: ...

    @abstractmethod
    async def add_bonus_days(self, mz_username: str, bonus_days: int) -> dict: ...

    @abstractmethod
    async def set_status(self, mz_username: str, status: str) -> dict: ...

    @abstractmethod
    async def revoke_sub(self, mz_username: str) -> dict: ...

    @abstractmethod
    async def delete_user(self, mz_username: str) -> bool: ...

    @abstractmethod
    async def get_subscription_url(
        self, tg_id: int, mz_username: str | None = None
    ) -> str | None: ...

    @abstractmethod
    async def list_all_users(self) -> list[dict]: ...

    # ── ядро / ноды ──────────────────────────────────────────────────────
    @abstractmethod
    async def core_restart(self) -> bool: ...

    @abstractmethod
    async def get_nodes(self) -> list[dict]: ...

    @abstractmethod
    async def reconnect_node(self, node_id: int) -> None: ...
