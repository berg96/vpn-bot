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

from .base import PanelBackend

PANEL_BACKEND = os.environ.get("PANEL_BACKEND", "marzban").lower()


def _make_backend() -> PanelBackend:
    if PANEL_BACKEND == "marzban":
        from .marzban_backend import MarzbanBackend

        return MarzbanBackend()
    if PANEL_BACKEND == "marzneshin":
        from .marzneshin_backend import MarzneshinBackend  # появится при переезде

        return MarzneshinBackend()
    raise RuntimeError(f"Unknown PANEL_BACKEND={PANEL_BACKEND!r}")


backend: PanelBackend = _make_backend()


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
        return getattr(backend, name)
    except AttributeError:
        raise AttributeError(f"module 'panel' has no attribute {name!r}") from None
