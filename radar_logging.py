"""Shared logging setup for Radar bots.

Усиление логгинга для разбора инцидентов:

- `TimedRotatingFileHandler` → `/var/log/radar-bots/<bot>/bot.log` с суточной
  ротацией, retention=14 дней. Параллельно пишем в stdout — journalctl /
  docker logs остаются как краткосрочный fallback.
- `update_id` в каждом LogRecord через ContextVar + Filter — после этого
  `grep "u=818081509"` даёт всю историю одного update'а, включая внешние
  вызовы и subprocess.
- aiogram middleware (опциональный) ставит update_id в ContextVar.

Скопирован в каждый бот: DRY-violation принят — 7 копий маленького модуля,
при изменениях обновить sync'ом из `/root/lib/radar_logging.py`.
"""

import logging
from contextvars import ContextVar
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

update_id_var: ContextVar[int | None] = ContextVar("update_id", default=None)

LOG_FORMAT = "%(asctime)s u=%(update_id)s %(name)s %(levelname)s %(message)s"


class UpdateIdFilter(logging.Filter):
    """Подмешивает update_id из ContextVar в каждый LogRecord."""

    def filter(self, record: logging.LogRecord) -> bool:
        v = update_id_var.get()
        record.update_id = v if v is not None else "-"
        return True


def setup_logging(
    bot_name: str,
    level: int = logging.INFO,
    log_dir: str = "/var/log/radar-bots",
) -> None:
    """Настроить root logger: file (суточная ротация) + stdout. Вызывать
    однократно в самом начале процесса, до любых других logger-операций.

    После этого library-logs (aiogram, httpx и т.д.) идут в единый формат
    и параллельно в файл.
    """
    log_path = Path(log_dir) / bot_name
    log_path.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(LOG_FORMAT)
    filt = UpdateIdFilter()

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    sh.addFilter(filt)

    # Suffix: bot.log.2026-05-21
    fh = TimedRotatingFileHandler(
        log_path / "bot.log",
        when="midnight",
        backupCount=14,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    fh.addFilter(filt)

    root = logging.getLogger()
    root.setLevel(level)
    # Сбрасываем уже добавленные basicConfig-handlers, иначе будет дублирование.
    root.handlers = []
    root.addHandler(sh)
    root.addHandler(fh)


try:
    from aiogram import BaseMiddleware
    from aiogram.types import Update

    class UpdateIdLoggingMiddleware(BaseMiddleware):
        """outer-middleware: на время обработки update'а ставит его id
        в ContextVar, чтобы все логи получили `u=<id>` через
        `UpdateIdFilter`. Регистрировать через `dp.update.outer_middleware`.
        """

        async def __call__(self, handler, event, data):
            if isinstance(event, Update):
                token = update_id_var.set(event.update_id)
                try:
                    return await handler(event, data)
                finally:
                    update_id_var.reset(token)
            return await handler(event, data)
except ImportError:
    UpdateIdLoggingMiddleware = None  # type: ignore[assignment,misc]
