import logging
import os
import sys
import types
from pathlib import Path

os.environ.setdefault("BOT_TOKEN", "123456:TEST-token")
os.environ.setdefault("PANEL_BACKEND", "remnawave")
os.environ.setdefault("REMNAWAVE_URL", "http://127.0.0.1:1")
os.environ.setdefault("REMNAWAVE_TOKEN", "test-token")
os.environ.setdefault("REMNAWAVE_SQUAD", "00000000-0000-0000-0000-000000000000")
os.environ.setdefault("SUB_TOKEN_SECRET", "test-secret")
os.environ.setdefault("BERGOPS_REDIS_URL", "")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# радар-логгер пишет в /var/log при импорте bot.py — в тестах подменяем заглушкой
_stub = types.ModuleType("radar_logging")
_stub.setup_logging = lambda *a, **k: None
_stub.UpdateIdLoggingMiddleware = type(
    "UpdateIdLoggingMiddleware", (), {"__call__": lambda self, h, e, d: h(e, d)}
)
_stub.update_id_var = None
logging.basicConfig(level=logging.INFO)
sys.modules.setdefault("radar_logging", _stub)
