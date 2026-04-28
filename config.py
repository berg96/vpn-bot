import os
from dataclasses import dataclass

BOT_TOKEN = os.environ["BOT_TOKEN"]
BOT_USERNAME = os.environ.get("BOT_USERNAME", "radarshield_bot")
MARZBAN_URL = os.environ["MARZBAN_URL"]
MARZBAN_USER = os.environ["MARZBAN_USER"]
MARZBAN_PASS = os.environ["MARZBAN_PASS"]
ADMIN_TG_ID = int(os.environ.get("ADMIN_TG_ID", 0))

ALERT_BOT_TOKEN = os.environ.get("ALERT_BOT_TOKEN", "")
ALERT_CHAT_ID = int(os.environ.get("ALERT_CHAT_ID", 0))
ALERT_THREAD_ID = int(os.environ.get("ALERT_THREAD_ID", 0))

# BergOps queue — VPN-бот ставит сюда инфра-задачи (например «нода Alpha не
# поднялась после reconnect»), BergOps читает BLPOP и пробует чинить.
BERGOPS_REDIS_URL = os.environ.get("BERGOPS_REDIS_URL", "redis://127.0.0.1:6379/1")
BERGOPS_TASKS_KEY = os.environ.get("BERGOPS_TASKS_KEY", "bergops:tasks")

# Тарифы: (название, дней, Stars, описание_stars, копеек_rub, описание_rub)
PLANS = {
    "1m": ("1 месяц",  30,  150, "150 ⭐️",   25000,  "250 ₽"),
    "3m": ("3 месяца", 90,  380, "380 ⭐️",   59900,  "599 ₽"),
    "1y": ("1 год",    365, 1100, "1100 ⭐️", 189000, "1890 ₽"),
}

SUPPORT_LINK = "https://t.me/radarshield_support_bot"
