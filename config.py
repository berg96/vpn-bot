import os
from dataclasses import dataclass

BOT_TOKEN = os.environ["BOT_TOKEN"]
MARZBAN_URL = os.environ["MARZBAN_URL"]
MARZBAN_USER = os.environ["MARZBAN_USER"]
MARZBAN_PASS = os.environ["MARZBAN_PASS"]
ADMIN_TG_ID = int(os.environ.get("ADMIN_TG_ID", 0))

# Тарифы: (название, месяцев, Stars, описание)
PLANS = {
    "1m": ("1 месяц", 30, 150, "150 ⭐️"),
    "3m": ("3 месяца", 90, 380, "380 ⭐️"),
    "1y": ("1 год", 365, 1100, "1100 ⭐️"),
}

# Inbound-теги из Marzban
INBOUNDS = {
    "vless": ["VLESS_TCP_REALITY", "VLESS_XHTTP_REALITY"]
}

SUPPORT_LINK = "https://t.me/chigar2010"
