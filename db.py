import sqlite3
import os
from datetime import datetime, timezone

DB_PATH = os.environ.get("DB_PATH", "/data/vpn_bot.db")


def _conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db():
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER NOT NULL,
                plan_key TEXT NOT NULL,
                stars INTEGER NOT NULL,
                telegram_charge_id TEXT,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                tg_id INTEGER PRIMARY KEY,
                username TEXT,
                mz_username TEXT,
                first_seen TEXT NOT NULL,
                trial_activated INTEGER NOT NULL DEFAULT 0
            )
        """)
        # Миграции для существующих БД
        for col in ["mz_username TEXT", "sub_url TEXT"]:
            try:
                c.execute(f"ALTER TABLE users ADD COLUMN {col}")
            except Exception:
                pass


def record_user(tg_id: int, username: str | None) -> bool:
    """Возвращает True если пользователь новый."""
    with _conn() as c:
        cur = c.execute(
            "INSERT OR IGNORE INTO users (tg_id, username, first_seen) VALUES (?, ?, ?)",
            (tg_id, username, datetime.now(timezone.utc).isoformat()),
        )
        return cur.rowcount > 0


def get_mz_username(tg_id: int) -> str | None:
    with _conn() as c:
        row = c.execute(
            "SELECT mz_username FROM users WHERE tg_id=?", (tg_id,)
        ).fetchone()
        return row[0] if row else None


def set_mz_username(tg_id: int, mz_username: str):
    with _conn() as c:
        c.execute(
            "UPDATE users SET mz_username=? WHERE tg_id=?", (mz_username, tg_id)
        )


def get_sub_url(tg_id: int) -> str | None:
    with _conn() as c:
        row = c.execute("SELECT sub_url FROM users WHERE tg_id=?", (tg_id,)).fetchone()
        return row[0] if row else None


def set_sub_url(tg_id: int, sub_url: str):
    with _conn() as c:
        c.execute("UPDATE users SET sub_url=? WHERE tg_id=?", (sub_url, tg_id))


def is_trial_used(tg_id: int) -> bool:
    with _conn() as c:
        row = c.execute(
            "SELECT trial_activated FROM users WHERE tg_id=?", (tg_id,)
        ).fetchone()
        return bool(row and row[0])


def mark_trial_used(tg_id: int):
    with _conn() as c:
        c.execute(
            "UPDATE users SET trial_activated=1 WHERE tg_id=?", (tg_id,)
        )


def user_exists(tg_id: int) -> bool:
    with _conn() as c:
        row = c.execute("SELECT 1 FROM users WHERE tg_id=?", (tg_id,)).fetchone()
        return row is not None


def delete_user(tg_id: int):
    with _conn() as c:
        c.execute("DELETE FROM users WHERE tg_id=?", (tg_id,))


def record_payment(tg_id: int, plan_key: str, stars: int, charge_id: str):
    with _conn() as c:
        c.execute(
            "INSERT INTO payments (tg_id, plan_key, stars, telegram_charge_id, created_at) VALUES (?, ?, ?, ?, ?)",
            (tg_id, plan_key, stars, charge_id, datetime.now(timezone.utc).isoformat()),
        )


def get_user_payments(tg_id: int) -> list[dict]:
    with _conn() as c:
        rows = c.execute(
            "SELECT plan_key, stars, created_at FROM payments WHERE tg_id=? ORDER BY created_at DESC",
            (tg_id,),
        ).fetchall()
    return [{"plan": r[0], "stars": r[1], "at": r[2]} for r in rows]
