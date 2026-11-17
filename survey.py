import json
import logging
from datetime import datetime, timezone

import db
import panel

logger = logging.getLogger(__name__)
BONUS_DAYS = 7
MULTI = {1, 2}

QUESTIONS = (
    ("Пользуешься ли ты сейчас RadarShield?", ("Да, регулярно", "Иногда", "Пока не пользуюсь")),
    ("Что больше всего мешает пользоваться?", ("Подключение", "Скорость", "Надёжность", "Цена", "Ничего", "Другое")),
    ("На каком устройстве ты пользуешься или пробовал?", ("Android", "iPhone / iPad", "Windows", "Mac", "Другое")),
    ("Что улучшить в первую очередь?", ("Подключение", "Скорость", "Стабильность", "Инструкции", "Поддержку", "Другое")),
    ("Можно ли связаться с тобой, чтобы уточнить ответы?", ("Да", "Нет")),
)


def init():
    with db._conn() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS survey_responses (
            tg_id INTEGER PRIMARY KEY, answers TEXT NOT NULL DEFAULT '[]',
            completed_at TEXT, reward_status TEXT NOT NULL DEFAULT 'incomplete'
        )""")
        columns = {row[1] for row in conn.execute("PRAGMA table_info(survey_responses)")}
        if "draft" not in columns:
            conn.execute("ALTER TABLE survey_responses ADD COLUMN draft TEXT NOT NULL DEFAULT '[]'")


def get(tg_id):
    with db._conn() as conn:
        row = conn.execute(
            "SELECT answers, completed_at, reward_status, draft FROM survey_responses WHERE tg_id=?",
            (tg_id,),
        ).fetchone()
    return {"answers": json.loads(row[0]), "completed_at": row[1], "reward_status": row[2], "draft": json.loads(row[3])} if row else None


def start(tg_id):
    with db._conn() as conn:
        conn.execute("INSERT OR IGNORE INTO survey_responses (tg_id) VALUES (?)", (tg_id,))
    return get(tg_id)


def start_test(tg_id):
    with db._conn() as conn:
        conn.execute("DELETE FROM survey_responses WHERE tg_id=?", (-tg_id,))
    return start(-tg_id)


def answer(tg_id, question, choice):
    if not 0 <= question < len(QUESTIONS) or not 0 <= choice < len(QUESTIONS[question][1]):
        return None
    with db._conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT answers, completed_at FROM survey_responses WHERE tg_id=?", (tg_id,)).fetchone()
        if not row or row[1]:
            return None
        answers = json.loads(row[0])
        if len(answers) != question:
            return None
        answers.append(choice)
        done = len(answers) == len(QUESTIONS)
        conn.execute(
            "UPDATE survey_responses SET answers=?, completed_at=?, reward_status=?, draft='[]' WHERE tg_id=?",
            (json.dumps(answers), datetime.now(timezone.utc).isoformat() if done else None,
             ("test" if tg_id < 0 else "pending") if done else "incomplete", tg_id),
        )
    return get(tg_id)


def toggle(tg_id, question, choice):
    if question not in MULTI or not 0 <= choice < len(QUESTIONS[question][1]):
        return None
    with db._conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT answers, draft, completed_at FROM survey_responses WHERE tg_id=?", (tg_id,)).fetchone()
        if not row or row[2] or len(json.loads(row[0])) != question:
            return None
        draft = json.loads(row[1])
        if question == 1 and choice == 4:
            draft = [] if 4 in draft else [4]
        else:
            draft = [v for v in draft if v != 4 or question != 1]
            draft = [v for v in draft if v != choice] if choice in draft else draft + [choice]
        conn.execute("UPDATE survey_responses SET draft=? WHERE tg_id=?", (json.dumps(draft), tg_id))
    return get(tg_id)


def advance(tg_id, question):
    with db._conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT answers, draft, completed_at FROM survey_responses WHERE tg_id=?", (tg_id,)).fetchone()
        if not row or row[2] or question not in MULTI:
            return None
        answers, draft = json.loads(row[0]), json.loads(row[1])
        if len(answers) != question or not draft:
            return None
        answers.append(sorted(draft))
        conn.execute("UPDATE survey_responses SET answers=?, draft='[]' WHERE tg_id=?", (json.dumps(answers), tg_id))
    return get(tg_id)


def back(tg_id, question):
    with db._conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT answers, completed_at FROM survey_responses WHERE tg_id=?", (tg_id,)).fetchone()
        if not row or row[1]:
            return None
        answers = json.loads(row[0])
        if len(answers) != question or not answers:
            return None
        previous = answers.pop()
        draft = previous if isinstance(previous, list) else [previous]
        conn.execute("UPDATE survey_responses SET answers=?, draft=? WHERE tg_id=?", (json.dumps(answers), json.dumps(draft), tg_id))
    return get(tg_id)


def _set_status(tg_id, old, new):
    with db._conn() as conn:
        result = conn.execute(
            "UPDATE survey_responses SET reward_status=? WHERE tg_id=? AND reward_status=?",
            (new, tg_id, old),
        )
        return result.rowcount == 1


async def grant_reward(tg_id):
    if tg_id <= 0:
        return "test"
    mutation_started = False
    try:
        username = db.ensure_mz_username(tg_id, f"tg_{tg_id}")
        async with panel.mutation_lock(username):
            _set_status(tg_id, "processing", "pending")
            if not _set_status(tg_id, "pending", "processing"):
                state = get(tg_id)
                return state["reward_status"] if state else "missing"
            user = await panel.backend.get_user_strict(tg_id, username)
            if user and (not user.get("expire") or user["expire"] >= 4070908800):
                _set_status(tg_id, "processing", "forever")
                return "forever"
            if user and (user.get("status") not in ("active", "expired") or
                         (user.get("data_limit", 0) and user.get("used_traffic", 0) >= user["data_limit"])):
                _set_status(tg_id, "processing", "manual")
                return "manual"
            if not _set_status(tg_id, "processing", "uncertain"):
                return "manual"
            mutation_started = True
            if user:
                await panel.backend.add_bonus_days(username, BONUS_DAYS)
            else:
                created = await panel.backend.create_trial_user(tg_id, days=BONUS_DAYS, data_limit_gb=0, mz_username=username)
                if created.get("subscription_url"):
                    db.set_sub_url(tg_id, created["subscription_url"])
                db.mark_trial_used(tg_id)
            _set_status(tg_id, "uncertain", "granted")
            db.delete_reminder_events(tg_id)
            return "granted"
    except Exception:
        logger.exception("survey reward failed for tg_id=%s (mutation_started=%s)", tg_id, mutation_started)
        if not mutation_started:
            _set_status(tg_id, "processing", "pending")
            return "pending"
        return "uncertain"
