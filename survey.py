import json
from datetime import datetime, timezone

import db

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


def get(tg_id):
    with db._conn() as conn:
        row = conn.execute(
            "SELECT answers, completed_at, reward_status FROM survey_responses WHERE tg_id=?",
            (tg_id,),
        ).fetchone()
    return {"answers": json.loads(row[0]), "completed_at": row[1], "reward_status": row[2]} if row else None


def start(tg_id):
    with db._conn() as conn:
        conn.execute("INSERT OR IGNORE INTO survey_responses (tg_id) VALUES (?)", (tg_id,))
    return get(tg_id)


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
            "UPDATE survey_responses SET answers=?, completed_at=?, reward_status=? WHERE tg_id=?",
            (json.dumps(answers), datetime.now(timezone.utc).isoformat() if done else None,
             "pending" if done else "incomplete", tg_id),
        )
    return get(tg_id)
