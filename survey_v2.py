import json
from datetime import datetime, timezone

import db

QUESTIONS = {
    0: ("Пользуешься ли ты сейчас RadarShield?", ("Да, пользуюсь", "Раньше пользовался, сейчас нет", "Пока не пробовал")),
    1: ("Что хотелось бы улучшить?", ("Скорость", "Стабильность соединения", "Простоту подключения", "Инструкции", "Поддержку", "Цену или тарифы", "Меня всё устраивает", "Другое")),
    2: ("Почему пока не попробовал RadarShield?", ("Не разобрался, как подключить", "Пока не было необходимости", "Пользуюсь другим сервисом", "Не подошла цена", "Не успел попробовать", "Другое")),
    3: ("На каких устройствах ты пробовал RadarShield?", ("Android", "iPhone или iPad", "Windows", "Mac", "Другое устройство")),
    4: ("Как ты узнал о RadarShield?", ("От друга или знакомого", "В Telegram", "Нашёл в интернете", "Другое", "Не помню")),
    5: ("Ты бы порекомендовал RadarShield другу?", ("Да", "Возможно", "Нет", "Затрудняюсь ответить")),
    6: ("Хочешь что-то добавить? Например, расскажи, чего тебе не хватает или что понравилось.", ("Написать", "Пропустить")),
    7: ("Можно ли связаться с тобой, чтобы уточнить ответы?", ("Да", "Нет")),
}
MULTI = {1, 2, 3}
OTHER = {1: 7, 2: 5}


def init():
    with db._conn() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS survey_v2 (
            tg_id INTEGER PRIMARY KEY, step INTEGER NOT NULL DEFAULT 0,
            answers TEXT NOT NULL DEFAULT '{}', draft TEXT NOT NULL DEFAULT '[]',
            completed_at TEXT, waiting TEXT, prompt_id INTEGER
        )""")


def get(tg_id):
    with db._conn() as c:
        row = c.execute("SELECT step, answers, draft, completed_at, waiting, prompt_id FROM survey_v2 WHERE tg_id=?", (tg_id,)).fetchone()
    if not row:
        return None
    return dict(step=row[0], answers=json.loads(row[1]), draft=json.loads(row[2]), completed_at=row[3], waiting=row[4], prompt_id=row[5])


def start(tg_id):
    with db._conn() as c:
        c.execute("INSERT OR IGNORE INTO survey_v2 (tg_id) VALUES (?)", (tg_id,))
    return get(tg_id)


def path(answers):
    return [0, 2, 4, 6, 7] if answers.get('0') == 2 else [0, 1, 3, 4, 5, 6, 7]


def update(tg_id, step, action, value=None):
    with db._conn() as c:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute("SELECT step, answers, draft, completed_at, waiting FROM survey_v2 WHERE tg_id=?", (tg_id,)).fetchone()
        if not row or row[3] or row[0] != step:
            return None
        answers, draft = json.loads(row[1]), json.loads(row[2])
        waiting = row[4]
        if waiting and action not in ("text", "skip", "back"):
            return None
        if action == "back":
            if waiting:
                waiting = None
            else:
                route = path(answers)
                pos = route.index(step)
                if pos == 0:
                    return None
                step = route[pos - 1]
                draft = answers.get(str(step), []) if step in MULTI else []
        elif action == "toggle" and step in MULTI and isinstance(value, int) and 0 <= value < len(QUESTIONS[step][1]):
            if step == 1 and value == 6:
                draft = [] if 6 in draft else [6]
            else:
                if step == 1:
                    draft = [v for v in draft if v != 6]
                draft = [v for v in draft if v != value] if value in draft else draft + [value]
            if step in OTHER and OTHER[step] not in draft:
                answers.pop(str(step) + '_other', None)
        elif action == "next" and step in MULTI and draft:
            answers[str(step)] = sorted(draft)
            if step in OTHER and OTHER[step] in draft and str(step) + '_other' not in answers:
                waiting = str(step) + '_other'
            else:
                step = path(answers)[path(answers).index(step) + 1]
                draft = answers.get(str(step), []) if step in MULTI else []
        elif action == "choice" and step not in MULTI and isinstance(value, int) and 0 <= value < len(QUESTIONS[step][1]):
            if step == 6 and value == 0:
                waiting = '6'
            else:
                if step == 0 and answers.get('0') != value:
                    answers = {}
                answers[str(step)] = value
                if step == 7:
                    c.execute("UPDATE survey_v2 SET answers=?, completed_at=?, waiting=NULL WHERE tg_id=?", (json.dumps(answers, ensure_ascii=False), datetime.now(timezone.utc).isoformat(), tg_id))
                    c.commit()
                    return get(tg_id)
                step = path(answers)[path(answers).index(step) + 1]
                draft = answers.get(str(step), []) if step in MULTI else []
        elif action in ("text", "skip") and waiting:
            if action == "text" and (not isinstance(value, str) or not value.strip() or len(value) > 500):
                return None
            answers[waiting] = value.strip() if action == "text" else None
            waiting = None
            step = path(answers)[path(answers).index(step) + 1]
            draft = answers.get(str(step), []) if step in MULTI else []
        else:
            return None
        c.execute("UPDATE survey_v2 SET step=?, answers=?, draft=?, waiting=?, prompt_id=NULL WHERE tg_id=?", (step, json.dumps(answers, ensure_ascii=False), json.dumps(draft), waiting, tg_id))
    return get(tg_id)


def set_prompt(tg_id, step, message_id):
    with db._conn() as c:
        c.execute("UPDATE survey_v2 SET prompt_id=? WHERE tg_id=? AND step=? AND waiting IS NOT NULL", (message_id, tg_id, step))
