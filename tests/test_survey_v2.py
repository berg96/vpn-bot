import db
import survey
import survey_v2 as s
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock


def setup(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "bot.db"))
    db.init_db()
    survey.init()
    s.init()
    s.start(1)


def test_never_used_branch_and_optional_comment(tmp_path, monkeypatch):
    setup(tmp_path, monkeypatch)
    assert s.update(1, 0, "choice", 2)["step"] == 2
    s.update(1, 2, "toggle", 5)
    assert s.update(1, 2, "next")["waiting"] == "2_other"
    assert s.update(1, 2, "text", "x" * 501) is None
    assert s.update(1, 2, "text", "Не хватило времени")["step"] == 4
    assert s.update(1, 4, "choice", 0)["step"] == 6
    assert s.update(1, 6, "choice", 1)["step"] == 7
    state = s.update(1, 7, "choice", 1)
    assert state["completed_at"]
    assert state["answers"]["2_other"] == "Не хватило времени"
    assert "3" not in state["answers"] and "5" not in state["answers"]
    assert s.update(1, 7, "choice", 0) is None


def test_used_branch_back_multiple_and_exclusive(tmp_path, monkeypatch):
    setup(tmp_path, monkeypatch)
    assert s.update(1, 0, "choice", 0)["step"] == 1
    s.update(1, 1, "toggle", 0)
    assert s.update(1, 1, "toggle", 6)["draft"] == [6]
    s.update(1, 1, "toggle", 1)
    assert s.update(1, 1, "next")["step"] == 3
    assert s.update(1, 3, "back")["draft"] == [1]
    s.update(1, 1, "next")
    s.update(1, 3, "toggle", 0)
    s.update(1, 3, "toggle", 2)
    assert s.update(1, 3, "next")["step"] == 4
    assert s.update(1, 4, "choice", 1)["step"] == 5
    assert s.update(1, 5, "choice", 2)["step"] == 6
    assert s.update(1, 6, "choice", 0)["waiting"] == "6"
    assert s.update(1, 6, "text", "Отлично")["step"] == 7
    assert s.update(1, 7, "choice", 0)["completed_at"]


def test_old_reward_is_never_reopened(tmp_path, monkeypatch):
    setup(tmp_path, monkeypatch)
    survey.start(1)
    with db._conn() as c:
        c.execute("UPDATE survey_responses SET completed_at='old', reward_status='granted' WHERE tg_id=1")
    survey.prepare_v2_reward(1)
    assert survey.get(1)["reward_status"] == "granted"


def test_campaign_excludes_completed_and_opted_out(tmp_path, monkeypatch):
    setup(tmp_path, monkeypatch)
    for tg_id in (1, 2, 3, 4):
        db.record_user(tg_id, None)
    survey.start(1)
    with db._conn() as c:
        c.execute("UPDATE survey_responses SET completed_at='done' WHERE tg_id=1")
        c.execute("INSERT INTO survey_v2 (tg_id, completed_at) VALUES (2, 'done')")
        c.execute("UPDATE users SET notify_opt_out=1 WHERE tg_id=3")
    assert [u["tg_id"] for u in db.get_survey_recipients()] == [4]


def test_back_and_remove_other_deletes_text(tmp_path, monkeypatch):
    setup(tmp_path, monkeypatch)
    s.update(1, 0, "choice", 2)
    s.update(1, 2, "toggle", 5)
    s.update(1, 2, "next")
    s.update(1, 2, "text", "старый ответ")
    s.update(1, 4, "back")
    state = s.update(1, 2, "toggle", 5)
    assert "2_other" not in state["answers"]
    s.update(1, 2, "toggle", 4)
    assert s.update(1, 2, "next")["step"] == 4


@pytest.mark.asyncio
async def test_repeat_last_button_recovers_completed_survey(tmp_path, monkeypatch):
    import bot

    setup(tmp_path, monkeypatch)
    s.update(1, 0, "choice", 2)
    s.update(1, 2, "toggle", 4)
    s.update(1, 2, "next")
    s.update(1, 4, "choice", 0)
    s.update(1, 6, "choice", 1)
    s.update(1, 7, "choice", 1)
    show = AsyncMock()
    monkeypatch.setattr(bot, "_survey2_show", show)
    call = SimpleNamespace(answer=AsyncMock(), message=SimpleNamespace(chat=SimpleNamespace(type="private")),
                           from_user=SimpleNamespace(id=1), data="s2:choice:7:1")
    await bot.survey2_callback(call)
    assert show.await_args.args[2]["completed_at"]
