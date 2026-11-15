import db
import survey


def test_survey_progress_and_unique_reward_queue(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "survey.db"))
    survey.init()
    assert survey.start(123)["answers"] == []
    for question in range(len(survey.QUESTIONS)):
        state = survey.answer(123, question, 0)
        assert len(state["answers"]) == question + 1
        assert survey.answer(123, question, 0) is None
    assert state["reward_status"] == "pending"
    assert state["completed_at"]
    assert survey.start(123) == state


def test_survey_rejects_out_of_order_or_invalid_answers(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "survey.db"))
    survey.init()
    survey.start(1)
    assert survey.answer(1, 1, 0) is None
    assert survey.answer(1, 0, 99) is None
    assert survey.answer(2, 0, 0) is None
    assert survey.get(1)["answers"] == []
