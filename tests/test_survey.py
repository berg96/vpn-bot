import db
import survey
import asyncio
from unittest.mock import AsyncMock

import pytest


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


def completed(tmp_path, monkeypatch, tg_id=1):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "survey.db"))
    db.init_db()
    survey.init()
    db.record_user(tg_id, None)
    db.set_mz_username(tg_id, "test_user")
    survey.start(tg_id)
    for question in range(len(survey.QUESTIONS)):
        survey.answer(tg_id, question, 0)


@pytest.mark.asyncio
async def test_award_once_and_preserve_existing_account(tmp_path, monkeypatch):
    completed(tmp_path, monkeypatch)
    monkeypatch.setattr(survey.panel.backend, "get_user_strict", AsyncMock(return_value={"expire": 1900000000, "status": "active"}))
    grant = AsyncMock(return_value={})
    monkeypatch.setattr(survey.panel.backend, "add_bonus_days", grant)
    assert await survey.grant_reward(1) == "granted"
    assert await survey.grant_reward(1) == "granted"
    grant.assert_awaited_once_with("test_user", 7)


@pytest.mark.asyncio
async def test_lookup_failure_is_retryable_but_uncertain_write_is_not(tmp_path, monkeypatch):
    completed(tmp_path, monkeypatch)
    lookup = AsyncMock(side_effect=RuntimeError("down"))
    monkeypatch.setattr(survey.panel.backend, "get_user_strict", lookup)
    assert await survey.grant_reward(1) == "pending"
    lookup.side_effect = None
    lookup.return_value = {"expire": 1900000000, "status": "active"}
    grant = AsyncMock(side_effect=TimeoutError())
    monkeypatch.setattr(survey.panel.backend, "add_bonus_days", grant)
    assert await survey.grant_reward(1) == "uncertain"
    assert await survey.grant_reward(1) == "uncertain"
    assert grant.await_count == 1


@pytest.mark.asyncio
async def test_new_and_forever_accounts(tmp_path, monkeypatch):
    completed(tmp_path, monkeypatch)
    monkeypatch.setattr(survey.panel.backend, "get_user_strict", AsyncMock(return_value=None))
    create = AsyncMock(return_value={})
    monkeypatch.setattr(survey.panel.backend, "create_trial_user", create)
    assert await survey.grant_reward(1) == "granted"
    create.assert_awaited_once_with(1, days=7, data_limit_gb=0, mz_username="test_user")
    assert db.is_trial_used(1)
    db.record_user(2, None)
    survey.start(2)
    for question in range(len(survey.QUESTIONS)):
        survey.answer(2, question, 0)
    monkeypatch.setattr(survey.panel.backend, "get_user_strict", AsyncMock(return_value={"expire": 4070908800, "status": "active"}))
    assert await survey.grant_reward(2) == "forever"
    assert create.await_count == 1


@pytest.mark.asyncio
async def test_parallel_payment_and_bonus_are_serialized(tmp_path, monkeypatch):
    completed(tmp_path, monkeypatch)
    current = {"expire": 1900000000, "status": "active"}
    async def lookup(*args):
        return dict(current)
    async def update(*args, **kwargs):
        value = current["expire"]
        await asyncio.sleep(0.05)
        current["expire"] = value + (7 if args[0] == "test_user" else 30) * 86400
        return dict(current)
    monkeypatch.setattr(survey.panel.backend, "get_user_strict", lookup)
    monkeypatch.setattr(survey.panel.backend, "add_bonus_days", update)
    monkeypatch.setattr(survey.panel.backend, "create_or_extend_user", update)
    result = await asyncio.gather(survey.grant_reward(1), survey.panel.create_or_extend_user(1, 30, mz_username="test_user"))
    assert result[0] == "granted"
    assert current["expire"] == 1900000000 + 37 * 86400


@pytest.mark.asyncio
async def test_different_users_are_not_blocked(tmp_path, monkeypatch):
    completed(tmp_path, monkeypatch)
    async with survey.panel.mutation_lock("one"):
        async def second():
            async with survey.panel.mutation_lock("two"):
                return True
        assert await asyncio.wait_for(second(), timeout=1)


@pytest.mark.asyncio
async def test_recover_interrupted_before_panel_write(tmp_path, monkeypatch):
    completed(tmp_path, monkeypatch)
    survey._set_status(1, "pending", "processing")
    monkeypatch.setattr(survey.panel.backend, "get_user_strict", AsyncMock(return_value={"expire": 1900000000, "status": "active"}))
    monkeypatch.setattr(survey.panel.backend, "add_bonus_days", AsyncMock(return_value={}))
    assert await survey.grant_reward(1) == "granted"


@pytest.mark.asyncio
async def test_exhausted_traffic_needs_manual_attention(tmp_path, monkeypatch):
    completed(tmp_path, monkeypatch)
    monkeypatch.setattr(survey.panel.backend, "get_user_strict", AsyncMock(return_value={"expire": 1900000000, "status": "limited", "data_limit": 5, "used_traffic": 5}))
    grant = AsyncMock()
    monkeypatch.setattr(survey.panel.backend, "add_bonus_days", grant)
    assert await survey.grant_reward(1) == "manual"
    grant.assert_not_awaited()


def test_username_assignment_is_stable(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "survey.db"))
    db.init_db()
    db.record_user(1, None)
    assert db.ensure_mz_username(1, "first") == "first"
    assert db.ensure_mz_username(1, "second") == "first"


@pytest.mark.asyncio
async def test_landing_reactivation_does_not_replace_new_bonus(tmp_path, monkeypatch):
    import bot

    completed(tmp_path, monkeypatch)
    monkeypatch.setattr(survey.panel.backend, "get_user_strict", AsyncMock(return_value={"expire": 4070000000, "status": "active"}))
    add = AsyncMock(return_value={"expire": 4070086400})
    extend = AsyncMock()
    monkeypatch.setattr(survey.panel.backend, "add_bonus_days", add)
    monkeypatch.setattr(survey.panel.backend, "extend_user", extend)
    await bot._reactivate_landing(1, "test_user")
    add.assert_awaited_once_with("test_user", 1)
    extend.assert_not_awaited()


@pytest.mark.asyncio
async def test_new_landing_preserves_concurrent_extension(tmp_path, monkeypatch):
    import bot

    completed(tmp_path, monkeypatch)
    monkeypatch.setattr(survey.panel.backend, "get_user_strict", AsyncMock(return_value={"expire": 1900000000, "status": "active"}))
    add = AsyncMock(return_value={"expire": 1900604800})
    extend = AsyncMock()
    monkeypatch.setattr(survey.panel.backend, "add_bonus_days", add)
    monkeypatch.setattr(survey.panel.backend, "extend_user", extend)
    await bot._activate_new_landing(1, "test_user", "2026-11-16T00:00:00+00:00")
    add.assert_awaited_once_with("test_user", 7)
    extend.assert_not_awaited()


def test_landing_identity_is_reserved_on_registration(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "survey.db"))
    db.init_db()
    with db._conn() as conn:
        conn.execute("INSERT INTO landing_leads (token, mz_username, device_id, ip_hash, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
                     ("token", "lp_test", "device", "hash", "2026", "2027"))
    assert db.claim_landing_for_new_user("token", 1, None)
    assert not db.claim_landing_for_new_user("token", 2, None)
    assert not db.user_exists(2)
    assert db.ensure_mz_username(1, "tg_1") == "lp_test"
