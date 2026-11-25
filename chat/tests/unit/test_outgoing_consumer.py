"""Тесты консьюмера исходящих сообщений: ретраи, DLT, идемпотентность."""

import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.consumers import outgoing_consumer


@pytest.fixture
def event_data():
    return {
        "conversation_id": str(uuid.uuid4()),
        "message_id": 42,
        "crm_message_id": "crm-1",
        "phone": "+79990001122",
        "text": "hi",
        "user_id": str(uuid.uuid4()),
        "user_email": "user@test.com",
        "media": [],
    }


@pytest.fixture
def fake_session():
    @asynccontextmanager
    async def _cm():
        yield MagicMock()
    return _cm


def make_message(topic, value):
    return SimpleNamespace(topic=topic, value=value)


# ---------- process_message ----------

@pytest.mark.asyncio
async def test_process_message_skips_already_sent_message(event_data):
    chat_service = MagicMock()
    chat_service.msg_repo.get_by_id = AsyncMock(
        return_value=SimpleNamespace(id=event_data["message_id"], channel_msg_id="already-sent")
    )
    chat_service.create_msg_vk = AsyncMock()

    await outgoing_consumer.process_message(event_data, "vk", chat_service, MagicMock())

    chat_service.create_msg_vk.assert_not_called()


@pytest.mark.asyncio
async def test_process_message_sends_when_not_sent_yet(event_data):
    chat_service = MagicMock()
    chat_service.msg_repo.get_by_id = AsyncMock(
        return_value=SimpleNamespace(id=event_data["message_id"], channel_msg_id=None)
    )
    chat_service.create_msg_vk = AsyncMock()

    await outgoing_consumer.process_message(event_data, "vk", chat_service, MagicMock())

    chat_service.create_msg_vk.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_message_edit_action(event_data):
    chat_service = MagicMock()
    chat_service.edit_outgoing_message = AsyncMock()
    event_data["action"] = "edit"
    event_data["line_id"] = str(uuid.uuid4())

    await outgoing_consumer.process_message(event_data, "vk", chat_service, MagicMock())

    chat_service.edit_outgoing_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_message_delete_action(event_data):
    chat_service = MagicMock()
    chat_service.delete_message = AsyncMock()
    event_data["action"] = "delete"
    event_data["line_id"] = str(uuid.uuid4())

    await outgoing_consumer.process_message(event_data, "vk", chat_service, MagicMock())

    chat_service.delete_message.assert_awaited_once()


# ---------- ретраи ----------

@pytest.mark.asyncio
async def test_process_with_retries_succeeds_after_failures(event_data, fake_session):
    calls = []

    async def flaky(*args, **kwargs):
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("temporary")
        return None

    with patch.object(outgoing_consumer, "get_session", fake_session), \
         patch.object(outgoing_consumer, "process_message", new=AsyncMock(side_effect=flaky)), \
         patch.object(outgoing_consumer.asyncio, "sleep", new=AsyncMock()):
        ok, error = await outgoing_consumer.process_with_retries(event_data, "vk")

    assert ok is True
    assert error is None
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_process_with_retries_exhausted(event_data, fake_session):
    with patch.object(outgoing_consumer, "get_session", fake_session), \
         patch.object(outgoing_consumer, "process_message", new=AsyncMock(side_effect=RuntimeError("boom"))), \
         patch.object(outgoing_consumer.asyncio, "sleep", new=AsyncMock()):
        ok, error = await outgoing_consumer.process_with_retries(event_data, "vk")

    assert ok is False
    assert isinstance(error, RuntimeError)


# ---------- handle_message (DLT) ----------

@pytest.mark.asyncio
async def test_handle_message_commits_on_success(event_data, fake_session):
    consumer = MagicMock()
    consumer.commit = AsyncMock()
    dlt = MagicMock()
    dlt.send_and_wait = AsyncMock()
    message = make_message("vk_outgoing", event_data)

    with patch.object(outgoing_consumer, "get_session", fake_session), \
         patch.object(outgoing_consumer, "process_message", new=AsyncMock()), \
         patch.object(outgoing_consumer.asyncio, "sleep", new=AsyncMock()):
        await outgoing_consumer.handle_message(consumer, dlt, message)

    consumer.commit.assert_awaited_once()
    dlt.send_and_wait.assert_not_called()
    assert event_data["_attempt"] == 1


@pytest.mark.asyncio
async def test_handle_message_sends_to_dlt_after_retries(event_data, fake_session):
    consumer = MagicMock()
    consumer.commit = AsyncMock()
    dlt = MagicMock()
    dlt.send_and_wait = AsyncMock()
    message = make_message("vk_outgoing", event_data)

    with patch.object(outgoing_consumer, "get_session", fake_session), \
         patch.object(outgoing_consumer, "process_message", new=AsyncMock(side_effect=RuntimeError("boom"))), \
         patch.object(outgoing_consumer.asyncio, "sleep", new=AsyncMock()):
        await outgoing_consumer.handle_message(consumer, dlt, message)

    dlt.send_and_wait.assert_awaited_once()
    assert dlt.send_and_wait.await_args.args[0] == outgoing_consumer.DLT_TOPIC
    assert "_error" in event_data
    consumer.commit.assert_awaited_once()
