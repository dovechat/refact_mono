"""Тесты консьюмера входящих сообщений: ретраи и DLT."""

import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.consumers import incoming_consumer


@pytest.fixture
def event_data():
    return {
        "peer_phone": "+79990001122",
        "message": {"id": 1, "text": "hi", "date": 1700000000},
        "user_id": str(uuid.uuid4()),
    }


@pytest.fixture
def fake_session():
    @asynccontextmanager
    async def _cm():
        yield MagicMock()
    return _cm


def make_message(value):
    return SimpleNamespace(topic="telegram_user_incoming", value=value)


@pytest.mark.asyncio
async def test_process_with_retries_succeeds_after_failures(event_data, fake_session):
    calls = []

    async def flaky(*args, **kwargs):
        calls.append(1)
        if len(calls) < 2:
            raise RuntimeError("temporary")
        return None

    with patch.object(incoming_consumer, "get_session", fake_session), \
         patch.object(incoming_consumer, "process_message", new=AsyncMock(side_effect=flaky)), \
         patch.object(incoming_consumer.asyncio, "sleep", new=AsyncMock()):
        ok, error = await incoming_consumer.process_with_retries(event_data)

    assert ok is True
    assert error is None
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_process_with_retries_exhausted(event_data, fake_session):
    with patch.object(incoming_consumer, "get_session", fake_session), \
         patch.object(incoming_consumer, "process_message", new=AsyncMock(side_effect=RuntimeError("boom"))), \
         patch.object(incoming_consumer.asyncio, "sleep", new=AsyncMock()):
        ok, error = await incoming_consumer.process_with_retries(event_data)

    assert ok is False
    assert isinstance(error, RuntimeError)


@pytest.mark.asyncio
async def test_handle_message_commits_on_success(event_data, fake_session):
    consumer = MagicMock()
    consumer.commit = AsyncMock()
    dlt = MagicMock()
    dlt.send_and_wait = AsyncMock()
    message = make_message(event_data)

    with patch.object(incoming_consumer, "get_session", fake_session), \
         patch.object(incoming_consumer, "process_message", new=AsyncMock()), \
         patch.object(incoming_consumer.asyncio, "sleep", new=AsyncMock()):
        await incoming_consumer.handle_message(consumer, dlt, message)

    consumer.commit.assert_awaited_once()
    dlt.send_and_wait.assert_not_called()
    assert event_data["_attempt"] == 1


@pytest.mark.asyncio
async def test_handle_message_sends_to_dlt_after_retries(event_data, fake_session):
    consumer = MagicMock()
    consumer.commit = AsyncMock()
    dlt = MagicMock()
    dlt.send_and_wait = AsyncMock()
    message = make_message(event_data)

    with patch.object(incoming_consumer, "get_session", fake_session), \
         patch.object(incoming_consumer, "process_message", new=AsyncMock(side_effect=RuntimeError("boom"))), \
         patch.object(incoming_consumer.asyncio, "sleep", new=AsyncMock()):
        await incoming_consumer.handle_message(consumer, dlt, message)

    dlt.send_and_wait.assert_awaited_once()
    assert dlt.send_and_wait.await_args.args[0] == incoming_consumer.DLT_TOPIC
    assert "_error" in event_data
    consumer.commit.assert_awaited_once()
