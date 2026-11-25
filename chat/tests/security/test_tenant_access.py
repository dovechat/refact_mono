"""Проверки доступа по тенанту (IDOR).

Каждая операция с чатом/линией должна проверять, что тенант ресурса входит
в список тенантов пользователя. Иначе пользователь одного тенанта получает
доступ к чатам другого.
"""

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from app.services.chat_service import ChatService
from app.services.line_service import LineService


@pytest.fixture
def chat_service():
    db = MagicMock()
    service = ChatService(db)
    service.conv_repo = AsyncMock()
    service.msg_repo = AsyncMock()
    service.line_repo = AsyncMock()
    service.tenant_repo = AsyncMock()
    return service


@pytest.fixture
def line_service():
    db = MagicMock()
    service = LineService(db)
    service.line_repo = AsyncMock()
    service.channel_repo = AsyncMock()
    service.permission_repo = AsyncMock()
    return service


@pytest.fixture
def own_tenant():
    return uuid.uuid4()


@pytest.fixture
def foreign_tenant():
    return uuid.uuid4()


def _conversation(line_id):
    now = datetime.now(timezone.utc)
    return SimpleNamespace(
        id=uuid.uuid4(),
        line_id=line_id,
        channel_type="vk",
        crm_chat_id="c1",
        client_phone="+1",
        meta={},
        created_at=now,
        updated_at=now,
        is_closed=False,
    )


def _message(conversation_id):
    msg = MagicMock()
    msg.id = uuid.uuid4()
    msg.conversation_id = conversation_id
    msg.channel_msg_id = "ext-1"
    return msg


# ---------- get_conversation / get_messages ----------

@pytest.mark.asyncio
async def test_get_conversation_forbidden_for_foreign_tenant(chat_service, foreign_tenant):
    line_id = uuid.uuid4()
    conv = _conversation(line_id)
    chat_service.conv_repo.get_conversation_with_messages = AsyncMock(return_value=conv)
    chat_service.line_repo.get_by_id = AsyncMock(return_value=MagicMock(tenant_id=foreign_tenant))
    chat_service.tenant_repo.get_all_tenants = AsyncMock(return_value=[])

    with pytest.raises(HTTPException) as exc:
        await chat_service.get_conversation(conv.id, uuid.uuid4(), user_id=uuid.uuid4())

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_get_conversation_allowed_for_own_tenant(chat_service, own_tenant):
    line_id = uuid.uuid4()
    conv = _conversation(line_id)
    chat_service.conv_repo.get_conversation_with_messages = AsyncMock(return_value=conv)
    chat_service.conv_repo.get_line_phone_by_conversation = AsyncMock(return_value="+79990001122")
    chat_service.msg_repo.get_conversation_messages = AsyncMock(return_value=[])
    chat_service.line_repo.get_by_id = AsyncMock(return_value=MagicMock(tenant_id=own_tenant))
    chat_service.tenant_repo.get_all_tenants = AsyncMock(
        return_value=[SimpleNamespace(id=own_tenant, name="T1")]
    )

    result = await chat_service.get_conversation(conv.id, own_tenant, user_id=uuid.uuid4())

    assert result is not None
    assert result.my_phone == "+79990001122"


@pytest.mark.asyncio
async def test_get_messages_forbidden_for_foreign_tenant(chat_service, foreign_tenant):
    line_id = uuid.uuid4()
    conv = _conversation(line_id)
    chat_service.conv_repo.get_conversation_with_messages = AsyncMock(return_value=conv)
    chat_service.line_repo.get_by_id = AsyncMock(return_value=MagicMock(tenant_id=foreign_tenant))
    chat_service.tenant_repo.get_all_tenants = AsyncMock(return_value=[])

    with pytest.raises(HTTPException) as exc:
        await chat_service.get_messages(conv.id, uuid.uuid4(), user_id=uuid.uuid4())

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_get_messages_allowed_for_own_tenant(chat_service, own_tenant):
    line_id = uuid.uuid4()
    conv = _conversation(line_id)
    chat_service.conv_repo.get_conversation_with_messages = AsyncMock(return_value=conv)
    chat_service.line_repo.get_by_id = AsyncMock(return_value=MagicMock(tenant_id=own_tenant))
    chat_service.tenant_repo.get_all_tenants = AsyncMock(
        return_value=[SimpleNamespace(id=own_tenant, name="T1")]
    )
    chat_service.msg_repo.get_conversation_messages = AsyncMock(return_value=[])

    result = await chat_service.get_messages(conv.id, own_tenant, user_id=uuid.uuid4())

    assert result == []


# ---------- create_conversation ----------

@pytest.mark.asyncio
async def test_create_conversation_forbidden_for_foreign_line(chat_service, foreign_tenant):
    data = MagicMock(line_id=uuid.uuid4(), channel_type="vk", crm_chat_id=None, client_phone="+1", meta={})
    chat_service.line_repo.get_by_id = AsyncMock(return_value=MagicMock(tenant_id=foreign_tenant))
    chat_service.tenant_repo.get_all_tenants = AsyncMock(return_value=[])

    with pytest.raises(HTTPException) as exc:
        await chat_service.create_conversation(uuid.uuid4(), data, user_id=uuid.uuid4())

    assert exc.value.status_code == 403
    chat_service.conv_repo.create_conversation.assert_not_called()


@pytest.mark.asyncio
async def test_create_conversation_allowed_for_own_line(chat_service, own_tenant):
    line_id = uuid.uuid4()
    data = MagicMock(line_id=line_id, channel_type="vk", crm_chat_id=None, client_phone="+1", meta={})
    now = datetime.now(timezone.utc)
    new_conv = SimpleNamespace(
        id=uuid.uuid4(), line_id=line_id, channel_type="vk", crm_chat_id=None,
        client_phone="+1", meta={}, created_at=now, updated_at=now, is_closed=False,
    )
    chat_service.line_repo.get_by_id = AsyncMock(return_value=MagicMock(tenant_id=own_tenant))
    chat_service.tenant_repo.get_all_tenants = AsyncMock(
        return_value=[SimpleNamespace(id=own_tenant, name="T1")]
    )
    chat_service.conv_repo.create_conversation = AsyncMock(return_value=new_conv)

    result = await chat_service.create_conversation(own_tenant, data, user_id=uuid.uuid4())

    assert result is not None
    chat_service.conv_repo.create_conversation.assert_awaited_once()


# ---------- edit / delete ----------

@pytest.mark.asyncio
async def test_edit_outgoing_message_forbidden_for_foreign_tenant(chat_service, foreign_tenant):
    line_id = uuid.uuid4()
    conv = _conversation(line_id)
    msg = _message(conv.id)
    chat_service.msg_repo.get_by_id = AsyncMock(return_value=msg)
    chat_service.conv_repo.get_by_id = AsyncMock(return_value=conv)
    chat_service.line_repo.get_by_id = AsyncMock(return_value=MagicMock(tenant_id=foreign_tenant))
    chat_service.tenant_repo.get_all_tenants = AsyncMock(return_value=[])

    with pytest.raises(HTTPException) as exc:
        await chat_service.edit_outgoing_message(
            msg.id, "new", MagicMock(id=uuid.uuid4()), "+1", line_id, "vk",
        )

    assert exc.value.status_code == 403
    chat_service.msg_repo.update.assert_not_called()


@pytest.mark.asyncio
async def test_delete_message_forbidden_for_foreign_tenant(chat_service, foreign_tenant):
    line_id = uuid.uuid4()
    conv = _conversation(line_id)
    msg = _message(conv.id)
    chat_service.msg_repo.get_by_id = AsyncMock(return_value=msg)
    chat_service.conv_repo.get_by_id = AsyncMock(return_value=conv)
    chat_service.line_repo.get_by_id = AsyncMock(return_value=MagicMock(tenant_id=foreign_tenant))
    chat_service.tenant_repo.get_all_tenants = AsyncMock(return_value=[])

    with pytest.raises(HTTPException) as exc:
        await chat_service.delete_message(
            msg.id, MagicMock(id=uuid.uuid4()), "+1", line_id, "vk",
        )

    assert exc.value.status_code == 403
    chat_service.msg_repo.delete.assert_not_called()


# ---------- line channels ----------

@pytest.mark.asyncio
async def test_get_line_channels_forbidden_for_foreign_member(line_service, foreign_tenant):
    line_id = uuid.uuid4()
    line_service.line_repo.get_by_id = AsyncMock(
        return_value=MagicMock(id=line_id, tenant_id=foreign_tenant)
    )
    line_service.permission_repo.get_user_line_ids = AsyncMock(return_value=[])

    with pytest.raises(HTTPException) as exc:
        await line_service.get_line_channels(
            line_id, user_id=uuid.uuid4(), role="member", tenant_id=foreign_tenant,
        )

    assert exc.value.status_code == 403
    line_service.channel_repo.get_multi.assert_not_called()


@pytest.mark.asyncio
async def test_get_line_channels_allowed_for_own_member(line_service, own_tenant):
    line_id = uuid.uuid4()
    channels = [MagicMock()]
    line_service.line_repo.get_by_id = AsyncMock(
        return_value=MagicMock(id=line_id, tenant_id=own_tenant)
    )
    line_service.permission_repo.get_user_line_ids = AsyncMock(return_value=[line_id])
    line_service.channel_repo.get_multi = AsyncMock(return_value=channels)

    result = await line_service.get_line_channels(
        line_id, user_id=uuid.uuid4(), role="member", tenant_id=own_tenant,
    )

    assert result == channels
