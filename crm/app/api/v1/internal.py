"""Внутренние вызовы chat -> crm."""
import hmac
import uuid
from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from core.config import settings
from core.database import get_db
from crm.app.core.uow import CrmUnitOfWork
from crm.app.services.crm_service import CrmService


def verify_internal_token(x_internal_token: str | None = Header(None)) -> None:
    expected = settings.internal_api_token
    if not expected:
        raise HTTPException(503, "INTERNAL_API_TOKEN is not configured")
    if not x_internal_token or not hmac.compare_digest(x_internal_token, expected):
        raise HTTPException(401, "Invalid internal token")


internal_router = APIRouter(
    prefix="/internal", tags=["internal"], dependencies=[Depends(verify_internal_token)]
)


def get_uow(db: AsyncSession = Depends(get_db)) -> CrmUnitOfWork:
    return CrmUnitOfWork(db)


class SendIn(BaseModel):
    line_id: uuid.UUID
    conversation_id: uuid.UUID
    client_phone: str
    client_name: str
    text: str
    message_id: str
    channel_type: str
    media: list[dict] = []


class EditIn(BaseModel):
    line_id: uuid.UUID
    conversation_id: uuid.UUID
    message_id: str
    text: str
    channel_type: str


class DeleteIn(BaseModel):
    line_id: uuid.UUID
    conversation_id: uuid.UUID
    client_phone: str
    message_id: str
    channel_type: str


async def _tenant_id(uow: CrmUnitOfWork, line_id: uuid.UUID) -> uuid.UUID:
    line = await uow.lines.get_by_id(line_id)
    if not line:
        raise HTTPException(404, "line not found")
    return line.tenant_id


@internal_router.post("/send")
async def send(body: SendIn, uow: CrmUnitOfWork = Depends(get_uow)):
    tenant_id = await _tenant_id(uow, body.line_id)
    bx_chat = await CrmService(uow).send_to_crm(
        tenant_id=tenant_id, conversation_id=body.conversation_id,
        client_phone=body.client_phone, client_name=body.client_name,
        text=body.text, message_id=body.message_id,
        channel_type=body.channel_type, media=body.media,
    )
    return {"bx_chat": bx_chat}


@internal_router.post("/edit")
async def edit(body: EditIn, uow: CrmUnitOfWork = Depends(get_uow)):
    tenant_id = await _tenant_id(uow, body.line_id)
    await CrmService(uow).edit_to_crm(
        tenant_id=tenant_id, conversation_id=body.conversation_id,
        message_id=body.message_id, text=body.text, channel_type=body.channel_type,
    )
    return {"ok": True}


@internal_router.post("/delete")
async def delete(body: DeleteIn, uow: CrmUnitOfWork = Depends(get_uow)):
    tenant_id = await _tenant_id(uow, body.line_id)
    await CrmService(uow).delete_from_crm(
        tenant_id=tenant_id, conversation_id=body.conversation_id,
        client_phone=body.client_phone, message_id=body.message_id,
        channel_type=body.channel_type,
    )
    return {"ok": True}