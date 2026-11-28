"""CRM connection management (личный кабинет)."""
import uuid

from fastapi import APIRouter, Depends

from app.core.dependencies import get_crm_service
from app.schemas.crm_schemas import AmoAccountOut, AmoLineConnectRequest
from app.services.crm_service import CrmService

connections_router = APIRouter(prefix="/connections", tags=["connections"])


@connections_router.post("/amo/{tenant_id}/connect-channel")
async def amo_connect_channel(
    tenant_id: uuid.UUID,
    crm_service: CrmService = Depends(get_crm_service),
):
    return await crm_service.amo_connect_channel(tenant_id)


@connections_router.get("/amo", response_model=list[AmoAccountOut])
async def list_amo_accounts(
    crm_service: CrmService = Depends(get_crm_service),
):
    return await crm_service.list_amo_accounts()


@connections_router.post("/amo/line")
async def connect_amo_line(
    body: AmoLineConnectRequest,
    crm_service: CrmService = Depends(get_crm_service),
):
    return await crm_service.connect_amo_line(body)


@connections_router.delete("/amo/line/{line_id}")
async def disconnect_amo_line(
    line_id: uuid.UUID,
    crm_service: CrmService = Depends(get_crm_service),
):
    await crm_service.disconnect_amo_line(line_id)
    return {"status": "ok"}


@connections_router.get("/amo/lead/{lead_id}")
async def amo_lead_conversation(
    lead_id: str,
    crm_service: CrmService = Depends(get_crm_service),
):
    conversation_id, embed_token = await crm_service.get_conv_by_lead(
        lead_id
    )
    return {
        "conversation_id": str(conversation_id),
        "embed_token": embed_token,
    }