"""Исходящие сообщения, начатые из карточки контакта в CRM."""
from pathlib import Path

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse

from app.core.dependencies import get_crm_service
from app.schemas.crm_schemas import StartOutgoingRequest
from app.services.crm_service import CrmService

HTML_DIR = Path(__file__).resolve().parents[2] / "html"

outgoing_router = APIRouter(prefix="/outgoing", tags=["outgoing"])


@outgoing_router.get("/lines")
async def get_lines_for_member(
    member_id: str,
    crm_service: CrmService = Depends(get_crm_service),
):
    return await crm_service.get_lines_for_member(member_id)


@outgoing_router.post("/start")
async def start_outgoing(
    body: StartOutgoingRequest,
    crm_service: CrmService = Depends(get_crm_service),
):
    return await crm_service.start_outgoing(
        member_id=body.member_id,
        contact_id=body.contact_id,
        line_id=body.line_id,
        channel_account_id=body.channel_account_id,
        text=body.text,
    )


@outgoing_router.api_route("/contact-tab", methods=["GET", "POST"])
async def contact_tab():
    return FileResponse(HTML_DIR / "contact_tab.html")