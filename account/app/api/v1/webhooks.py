import hmac
import uuid
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import PlainTextResponse
from app.core.dependencies import get_webhook_service
from app.services.webhook_service import WebhookService
from core.config import settings
import logging

log = logging.getLogger("uvicorn.error")

router = APIRouter(
    tags=["Webhooks"],
    prefix="/webhooks"
)


def check_green_token(request: Request) -> None:
    """Green API sends webhookUrlToken in the Authorization header"""
    expected = settings.green_api_token
    header = request.headers.get("Authorization", "")
    if header.startswith("Bearer "):
        header = header[len("Bearer "):]
    if not expected or not hmac.compare_digest(
        header.encode(), expected.encode()
    ):
        raise HTTPException(status_code=401, detail="Invalid webhook token")


@router.post(
    "/whatsapp/green/{line_id}",
    dependencies=[Depends(check_green_token)],
)
async def whatsapp_green_webhook(
    line_id: uuid.UUID,
    request: Request,
    service: WebhookService = Depends(get_webhook_service)
):
    """Receive webhook from Green API for WhatsApp line"""
    body = await request.json()
    log.info(f"Green webhook for line {line_id}: {body.get('typeWebhook')}")
    await service.process_green_webhook(line_id, body)
    return {"status": "ok"}

@router.post("/vk/{line_id}")
async def whatsapp_vk_webhook(
    line_id: uuid.UUID,
    request: Request,
    service: WebhookService = Depends(get_webhook_service)
):
    """Receive callback from VK API for VK line"""
    body = await request.json()
    log.info(f"VK webhook for line {line_id}: {body.get('type')}")
    result = await service.process_vk_webhook(line_id, body)
    return PlainTextResponse(result)
