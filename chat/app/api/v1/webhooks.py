
import secrets
import uuid
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from core.config import settings
from app.services.webhook_service import WebhookService
from app.core.dependencies import get_webhook_service
from app.core.green_token import green_webhook_token

import logging
log = logging.getLogger("uvicorn.error")

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


def _extract_token(request: Request) -> str:
    """Токен из заголовков X-Internal-Token / webhookUrlToken (Green API) / Authorization: Bearer."""
    for header in ("x-internal-token", "webhookurltoken"):
        value = request.headers.get(header, "").strip()
        if value:
            return value
    auth_header = request.headers.get("authorization", "")
    return auth_header.removeprefix("Bearer ").strip()


@router.post("/whatsapp/green/{line_id}")
async def whatsapp_green_webhook(
    request: Request,
    line_id: uuid.UUID,
    service: WebhookService = Depends(get_webhook_service),
):
    payload = await request.json()

    token = _extract_token(request)
    instance_id = str(
        (payload.get("instanceData") or {}).get("idInstance") or ""
    )
    expected = green_webhook_token(instance_id)
    if not token or not expected or not secrets.compare_digest(
        token, expected
    ):
        log.error(f"Green webhook: invalid token for line {line_id}")
        raise HTTPException(status_code=403, detail="Invalid Green webhook token")

    # idInstance должен принадлежать этой линии
    if not instance_id:
        log.error(f"Green webhook: no idInstance for line {line_id}")
        raise HTTPException(status_code=403, detail="Missing instance id")
    line = await service.instance_repo.get_line_by_instance_id(instance_id)
    if not line or str(line.id) != str(line_id):
        log.error(f"Green webhook: instance {instance_id} does not belong to line {line_id}")
        raise HTTPException(status_code=403, detail="Instance does not belong to line")

    green_listener = request.app.state.green_listener
    await green_listener.handle_webhook(str(line_id), payload, token)
    return {"status": "ok"}


@router.post("/whatsapp/waba/{line_id}")
async def waba_webhook_header(
    line_id: uuid.UUID,
    request: Request,
):
    """WABA webhook без токена в URL (токен в заголовке X-Internal-Token)."""
    token = _extract_token(request)
    if not token:
        log.error(f"WABA webhook: missing token for line {line_id}")
        raise HTTPException(status_code=403, detail="Missing internal token")
    waba_listener = request.app.state.waba_listener
    payload = await request.json()
    await waba_listener.handle_webhook(str(line_id), token, payload)
    return {"status": "ok"}


@router.post("/whatsapp/waba/{line_id}/{token}")
async def waba_webhook(line_id: uuid.UUID, token: str, request: Request):
    """Совместимый вариант: токен в URL. В access-логах nginx его лучше скрывать."""
    header_token = _extract_token(request)
    effective_token = header_token or token
    waba_listener = request.app.state.waba_listener
    payload = await request.json()
    await waba_listener.handle_webhook(
        str(line_id), effective_token, payload
    )
    return {"status": "ok"}


@router.post("/vk/{line_id}")
async def vk_webhook(
    line_id: uuid.UUID,
    request: Request,
    service: WebhookService = Depends(get_webhook_service)
):
    """Receive webhook for VK line"""
    payload = await request.json()
    log.info(f"VK webhook for line {line_id}: {payload.get('type')}")
    vk_listener = request.app.state.vk_listener
    result = await vk_listener.handle_webhook(line_id, payload)
    return PlainTextResponse(result)