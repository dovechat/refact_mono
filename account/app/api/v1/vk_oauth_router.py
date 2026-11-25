import uuid
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Query
from fastapi.responses import RedirectResponse
from app.schemas.vk_oauth_schemas import VKOAuthStartResponse, VKOAuthConfirmRequest
from app.services.vk_oauth_service import VKOAuthService
from core.config import settings
from app.core.dependencies import (
    check_tenant_access,
    get_channel_account_repo,
    get_line_repo,
)

import logging
log = logging.getLogger(__name__)

vk_oauth_router = APIRouter(prefix="/tenants/{tenant_id}/lines/{line_id}/account/vk/oauth", tags=["vk_oauth"])


def get_vk_oauth_service(
    channel_repo=Depends(get_channel_account_repo),
    line_repo=Depends(get_line_repo),
) -> VKOAuthService:
    return VKOAuthService(channel_repo, line_repo)


@vk_oauth_router.get("/start", response_model=VKOAuthStartResponse)
async def vk_oauth_start(
    line_id: uuid.UUID,
    tenant_id: uuid.UUID = Depends(check_tenant_access),
    service: VKOAuthService = Depends(get_vk_oauth_service),
) -> VKOAuthStartResponse:
    await service.ensure_line(tenant_id, line_id)
    url = service.build_authorize_url(tenant_id, line_id)
    return VKOAuthStartResponse(authorize_url=url)


@vk_oauth_router.get("/callback")
async def vk_oauth_callback(
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
    service: VKOAuthService = Depends(get_vk_oauth_service),
):
    result_url = settings.VK_OAUTH_RESULT_URL
    if error or not code or not state:
        return RedirectResponse(f"{result_url}?{urlencode({'error': 'no_code'})}")

    try:
        payload, groups = await service.handle_callback(code, state)
    except Exception as e:
        log.warning("vk oauth callback failed: %s", type(e).__name__)
        return RedirectResponse(f"{result_url}?{urlencode({'error': type(e).__name__})}")

    query = [("payload", payload)] + [("g", f"{g.id}:{g.name}") for g in groups]
    return RedirectResponse(f"{result_url}?{urlencode(query)}")


@vk_oauth_router.post("/confirm")
async def vk_oauth_confirm(
    body: VKOAuthConfirmRequest,
    line_id: uuid.UUID,
    tenant_id: uuid.UUID = Depends(check_tenant_access),
    service: VKOAuthService = Depends(get_vk_oauth_service),
):
    return await service.confirm_group(
        body.payload, body.group_id, tenant_id, line_id
    )
