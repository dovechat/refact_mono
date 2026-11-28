"""Вебхуки CRM-систем."""
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from app.core.dependencies import get_crm_service
from app.core.forms import get_form_data
from app.services.crm_service import CrmService

BITRIX_INSTALL_FINISH_HTML = """<html>
<head><script src="//api.bitrix24.com/api/v1/"></script></head>
<body><script>BX24.init(function(){ BX24.installFinish(); });</script></body>
</html>"""

webhooks_router = APIRouter(prefix="/webhooks", tags=["webhooks"])


@webhooks_router.post("/bitrix")
async def bitrix_webhook(
    data: dict[str, str] = Depends(get_form_data),
    crm_service: CrmService = Depends(get_crm_service),
):
    await crm_service.handle_bitrix_incoming(data)
    return {"status": "ok"}


@webhooks_router.api_route("/install/bitrix", methods=["GET", "POST"])
async def bitrix_install():
    return HTMLResponse(content=BITRIX_INSTALL_FINISH_HTML)


@webhooks_router.post("/bitrix/check-wa-tg")
async def bitrix_check_wa_tg(
    data: dict[str, str] = Depends(get_form_data),
    crm_service: CrmService = Depends(get_crm_service),
):
    return await crm_service.check_wa_tg(data)


@webhooks_router.post("/amo/{amojo_id}")
async def amo_webhook(
    amojo_id: str,
    request: Request,
    crm_service: CrmService = Depends(get_crm_service),
):
    body = await request.body()
    signature = request.headers.get("X-Signature", "")
    # amojo_id может прийти как scope_id (channel_id_amojo_id)
    if "_" in amojo_id:
        amojo_id = amojo_id.split("_", 1)[1]
    await crm_service.handle_amo_incoming(
        body, amojo_id=amojo_id, signature=signature
    )
    return {"status": "ok"}