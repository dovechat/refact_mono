"""Обработчики установки приложения Bitrix24 и подключения amoCRM."""
import logging
import re
from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.core.dependencies import get_crm_service
from app.core.forms import get_form_data
from app.schemas.crm_schemas import (
    AmoSaveTokensRequest,
    BitrixAuthRequest,
    BitrixConnectRequest,
    BitrixSelectTenantRequest,
)
from app.services.crm_service import CrmService
from core.config import settings

log = logging.getLogger("uvicorn.error")

HTML_DIR = Path(__file__).resolve().parents[2] / "html"
templates = Jinja2Templates(directory=str(HTML_DIR))

# Секрет приложения и токены уходят на домен из запроса, поэтому
# принимаем только домены самих CRM.
_AMO_DOMAIN_RE = re.compile(
    r"[a-z0-9-]+\.(amocrm\.(ru|com)|kommo\.com)", re.IGNORECASE
)

install_router = APIRouter(prefix="/install", tags=["install"])
static_router = APIRouter(prefix="/html", tags=["html"])


def _checked_amo_domain(domain: str) -> str:
    if not _AMO_DOMAIN_RE.fullmatch(domain):
        raise HTTPException(status_code=400, detail="invalid amoCRM domain")
    return domain


def _amo_oauth_query(**extra: str) -> str:
    return urlencode({
        "client_id": settings.AMO_CLIENT_ID,
        "redirect_uri": settings.AMO_REDIRECT_URI,
        "response_type": "code",
        "state": "install",
        **extra,
    })


# --- Bitrix24 ---------------------------------------------------------

@install_router.get("/bitrix")
async def bitrix_placement_get(request: Request):
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "host_url": settings.host_url},
    )


@install_router.post("/bitrix")
@install_router.post("/bitrix/onappinstall")
async def bitrix_onappinstall(
    request: Request,
    data: dict[str, str] = Depends(get_form_data),
    crm_service: CrmService = Depends(get_crm_service),
):
    domain = request.query_params.get("DOMAIN", "")
    return await crm_service.handle_onappinstall(data, domain=domain)


@install_router.post("/bitrix/auth")
async def bitrix_auth(
    body: BitrixAuthRequest,
    crm_service: CrmService = Depends(get_crm_service),
):
    return await crm_service.get_lines_for_bitrix(
        email=body.email, password=body.password, bx_auth=body.bx_auth
    )


@install_router.post("/bitrix/connect")
async def bitrix_connect(
    body: BitrixConnectRequest,
    crm_service: CrmService = Depends(get_crm_service),
):
    return await crm_service.connect_line(
        line_id=body.line_id, bx_auth=body.bx_auth
    )


@install_router.post("/bitrix/select-tenant")
async def bitrix_select_tenant(
    body: BitrixSelectTenantRequest,
    crm_service: CrmService = Depends(get_crm_service),
):
    return await crm_service.get_lines_for_tenant(
        jwt_token=body.jwt_token,
        tenant_id=body.tenant_id,
        bx_auth=body.bx_auth,
    )


# --- amoCRM -----------------------------------------------------------

@install_router.get("/amo")
async def amo_install_redirect():
    query = _amo_oauth_query(mode="popup")
    return RedirectResponse(f"{settings.AMO_OAUTH_URL}?{query}")


@install_router.get("/amo/callback")
async def amo_oauth_callback(
    code: str | None = None,
    referer: str = "",
    crm_service: CrmService = Depends(get_crm_service),
):
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    domain = referer.removeprefix("https://").removeprefix("http://")
    return await crm_service.handle_amo_install(
        code=code, domain=_checked_amo_domain(domain.rstrip("/"))
    )


@install_router.get("/amo/auth-url")
async def amo_auth_url():
    return JSONResponse(
        {"url": f"{settings.AMO_AUTHORIZE_URL}?{_amo_oauth_query()}"}
    )


@install_router.post("/amo/save-tokens")
async def amo_save_tokens(
    body: AmoSaveTokensRequest,
    crm_service: CrmService = Depends(get_crm_service),
):
    _checked_amo_domain(body.domain)
    return await crm_service.save_amo_tokens(body)


# --- Статические страницы ---------------------------------------------

@static_router.api_route("/lines.html", methods=["GET", "POST"])
async def lines_placement():
    return FileResponse(HTML_DIR / "lines.html")


@static_router.api_route("/success.html", methods=["GET", "POST"])
async def success_placement():
    return FileResponse(HTML_DIR / "success.html")


@static_router.api_route("/login.html", methods=["GET", "POST"])
async def login_placement():
    return FileResponse(HTML_DIR / "login.html")


@static_router.api_route("/install_finish.html", methods=["GET", "POST"])
async def install_finish():
    return FileResponse(HTML_DIR / "install_finish.html")


@static_router.api_route("/html/contact-tab.html", methods=["GET", "POST"])
async def contact_tab():
    return FileResponse(HTML_DIR / "contact_tab.html")