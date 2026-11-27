import logging
import httpx
from fastapi.encoders import jsonable_encoder
from core.config import settings
from app.core.crm_flag import crm_enabled

log = logging.getLogger("uvicorn.error")


class CrmClient:
    def __init__(self) -> None:
        self._base = settings.crm_internal_url.rstrip("/") + "/api/v1/internal"
        self._headers = {"X-Internal-Token": settings.internal_api_token}

    async def _post(self, path: str, payload: dict) -> dict:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self._base}/{path}", json=jsonable_encoder(payload), headers=self._headers
            )
            resp.raise_for_status()
            return resp.json()

    async def send_to_crm(self, **kw) -> str | None:
        return (await self._post("send", kw)).get("bx_chat")

    async def edit_to_crm(self, **kw) -> None:
        try:
            await self._post("edit", kw)
        except Exception as e:
            log.error(f"edit_to_crm failed: {e}")

    async def delete_from_crm(self, **kw) -> None:
        try:
            await self._post("delete", kw)
        except Exception as e:
            log.error(f"delete_from_crm failed: {e}")


def get_crm_client() -> CrmClient | None:
    return CrmClient() if crm_enabled() else None