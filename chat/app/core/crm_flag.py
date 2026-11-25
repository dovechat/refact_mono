"""Флаг включения CRM (env CRM_ENABLED, по умолчанию выключен)."""
import os

from fastapi import HTTPException, status


def crm_enabled() -> bool:
    value = os.getenv("CRM_ENABLED", "false")
    return value.strip().lower() in ("1", "true", "yes", "on")


async def require_crm_enabled() -> None:
    if not crm_enabled():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Not Found",
        )

