"""Авторизация межсервисных вызовов tg_connector.

Сервис общается только с соседними сервисами монорепы (chat, crm, airflow),
поэтому все эндпоинты, кроме read-only /status, закрыты заголовком
X-Internal-Token — тем же значением, что и в chat <-> crm.
"""

import secrets

from fastapi import Header, HTTPException, status

from core.config import settings


async def require_internal_token(x_internal_token: str | None = Header(default=None)) -> None:
    """Проверка заголовка X-Internal-Token (constant-time сравнение)."""
    expected = settings.internal_api_token
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="INTERNAL_API_TOKEN is not configured",
        )
    if not x_internal_token or not secrets.compare_digest(x_internal_token, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid internal token",
        )


def internal_headers() -> dict:
    """Заголовок для исходящих вызовов к tg_connector."""
    return {"X-Internal-Token": settings.internal_api_token}
