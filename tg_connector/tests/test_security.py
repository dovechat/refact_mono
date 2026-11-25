"""Тесты авторизации tg_connector (заголовок X-Internal-Token)."""

import pytest
from fastapi import HTTPException

from core.config import settings
from security import require_internal_token


@pytest.fixture
def token():
    value = settings.internal_api_token
    if not value:
        pytest.skip("INTERNAL_API_TOKEN не настроен")
    return value


@pytest.mark.asyncio
async def test_valid_token_passes(token):
    await require_internal_token(token)


@pytest.mark.asyncio
async def test_missing_token_401(token):
    with pytest.raises(HTTPException) as exc:
        await require_internal_token(None)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_wrong_token_401(token):
    with pytest.raises(HTTPException) as exc:
        await require_internal_token("wrong-token")
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_empty_token_401(token):
    with pytest.raises(HTTPException) as exc:
        await require_internal_token("")
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_token_not_configured_503(monkeypatch):
    monkeypatch.setattr(settings, "internal_api_token", "", raising=False)
    with pytest.raises(HTTPException) as exc:
        await require_internal_token("any")
    assert exc.value.status_code == 503


@pytest.mark.asyncio
async def test_all_endpoints_except_status_are_protected():
    """Все эндпоинты, кроме read-only /status, требуют токен."""
    from api import app
    from fastapi.routing import APIRoute

    protected = 0
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        if route.path == "/status":
            continue
        deps = [d.dependency for d in route.dependencies]
        assert require_internal_token in deps, f"{route.path} не защищён"
        protected += 1

    assert protected > 0
