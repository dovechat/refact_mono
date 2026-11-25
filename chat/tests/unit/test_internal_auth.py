"""Авторизация межсервисных вызовов /internal/* (X-Internal-Token)."""

import pytest
from fastapi import HTTPException

from app.core.dependencies import require_internal_token
from core.config import settings


@pytest.mark.asyncio
async def test_require_internal_token_accepts_valid_token():
    expected = settings.internal_api_token
    if not expected:
        pytest.skip("INTERNAL_API_TOKEN не настроен")
    await require_internal_token(expected)


@pytest.mark.asyncio
async def test_require_internal_token_rejects_wrong_token():
    expected = settings.internal_api_token
    if not expected:
        pytest.skip("INTERNAL_API_TOKEN не настроен")
    with pytest.raises(HTTPException) as exc:
        await require_internal_token("wrong-token")
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_require_internal_token_rejects_missing_token():
    expected = settings.internal_api_token
    if not expected:
        pytest.skip("INTERNAL_API_TOKEN не настроен")
    with pytest.raises(HTTPException) as exc:
        await require_internal_token(None)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_require_internal_token_503_when_not_configured(monkeypatch):
    monkeypatch.setattr(settings, "internal_api_token", "", raising=False)
    with pytest.raises(HTTPException) as exc:
        await require_internal_token("any")
    assert exc.value.status_code == 503
