import time
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from models.tenants import UserStatus
from services import auth_service as auth_module
from services.auth_service import AuthService, create_invite_token
from core import revocation


class FakeRedis:
    def __init__(self):
        self.data = {}

    async def set(self, key, value, ex=None):
        self.data[key] = str(value)

    async def setex(self, key, ttl, value):
        self.data[key] = str(value)

    async def mget(self, keys):
        return [self.data.get(k) for k in keys]

    async def getdel(self, key):
        return self.data.pop(key, None)

    async def delete(self, *keys):
        for k in keys:
            self.data.pop(k, None)


@pytest.fixture
def fake_rdb(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(revocation, "rdb", fake)
    monkeypatch.setattr(auth_module, "rdb", fake)
    return fake


@pytest.mark.asyncio
async def test_revoke_blocks_older_token(fake_rdb):
    user_id, tenant_id = uuid.uuid4(), uuid.uuid4()
    now = int(time.time())
    await revocation.revoke_access(user_id, tenant_id)

    assert await revocation.is_revoked(user_id, tenant_id, now - 10) is True
    assert await revocation.is_revoked(user_id, tenant_id, now + 5) is False


@pytest.mark.asyncio
async def test_revoke_is_scoped_to_tenant(fake_rdb):
    user_id, t1, t2 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    await revocation.revoke_access(user_id, t1)
    old = int(time.time()) - 10

    assert await revocation.is_revoked(user_id, t1, old) is True
    assert await revocation.is_revoked(user_id, t2, old) is False


@pytest.mark.asyncio
async def test_user_wide_revoke_hits_all_tenants(fake_rdb):
    user_id, tenant_id = uuid.uuid4(), uuid.uuid4()
    await revocation.revoke_access(user_id)

    assert await revocation.is_revoked(user_id, tenant_id, int(time.time()) - 10) is True


@pytest.mark.asyncio
async def test_token_without_iat_is_revoked_only_if_mark_exists(fake_rdb):
    user_id = uuid.uuid4()
    assert await revocation.is_revoked(user_id, None, None) is False
    await revocation.revoke_access(user_id)
    assert await revocation.is_revoked(user_id, None, None) is True


@pytest.mark.asyncio
async def test_accept_invite_is_single_use(fake_rdb, monkeypatch):
    user = MagicMock(id=uuid.uuid4(), role=None)
    repo = MagicMock()
    repo.get_by_id = AsyncMock(return_value=user)
    repo.update = AsyncMock()
    monkeypatch.setattr(auth_module, "UserRepository", lambda db: repo)

    jwt_service = MagicMock()
    jwt_service.password_hash.return_value = "hashed"
    jwt_service.create_token_pair.return_value = {"access_token": "a", "refresh_token": "r"}
    service = AuthService(jwt_service=jwt_service)

    token = await create_invite_token(user.id)
    result = await service.accept_invite(token, "password123", db=MagicMock())

    assert result["access_token"] == "a"
    repo.update.assert_awaited_once_with(user, password_hash="hashed", status=UserStatus.active)

    with pytest.raises(HTTPException) as exc:
        await service.accept_invite(token, "password123", db=MagicMock())
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_accept_invite_rejects_unknown_token(fake_rdb):
    service = AuthService(jwt_service=MagicMock())
    with pytest.raises(HTTPException) as exc:
        await service.accept_invite("nope", "password123", db=MagicMock())
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_refresh_rejects_removed_member(fake_rdb, monkeypatch):
    tenant_id = uuid.uuid4()
    payload = MagicMock(sub=str(uuid.uuid4()), tenant_id=str(tenant_id), role=None)
    jwt_service = MagicMock()
    jwt_service.verify_token.return_value = payload

    repo = MagicMock()
    repo.get_by_id = AsyncMock(return_value=MagicMock(id=uuid.uuid4(), status=UserStatus.active))
    repo.get_membership = AsyncMock(return_value=None)
    monkeypatch.setattr(auth_module, "UserRepository", lambda db: repo)
    monkeypatch.setattr(auth_module, "is_throttled", AsyncMock(return_value=False))

    service = AuthService(jwt_service=jwt_service)
    with pytest.raises(HTTPException) as exc:
        await service.refresh("refresh-token", db=MagicMock())
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_refresh_keeps_member_role_from_db(fake_rdb, monkeypatch):
    tenant_id = uuid.uuid4()
    payload = MagicMock(sub=str(uuid.uuid4()), tenant_id=str(tenant_id), role=None)
    jwt_service = MagicMock()
    jwt_service.verify_token.return_value = payload
    jwt_service.create_token_pair.return_value = {"access_token": "a", "refresh_token": "r"}

    user = MagicMock(id=uuid.uuid4(), status=UserStatus.active, role=None)
    repo = MagicMock()
    repo.get_by_id = AsyncMock(return_value=user)
    repo.get_membership = AsyncMock(return_value=MagicMock(role="admin"))
    monkeypatch.setattr(auth_module, "UserRepository", lambda db: repo)
    monkeypatch.setattr(auth_module, "is_throttled", AsyncMock(return_value=False))

    service = AuthService(jwt_service=jwt_service)
    await service.refresh("refresh-token", db=MagicMock())

    kwargs = jwt_service.create_token_pair.call_args.kwargs
    assert kwargs["member"] == "admin"
    assert kwargs["tenant_id"] == str(tenant_id)
