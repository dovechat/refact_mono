import time
import uuid

from core.config import settings
from core.redis import rdb


def _key(user_id: str | uuid.UUID, tenant_id: str | uuid.UUID | None = None) -> str:
    if tenant_id:
        return f"revoked:{user_id}:{tenant_id}"
    return f"revoked:{user_id}"


async def revoke_access(user_id: str | uuid.UUID, tenant_id: str | uuid.UUID | None = None) -> None:
    """Все access-токены, выданные до этого момента, перестают работать.
    С tenant_id: только токены этого пользователя в этом тенанте."""
    ttl = settings.access_token_expire_minutes * 60 + 60
    await rdb.set(_key(user_id, tenant_id), int(time.time()), ex=ttl)


async def is_revoked(
    user_id: str | uuid.UUID,
    tenant_id: str | uuid.UUID | None,
    issued_at: int | None,
) -> bool:
    keys = [_key(user_id)]
    if tenant_id:
        keys.append(_key(user_id, tenant_id))
    values = await rdb.mget(keys)
    for value in values:
        if value is None:
            continue
        if issued_at is None or int(value) > int(issued_at):
            return True
    return False
