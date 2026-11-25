import uuid
from typing import List
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.line_repo import LineRepository
from repo.line_repo import ChannelAccountRepository
from app.repositories.permission_repo import PermissionRepository

import logging
log = logging.getLogger(__name__)

class LineService:
    def __init__(self, db: AsyncSession):
        self.permission_repo = PermissionRepository(db)
        self.line_repo = LineRepository(db)
        self.channel_repo = ChannelAccountRepository(db)

    async def get_user_lines(self, user_id: uuid.UUID, role: str, tenant_id: uuid.UUID) -> List:
        if role == "owner":
            log.info(f"Owner {user_id}, all lines for tenant {tenant_id}")
            return await self.line_repo.get_multi(filters={"tenant_id": tenant_id})
        else:
            line_ids = await self.permission_repo.get_user_line_ids(user_id)
            log.info(f"Member {user_id}, line_ids {line_ids}")
            if not line_ids:
                return []
            return await self.line_repo.get_multi(filters={"id": line_ids})
    
    async def get_line_channels(
            self,
            line_id: uuid.UUID,
            user_id: uuid.UUID | None = None,
            role: str | None = None,
            tenant_id: uuid.UUID | None = None,
    ) -> List:
        """Каналы линии. Доступ проверяется по роли/правам пользователя."""
        log.info(
            f"Get channels line_id={line_id} user_id={user_id} "
            f"role={role}"
        )

        line = await self.line_repo.get_by_id(line_id)
        if not line:
            raise HTTPException(status_code=404, detail="Line not found")

        if role == "owner":
            # владелец тенанта видит только линии своего тенанта
            if line.tenant_id != tenant_id:
                log.error(f"IDOR: line {line_id} tenant {line.tenant_id} != {tenant_id}")
                raise HTTPException(status_code=403, detail="Forbidden")
        else:
            line_ids = await self.permission_repo.get_user_line_ids(user_id)
            if not line_ids or line_id not in line_ids:
                log.error(f"IDOR: user {user_id} has no access to line {line_id}")
                raise HTTPException(status_code=403, detail="Forbidden")

        return await self.channel_repo.get_multi(filters={"line_id": line_id})
