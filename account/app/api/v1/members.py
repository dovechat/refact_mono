from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import check_tenant_access, check_tenant_manager
from app.schemas.member_schemas import (
    MemberCreate,
    MemberResponse,
    LinePermissionCreate,
    LinePermissionResponse
)
from app.services.member_service import MemberService
from core.database import get_db

members_router = APIRouter(
    prefix="/tenants/{tenant_id}/members",
    tags=["members"],
)


@members_router.post("", response_model=MemberResponse, status_code=201)
async def add_member(
    data: MemberCreate,
    tenant_id: UUID = Depends(check_tenant_manager),
    db: AsyncSession = Depends(get_db),
):
    """Добавить сотрудника в тенант"""
    service = MemberService(db)
    return await service.add_member(tenant_id, data)


@members_router.get("", response_model=List[MemberResponse])
async def list_members(
    skip: int = 0,
    limit: int = 100,
    tenant_id: UUID = Depends(check_tenant_access),
    db: AsyncSession = Depends(get_db),
):
    """Список сотрудников тенанта"""
    service = MemberService(db)
    return await service.list_members(tenant_id, skip, limit)


@members_router.delete("/{user_id}", status_code=204)
async def remove_member(
    user_id: UUID,
    tenant_id: UUID = Depends(check_tenant_manager),
    db: AsyncSession = Depends(get_db),
):
    """Удалить сотрудника из тенанта"""
    service = MemberService(db)
    await service.remove_member(tenant_id, user_id)
    return None


@members_router.post(
    "/line-permissions",
    response_model=LinePermissionResponse,
    status_code=201,
)
async def add_line_permission(
    data: LinePermissionCreate,
    tenant_id: UUID = Depends(check_tenant_manager),
    db: AsyncSession = Depends(get_db),
):
    """Выдать доступ к линии"""
    service = MemberService(db)
    return await service.add_line_permission(tenant_id, data)


@members_router.get(
    "/line-permissions",
    response_model=List[LinePermissionResponse],
)
async def get_line_permissions(
    user_id: UUID,
    tenant_id: UUID = Depends(check_tenant_access),
    db: AsyncSession = Depends(get_db),
):
    """Получить список доступов к линиям для сотрудника"""
    service = MemberService(db)
    return await service.get_line_permissions(tenant_id, user_id)


@members_router.delete(
    "/line-permissions/{user_id}/{line_id}",
    status_code=204,
)
async def remove_line_permission(
    user_id: UUID,
    line_id: UUID,
    tenant_id: UUID = Depends(check_tenant_manager),
    db: AsyncSession = Depends(get_db),
):
    """Отозвать доступ к линии"""
    service = MemberService(db)
    await service.remove_line_permission(tenant_id, user_id, line_id)
    return None
