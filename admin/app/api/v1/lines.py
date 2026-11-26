"""Admin API: lines management."""
from uuid import UUID

from fastapi import APIRouter, Query

from app.core.dependencies import AdminUser, LineService
from app.schemas.admin_schemas import (
    ExtendLineRequest,
    LineListResponse,
    LineOut,
    MessageResponse,
    SetLineStatusRequest,
)

router = APIRouter(prefix="/lines", tags=["Admin — Lines"])


@router.get("", response_model=LineListResponse)
async def list_lines(
    service: LineService,
    admin: AdminUser,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    tenant_id: UUID | None = Query(None),
    channel_type: str | None = Query(None),
    status: str | None = Query(None),
    name: str | None = Query(None),
):
    """Список всех линий по всем тенантам с фильтрацией."""
    return await service.list_lines(
        skip=skip,
        limit=limit,
        tenant_id=tenant_id,
        channel_type=channel_type,
        status=status,
        actor_id=UUID(admin["sub"]),
        name=name,
    )


@router.get("/{line_id}", response_model=LineOut)
async def get_line(
    line_id: UUID,
    service: LineService,
    admin: AdminUser,
):
    """Детали линии."""
    return await service.get_line(line_id, actor_id=UUID(admin["sub"]))


@router.patch("/{line_id}/status", response_model=MessageResponse)
async def set_line_status(
    line_id: UUID,
    request: SetLineStatusRequest,
    service: LineService,
    admin: AdminUser,
):
    """
    Изменить статус линии.
    Используется для принудительной блокировки, разблокировки и т.д.
    """
    return await service.set_line_status(
        line_id, request, actor_id=UUID(admin["sub"])
    )


@router.post("/{line_id}/extend", response_model=MessageResponse)
async def extend_line(
    line_id: UUID,
    request: ExtendLineRequest,
    service: LineService,
    admin: AdminUser,
):
    """Продлить срок действия линии на указанное количество дней."""
    return await service.extend_line(
        line_id, request, actor_id=UUID(admin["sub"])
    )