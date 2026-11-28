"""Обработчики роботов бизнес-процессов Bitrix24."""
from fastapi import APIRouter, Depends

from app.core.dependencies import get_crm_service
from app.core.forms import get_form_data
from app.services.crm_service import CrmService

robots_router = APIRouter(prefix="/robots", tags=["robots"])


@robots_router.post("/{robot_code}")
async def robot_webhook(
    robot_code: str,
    data: dict[str, str] = Depends(get_form_data),
    crm_service: CrmService = Depends(get_crm_service),
):
    return await crm_service.handle_robot(robot_code, data)