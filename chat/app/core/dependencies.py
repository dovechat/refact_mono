import secrets
from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.database import get_db
from app.repositories.chat_repo import ChatRepository
from app.services.chat_service import ChatService
# from app.services.message_service import MessageService
from app.services.status import StatusService
from app.services.websocket_service import WebSocketService
from app.services.webhook_service import WebhookService


async def get_chat_repo(db: AsyncSession = Depends(get_db)) -> ChatRepository:
    return ChatRepository(db)

async def get_chat_service(db: AsyncSession = Depends(get_db)) -> ChatService:
    return ChatService(db)

async def get_websocket_service(db: AsyncSession = Depends(get_db)) -> WebSocketService:
    return WebSocketService(db)

# async def get_message_service(db: AsyncSession = Depends(get_db)) -> MessageService:
#    return MessageService(db)

async def get_webhook_service(db: AsyncSession = Depends(get_db)) -> WebhookService:
    return WebhookService(db)

async def get_status_service(db: AsyncSession = Depends(get_db)) -> StatusService:
    return StatusService(db)


internal_token_scheme = APIKeyHeader(name="X-Internal-Token", auto_error=False)


async def require_internal_token(token: str | None = Security(internal_token_scheme)) -> None:
    """Авторизация межсервисных вызовов chat <-> crm (заголовок X-Internal-Token)."""
    expected = settings.internal_api_token
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="INTERNAL_API_TOKEN is not configured",
        )
    if not token or not secrets.compare_digest(token, expected):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid internal token")

'''
def check_conversation_access(
        conversation_id: uuid.UUID,
        current_user: UserContext = Depends(get_current_user),
        chat_service: ChatService = Depends(get_chat_service)
):
    """Verify user has access to conversation"""

    async def access_checker():
        conversation = await chat_service.get_conversation(
            conversation_id=conversation_id,
            tenant_id=current_user.tenant_id
        )
        if not conversation:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Conversation not found")
        return conversation_id

    return access_checker
'''