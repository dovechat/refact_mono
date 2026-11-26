import uuid
from typing import Dict, List
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from models.messages import MessageDirection, MessageStatus
from app.repositories.chat_repo import ChatRepository
from app.repositories.message_repo import MessageRepository


class WebSocketService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.chat_repo = ChatRepository(db)
        self.msg_repo = MessageRepository(db)
        self.subscriptions: Dict[uuid.UUID, List[uuid.UUID]] = {}  # conversation_id -> [user_id]

    async def subscribe_to_conversation(
            self,
            conversation_id: uuid.UUID,
            tenant_id: uuid.UUID,
            user_id: uuid.UUID
    ):
        """Subscribe user to conversation updates"""
        conversation = await self.chat_repo.get_conversation_for_tenant(conversation_id, tenant_id)

        if not conversation:
            raise ValueError("Conversation not found or access denied")

        if conversation_id not in self.subscriptions:
            self.subscriptions[conversation_id] = []

        if user_id not in self.subscriptions[conversation_id]:
            self.subscriptions[conversation_id].append(user_id)

    async def save_message(
            self,
            conversation_id: uuid.UUID,
            tenant_id: uuid.UUID,
            user_id: uuid.UUID,
            message: dict
    ) -> dict:
        """Save incoming WebSocket message to database"""
        conversation = await self.chat_repo.get_conversation_for_tenant(conversation_id, tenant_id)

        if not conversation:
            raise ValueError("Conversation not found or access denied")

        # Create message record
        new_message = await self.msg_repo.create(
            conversation_id=conversation_id,
            direction=MessageDirection.outbound,
            status=MessageStatus.sent,
            text=message.get("text"),
            raw_content=message
        )
        await self.chat_repo.update(conversation, updated_at=datetime.now(timezone.utc))

        return {
            "id": new_message.id,
            "conversation_id": str(new_message.conversation_id),
            "direction": new_message.direction.value,
            "status": new_message.status.value,
            "text": new_message.text,
            "created_at": new_message.created_at.isoformat(),
            "raw_content": new_message.raw_content
        }

    async def get_subscribed_users(self, conversation_id: uuid.UUID) -> List[uuid.UUID]:
        """Get list of users subscribed to conversation"""
        return self.subscriptions.get(conversation_id, [])

    async def unsubscribe_from_conversation(
            self,
            conversation_id: uuid.UUID,
            user_id: uuid.UUID
    ):
        """Unsubscribe user from conversation"""
        if conversation_id in self.subscriptions:
            if user_id in self.subscriptions[conversation_id]:
                self.subscriptions[conversation_id].remove(user_id)

            if not self.subscriptions[conversation_id]:
                del self.subscriptions[conversation_id]