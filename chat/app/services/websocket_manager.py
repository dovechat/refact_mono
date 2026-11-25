import json
from contextlib import asynccontextmanager
from fastapi import WebSocket
from typing import Dict, List
import uuid
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from core.database import get_db
from core.redis import rdb
from models.messages import Conversation
from repo.user import UserRepository
from app.repositories.permission_repo import PermissionRepository

import logging

log = logging.getLogger(__name__)
get_session = asynccontextmanager(get_db)


class ConnectionManager:
    """Менеджер WS-соединений.

    Подписка на Redis Pub/Sub живёт в app/core/redis_subscriber.py (её вызывает main.py).
    Здесь дублирующей подписки нет — иначе клиент получал бы каждое событие дважды.
    """

    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}
        self.tenant_connections: Dict[str, List[WebSocket]] = {}

    async def connect(
            self,
            websocket: WebSocket,
            conversation_id: str,
            app,
            user_id: str,
            tenant_id: str,
            role: str,
    ) -> bool | None:
        try:
            conv_uuid = uuid.UUID(str(conversation_id))
            user_uuid = uuid.UUID(user_id)
        except ValueError as e:
            log.error(
                "\n\n !!! --- ERROR --- !!! "
                "chat/app/services/websocket_manager.py "
                "WebSocketManager.connect "
                f"bad uuid e={e}\n\n"
            )
            await websocket.close(code=1008, reason="Bad id")
            return False
        # Проверка доступа
        async with get_session() as db:
            # Загружаем conversation вместе с линией
            stmt = select(Conversation).where(Conversation.id == conv_uuid).options(
                selectinload(Conversation.line))
            result = await db.execute(stmt)
            conv = result.scalar_one_or_none()

            if not conv:
                await websocket.close(code=1008, reason="Conversation not found")
                return False

            user_repo = UserRepository(db)
            membership = await user_repo.get_membership(
                user_id=user_uuid,
                tenant_id=conv.line.tenant_id
            )
            if not membership:
                await websocket.close(code=1008, reason="Access denied")
                return False

            if role != "owner":
                line_ids = await PermissionRepository(
                    db
                ).get_user_line_ids(user_uuid)
                if not line_ids or conv.line_id not in line_ids:
                    await websocket.close(
                        code=1008, reason="Access denied"
                    )
                    return False

        # Добавляем соединение в список
        if conversation_id not in self.active_connections:
            self.active_connections[conversation_id] = []
        self.active_connections[conversation_id].append(websocket)

        return True

    async def disconnect(self, conversation_id: str, websocket: WebSocket):
        connections = self.active_connections.get(conversation_id)
        if connections:
            if websocket in connections:
                connections.remove(websocket)
            if not connections:
                self.active_connections.pop(conversation_id, None)

        log.info(f"Disconnected: conversation={conversation_id}")

    async def send_new_message(self, conversation_id: str, message_data: dict):
        conversation_id = str(conversation_id)
        await rdb.publish(f"ws:new_message:{conversation_id}", json.dumps(message_data))

    async def send_message_status(self, conversation_id: str, data: dict):
        conversation_id = str(conversation_id)
        await rdb.publish(f"ws:message_status:{conversation_id}", json.dumps(data))

    async def connect_tenant(self, websocket: WebSocket, tenant_id: str):
        if tenant_id not in self.tenant_connections:
            self.tenant_connections[tenant_id] = []
        self.tenant_connections[tenant_id].append(websocket)
    
    async def disconnect_tenant(self, tenant_id: str, websocket: WebSocket):
        connections = self.tenant_connections.get(tenant_id)
        if connections:
            if websocket in connections:
                connections.remove(websocket)
            if not connections:
                self.tenant_connections.pop(tenant_id, None)
    
    async def send_new_conversation(self, tenant_id: str, conversation_data: dict):
        tenant_id = str(tenant_id)
        log.info(f"send_new_conversation tenant_id={tenant_id}")
        await rdb.publish(f"ws:new_conversation:{tenant_id}", json.dumps(conversation_data))

    async def send_message_update(self, conversation_id: str, data: dict):
        conversation_id = str(conversation_id)
        await rdb.publish(f"ws:message_update:{conversation_id}", json.dumps(data))

    async def edit_message(
        self,
        conversation_id,
        message_data: dict,
    ):
        await rdb.publish(
            f"ws:message_update:{conversation_id}",
            json.dumps(message_data),
        )

    async def delete_message(
        self,
        conversation_id,
        message_data: dict,
    ):
        await rdb.publish(
            f"ws:message_update:{conversation_id}",
            json.dumps(message_data),
        )


manager = ConnectionManager()