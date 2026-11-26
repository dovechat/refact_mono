import base64
import httpx
import json
import uuid

from datetime import datetime
from types import SimpleNamespace
from typing import List, Optional, Tuple
from fastapi import Depends, HTTPException, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession
from core.redis import rdb
from app.repositories.chat_repo import ChatRepository
from app.repositories.message_repo import MessageRepository
from repo.line_repo import ChannelAccountRepository, LineRepository
from repo.message_repo import MediaRepository
from repo.tenant_repo import TenantRepository
from app.schemas.chat_schemas import (
    ChannelAvailable,
    ConversationResponse,
    CrmMessageIn,
    CrmMessageOut,
    LineWithChannelsResponse,
    MessageCreate,
    MessageResponse,
    MessageStatus,
    ConversationListResponse
)
from app.senders.factory import SenderFactory
from app.services.websocket_manager import manager
from core.config import settings
from core.dependencies import get_current_user
from infra.crypto import decrypt
from models.messages import Conversation, Message, MessageDirection
from notification.notification_caller import notify_connection_error
from schemas.auth_schemas import UserContext
from services.storage_service import StorageService

import logging
log = logging.getLogger("uvicorn.error")

class ChatService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.ch_acc_repo = ChannelAccountRepository(db)
        self.conv_repo = ChatRepository(db)
        self.msg_repo = MessageRepository(db)
        self.tenant_repo = TenantRepository(db)
        self.media_repo = MediaRepository(db)
        self.line_repo = LineRepository(db)
        self.storage = StorageService()

    async def _user_tenant_ids(self, user_id: uuid.UUID) -> List[uuid.UUID]:
        """ID тенантов, к которым у пользователя есть membership."""
        tenants = await self.tenant_repo.get_all_tenants(user_id=user_id)
        return [t.id for t in tenants]

    async def _assert_line_access(
            self,
            line_id: uuid.UUID,
            user_id: uuid.UUID | None = None,
            tenant_id: uuid.UUID | None = None,
    ):
        """Проверить, что линия принадлежит пользователю (иначе 403)."""
        line = await self.line_repo.get_by_id(line_id)
        if not line:
            raise HTTPException(status_code=404, detail="Line not found")
        if user_id is None and tenant_id is None:
            # проверку нельзя пропускать: без контекста пользователя доступа нет
            log.error(f"IDOR: access check without user_id/tenant_id for line {line_id}")
            raise HTTPException(status_code=403, detail="Forbidden")
        if user_id is not None:
            tenant_ids = await self._user_tenant_ids(user_id)
            if line.tenant_id not in tenant_ids:
                log.error(f"IDOR: line {line_id} tenant {line.tenant_id} not in user tenants {tenant_ids}")
                raise HTTPException(status_code=403, detail="Forbidden")
        elif tenant_id is not None and line.tenant_id != tenant_id:
            log.error(f"IDOR: line {line_id} tenant {line.tenant_id} != {tenant_id}")
            raise HTTPException(status_code=403, detail="Forbidden")
        return line

    async def _assert_conversation_access(
            self,
            conversation,
            user_id: uuid.UUID | None = None,
            tenant_id: uuid.UUID | None = None,
    ):
        """Проверить доступ к чату: чат -> линия -> тенант."""
        if conversation is None:
            raise HTTPException(status_code=404, detail="Conversation not found")
        await self._assert_line_access(conversation.line_id, user_id=user_id, tenant_id=tenant_id)
        return conversation

    async def _assert_message_access(self, message, user_id: uuid.UUID | None = None):
        conv = await self.conv_repo.get_by_id(message.conversation_id)
        if not conv:
            raise HTTPException(status_code=404, detail="Conversation not found")
        await self._assert_conversation_access(conv, user_id=user_id)
        return conv

    async def _init_sender(
            self,
            channel_type: str,
            conv_id: uuid.UUID,
    ):
        sender = SenderFactory.get_sender(channel_type)
        channel_acc = await self.conv_repo.get_channel_acc_by_conversation(conv_id)
        if channel_type == "telegram_user":
            # sender.set_listener()
            sender.set_line_id(channel_acc.line_id)
            sender.set_conv_id(conv_id)
            return sender

        creds = json.loads(decrypt(channel_acc.credentials_encrypted["encrypted_data"]))
        if channel_type == "telegram_bot":
            sender.set_bot_token(creds["bot_token"])
        else:
            sender.set_creds(creds)

        sender.set_line_id(channel_acc.line_id)

        log.info(f"_init_sender channel_type={channel_type} conv_id={conv_id}")
        return sender

    async def _process_queue_outgoing(
            self,
            channel_type: str,
            data: MessageCreate,
            current_user: UserContext,
    ) -> MessageResponse:
        message = await self.msg_repo.get_by_id(data.message_id)

        conv = await self.conv_repo.get_by_id(data.conversation_id)

        if conv.client_phone:
            data.phone = conv.client_phone

        if message is None:
            raise HTTPException(status_code=404, detail="Message not found")

        if data.media:
            for m in data.media:
                try:
                    async with self.db.begin_nested():
                        await self.media_repo.create(
                            message_id=message.id, **m
                        )
                except Exception as e:
                    log.error(
                        "\n\n !!! --- ERROR --- !!! "
                        "chat/app/services/chat_service.py "
                        "ChatService._process_queue_outgoing "
                        f"media create failed e={e}\n\n"
                    )
            await self.db.refresh(message, attribute_names=["media"])

        await manager.send_new_message(str(data.conversation_id), {
            "id": str(message.id),
            "text": message.text,
            "direction": "outbound",
            "created_at": message.created_at.isoformat(),
            "user_id": str(current_user.id),
            "user_email": current_user.email,
            "status": "sent",
            "media": [{**m, "url": self.storage.presigned_url(m["id"])} for m in (data.media or [])],
        })

        sender = await self._init_sender(channel_type, data.conversation_id)
        result = None
        success = False
        error: Exception | None = None
        try:
            result = await sender.send(data, self.db)
            success = True
        except Exception as e:
            error = e
            log.error(
                "\n\n !!! --- ERROR --- !!! "
                "chat/app/services/chat_service.py "
                "ChatService._process_queue_outgoing "
                f"send failed channel_type={channel_type} e={e}\n\n"
            )
            if isinstance(e, (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout, httpx.NetworkError)):
                await notify_connection_error(conv.line_id, channel_type, self.db)

        if isinstance(result, dict):
            raw_content = result
            raw_content["crm_message_id"] = data.crm_message_id
        elif result:
            raw_content = {"crm_message_id": data.crm_message_id}
        else:
            raw_content = None

        await self._finalize_message(
            message=message,
            conversation_id=data.conversation_id,
            channel_type=channel_type,
            data=data,
            current_user=current_user,
            success=success,
            channel_msg_id=sender.extract_msg_id(result),
            raw_content=raw_content,
            error=error,
        )
        return MessageResponse.model_validate(message)

    async def create_msg_telegram_bot(self, data, files=[], current_user=None):
        return await self._process_queue_outgoing("telegram_bot", data, current_user)

    async def create_msg_telegram_user(self, data, files=[], current_user=None):
        return await self._process_queue_outgoing("telegram_user", data, current_user)

    async def create_msg_whatsapp_green(self, data, files=[], current_user=None):
        return await self._process_queue_outgoing("whatsapp_green", data, current_user)

    async def create_msg_waba(self, data, files=[], current_user=None):
        return await self._process_queue_outgoing("waba", data, current_user)

    async def create_msg_vk(self, data, files=[], current_user=None):
        return await self._process_queue_outgoing("vk", data, current_user)

    async def edit_outgoing_message(
            self,
            message_id: int,
            text: str,
            current_user: UserContext,
            phone: str,
            line_id: uuid.UUID,
            channel_type: str
    ) -> MessageResponse:
        message = await self.msg_repo.get_by_id(message_id)
        if not message:
            raise HTTPException(status_code=404, detail="Message not found")
        if not message.channel_msg_id:
            raise HTTPException(status_code=400, detail="no channel_msg_id")

        await self._assert_message_access(message, user_id=current_user.id if current_user else None)

        sender = await self._init_sender(channel_type, message.conversation_id)

        try:
            result = await sender.edit(message.channel_msg_id, phone, text)
            await self.msg_repo.update(message, text=text)
        except Exception as e:
            log.error(
                "\n\n !!! --- ERROR --- !!! "
                "chat/app/services/chat_service.py "
                "ChatService.edit_outgoing_message "
                f"edit send failed channel_type={channel_type} e={e}\n\n"
            )
            if isinstance(e, (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout, httpx.NetworkError)):
                await notify_connection_error(line_id, channel_type, self.db)
            # WS-событие не отправляем: в канале остался старый текст
            raise HTTPException(status_code=502, detail=f"Failed to edit message in channel: {e}")

        await manager.send_message_update(str(message.conversation_id,), {
            "type": "edit",
            "message_id": str(message.id),
            "text": text,
        })
        return MessageResponse.model_validate(message)

    async def delete_message(self, message_id: int, current_user: UserContext, phone: str, line_id: uuid.UUID,
                             channel_type: str, crm_service=None) -> None:
        message = await self.msg_repo.get_by_id(message_id)
        if not message:
            raise HTTPException(status_code=404, detail="message not found")
        await self._assert_message_access(message, user_id=current_user.id if current_user else None)
        if message.channel_msg_id:
            sender = await self._init_sender(channel_type, message.conversation_id)
            try:
                result = await sender.delete(message.channel_msg_id, phone)
            except Exception as e:
                log.error(
                    "\n\n !!! --- ERROR --- !!! "
                    "chat/app/services/chat_service.py "
                    "ChatService.delete_message "
                    f"channel delete failed channel_type={channel_type} e={e}\n\n"
                )
                if isinstance(e, (
                    httpx.ConnectError,
                    httpx.ConnectTimeout,
                    httpx.ReadTimeout,
                    httpx.NetworkError,
                )):
                    await notify_connection_error(
                        line_id, channel_type, self.db
                    )
                raise HTTPException(
                    status_code=502,
                    detail=f"Failed to delete message in channel: {e}",
                )

        crm_message_id = (message.raw_content or {}).get("crm_message_id")
        if crm_service and crm_message_id:
            line = await self.line_repo.get_by_id(line_id)
            try:
                await crm_service.delete_from_crm(
                    tenant_id=line.tenant_id,
                    conversation_id=message.conversation_id,
                    client_phone=phone,
                    message_id=crm_message_id,
                    channel_type=channel_type,
                )
            except Exception as e:
                log.error(
                    "\n\n !!! --- ERROR --- !!! "
                    "chat/app/services/chat_service.py "
                    "ChatService.delete_message "
                    f"delete_from_crm failed e={e}\n\n"
                )

        await self.msg_repo.delete(message)

        await manager.send_message_update(str(message.conversation_id), {
            "type": "delete",
            "message_id": str(message.id),
        })


    async def get_conversations_filtered(
            self,
            user_id: uuid.UUID,
            line_id: uuid.UUID | None = None,
            channel_type: str | None = None,
            is_closed: bool | None = None,
    ) -> list[ConversationListResponse]:
        tenants = await self.tenant_repo.get_all_tenants(user_id=user_id)
        tenant_ids = [t.id for t in tenants]
        if not tenant_ids:
            return []

        rows = await self.conv_repo.get_conversations_with_filters(
            tenant_ids=tenant_ids,
            line_id=line_id,
            channel_type=channel_type,
            is_closed=is_closed,
        )

        last_msgs = await self.msg_repo.get_last_messages_map(
            [row[0].id for row in rows]
        )

        conversations = []
        for conv, line_name, tenant_id, unread_count, last_msg_at in rows:
            last_msg = None
            last_msg_obj = last_msgs.get(conv.id)
            if last_msg_obj:
                last_msg = MessageResponse.model_validate(last_msg_obj)
            conversations.append(ConversationListResponse(
                **conv.__dict__,
                line_name=line_name,
                tenant_id=tenant_id,
                unread_count=unread_count,
                last_message=last_msg
            ))
        return conversations



    async def get_lines_with_available_channels(self, user_id: uuid.UUID) -> list[LineWithChannelsResponse]:
        tenants = await self.tenant_repo.get_all_tenants(user_id=user_id)
        tenant_map = {t.id: t.name for t in tenants}

        lines: dict[uuid.UUID, LineWithChannelsResponse] = {}
        for tenant in tenants:
            rows = await self.conv_repo.get_lines_with_available_channels(tenant.id)
            for row in rows:
                if row.id not in lines:
                    lines[row.id] = LineWithChannelsResponse(
                        id=row.id,
                        name=row.name,
                        tenant_id=tenant.id,
                        tenant_name=tenant_map[tenant.id],
                        channels=[]
                    )
                lines[row.id].channels.append(ChannelAvailable(channel_type=row.channel_type))

        return list(lines.values())


    async def get_conversation(
            self,
            conversation_id: uuid.UUID,
            tenant_id: uuid.UUID,
            user_id: uuid.UUID | None = None,
    ) -> Optional[ConversationResponse]:
        """Get conversation with all messages"""
        conversation = await self.conv_repo.get_conversation_with_messages(conversation_id)

        if not conversation:
            return None

        await self._assert_conversation_access(conversation, user_id=user_id, tenant_id=tenant_id)

        messages = await self.msg_repo.get_conversation_messages(
            conversation_id, skip=0, limit=1000
        )

        # my_phone = None
        # if conversation.channel_type != "telegram_bot":
        my_phone = await self.conv_repo.get_line_phone_by_conversation(conversation_id)

        return ConversationResponse(
            **conversation.__dict__,
            messages=[MessageResponse.model_validate(msg) for msg in messages],
            my_phone=my_phone,
        )

    async def get_messages(
            self,
            conversation_id: uuid.UUID,
            tenant_id: uuid.UUID,
            user_id: uuid.UUID | None = None,
            skip: int = 0,
            limit: int = 100
    ) -> List[MessageResponse]:
        """Get messages for conversation"""
        # Verify access: чат -> линия -> тенант пользователя
        conv = await self.conv_repo.get_conversation_with_messages(conversation_id)
        if not conv:
            return []

        await self._assert_conversation_access(conv, user_id=user_id, tenant_id=tenant_id)

        messages = await self.msg_repo.get_conversation_messages(
            conversation_id, skip, limit
        )
        messages = list(reversed(messages))
        result = []
        for msg in messages:
            msg_dict = dict(msg.__dict__)
            filename = None
            if msg.raw_content:
                if msg.raw_content.get("file_name") is not None:
                    filename = msg.raw_content.get("file_name") 
                if filename is None and msg.raw_content is not None:
                    raw = msg.raw_content or {}
                    filename = (
                        raw.get("messageData", {})
                        .get("fileMessageData", {})
                        .get("fileName")
                    )

            if msg.user:
                msg_dict['user_email'] = msg.user.email
                msg_dict['user_full_name'] = msg.user.full_name
            
            msg_dict['file_name'] = filename

            media_list = []
            for m in msg.media:
                media_list.append({
                    "id": m.id,
                    "file_name": m.file_name,
                    "mime_type": m.mime_type,
                    "url": self.storage.presigned_url(m.id),
                })
            msg_dict['media'] = media_list

            result.append(MessageResponse.model_validate(msg_dict))
        return result


    async def create_conversation(self, tenant_id, data=None, user_id=None) -> ConversationResponse:
        # Проверяем, что линия принадлежит тенанту пользователя
        await self._assert_line_access(data.line_id, user_id=user_id, tenant_id=tenant_id)

        conversation = await self.conv_repo.create_conversation(
            line_id=data.line_id, channel_type=data.channel_type,
            crm_chat_id=data.crm_chat_id, client_phone=data.client_phone, meta=data.meta
        )

        if data.channel_type == "telegram_user":
            tg_acc = await self.ch_acc_repo.get_by_line_and_type(data.line_id, "telegram_user")
            if tg_acc:
                try:
                    tg_headers = {"X-Internal-Token": settings.internal_api_token}
                    async with httpx.AsyncClient() as client:
                        await client.post(f"http://tg-connector:8000/channels/{data.line_id}/stop",
                                          headers=tg_headers)
                        await client.post(f"http://tg-connector:8000/channels/{data.line_id}/start",
                                          params={"phone": tg_acc.phone},
                                          headers=tg_headers)
                    log.info(f"create_conversation: tg-connector restart OK conv={conversation.id}")
                except Exception as e:
                    log.error(
                        "\n\n !!! --- ERROR --- !!! "
                        "chat/app/services/chat_service.py "
                        "ChatService.create_conversation "
                        f"tg-connector restart failed e={e}\n\n"
                    )

        return ConversationResponse(**conversation.__dict__, messages=[])
    
    
    @staticmethod
    def _conv_cache_payload(conv) -> dict:
        """Полный набор полей чата, который нужен дальше по коду (в т.ч. SimpleNamespace)."""
        channel_type = getattr(conv, "channel_type", None)
        channel_type = getattr(channel_type, "value", channel_type)
        line_id = getattr(conv, "line_id", None)
        return {
            "id": str(conv.id),
            "is_closed": bool(conv.is_closed),
            "client_phone": getattr(conv, "client_phone", None),
            "channel_type": channel_type,
            "line_id": str(line_id) if line_id else None,
        }

    async def conv_msg(
        self,
        data: MessageCreate,
        current_user: UserContext = Depends(get_current_user),
    ) -> Tuple[Conversation, Message]:
        # Verify conversation exists and belongs to tenant

        cache_key = f"conv:{data.conversation_id}"
        cached = await rdb.get(cache_key)
        if cached:
            conv = SimpleNamespace(**json.loads(cached))
        else:
            conv = await self.conv_repo.get_by_id(data.conversation_id)
            if not conv:
                raise ValueError("Conversation not found")
            await rdb.set(cache_key, json.dumps(self._conv_cache_payload(conv)), ex=10)

        # Автооткрытие закрытого чата при новом сообщении
        if conv.is_closed:
            await self.conv_repo.set_conversation_closed(data.conversation_id, False)
            await self.db.commit()
            conv.is_closed = False
            await rdb.delete(f"conv:{data.conversation_id}")
            await rdb.set(cache_key, json.dumps(self._conv_cache_payload(conv)), ex=10)

        message = await self.msg_repo.create_message(
            user_id=current_user.id if data.direction == MessageDirection.outbound else None,
            conversation_id=data.conversation_id,
            channel_msg_id=data.channel_msg_id,
            direction=data.direction,
            text=data.text,
            raw_content=data.raw_content
        )

        if data.media:
            for m in data.media:
                m_db = {k: v for k, v in m.items() if k != "url"}
                try:
                    async with self.db.begin_nested():
                        await self.media_repo.create(message_id=message.id, **m_db)
                except Exception as e:
                    log.error(
                        "\n\n !!! --- ERROR --- !!! "
                        "chat/app/services/chat_service.py "
                        "ChatService.conv_msg "
                        f"media create failed e={e}\n\n"
                    )

        return conv, message

    
    async def _finalize_message(
            self,
            message,
            conversation_id: uuid.UUID,
            channel_type: str,
            data: MessageCreate,
            current_user: UserContext,
            success: bool = True,
            channel_msg_id: str = None,
            raw_content: dict = None,
            error: Exception = None,
    ):
        """Общая обработка результата отправки"""
        if not success:
            # Ошибка отправки в канал: фиксируем статус error и сообщаем фронту
            error_text = str(error) if error else "send failed"
            log.error(f"_finalize_message send failed conversation_id={conversation_id} error={error_text}")
            try:
                await self.msg_repo.update(message, status=MessageStatus.error)
            except Exception as e:
                log.error(
                    "\n\n !!! --- ERROR --- !!! "
                    "chat/app/services/chat_service.py "
                    "ChatService._finalize_message "
                    f"set error status failed e={e}\n\n"
                )
            try:
                await manager.send_message_status(str(conversation_id), {
                    "message_id": str(message.id),
                    "status": "error",
                    "error": error_text,
                })
            except Exception as e:
                log.error(
                    "\n\n !!! --- ERROR --- !!! "
                    "chat/app/services/chat_service.py "
                    "ChatService._finalize_message "
                    f"error status notify failed e={e}\n\n"
                )
            return

        if success:
            # 2. Обновляем message (channel_msg_id, raw_content)
            if channel_msg_id or raw_content:
                await self.msg_repo.update(
                    message,
                    channel_msg_id=channel_msg_id,
                    raw_content=raw_content
                )
            
            # 3. Статус sent
            try:
                await manager.send_message_status(str(conversation_id), {
                    "message_id": str(message.id),
                    "status": "sent",
                    # "mime_type": data.raw_content.get("mime_type"),
                    "media": [{**m, "url": self.storage.presigned_url(m["id"])} for m in (data.media or [])],
                })
            except Exception as e:
                log.error(
                    "\n\n !!! --- ERROR --- !!! "
                    "chat/app/services/chat_service.py "
                    "ChatService._finalize_message "
                    f"send_message_status failed e={e}\n\n"
                )


    async def upload(self, media_id: uuid.UUID, file: UploadFile) -> dict:
        """Сохранить загруженный файл в S3 (вызывается из /upload)."""
        if self.storage.exists(media_id):
            raise HTTPException(status_code=409, detail="Media already exists")
        max_size = 100 * 1024 * 1024
        file_bytes = await file.read(max_size + 1)
        if len(file_bytes) > max_size:
            raise HTTPException(status_code=413, detail="File too large")
        return await self.storage.save_file(file_bytes, media_id, file.filename, file.content_type)

    async def _save_uploaded_file(self, file: UploadFile, media_id: uuid.UUID) -> dict:
        file_bytes = await file.read()
        return await self.storage.save_file(file_bytes, media_id, file.filename, file.content_type)


    def clean_raw_content(self, obj):
        """Рекурсивно преобразует все datetime в строки"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return base64.b64encode(obj).decode('ascii')
        elif isinstance(obj, dict):
            return {k: self.clean_raw_content(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self.clean_raw_content(i) for i in obj]
        return obj


    async def update_message_status(self, external_id: str, status: str) -> None:
        """Обновить статус сообщения по внешнему (канальному) id."""
        message = await self.msg_repo.get_message_by_channel_msg_id(external_id)
        if not message:
            log.warning(f"update_message_status: message not found external_id={external_id}")
            return
        await self.msg_repo.set_message_status(message.id, status)
        await self.db.commit()
        try:
            await manager.send_message_status(str(message.conversation_id), {
                "message_id": str(message.id),
                "status": status,
            })
        except Exception as e:
            log.error(
                "\n\n !!! --- ERROR --- !!! "
                "chat/app/services/chat_service.py "
                "ChatService.update_message_status "
                f"ws notify failed e={e}\n\n"
            )

    async def get_or_create_conversation(self, line_id: str, peer_phone: str,  channel_type: str):
        phone = (peer_phone or "").strip()
        if not phone:
            res = await self.conv_repo.get_conv_by_line_and_channel_type(line_id, channel_type)
            if not res:
                res = await self.conv_repo.create_conversation(line_id, channel_type)
            return res
        digits = phone.lstrip("+")
        found = await self.conv_repo.get_multi(
            filters={
                "line_id": line_id,
                "channel_type": channel_type,
                "client_phone": [phone, digits, f"+{digits}"],
            },
            limit=1,
        )
        if found:
            return found[0]
        return await self.conv_repo.create_conversation(line_id, channel_type, client_phone=phone)

    async def save_incoming_message(
            self,
            conversation_id: uuid.UUID | None,
            peer_phone: str,
            event,
            media_id: uuid.UUID = None,
            file_name: str = None,
            mime_type: str = None,
            media_items: list | None = None,
            text: str | None = None,
            channel_msg_id: str | None = None,
    ) -> MessageResponse:

        if conversation_id is None:
            conversation = await self.get_or_create_conversation(
                line_id=event.line_id,
                peer_phone=peer_phone,
                channel_type="telegram_user",
            )
            conversation_id = conversation.id

        media = None

        if media_items:
            media = self._normalize_media_items(media_items)
        elif media_id:
            media = [
                {
                    "id": str(media_id),
                    "file_name": file_name,
                    "mime_type": mime_type,
                }
            ]

        data = MessageCreate(
            conversation_id=conversation_id,
            direction=MessageDirection.inbound,
            text=text if text is not None else (event.message.text if event.message.text else None),
            channel_msg_id=channel_msg_id or str(event.message.id),
            media=media,
        )
        _, message = await self.conv_msg(data)

        await self._publish_message(
            message,
            peer_phone,
            data.media,
        )
        
        return MessageResponse.model_validate(message)

    async def _publish_message(
            self,
            message,
            peer_phone: str,
            media_items: list | None = None,
    ):
        from app.services.websocket_manager import manager

        media = []
        for item in (media_items or message.media or []):
            media.append({
                **item,
                "url": self.storage.presigned_url(uuid.UUID(item["id"])),
            })

        await manager.send_new_message(
            message.conversation_id,
            {
                "id": str(message.id),
                "text": message.text,
                "direction": message.direction.value,
                "created_at": message.created_at.isoformat(),
                "phone": peer_phone,
                "media": media,
                "is_new": True,
            },
        )

    async def edit_message(
            self,
            conversation_id: uuid.UUID,
            channel_msg_id: str,
            text: str | None,
    ):
        messages = await self.msg_repo.get_multi(
            filters={
                "conversation_id": conversation_id,
                "channel_msg_id": channel_msg_id,
            }
        )

        if not messages:
            log.error(
                f"edit_message: message not found "
                f"conv={conversation_id} msg_id={channel_msg_id}"
            )
            return

        message = messages[0]

        await self.msg_repo.update(
            message,
            text=text or "",
        )

        await self.db.commit()

        await manager.send_message_update(
            str(conversation_id),
            {
                "type": "edit",
                "message_id": str(message.id),
                "text": text or "",
            },
        )

    async def delete_incoming_message(
            self,
            conversation_id: uuid.UUID,
            channel_msg_ids: list[str],
    ):
        messages = await self.msg_repo.get_multi(
            filters={
                "conversation_id": conversation_id,
                "channel_msg_id": channel_msg_ids,
            }
        )

        if not messages:
            return

        for message in messages:
            await self.msg_repo.delete(message)

        await self.db.commit()

        for message in messages:
            await manager.send_message_update(
                str(conversation_id),
                {
                    "type": "delete",
                    "message_id": str(message.id),
                },
            )


    @staticmethod
    def _normalize_media_items(media_items: list | None) -> list:
        """Привести вложения к единому виду: ключ `id` (а не media_id), file_name, mime_type."""
        result = []
        for item in (media_items or []):
            if not item:
                continue
            if not isinstance(item, dict):
                continue
            media_id = item.get("id") or item.get("media_id")
            if not media_id:
                continue
            result.append({
                "id": str(media_id),
                "file_name": item.get("file_name") or item.get("fileName"),
                "mime_type": item.get("mime_type") or item.get("mimeType"),
            })
        return result

    async def save_incoming_green(
            self,
            conversation_id: uuid.UUID,
            payload: dict,
            media_id: uuid.UUID | None = None,
            file_name: str | None = None,
            file_url: str | None = None,
            original_file_name: str | None = None,
            mime_type: str | None = None,
            text: str | None = None,
    ) -> MessageResponse:
        """Сохранить входящее сообщение из WhatsApp Green"""

        if file_url:
            async with httpx.AsyncClient() as client:
                res = await client.get(file_url)
                if res:
                    file_bytes = res.content
                    if file_bytes:
                        await self.storage.save_file(file_bytes, media_id, original_file_name or file_name, mime_type)

        raw_content = payload
        if mime_type:
            raw_content["mime_type"] = mime_type

        media = []
        if media_id:
            media = self._normalize_media_items([{
                "id": str(media_id),
                "file_name": original_file_name or file_name,
                "mime_type": mime_type,
            }])

        data = MessageCreate(
            conversation_id=conversation_id,
            direction=MessageDirection.inbound,
            text=text,
            channel_msg_id=payload.get("idMessage"),
            media=media,
            raw_content=raw_content
        )

        _, message = await self.conv_msg(data)
        return MessageResponse.model_validate(message)



    async def save_incoming_waba(
            self,
            conversation_id: uuid.UUID,
            external_id: str,
            text: str | None,
            media_items: list | None = None,
            raw_data: dict | None = None
    ) -> MessageResponse:
        """Сохранить входящее сообщение из WABA"""

        data = MessageCreate(
            conversation_id=conversation_id,
            direction=MessageDirection.inbound,
            text=text or "",
            channel_msg_id=external_id,
            media=self._normalize_media_items(media_items),
            raw_content=raw_data,
        )

        _, message = await self.conv_msg(data, None)
        return MessageResponse.model_validate(message)

    async def save_incoming_vk(
            self,
            conversation_id: uuid.UUID,
            external_id: str,
            text: str | None,
            msg_type: str,
            media_items: list | None = None,
            raw_data: dict | None = None
    ) -> MessageResponse:
        data = MessageCreate(
            conversation_id=conversation_id,
            direction=MessageDirection.inbound,
            text=text or "",
            channel_msg_id=external_id,
            media=self._normalize_media_items(media_items),
            raw_content=raw_data
        )
        _, message = await self.conv_msg(data, None)
        return MessageResponse.model_validate(message)

    async def close_or_open_conversation(
        self,
        conversation_id: uuid.UUID,
        is_closed: bool,
        user_id: uuid.UUID,
    ) -> Conversation:
        tenants = await self.tenant_repo.get_all_tenants(user_id=user_id)
        tenant_ids = [t.id for t in tenants]
    
        conv = await self.conv_repo.get_by_id(conversation_id)
        if conv is None:
            raise HTTPException(status_code=404, detail="Conversation not found")
    
        line = await self.line_repo.get_by_id(conv.line_id)
        if line.tenant_id not in tenant_ids:
            raise HTTPException(status_code=403, detail="Forbidden")
    
        updated = await self.conv_repo.set_conversation_closed(conversation_id, is_closed)
        await self.db.commit()
        await rdb.delete(f"conv:{conversation_id}")
        await self.db.commit()
        return updated

    async def save_crm_incoming(self, data: CrmMessageIn):

        conv = await self.conv_repo.get_multi(
            filters={"line_id": data.line_id, "crm_chat_id": data.crm_chat_id},
            limit=1
        )

        if not conv:
            return {"status": "not_found"}
        conv = conv[0]


        msg = await self.msg_repo.create(
            conversation_id=conv.id,
            direction=MessageDirection.inbound,
            status=MessageStatus.sent,
            text=data.text,
            raw_content=data.raw_content,
        )

        for m in data.media:
            await self.media_repo.create(
                message_id=msg.id,
                id=uuid.UUID(m["id"]),
                file_name=m["file_name"],
                mime_type=m["mime_type"],
            )


        await manager.send_new_message(str(conv.id), {
            "id": str(msg.id),
            "text": msg.text,
            "direction": "inbound",
            "created_at": msg.created_at.isoformat(),
            "status": "sent",
            "media": [{"id": m["id"], "file_name": m["file_name"], "mime_type": m["mime_type"]} for m in data.media],
        })

        # channel_acc = await self.ch_acc_repo.get_by_line_and_type(conv.line_id, conv.channel_type)
        # creds = decrypt(channel_acc.credentials_encrypted) if channel_acc else None

        channel_acc = await self.conv_repo.get_channel_acc_by_conversation(conv.id)
        creds = None
        if channel_acc:
            creds = json.loads(
                decrypt(channel_acc.credentials_encrypted["encrypted_data"])
            )


        if conv.channel_type == "telegram_bot" and creds:
            from app.senders.telegram_bot import TelegramBotSender
            sender = TelegramBotSender()
            sender.set_bot_token(creds["bot_token"])
            await sender.send(MessageCreate(
                conversation_id=conv.id,
                direction=MessageDirection.outbound,
                phone=conv.client_phone,
                text=data.text,
                status=MessageStatus.sent,
                media=data.media
            ))

        return {"status": "ok"}


    async def _crm_channel(self, data: CrmMessageOut):
        if data.channel_account_id:
            acc = await self.ch_acc_repo.get_by_id(
                data.channel_account_id
            )
        else:
            found = await self.ch_acc_repo.get_multi(
                filters={
                    "line_id": data.line_id,
                    "channel_type": data.channel_type,
                },
                limit=1,
            )
            acc = found[0] if found else None
        if not acc:
            return None
        creds = json.loads(
            decrypt(acc.credentials_encrypted["encrypted_data"])
        )
        ch_type = getattr(acc.channel_type, "value", acc.channel_type)
        return acc, creds, ch_type

    async def start_crm_outgoing(self, data: CrmMessageOut) -> dict:
        convs = await self.conv_repo.get_multi(
            filters={"line_id": data.line_id, "client_phone": data.client_phone},
            limit=1,
        )
        if convs:
            conv = convs[0]
            if conv.crm_chat_id:
                return {"status": "exists", "conversation_id": str(conv.id)}
        else:
            conv = await self.conv_repo.create(
                line_id=data.line_id,
                client_phone=data.client_phone,
                channel_type=data.channel_type,
                crm_chat_id=f"dc_{data.crm_connection_id}",
            )

        found = await self._crm_channel(data)
        if not found:
            return {"error": "Канал не найден"}
        _, creds, channel_type = found

        sender = SenderFactory.get_sender(channel_type)
        if hasattr(sender, "set_bot_token"):
            sender.set_bot_token(creds.get("bot_token"))

        await sender.send(MessageCreate(
            conversation_id=conv.id,
            direction=MessageDirection.outbound,
            phone=data.client_phone,
            text=data.text,
            status=MessageStatus.sent,
        ))

        return {"status": "ok", "conversation_id": str(conv.id)}

    async def robot_send(self, data: CrmMessageOut) -> dict:
        filters = {"line_id": data.line_id, "client_phone": data.client_phone}
        if data.channel_type:
            filters["channel_type"] = data.channel_type
        
        convs = await self.conv_repo.get_multi(
            filters=filters,
            limit=1,
        )
        if convs:
            conv = convs[0]
        else:
            conv = await self.conv_repo.create(
                line_id=data.line_id,
                client_phone=data.client_phone,
                channel_type=data.channel_type,
                crm_chat_id=f"dc_{data.crm_connection_id}",
            )

        found = await self._crm_channel(data)
        if not found:
            return {"error": "Канал не найден"}
        _, creds, channel_type = found

        msg = await self.msg_repo.create(
            conversation_id=conv.id,
            direction=MessageDirection.outbound,
            status=MessageStatus.sent,
            text=data.text,
        )

        media = []
        if data.media:
            for m in data.media:
                await self.media_repo.create(
                    message_id=msg.id,
                    id=uuid.UUID(m["id"]),
                    file_name=m["file_name"],
                    mime_type=m["mime_type"],
                )
            media = data.media

        # sender = SenderFactory.get_sender(channel_type)
        sender = await self._init_sender(channel_type, conv.id)
        if hasattr(sender, "set_bot_token"):
            sender.set_bot_token(creds.get("bot_token"))

        await sender.send(MessageCreate(
            conversation_id=conv.id,
            direction=MessageDirection.outbound,
            phone=data.client_phone,
            text=data.text,
            status=MessageStatus.sent,
            media=media,
        ))

        return {"status": "ok", "conversation_id": str(conv.id), "message_id": str(msg.id)}