import logging
import secrets
import httpx
import uuid
from typing import Dict, TYPE_CHECKING
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from core.config import settings
from app.core.crm_flag import crm_enabled
from core.database import get_session
from app.repositories.message_repo import MessageRepository
from repo.line_repo import ChannelAccountRepository
from app.services.websocket_manager import manager
from notification.notification_caller import notify_connection_error
from app.core.crm_flag import crm_enabled
from app.core.green_token import green_webhook_token

if TYPE_CHECKING:  # pragma: no cover
    from app.services.chat_service import ChatService


log = logging.getLogger(__name__)

class GreenWhatsAppConnection:
    """Соединение с Green API для одной линии"""

    def __init__(self, id_instance: str, api_token: str, line_id=None, db: AsyncSession =None,):
        self.id_instance = id_instance
        self.api_token = api_token
        self.line_id = line_id
        self.db = db

    def api_url(self):
        return f"{settings.proxy_green_url}/waInstance{self.id_instance}"

    def media_url(self):
        return f"{settings.proxy_green_url}/waInstance{self.id_instance}"

    def send_text_url(self):
        return f"{self.api_url()}/sendMessage/{self.api_token}"

    def send_file_url(self):
        return f"{self.media_url()}/sendFileByUrl/{self.api_token}"

    async def send(self, url, payload):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                return response.json()
        except Exception as e:
            log.error(
                "\n\n !!! --- ERROR --- !!! "
                "chat/app/consumers/green_whatsapp.py "
                "GreenWhatsAppConnection.send failed "
                f"e={e}\n\n"
            )
            try:
                await notify_connection_error(self.line_id, channel_type="whatsapp_green", db=self.db)
            except Exception as ne:
                log.error(f"notify_connection_error failed: {ne}")
            raise


    async def send_message(self, chat_id: str, text: str) -> dict:
        """Отправить сообщение через Green API"""
        url = self.send_text_url()
        payload = {
            "chatId": chat_id,
            "message": text
        }
        return await self.send(url, payload)

    async def send_file(self, chat_id: str, file_url: str, caption: str = "", file_name: str = "file") -> dict:
        """Отправить файл"""
        url = self.send_file_url()
        payload = {
            "chatId": chat_id,
            "urlFile": file_url,
            "fileName": file_name,
            "caption": caption
        }
        return await self.send(url, payload)

    def edit_message_url(self):
        return f"{self.api_url()}/editMessage/{self.api_token}"
    
    def delete_message_url(self):
        return f"{self.api_url()}/deleteMessage/{self.api_token}"
    
    async def edit_message(self, chat_id: str, id_message: str, text: str) -> dict:
        url = self.edit_message_url()
        payload = {
            "chatId": chat_id,
            "idMessage": id_message,
            "message": text
        }
        return await self.send(url, payload)
    
    async def delete_message(self, chat_id: str, id_message: str) -> dict:
        url = self.delete_message_url()
        payload = {
            "chatId": chat_id,
            "idMessage": id_message,
            "onlySenderDelete": False
        }
        return await self.send(url, payload)


class GreenWhatsAppListener:
    """Слушатель вебхуков от Green API"""

    def __init__(self):
        self.connections: Dict[str, GreenWhatsAppConnection] = {}
        log.info("GreenWhatsAppListener initialized")

    from core.metrics import prometheus_metrics

    @prometheus_metrics("whatsapp_green")
    async def handle_webhook(self, line_id: str, payload: dict, token):
            """Обработать входящий вебхук от Green API"""
            from app.services.chat_service import ChatService
            instance_id = str(
                (payload.get("instanceData") or {}).get("idInstance") or ""
            )
            expected = green_webhook_token(instance_id)
            if not token or not expected or not secrets.compare_digest(
                str(token).encode(), expected.encode()
            ):
                msg = "Green webhook auth token mismatch"
                log.error(f"!!! -- ERROR -- {msg} line_id={line_id}")
                raise HTTPException(status_code=403, detail=msg)
    
            event_type = payload.get("typeWebhook")
            log.info(f"Green webhook for line {line_id}, type: {event_type}")
    
            # !!! ----  remove after wa queue will stop to spam ---- !!!
            # if event_type != "incomingMessageReceived":
            #    return
    
            if event_type == "incomingMessageReceived":
                async with get_session() as db:
                    chat_service = ChatService(db)
                    await self._handle_incoming_message(line_id, payload, chat_service, db)
            elif event_type == "outgoingMessageStatus":
                await self._handle_message_status(line_id, payload)
            elif event_type == "stateInstanceChanged":
                await self._handle_instance_state(line_id, payload)
            else:
                log.warning(f"Unhandled webhook type: {event_type}")

    async def _handle_incoming_message(self, line_id: str, payload: dict, chat_service: "ChatService", db: AsyncSession):
        """Обработка входящего сообщения"""
        message_data = payload.get("messageData", {})
        sender_data = payload.get("senderData", {})

        # Извлекаем номер телефона из chatId (вида 79991234567@c.us)
        chat_id = sender_data.get("chatId", "")
        phone = chat_id.replace("@c.us", "")
        phone = f"+{phone}"

        msg_type = message_data.get("typeMessage")

        media_id = None
        file_url = None
        file_name = None
        mime_type = None
        original_file_name = None


        text = None
        if msg_type == "editedMessage":
            edited_data = message_data.get("editedMessageData", {})
            stanza_id = edited_data.get("stanzaId")
            new_text = edited_data.get("textMessage")
            text = new_text
            msg_repo = MessageRepository(db)
            message = await msg_repo.get_message_by_channel_msg_id(stanza_id)

            if message:
                msg_conv = await chat_service.conv_repo.get_by_id(
                    message.conversation_id
                )
                if not msg_conv or str(msg_conv.line_id) != str(line_id):
                    message = None

            if message:
                await msg_repo.update(message, text=new_text)
                await manager.send_message_update(str(message.conversation_id), {
                    "type": "edit",
                    "message_id": str(message.id),
                    "text": new_text,
                })
                
                crm_client = get_crm_client()
                if crm_client:
                    try:
                        await crm_client.edit_to_crm(
                            line_id=line_id,
                            conversation_id=message.conversation_id,
                            message_id=str(message.id),
                            text=new_text,
                            channel_type="whatsapp_green",
                        )
                    except Exception as e:
                        log.error(f"CRM edit_to_crm failed: {e}")

            return


        conv = None
        try:
            conv = await chat_service.conv_repo.get_conversations_by_phone(line_id, phone, "whatsapp_green")
        except Exception as e:
            log.error(
                "\n\n !!! --- ERROR --- !!! "
                "chat/app/consumers/green_whatsapp.py "
                "GreenWhatsAppListener._handle_incoming_message "
                f"get_conversations_by_phone failed e={e}\n\n"
            )

        if msg_type == "deletedMessage":
            stanza_id = message_data.get("deletedMessageData", {}).get("stanzaId")
            if stanza_id:
                msg_repo = MessageRepository(db)
                message = await msg_repo.get_message_by_channel_msg_id(stanza_id)
                if message:
                    msg_conv = await chat_service.conv_repo.get_by_id(
                        message.conversation_id
                    )
                    if not msg_conv or str(msg_conv.line_id) != str(line_id):
                        message = None
                if message:
                    crm_message_id = (message.raw_content or {}).get("crm_message_id") or message.id
                    conversation_id = message.conversation_id
                    if not conversation_id:
                        conversation_id = conv.id
                    message_id_local = message.id

                    await msg_repo.delete(message)
                    await manager.send_message_update(str(conversation_id), {
                        "type": "delete",
                        "message_id": str(message_id_local),
                    })

                    crm_client = get_crm_client()
                    if crm_message_id and crm_client:
                        try:
                            await crm_client.delete_from_crm(
                                line_id=line_id,
                                conversation_id=conversation_id,
                                client_phone="",
                                message_id=str(crm_message_id),
                                channel_type="whatsapp_green",
                            )
                        except Exception as e:
                            log.error(f"CRM delete_from_crm failed: {e}")
            return


        if msg_type == "textMessage":
            text = message_data.get("textMessageData", {}).get("textMessage")
        if msg_type in ["imageMessage", "videoMessage", "documentMessage", "audioMessage"]:
            media_data = message_data.get("fileMessageData", {})  # всегда fileMessageData
            text = media_data.get("caption", "")
            file_url = media_data.get("downloadUrl")
            mime_type = media_data.get("mimeType")
            media_id = uuid.uuid4()
            file_name_rcv = media_data.get("fileName")
            ext = None
            if msg_type == "imageMessage":
                ext = (mime_type or "").split("/")[1] if mime_type else None
            elif file_name_rcv and "." in file_name_rcv:
                ext = file_name_rcv.split(".")[-1]
            if ext:
                file_name = f"{media_id}.{ext}"
            else:
                file_name = media_id
            original_file_name = file_name_rcv

        if conv is None:
            return


        # Сохраняем в БД
        try:
            message = await chat_service.save_incoming_green(
                conversation_id=conv.id,
                payload=payload,
                media_id=media_id,
                file_name=file_name,
                file_url=file_url,
                original_file_name=original_file_name,
                mime_type=mime_type,
                text=text,
            )
        except Exception as e:
            log.error(
                "\n\n !!! --- ERROR --- !!! "
                "chat/app/consumers/green_whatsapp.py "
                "GreenWhatsAppListener._handle_incoming_message "
                f"save_incoming_green failed e={e}\n\n"
            )
            return


        ws_message = {
            "id": str(message.id),
            "text": message.text,
            "direction": "inbound",
            "created_at": message.created_at.isoformat(),
            "phone": phone
        }


        if media_id:
            ws_message["media"] = [
                {
                    "id": str(media_id),
                    "file_name": original_file_name,
                    "mime_type": mime_type,
                    "url": chat_service.storage.presigned_url(media_id)
                }
            ]

        await manager.send_new_message(conv.id, ws_message)

        crm_client = get_crm_client()
        if not crm_client:
            return

        try:
            sender_name = sender_data.get("senderName", "")
            log.info(f"green wh send_to_crm line_id {line_id} conversation_id {conv.id}")

            await crm_client.send_to_crm(
                line_id=line_id,
                conversation_id=conv.id,
                client_phone=phone,
                client_name=sender_name,
                text=text or "",
                message_id=str(message.id),
                channel_type="whatsapp_green",
                media=[{"id": str(media_id), "file_name": original_file_name, "mime_type": mime_type,
                        "file_size": 0}] if media_id else [],
            )
        except Exception as e:
            log.error(f"CRM send_to_crm failed: {e}")

    async def _handle_message_status(self, line_id: str, payload: dict):
        external_id = payload.get("idMessage")
        status = payload.get("status")

        if not external_id or not status:
            log.warning(f"Missing fields in message status: {payload}")
            return

        async with get_session() as db:
            chat_repo = MessageRepository(db)
            msg = await chat_repo.get_message_by_channel_msg_id(external_id)
            if status == "failed":
                status = "error"
            if msg:
                await chat_repo.set_message_status(msg.id, status)
                await manager.send_message_status(str(msg.conversation_id), {
                    "message_id": str(msg.id),
                    "status": status
                })

    async def _handle_instance_state(self, line_id: str, payload: dict):
        """Обработка изменения статуса инстанса"""
        state = payload.get("stateInstance")
        conn_status = state
        error_reason = ''
        if state in ["blocked", "sleepMode", "notAuthorized"]:
            log.warning(f"Green instance state={state} line_id={line_id}")
            await notify_connection_error(line_id, "whatsapp_green")
            conn_status = "error" if state == "blocked" else "disconnected"
            error_reason = "account_blocked" if state == "blocked" else "phone_offline"
        elif state == "authorized":
            conn_status = "connected"
        elif state == "starting":
            conn_status = "disconnected"
        else:
            return

        async with get_session() as db:
            try:
                repo = ChannelAccountRepository(db)
                await repo.update_connection_status(
                    line_id=line_id,
                    status=conn_status,
                    channel_type="whatsapp_green",
                    connection_error=error_reason
                )
                if conn_status in ("error", "disconnected"):
                    await notify_connection_error(line_id, "whatsapp_green",db)
            except Exception as e:
                log.error(
                    "\n\n !!! --- ERROR --- !!! "
                    "chat/app/consumers/green_whatsapp.py "
                    "GreenWhatsAppListener._handle_instance_state "
                    f"update_connection_status failed e={e}\n\n"
                )
                raise


    async def send_message(self, line_id: str, chat_id: str, text: str) -> dict:
        """Отправить сообщение (вызывается из chat_service)"""
        connection = self.connections.get(line_id)
        if not connection:
            raise ValueError(f"No connection for line {line_id}")

        return await connection.send_message(chat_id, text)


# Глобальный экземпляр слушателя (будет создаваться в main.py)
# green_listener = GreenWhatsAppListener()