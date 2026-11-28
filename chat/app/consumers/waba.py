import logging
import secrets
import httpx
import json
import uuid
from typing import Dict, TYPE_CHECKING
from fastapi import HTTPException
from core.config import settings
from app.core.crm_flag import crm_enabled
from core.database import get_session
from core.metrics import webhook_processing_seconds, webhook_errors_total
from core.redis import rdb
from infra.crypto import decrypt
from app.repositories.chat_repo import ChatRepository
from app.repositories.message_repo import MessageRepository
from notification.notification_caller import notify_connection_error
from repo.line_repo import ChannelAccountRepository
from app.services.websocket_manager import manager
from services.storage_service import StorageService
from app.core.crm_flag import crm_enabled

if TYPE_CHECKING:  # pragma: no cover
    from app.services.chat_service import ChatService


log = logging.getLogger(__name__)

# URL WABA API берём из настроек (по умолчанию — sandbox)
WABA_API_URL = settings.waba_api_url

class WabaConnection:
    """Соединение с 360dialog API для одной линии"""

    def __init__(self, api_key: str, line_id=None, db=None):
        self.api_key = api_key
        self.line_id = line_id
        self.db = db
        self.headers = {
            "D360-API-KEY": api_key,
            "Content-Type": "application/json"
        }

    async def send(self, endpoint: str, payload: dict) -> dict:
        url = f"{WABA_API_URL}/{endpoint}"
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=payload, headers=self.headers)
                response.raise_for_status()
                return response.json()
        except Exception as e:
            log.error(f"WabaConnection.send failed: {e}")
            try:
                await notify_connection_error(self.line_id, channel_type="waba", db=self.db)
            except Exception as ne:
                log.error(f"notify_connection_error failed: {ne}")
            raise

    async def send_message(self, to: str, text: str) -> dict:
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "text",
            "text": {"preview_url": False, "body": text},
        }
        return await self.send("messages", payload)

    async def send_template(self, to: str, template_name: str, language: str = "ru",
                            components: list | None = None) -> dict:
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "template",
            "template": {
                "name": template_name,
                "language": {"code": language},
                "components": components or [],
            },
        }
        return await self.send("messages", payload)


    async def send_file(self, to: str, file_url: str, mime_type: str | None = None, caption: str = "") -> dict:
        # Определяем тип медиа по расширению или mime_type
        # Упрощенно: считаем что это документ
        
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "document",
            "document": {
                "link": file_url,
                "caption": caption
            }
        }
        pl = {"link": file_url, "caption": caption}
        mime_main = (mime_type or "").split("/")[0]
        if mime_main not in ("image", "audio", "video"):
            mime_main = "document"
        payload.pop("document", None)
        payload["type"] = mime_main
        payload[mime_main] = pl


        return await self.send("messages", payload)


class WabaListener:
    """Слушатель вебхуков от 360dialog (Meta Cloud API)"""

    def __init__(self):
        self.connections: Dict[str, WabaConnection] = {}
        self.storage = StorageService()
        log.info("WabaListener initialized")

    from core.metrics import prometheus_metrics

    @prometheus_metrics("waba")
    async def handle_webhook(self, line_id: str, token: str, payload: dict):
        # prometheus metrics
        try:
            with webhook_processing_seconds.labels(channel_type="waba", event_type="message").time():
                from app.services.chat_service import ChatService
        
        
                # Парсим структуру Meta
                entries = payload.get("entry", [])
                if not entries:
                    log.error("WABA webhook payload has empty entry list")
                    return
                entry = entries[0]
                changes = entry.get("changes", [{}])[0]
                value = changes.get("value", {})
        
                if "messages" in value:
                    async with get_session() as db:
                        chat_service = ChatService(db)
                        await self._handle_incoming_message(line_id, value, chat_service, token)
        
                elif "statuses" in value:
                    await self._handle_message_status(line_id, value)
                else:
                    log.warning(f"Unhandled webhook structure: {value.keys()}")
            webhook_errors_total.labels(channel_type="waba", status="success").inc()
        except Exception:
            webhook_errors_total.labels(channel_type="waba", status="error").inc()
            raise



    async def download_media(self, media_url: str, api_key: str) -> bytes:  # передавай полный url из webhook

        if not media_url:
            raise ValueError("media_url is empty")

        headers = {"D360-API-KEY": api_key}

        if 'lookaside.fbsbx.com' in media_url:
            # Проксируем через 360dialog
            proxy_url = media_url.replace('https://lookaside.fbsbx.com', 'https://waba-v2.360dialog.io')
            # Убери лишние слеши если есть (иногда \\)
            proxy_url = proxy_url.replace('\\', '')
        else:
            # Прямая ссылка — качаем как есть
            proxy_url = media_url

        async with httpx.AsyncClient() as client:
            resp = await client.get(proxy_url, headers=headers)
            if resp.status_code != 200:
                raise Exception(f"Failed: {resp.status_code} - {resp.text}")
            return resp.content

    async def _handle_incoming_message(self, line_id: str, value: dict, chat_service: "ChatService", token: str):
        with webhook_processing_seconds.labels(channel_type="waba", event_type="message").time():
            db = chat_service.conv_repo.session
            repo = ChannelAccountRepository(db)
            account = await repo.get_by_line_and_type(uuid.UUID(line_id), "waba")
            if not account:
                raise ValueError(f"No channel account for line {line_id}")
            creds = decrypt(account.credentials_encrypted["encrypted_data"])
            api_key = json.loads(creds)["api_key"]
            creds_token =  json.loads(creds)["token"]
            
            
            if not secrets.compare_digest(
                str(creds_token).encode(), str(token).encode()
            ):
                raise HTTPException(status_code=403, detail="Secure token mismatch")
    
            for msg in value.get("messages", []):
                msg_id = msg["id"]
    
                dedup_key = f"waba_msg:{msg_id}"
                if await rdb.get(dedup_key):
                    continue
                await rdb.set(dedup_key, "1", ex=300)
    
                from_number = msg["from"]
                msg_type = msg["type"]
    
                text = None
                media_id = None
                file_name = None
                mime_type = None
                file_bytes = None
    
                if msg_type == "text":
                    text = msg["text"]["body"]
                elif msg_type in ["image", "audio", "video", "document"]:
                    media = msg.get(msg_type, {})
                    if "id" in media:
                        # Скачиваем из WABA
                        try:
                            file_bytes = await self.download_media(media["url"], api_key)
                            mime_type = media.get("mime_type")
                            file_name = media.get("filename")
                            media_id = uuid.uuid4()
                        except Exception as e:
                            log.error(
                                "\n\n !!! --- ERROR --- !!! "
                                "chat/app/consumers/waba.py "
                                "WabaListener._handle_incoming_message "
                                f"download_media failed e={e}\n\n"
                            )
                            file_bytes = None
                            mime_type = None
                            file_name = None
                            media_id = None
                        # Сохраняем в MinIO
                        if file_bytes:
                            await self.storage.save_file(
                                file_bytes=file_bytes,
                                media_id=media_id,
                                filename=file_name or f"{media_id}.bin",
                                mime_type=mime_type or "application/octet-stream"
                            )
                    text = media.get("caption", "")
    
                phone = f"+{from_number}"
    
                conv = await chat_service.conv_repo.get_conversations_by_phone(line_id, phone, "waba")
                if not conv:
                    log.warning(f"Conversation not found for line {line_id}, phone {phone}")
                    continue
    
                media_items = []
                if media_id:
                    media_items = [{
                        "id": str(media_id),
                        "file_name": file_name,
                        "mime_type": mime_type,
                    }]
    
                message = await chat_service.save_incoming_waba(
                    conversation_id=conv.id,
                    external_id=msg_id,
                    text=text,
                    media_items=media_items if media_items else None,
                    raw_data=value,
                )
    
                ws_message = {
                    "id": str(message.id),
                    "text": message.text,
                    "direction": "inbound",
                    "created_at": message.created_at.isoformat(),
                    "phone": phone,
                    "media": media_items,
                }
    
                await manager.send_new_message(conv.id, ws_message)
    
                crm_client = get_crm_client()
                if crm_client:
                    try:
                        await crm_client.send_to_crm(
                            line_id=line_id,
                            conversation_id=conv.id,
                            client_phone=phone,
                            client_name="",
                            text=text or "",
                            message_id=str(message.id),
                            channel_type="waba",
                            media=[{"id": m["id"], "file_name": m["file_name"], "mime_type": m["mime_type"], "file_size": 0} for m
                                   in media_items],
                        )
                    except Exception as e:


    async def _handle_message_status(self, line_id: str, value: dict):
        """Обработка статусов сообщений"""
        for status_update in value.get("statuses", []):
            msg_id = status_update["id"]
            status = status_update["status"]
            if status == "failed":
                log.warning(f"Message {msg_id} failed, skipping status update")
                continue

            dedup_key = f"waba_status:{msg_id}:{status}"
            if await rdb.get(dedup_key):
                continue
            await rdb.set(dedup_key, "1", ex=300)

            async with get_session() as db:
                msg_repo = MessageRepository(db)
                msg = await msg_repo.get_message_by_channel_msg_id(msg_id)
                if msg:
                    await msg_repo.set_message_status(msg.id, status)
                    await manager.send_message_status(str(msg.conversation_id), {
                        "message_id": str(msg.id),
                        "status": status,
                        "mime_type": msg.raw_content.get("mime_type") if msg.raw_content else None
                    })

    async def _get_connection(self, line_id: str) -> WabaConnection:
        if line_id not in self.connections:
            async with get_session() as db:
                repo = ChannelAccountRepository(db)
                account = await repo.get_by_line(uuid.UUID(line_id))
                if not account:
                    raise ValueError(f"No channel account for line {line_id}")
                creds = decrypt(account.credentials_encrypted["encrypted_data"])
                api_key = json.loads(creds)["api_key"]
                self.connections[line_id] = WabaConnection(
                    api_key, line_id=line_id
                )

        return self.connections[line_id]

    async def send_message(self, line_id: str, to: str, text: str) -> dict:
        connection = await self._get_connection(line_id)
        return await connection.send_message(to, text)

    async def send_file(self, line_id: str, to: str, file_url: str, caption: str = "") -> dict:
        connection = await self._get_connection(line_id)
        return await connection.send_file(to, file_url, caption=caption)