import httpx
import secrets
import uuid
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.repositories.chat_repo import ChatRepository
from app.repositories.message_repo import MessageRepository
from app.services import websocket_manager
from core.config import settings
from app.services.crm_client import get_crm_client
from core.metrics import webhook_processing_seconds, webhook_errors_total
from infra.utils import decrypt_credentials
from repo.line_repo import ChannelAccountRepository, LineRepository
from repo.membership_repo import MembershipRepository
from repo.message_repo import MediaRepository
from services.storage_service import StorageService

import logging
log = logging.getLogger("uvicorn.error")

class BotService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.channel_repo = ChannelAccountRepository(db)
        self.line_repo = LineRepository(db)
        self.conv_repo = ChatRepository(db)
        self.msg_repo = MessageRepository(db)
        self.media_repo = MediaRepository(db)
        self.membership_repo = MembershipRepository(db)
        self.storage = StorageService()

    async def _handle_edited_message(self, line_id: uuid.UUID, line, edited: dict):
        chat = edited.get("chat", {})
        chat_id = chat.get("id")
        message_id = edited.get("message_id")
        text = edited.get("text") or edited.get("caption", "")

        if not chat_id or not message_id:
            log.error("Ignoring invalid edited_message update")
            return {"ok": True}

        conversation, _ = await self.conv_repo.get_or_create_by_chat_id(
            line_id=line_id,
            chat_id=str(chat_id),
            tenant_id=line.tenant_id,
            client_phone=chat.get("username") or str(chat_id),
        )

        messages = await self.msg_repo.get_multi(
            filters={"conversation_id": conversation.id, "channel_msg_id": str(message_id)}
        )
        if not messages:
            log.error(f"_handle_edited_message: message not found chat_id={chat_id} message_id={message_id}")
            return {"ok": True}

        db_message = messages[0]
        await self.msg_repo.update(db_message, text=text)

        await websocket_manager.manager.send_message_update(str(conversation.id), {
            "type": "edit",
            "message_id": str(db_message.id),
            "text": text,
        })

        crm_client = get_crm_client()
        if crm_client:
            try:
                await crm_client.edit_to_crm(
                    line_id=line.id,
                    conversation_id=conversation.id,
                    message_id=str(db_message.id),
                    text=text,
                    channel_type="telegram_bot",
                )
            except Exception as e:
                log.error(f"_handle_edited_message: edit_to_crm failed: {e}")

        return {"ok": True}


    async def handle_webhook(self, line_id: uuid.UUID, update_data: dict, request):
        """Handle incoming update from Telegram Bot API"""
        # Get line and bot token
        
        # prometheus wrap
        try:
            with webhook_processing_seconds.labels(channel_type="telegram_bot", event_type="message").time():
                line = await self.line_repo.get_by_id(line_id)
                if not line:
                    log.error(f"Line {line_id} not found")
                    return {"ok": False}
        
                channel_acc = await self.channel_repo.get_by_line_and_type(line_id, "telegram_bot")
                if not channel_acc:
                    log.error(f"Channel account not found line {line_id}")
                    return {"ok": False}
                creds = decrypt_credentials(channel_acc.credentials_encrypted)
                secret_token = request.headers.get("x-telegram-bot-api-secret-token")
                db_secret_token = creds.get("secret_token") or ""
                if not db_secret_token:
                    log.error(f"!!! -- ERROR --- secret_token is not configured for line {line_id}")
                    raise HTTPException(status_code=403, detail="Bot secure token is not configured")
                if not secret_token or not secrets.compare_digest(secret_token, db_secret_token):
                    log.error(f"!!! -- ERROR --- Invalid Telegram secret_token for line {line_id}")
                    raise HTTPException(status_code=403, detail="Bot secure token mismatch")
        
                edited = update_data.get("edited_message")
                if edited:
                    await self._handle_edited_message(line_id, line, edited)
                    return {"ok": True}

                # Extract message data
                message = update_data.get("message", {}) or update_data.get("edited_message", {}) 
                log.info(f"message {message}")
                chat = message.get("chat", {})
                log.info(f"chat {chat}")
                chat_id = chat.get("id")
                message_id = message.get("message_id")
        
                # Check if it's a valid message
                has_media = any(key in message for key in ["photo", "document", "video", "audio"])
                text = message.get("text") or message.get("caption", "")

                if not chat_id or (not text and not has_media):
                    log.error("Ignoring non-message update")
                    return {"ok": True}

                # Find or create conversation
                conversation, is_new = await self.conv_repo.get_or_create_by_chat_id(
                    line_id=line_id,
                    chat_id=str(chat_id),
                    tenant_id=line.tenant_id,
                    client_phone=chat.get("username") or str(chat_id),
                )
        
                log.info(f"is_new={is_new} conversation_id={conversation.id} chat_id={chat_id} line_id={line_id}")
        
                # Get bot token from line credentials
                bot_token = creds["bot_token"]
                raw_content = update_data

                # Process media if present
                media_id = None
                if has_media:
                    media_id, file_name, mime_type, raw_content, file_size = await self._process_media_message(
                        message, bot_token, raw_content
                    )
        
                db_message = await self.msg_repo.create_message(
                    conversation_id=conversation.id,
                    direction="inbound",
                    text=text,
                    channel_msg_id=str(message_id),
                    raw_content=raw_content,
                    user_id=None
                )
                if media_id:
                    await self.media_repo.create(message_id=db_message.id, id=media_id, file_name=file_name,
                                                 mime_type=mime_type)

                log.info(f"db_message {db_message.id}")
        
                # 8. Send to WebSocket
                ws_message = {
                    "id": str(db_message.id),
                    "text": text,
                    "direction": "inbound",
                    "created_at": db_message.created_at.isoformat(),
                    "phone": str(chat_id)
                }
        
        
                if media_id:
                    ws_message["media"] = [{"id": str(media_id), "file_name": file_name, "mime_type": mime_type, "url": self.storage.presigned_url(media_id)}]
        
                if is_new:
                    members = await self.membership_repo.get_by_tenant(line.tenant_id)
                    for m in members:
                        await websocket_manager.manager.send_new_conversation(
                            str(m.user_id),
                            {
                                "id": str(conversation.id),
                                "line_id": str(line_id),
                                "channel_type": "telegram_bot",
                                "client_phone": str(chat_id),
                                "crm_chat_id": str(chat_id),
                            }
                        )
        
        
                await websocket_manager.manager.send_new_message(str(conversation.id), ws_message)
                log.info("send_new_message done")
        
                crm_client = get_crm_client()
                if crm_client:
                    try:
                        await crm_client.send_to_crm(
                            line_id=line_id,
                            conversation_id=conversation.id,
                            client_phone=str(message.get("from", {}).get("id") or chat_id),
                            client_name=chat.get("first_name") or "",
                            text=text or "",
                            message_id=str(db_message.id),
                            channel_type="telegram_bot",
                            media=[{"id": str(media_id), "file_name": file_name, "mime_type": mime_type, "file_size": file_size}] if media_id else []
                        )
                    except Exception as e:
                        log.error(f"CRM send_to_crm failed: {e}")

            webhook_errors_total.labels(channel_type="telegram_bot", status="success").inc()
            return {"ok": True}
        except HTTPException:
            raise
        except Exception as e:
            log.error(
                "\n\n !!! --- ERROR --- !!! "
                "chat/app/services/bot_service.py "
                "BotService.handle_webhook "
                f"webhook failed e={e}\n\n"
            )
            webhook_errors_total.labels(channel_type="telegram_bot", status="error").inc()
            return {"ok": False}

    async def _process_media_message(
            self, message: dict, bot_token: str, raw_content: dict
    ) -> tuple[str | None, str | None, str | None, dict | None, int | None]:
        """
        Process media from Telegram message, download and save to S3
        Returns: (media_id, file_name, mime_type)
        """
        media_id = None
        file_name = None
        mime_type = None
        file_size = None
        file_bytes = None
        

        try:
            # Handle photo
            if "photo" in message:
                # Get the largest photo
                photo = message["photo"][-1]
                file_id = photo["file_id"]
                file_name = f"photo_{file_id}.jpg"
                mime_type = "image/jpeg"

                # Get file path from Telegram
                async with httpx.AsyncClient() as client:
                    # Get file info
                    file_info_url = f"{settings.tg_bot_send_url}/bot{bot_token}/getFile"
                    file_info_resp = await client.post(file_info_url, json={"file_id": file_id})
                    file_info = file_info_resp.json()

                    if not file_info.get("ok"):
                        log.error(f"Failed to get file info: {file_info}")
                        return None, None, None, None, None

                    file_path = file_info["result"]["file_path"]

                    # Download file
                    file_url = f"{settings.tg_bot_send_url}/file/bot{bot_token}/{file_path}"
                    file_resp = await client.get(file_url)
                    file_bytes = file_resp.content
                    file_size = len(file_bytes)

            # Handle document
            elif "document" in message:
                doc = message["document"]
                file_id = doc["file_id"]
                file_name = doc.get("file_name", "document")
                mime_type = doc.get("mime_type", "application/octet-stream")

                # Same download logic as above
                async with httpx.AsyncClient() as client:
                    file_info_url = f"{settings.tg_bot_send_url}/bot{bot_token}/getFile"
                    file_info_resp = await client.post(file_info_url, json={"file_id": file_id})
                    file_info = file_info_resp.json()

                    if not file_info.get("ok"):
                        log.error(f"Failed to get file info: {file_info}")
                        return None, None, None, None, None

                    file_path = file_info["result"]["file_path"]
                    file_url = f"{settings.tg_bot_send_url}/file/bot{bot_token}/{file_path}"
                    file_resp = await client.get(file_url)
                    file_bytes = file_resp.content
                    file_size = len(file_bytes)

            if file_bytes:
                media_id = str(uuid.uuid4())
                await self.storage.save_file(file_bytes, media_id, file_name, mime_type)

        except Exception as e:
            log.error(f"Error processing media: {e}")
            return None, None, None, None, None

        if media_id:
            raw_content["file_name"] = file_name
            raw_content["mime_type"] = mime_type

        return media_id, file_name, mime_type, raw_content, file_size


    async def send_message(self, bot_token: str, chat_id: str, text: str):
        """Send message via Telegram Bot API"""
        url = f"{settings.tg_bot_send_url}/bot{bot_token}/sendMessage"
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                json={
                    "chat_id": chat_id,
                    "text": text
                }
            )
            response.raise_for_status()
            return response.json()

    async def check_alive(self, acc) -> bool:
        creds = decrypt_credentials(acc.credentials_encrypted)
        bot_token = creds.get("bot_token")
        import httpx
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.get(f"https://api.telegram.org/bot{bot_token}/getMe")
                return resp.status_code == 200
        except Exception:
            return False
