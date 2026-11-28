import asyncio
import json
import uuid
from aiokafka import AIOKafkaProducer
from fastapi import HTTPException, UploadFile
from loguru import logger
from core.config import settings
from core.crm_contract import CRM_EVENTS_TOPIC
from services.storage_service import StorageService

CHANNEL_TOPICS = {
    "telegram_bot": "telegram_bot_outgoing",
    "telegram_user": "telegram_user_outgoing",
    "vk": "vk_outgoing",
    "waba": "waba_outgoing",
    "whatsapp_green": "whatsapp_green_outgoing",
}


MAX_FILE_SIZE = 100 * 1024 * 1024


class KafkaEventProducer:
    def __init__(self):
        self.producer = None
        self._lock = asyncio.Lock()

    async def start(self):
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks="all",
            request_timeout_ms=40000,
            retry_backoff_ms=100,
        )
        await producer.start()
        self.producer = producer
        logger.info(f"Kafka producer initialized for {settings.kafka_bootstrap_servers}")

    async def send_message(
            self,
            channel_type: str,
            conversation_id: uuid.UUID,
            phone: str,
            crm_message_id: str,
            text: str,
            user_id: uuid.UUID,
            user_email: str,
            message_id: int,
            files: list[UploadFile] | None = None,
            template: dict | None = None,
    ) -> None:
        topic = CHANNEL_TOPICS.get(channel_type)
        if not topic:
            raise ValueError(f"Unknown channel_type: {channel_type}")

        media = []
        if files:
            storage = StorageService()
            for file in files:
                media_id = uuid.uuid4()
                # читаем файл до того, как он закроется
                file_bytes = await file.read(MAX_FILE_SIZE + 1)
                if len(file_bytes) > MAX_FILE_SIZE:
                    raise HTTPException(
                        status_code=413, detail="File too large"
                    )
                await storage.save_file(file_bytes, media_id, file.filename, file.content_type or "application/octet-stream")
                logger.info(f"KafkaEventProducer.send_message file saved media_id={media_id}")
                media.append({
                    "id": str(media_id),
                    "file_name": file.filename,
                    "mime_type": file.content_type or "application/octet-stream",
                })

        payload = {
            "conversation_id": str(conversation_id),
            "phone": phone,
            "crm_message_id": crm_message_id,
            "text": text,
            "media": media,
            "user_id": str(user_id),
            "user_email": user_email,
            "channel_type": channel_type,
            "message_id": message_id,
            "template": template,
            "action": "send",
        }

        await self._send(topic, payload, conversation_id)


    async def send_edit_message(
            self,
            channel_type: str,
            conversation_id: uuid.UUID,
            message_id: int,
            text: str,
            user_id: uuid.UUID,
            user_email: str,
            phone: str,
            line_id: str,
    ) -> None:
        topic = CHANNEL_TOPICS.get(channel_type)
        if not topic:
            raise ValueError(f"Unknown channel_type: {channel_type}")

        payload = {
            "channel_type": channel_type,
            "conversation_id": str(conversation_id),
            "message_id": message_id,
            "text": text,
            "user_id": str(user_id),
            "user_email": user_email,
            "action": "edit",
            "phone": phone,
            "line_id": line_id,
        }

        await self._send(topic, payload, conversation_id)

    async def send_delete_message(
            self,
            channel_type: str,
            conversation_id: uuid.UUID,
            message_id: int,
            user_id: uuid.UUID,
            user_email: str,
            phone: str,
            line_id: str,
    ) -> None:
        topic = CHANNEL_TOPICS.get(channel_type)
        if not topic:
            raise ValueError(f"Unknown channel_type: {channel_type}")

        payload = {
            "channel_type": channel_type,
            "conversation_id": str(conversation_id),
            "message_id": message_id,
            "user_id": str(user_id),
            "user_email": user_email,
            "action": "delete",
            "phone": phone,
            "line_id": line_id,
        }

        await self._send(topic, payload, conversation_id)


    async def send_crm_event(self, action: str, conversation_id: uuid.UUID, **fields) -> None:
        """Событие для crm. Ключ = conversation_id, чтобы порядок событий разговора сохранялся."""
        payload = {"action": action, "conversation_id": str(conversation_id), **fields}
        await self._send(CRM_EVENTS_TOPIC, payload, conversation_id)

    async def _send(self, topic: str, payload: dict, conversation_id: uuid.UUID):
        if not self.producer:
            async with self._lock:
                if not self.producer:
                    await self.start()

        try:
            # send_and_wait: ждём подтверждения брокера, иначе API ответил бы 200,
            # а сообщение могло бы потеряться.
            await self.producer.send_and_wait(
                topic=topic, value=payload, key=str(conversation_id).encode()
            )
            logger.info(f"KafkaEventProducer.send_message sent topic={topic} conversation_id={conversation_id}")
        except Exception as e:
            logger.error(
                f"KafkaEventProducer._send failed topic={topic}: {e}"
            )
            try:
                await self.producer.stop()
            except Exception:
                pass
            self.producer = None
            raise


    async def close(self):
        if self.producer:
            await self.producer.stop()


kafka_producer = KafkaEventProducer()