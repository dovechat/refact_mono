"""Консьюмер событий chat -> crm (топик crm_events).

Запуск отдельным процессом: python -m app.consumers.crm_events (рабочая папка /mono/crm).
"""
import asyncio
import json
import logging
import os
import uuid

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from core.config import settings
from core.crm_contract import (
    CRM_ACTION_DELETE,
    CRM_ACTION_EDIT,
    CRM_ACTION_SEND,
    CRM_EVENTS_TOPIC,
    normalize_crm_send_result,
)
from core.database import get_session
from app.core.uow import CrmUnitOfWork
from app.services.crm_service import CrmService

log = logging.getLogger(__name__)

DLT_TOPIC = "crm_events_dlt"
MAX_RETRIES = 5


async def process_event(event: dict) -> None:
    action = event.get("action")
    line_id = uuid.UUID(str(event["line_id"]))
    conversation_id = uuid.UUID(str(event["conversation_id"]))

    async with get_session() as db:
        uow = CrmUnitOfWork(db)
        line = await uow.lines.get_by_id(line_id)
        if not line:
            log.warning(f"crm_events: line not found line_id={line_id}, skip")
            return
        service = CrmService(uow)

        if action == CRM_ACTION_SEND:
            result = await service.send_to_crm(
                tenant_id=line.tenant_id,
                conversation_id=conversation_id,
                client_phone=event["client_phone"],
                client_name=event.get("client_name") or "",
                text=event.get("text") or "",
                message_id=event["message_id"],
                channel_type=event["channel_type"],
                media=event.get("media") or [],
            )
            # crm_chat_id в разговор уже записал CrmService; здесь только id сообщения в CRM (Bitrix)
            _chat_id, crm_message_id = normalize_crm_send_result(result)
            if crm_message_id:
                message = await uow.msg_repo.get_by_id(int(event["message_id"]))
                if message:
                    await uow.msg_repo.update(
                        message,
                        raw_content={"crm_message_id": crm_message_id, "bx_real_id": crm_message_id},
                    )

        elif action == CRM_ACTION_EDIT:
            await service.edit_to_crm(
                tenant_id=line.tenant_id,
                conversation_id=conversation_id,
                message_id=event["message_id"],
                text=event.get("text") or "",
                channel_type=event["channel_type"],
            )

        elif action == CRM_ACTION_DELETE:
            await service.delete_from_crm(
                tenant_id=line.tenant_id,
                conversation_id=conversation_id,
                client_phone=event.get("client_phone") or "",
                message_id=event["message_id"],
                channel_type=event["channel_type"],
            )

        else:
            log.warning(f"crm_events: unknown action {action!r}, skip")


async def handle_message(consumer, dlt_producer, message) -> None:
    event = message.value
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await process_event(event)
            await consumer.commit()
            return
        except Exception as e:
            last_error = e
            log.error(f"crm_events failed (attempt {attempt}): {e}", exc_info=True)
            await asyncio.sleep(1)
    event["_error"] = str(last_error)
    await dlt_producer.send_and_wait(DLT_TOPIC, value=event)
    await consumer.commit()


async def start_consumer(group_id: str = "crm-events-group") -> None:
    consumer = AIOKafkaConsumer(
        CRM_EVENTS_TOPIC,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    dlt_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    while True:
        try:
            await consumer.start()
            await dlt_producer.start()
            log.info("crm_events consumer started")
            break
        except Exception as e:
            log.error(f"Failed to connect to Kafka, retrying in 5s: {e}")
            await asyncio.sleep(5)
    try:
        async for message in consumer:
            await handle_message(consumer, dlt_producer, message)
    finally:
        await consumer.stop()
        await dlt_producer.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(start_consumer(group_id=os.getenv("KAFKA_GROUP_ID", "crm-events-group")))