import asyncio
import json
import logging
import os

from uuid import UUID
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from core.config import settings
from core.crm_contract import normalize_crm_send_result
from core.database import get_session
from app.core.crm_flag import crm_enabled
from app.core.log_filter import install_log_masking
from app.schemas.telegram_schemas import IncomingEvent, IncomingMessage
from app.services.chat_service import ChatService
from app.services.websocket_manager import manager


log = logging.getLogger(__name__)

DLT_TOPIC = "incoming_dlt"
MAX_RETRIES = 5

async def process_message(
    event_data: dict,
    chat_service: ChatService,
    crm_uow=None,
    crm_service=None,
):

    line_id_ = event_data.get("line_id", "")

    line_id_1 = UUID(line_id_)

    conversation = await chat_service.get_or_create_conversation(
        line_id=line_id_1,
        peer_phone=event_data.get("peer_phone", ""),
        channel_type="telegram_user",
    )
    conversation_id = conversation.id


    event_type = event_data.get("event_type")

    if event_type == "message_created":
        event = IncomingEvent(
            message=IncomingMessage(
                **event_data["message"],
            ),
            line_id=line_id_1,
        )

    elif event_type == "message_edit":
        try:
            messages = await chat_service.msg_repo.get_multi(
                filters={
                    "conversation_id": conversation_id,
                    "channel_msg_id": [str(event_data["message"]["id"])],
                }
            )
            if crm_service and messages:
                line = await crm_service._uow.lines.get_by_id(line_id_1)
                message = messages[0]
                await crm_service.edit_to_crm(
                    tenant_id=line.tenant_id,
                    conversation_id=conversation_id,
                    message_id=str(message.id),
                    text=event_data["message"].get("text") or "",
                    channel_type="telegram_user",
                )
        except Exception as e:
            log.error(f"CRM edit_to_crm failed: {e}")
        await chat_service.edit_message(
            conversation_id=conversation_id,
            channel_msg_id=str(event_data["message"]["id"]),
            text=event_data["message"].get("text"),
        )
        return

    elif event_type == "message_delete":
        try:
            messages = await chat_service.msg_repo.get_multi(
                filters={
                    "conversation_id": conversation_id,
                    "channel_msg_id": [str(i) for i in event_data["message_ids"]],
                }
            )

            line = None
            if crm_service:
                line = await crm_service._uow.lines.get_by_id(line_id_1)
            for message in (messages if crm_service else []):
                await crm_service.delete_from_crm(
                    tenant_id=line.tenant_id,
                    conversation_id=conversation_id,
                    client_phone="",
                    message_id=str(message.id),
                    channel_type="telegram_user",
                )


        except Exception as e:
            log.error(f"CRM delete_from_crm failed: {e}")

        await chat_service.delete_incoming_message(
            conversation_id=conversation_id,
            channel_msg_ids=[str(i) for i in event_data["message_ids"]],
        )
        return


    media = event_data["message"].get("media", [])
    

    message_data = await chat_service.save_incoming_message(
        conversation_id=conversation_id,
        peer_phone=event_data["peer_phone"],
        event=event,
        media_items=media,
    )

    if event_type == "message_created" and crm_service:
        try:
            line = await crm_uow.lines.get_by_id(line_id_1)
            sender = event_data["message"].get("sender") or {}

            bx_chat = await crm_service.send_to_crm(
                tenant_id=line.tenant_id,
                conversation_id=conversation_id,
                client_phone=event_data["peer_phone"],
                client_name=sender.get("username") or "",
                text=event_data["message"].get("text") or "",
                message_id=str(message_data.id),  # str(event_data["message"]["id"]),
                channel_type="telegram_user",
                media=media,
            )
            bx_chat_id, crm_message_id = normalize_crm_send_result(bx_chat)
            log.info(f"send_to_crm bx_chat_id={bx_chat_id} crm_message_id={crm_message_id}")
            if bx_chat_id:
                await chat_service.conv_repo.update(
                    await chat_service.conv_repo.get_by_id(conversation_id),
                    crm_chat_id=bx_chat_id,
                )
            if crm_message_id:
                msg_orm = await chat_service.msg_repo.get_by_id(message_data.id)
                await chat_service.msg_repo.update(
                    msg_orm,
                    raw_content={"crm_message_id": crm_message_id, "bx_real_id": crm_message_id},
                )

        except Exception as e:
            log.error(f"CRM send_to_crm failed: {e}")



    await manager.send_new_message(
        conversation_id,
        message_data.model_dump(mode="json"),
    )

async def process_with_retries(
    event_data: dict,
    max_retries: int = MAX_RETRIES,
) -> tuple[bool, Exception | None]:
    """Попытки обработки входящего сообщения (каждая — в новой сессии БД)."""
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            async with get_session() as db:
                chat_service = ChatService(db)
                crm_uow = None
                crm_service = None
                if crm_enabled():
                    from crm.app.services.crm_service import CrmService
                    from crm.app.core.uow import CrmUnitOfWork
                    crm_uow = CrmUnitOfWork(db)
                    crm_service = CrmService(crm_uow)
                await process_message(
                    event_data,
                    chat_service,
                    crm_uow,
                    crm_service,
                )
            return True, None
        except Exception as e:
            last_error = e
            log.error(
                f"Incoming message failed (attempt {attempt}): {e}",
                exc_info=True,
            )
            await asyncio.sleep(1)
    return False, last_error


async def handle_message(consumer, dlt_producer, message) -> None:
    """Обработать входящее сообщение: ретраи -> DLT -> коммит оффсета."""
    event_data = message.value
    attempt = event_data.get("_attempt", 0) + 1
    event_data["_attempt"] = attempt

    processed, error = await process_with_retries(event_data)
    if processed:
        await consumer.commit()
    else:
        # оффсет не коммитим до успеха; после исчерпания лимита — в DLT
        event_data["_error"] = str(error)
        log.error(f"Incoming message moved to DLT: {error}")
        await dlt_producer.send_and_wait(DLT_TOPIC, value=event_data)
        await consumer.commit()


async def start_consumer(
    group_id: str = "incoming-consumer-group",
    topics: list[str] | None = None,
):
    consumer = AIOKafkaConsumer(
        *(topics or [
            "telegram_user_incoming",
            "telegram_user_message_edit",
            "telegram_user_message_delete",
        ]),
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
            log.info("Incoming consumer started")
            break
        except Exception as e:
            log.error(
                f"Failed to connect to Kafka, retrying in 5s: {e}"
            )
            await asyncio.sleep(5)

    try:
        async for message in consumer:
            await handle_message(consumer, dlt_producer, message)

    finally:
        await consumer.stop()
        await dlt_producer.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    install_log_masking()

    group_id = os.getenv(
        "KAFKA_GROUP_ID",
        "incoming-consumer-group",
    )

    asyncio.run(
        start_consumer(group_id=group_id)
    )