# /mono/chat/app/consumers/dlt_consumer.py
import json
import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from core.config import settings
from core.database import get_db
from app.services.chat_service import ChatService
from app.schemas.chat_schemas import MessageCreate, MessageDirection, MessageStatus
from schemas.auth_schemas import UserContext
from app.core.crm_flag import crm_enabled
from app.core.log_filter import install_log_masking

log = logging.getLogger(__name__)
get_session = asynccontextmanager(get_db)

DLT_TOPIC = "outgoing_dlt"
INCOMING_DLT_TOPIC = "incoming_dlt"
DLT_TOPICS = [DLT_TOPIC, INCOMING_DLT_TOPIC]
DLT_GROUP_ID = "outgoing-dlt-group"
MAX_RETRIES = 5
RETRY_INTERVAL = 60

async def process_dlt_message(
    event_data: dict,
    chat_service: ChatService,
):
    channel_type = event_data.get("channel_type")
    data = MessageCreate(
        message_id=event_data.get("message_id"),
        conversation_id=uuid.UUID(event_data["conversation_id"]),
        direction=MessageDirection.outbound,
        phone=event_data["phone"],
        text=event_data["text"],
        status=MessageStatus.sent,
        media=event_data.get("media", []),
    )
    current_user = UserContext(
        id=uuid.UUID(event_data["user_id"]),
        email=event_data["user_email"],
    )
    handler = getattr(chat_service, settings.CHANNEL_HANDLERS[channel_type])
    kwargs = dict(data=data, files=[], current_user=current_user)
    await handler(**kwargs)


async def process_dlt_incoming(event_data: dict, chat_service: ChatService):
    """Повтор входящего сообщения (формат incoming_consumer.process_message)."""
    from app.consumers.incoming_consumer import process_message as process_incoming

    crm_uow = None
    crm_service = None
    if crm_enabled():
        from crm.app.services.crm_service import CrmService
        from crm.app.core.uow import CrmUnitOfWork
        crm_uow = CrmUnitOfWork(chat_service.db)
        crm_service = CrmService(crm_uow)
    await process_incoming(event_data, chat_service, crm_uow, crm_service)


async def start_dlt_consumer():
    consumer = AIOKafkaConsumer(
        *DLT_TOPICS,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=DLT_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
    )

    while True:
        try:
            await consumer.start()
            log.info("DLT consumer started")
            break
        except Exception as e:
            log.error(f"Failed to connect to Kafka, retrying in 5s: {e}")
            await asyncio.sleep(5)

    dlt_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    await dlt_producer.start()

    try:
        async for message in consumer:
            event_data = message.value
            conv_id = event_data.get("conversation_id")
            attempt = event_data.get("_dlt_attempt", 0) + 1
            event_data["_dlt_attempt"] = attempt
            log.info(
                f"DLT retry attempt={attempt} conversation_id={conv_id}"
            )
            try:
                # новая сессия на каждую попытку
                async with get_session() as db:
                    chat_service = ChatService(db)
                    if message.topic == INCOMING_DLT_TOPIC:
                        await process_dlt_incoming(event_data, chat_service)
                    else:
                        await process_dlt_message(event_data, chat_service)
                await consumer.commit()
                log.info(f"DLT message resolved conversation_id={conv_id}")
            except Exception as e:
                log.error(f"DLT retry failed attempt={attempt}: {e}")
                if attempt >= MAX_RETRIES:
                    log.error(
                        f"DLT message dropped after {attempt} attempts "
                        f"conversation_id={conv_id}"
                    )
                else:
                    event_data["_error"] = str(e)
                    await dlt_producer.send_and_wait(
                        message.topic, value=event_data
                    )
                await consumer.commit()
                await asyncio.sleep(RETRY_INTERVAL)
    finally:
        await consumer.stop()
        await dlt_producer.stop()



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    install_log_masking()
    asyncio.run(start_dlt_consumer())