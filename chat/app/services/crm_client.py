import logging
import uuid

from fastapi.encoders import jsonable_encoder

from app.core.crm_flag import crm_enabled
from app.core.kafka_producer import kafka_producer
from core.crm_contract import CRM_ACTION_DELETE, CRM_ACTION_EDIT, CRM_ACTION_SEND

log = logging.getLogger("uvicorn.error")


class CrmClient:
    """chat -> crm через Kafka: публикует события, ответа не ждёт.
    crm сам отправляет в CRM и записывает crm_chat_id / crm_message_id в БД."""

    async def _publish(self, action: str, conversation_id: uuid.UUID | str, **fields) -> None:
        try:
            await kafka_producer.send_crm_event(
                action, conversation_id, **jsonable_encoder(fields)
            )
        except Exception as e:
            log.error(f"{action}_to_crm publish failed: {e!r}")

    async def send_to_crm(
        self,
        *,
        line_id: uuid.UUID | str,
        conversation_id: uuid.UUID | str,
        client_phone: str,
        client_name: str,
        text: str,
        message_id: str,
        channel_type: str,
        media: list[dict] | None = None,
    ) -> None:
        await self._publish(
            CRM_ACTION_SEND, conversation_id,
            line_id=line_id, client_phone=client_phone, client_name=client_name,
            text=text, message_id=message_id, channel_type=channel_type,
            media=media or [],
        )

    async def edit_to_crm(
        self,
        *,
        line_id: uuid.UUID | str,
        conversation_id: uuid.UUID | str,
        message_id: str,
        text: str,
        channel_type: str,
    ) -> None:
        await self._publish(
            CRM_ACTION_EDIT, conversation_id,
            line_id=line_id, message_id=message_id, text=text, channel_type=channel_type,
        )

    async def delete_from_crm(
        self,
        *,
        line_id: uuid.UUID | str,
        conversation_id: uuid.UUID | str,
        client_phone: str,
        message_id: str,
        channel_type: str,
    ) -> None:
        await self._publish(
            CRM_ACTION_DELETE, conversation_id,
            line_id=line_id, client_phone=client_phone,
            message_id=message_id, channel_type=channel_type,
        )


_client = CrmClient()


def get_crm_client() -> CrmClient | None:
    """None, если интеграция выключена (CRM_ENABLED), иначе клиент."""
    return _client if crm_enabled() else None