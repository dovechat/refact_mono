import asyncio
import json
import logging
from core.redis import rdb
from app.services.websocket_manager import manager

log = logging.getLogger(__name__)


async def _resilient_listen(pubsub):
    while True:
        try:
            async for message in pubsub.listen():
                yield message
        except Exception as e:
            log.error(
                "\n\n !!! --- ERROR --- !!! "
                "chat/app/core/redis_subscriber.py "
                f"_resilient_listen listen failed e={e}\n\n"
            )
            await asyncio.sleep(1)


async def redis_subscriber():
    pubsub = rdb.pubsub()
    await pubsub.psubscribe(
        "ws:new_message:*", 
        "ws:message_status:*", 
        "ws:message_update:*",
        "ws:new_conversation:*"
    )
    log.info("Redis subscriber started")
    async for message in _resilient_listen(pubsub):
        if message["type"] not in ("pmessage", "message"):
            continue
        try:
            channel = message["channel"]
            if isinstance(channel, bytes):
                channel = channel.decode("utf-8")

            raw_data = message["data"]
            if isinstance(raw_data, bytes):
                raw_data = raw_data.decode("utf-8")

            data = json.loads(raw_data)
            target_id = str(channel).split(":", 2)[2]

            if "new_message" in channel:
                if target_id in manager.active_connections:
                    for ws in manager.active_connections[target_id][:]:
                        try:
                            await ws.send_json({"type": "new_message", "data": data})
                        except Exception as e:
                            log.error(f"redis_subscriber send_new_message error: {e}")
                            await manager.disconnect(target_id, ws)

            elif "message_status" in channel:
                if target_id in manager.active_connections:
                    for ws in manager.active_connections[target_id][:]:
                        try:
                            await ws.send_json({"type": "message_status", "data": data})
                        except Exception as e:
                            log.error(f"redis_subscriber send_message_status error: {e}")
                            await manager.disconnect(target_id, ws)

            elif "message_update" in channel:
                if target_id in manager.active_connections:
                    for ws in manager.active_connections[target_id][:]:
                        try:
                            await ws.send_json({"type": "message_update", "data": data})
                        except Exception as e:
                            log.error(f"redis_subscriber send_message_update error: {e}")
                            await manager.disconnect(target_id, ws)

            elif "new_conversation" in channel:
                if target_id in manager.tenant_connections:
                    for ws in manager.tenant_connections[target_id][:]:
                        try:
                            await ws.send_json({"type": "new_conversation", "data": data})
                        except Exception as e:
                            log.error(f"redis_subscriber send_new_conversation error: {e}")
                            await manager.disconnect_tenant(target_id, ws)

        except Exception as e:
            log.error(f"redis_subscriber error: {e}")