"""Main FastAPI application for Chat service."""
import asyncio
from fastapi import Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from app.consumers.green_whatsapp import GreenWhatsAppListener
from app.consumers.waba import WabaListener
from app.consumers.vk import VkListener
from app.core.kafka_producer import kafka_producer
from core.config import settings
from app.core.redis_subscriber import redis_subscriber

from .v1.chat import router as chat_router
from .v1.websocket import router as websocket_router
from api.auth import public_router, private_router
from api.tenant import tenant_router
from api.lines import line_router
from .v1.webhooks import router as webhooks_router

import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
from app.core.log_filter import install_log_masking
install_log_masking()

from api.app_start import start

app = start(
    title="DoveChat Chat Service",
    description="Chat WebSocket and REST API for real-time messaging",
    server_prefix="/api"
)

# CORS-origins задаются ТОЛЬКО через окружение (BACKEND_CORS_ORIGINS, через запятую).
# Доменов в коде нет: внешний вход (API gateway / reverse proxy) задаёт их у себя,
# здесь — только для прямых обращений с браузера (локальная разработка).
CORS_ORIGINS = settings.cors_origins_list

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    # credentials вместе с origins="*" браузер всё равно не примет
    allow_credentials="*" not in CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    log.error(f"422 Validation Error for request {request.method} {request.url}")
    log.error(f"Errors: {[(e['loc'], e['msg']) for e in exc.errors()]}")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": exc.errors()}),
    )


async def redis_subscriber_worker():
    """Подписка на Redis Pub/Sub с автоперезапуском: падение молча убивало доставку WS."""
    while True:
        try:
            await redis_subscriber()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"redis subscriber crashed: {e}, restarting in 5s", exc_info=True)
            await asyncio.sleep(5)
        else:
            log.error("redis subscriber stopped unexpectedly, restarting in 5s")
            await asyncio.sleep(5)


@app.on_event("startup")
async def startup():
    app.state.green_listener = GreenWhatsAppListener()
    app.state.waba_listener = WabaListener()
    app.state.vk_listener = VkListener()
    app.state.redis_subscriber_task = asyncio.create_task(redis_subscriber_worker())


@app.on_event("shutdown")
async def shutdown():
    task = getattr(app.state, "redis_subscriber_task", None)
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f"Error stopping redis subscriber task: {e}")
    await kafka_producer.close()

prefix = "/v1"
app.include_router(public_router, prefix=prefix)
app.include_router(private_router, prefix=prefix)
app.include_router(chat_router, prefix=prefix)
app.include_router(tenant_router, prefix=prefix)
app.include_router(websocket_router, prefix="/ws")
app.include_router(line_router, prefix=prefix)
app.include_router(webhooks_router, prefix=prefix)