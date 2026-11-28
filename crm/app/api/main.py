"""CRM service entrypoint."""
import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.v1.connections import connections_router
from app.api.v1.install import install_router, static_router
from app.api.v1.outgoing import outgoing_router
from app.api.v1.robots import robots_router
from app.api.v1.webhooks import webhooks_router
from core.config import settings

log = logging.getLogger("uvicorn.error")

app = FastAPI(title="DoveChat CRM Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.crm_cors_origins,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    # Тело запроса не логируем: в нём могут быть пароли и токены.
    log.warning(
        "crm/app/api/main.py validation_exception_handler %s %s: %s",
        request.method,
        request.url.path,
        [(e["loc"], e["type"]) for e in exc.errors()],
    )
    errors = []
    for e in exc.errors():
        err = dict(e)
        if "ctx" in err and "error" in err["ctx"]:
            err["ctx"] = {"error": str(err["ctx"]["error"])}
        errors.append(err)
    return JSONResponse(status_code=422, content={"detail": errors})


for router in (
    webhooks_router,
    connections_router,
    install_router,
    outgoing_router,
    robots_router,
):
    app.include_router(router, prefix="/api/v1")
app.include_router(static_router, prefix="")