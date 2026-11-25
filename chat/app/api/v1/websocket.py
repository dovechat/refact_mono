from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import logging
import asyncio
import jwt
from core.config import settings
from app.services.websocket_manager import manager

log = logging.getLogger(__name__)

router = APIRouter()

async def validate_token(token: str):
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        return payload
    except jwt.PyJWTError:
        return None

@router.websocket("/chat/{conversation_id}")
async def websocket_endpoint(
        websocket: WebSocket,
        conversation_id: str,
):
    await websocket.accept()

    try:
        auth_data = await asyncio.wait_for(websocket.receive_json(), timeout=5.0)
        token = auth_data.get("token")
    except Exception:
        await websocket.close(code=1008, reason="Unauthorized")
        return

    user = await validate_token(token)
    if not user:
        await websocket.close(code=1008, reason="Unauthorized")
        return

    connected = await manager.connect(
        websocket,
        conversation_id,
        app=websocket.app,
        user_id=str(user.get("sub") or ""),
        tenant_id=str(user.get("tenant_id") or ""),
        role=user.get("role") or ""
    )
    if connected is False:
        return

    try:
        while True:
            data = await websocket.receive_text()
            if data == '{"type":"ping"}' or data == '{"type": "ping"}':
                continue
    except WebSocketDisconnect:
        log.info("Client disconnected")
        await manager.disconnect(conversation_id, websocket)
    except Exception as e:
        log.error(f"ERROR: WebSocket error: {e}", exc_info=True)
        await manager.disconnect(conversation_id, websocket)


@router.websocket("/tenant/{tenant_id}")
async def tenant_websocket(
    websocket: WebSocket,
    tenant_id: str,
):
    await websocket.accept()

    try:
        auth_data = await asyncio.wait_for(websocket.receive_json(), timeout=5.0)
        token = auth_data.get("token")
    except Exception:
        await websocket.close(code=1008, reason="Unauthorized")
        return

    user = await validate_token(token)
    if not user or str(user.get("tenant_id") or "") != str(tenant_id):
        await websocket.close(code=1008, reason="Unauthorized")
        return

    await manager.connect_tenant(websocket, tenant_id)
    try:
        while True:
            data = await websocket.receive_text()
            if data == '{"type":"ping"}' or data == '{"type": "ping"}':
                continue
    except WebSocketDisconnect:
        await manager.disconnect_tenant(tenant_id, websocket)
    except Exception as e:
        log.error(f"Tenant WS error: {e}")
        await manager.disconnect_tenant(tenant_id, websocket)


@router.websocket("/user")
async def user_websocket(
    websocket: WebSocket,
):
    await websocket.accept()

    try:
        auth_data = await asyncio.wait_for(websocket.receive_json(), timeout=5.0)
        token = auth_data.get("token")
    except Exception:
        await websocket.close(code=1008, reason="Unauthorized")
        return

    user = await validate_token(token)
    if not user:
        await websocket.close(code=1008, reason="Unauthorized")
        return

    user_id = str(user.get("sub") or "")
    if not user_id:
        await websocket.close(code=1008, reason="Unauthorized")
        return
    await manager.connect_tenant(websocket, user_id)
    try:
        while True:
            data = await websocket.receive_text()
            if data == '{"type":"ping"}' or data == '{"type": "ping"}':
                continue
    except WebSocketDisconnect:
        await manager.disconnect_tenant(user_id, websocket)
    except Exception as e:
        log.error(f"User WS error: {e}")
        await manager.disconnect_tenant(user_id, websocket)