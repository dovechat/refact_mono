import json
import uuid
from typing import List
from fastapi import (
    APIRouter, Depends, File, Form, HTTPException, Query, Request,
    UploadFile, status,
)
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.dependencies import get_current_user
from schemas.auth_schemas import UserContext
from app.core.dependencies import (
    get_chat_service,
    get_status_service,
    require_internal_token,
)
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
from app.services.status import StatusService
from app.core.crm_flag import require_crm_enabled
from app.core.kafka_producer import kafka_producer, MAX_FILE_SIZE
from app.services.bot_service import BotService
from app.services.chat_service import ChatService
from app.services.line_service import LineService
from app.schemas.chat_schemas import (
    ConversationCloseRequest,
    ConversationCreate,
    ConversationListResponse,
    ConversationResponse,
    CrmMessageIn,
    CrmMessageOut,
    LineResponse,
    LineWithChannelsResponse,
    MessageCreate,
    MessageDirection,
    MessageEditRequest,
    MessageResponse,
)

import logging
log = logging.getLogger("uvicorn.error")

router = APIRouter(tags=["Chat"])

def get_bot_service(db: AsyncSession = Depends(get_db)) -> BotService:
    return BotService(db)

def get_line_service(db: AsyncSession = Depends(get_db)) -> LineService:
    return LineService(db)


async def _enqueue_outgoing(
    channel_type: str,
    data: MessageCreate,
    files: list[UploadFile],
    current_user: UserContext,
    chat_service: ChatService,
    check_access: bool = True,
) -> MessageResponse:
    data = data.model_copy(update={"media": []})
    conv = await chat_service.conv_repo.get_by_id(data.conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    if check_access:
        if check_access:
            await chat_service._assert_conversation_access(conv, user_id=current_user.id)
        conv_channel = getattr(conv.channel_type, "value", conv.channel_type)
        if str(conv_channel) != channel_type:
            raise HTTPException(status_code=400, detail="Wrong channel")
    phone = (conv.client_phone or data.phone) if check_access else data.phone
    _, message = await chat_service.conv_msg(data, current_user)
    await kafka_producer.send_message(
        channel_type=channel_type,
        conversation_id=data.conversation_id,
        phone=phone,
        crm_message_id=data.crm_message_id,
        text=data.text,
        user_id=current_user.id,
        user_email=current_user.email,
        message_id=message.id,
        files=files,
        template=data.template,
    )
    log.info(f"_enqueue_outgoing crm_message_id={data.crm_message_id} channel_type={channel_type} message_id={message.id}")
    return MessageResponse.model_validate(message)


async def _enqueue_edit(
    message_id: int,
    data: MessageEditRequest,
    current_user: UserContext,
    chat_service: ChatService,
):
    message = await chat_service.msg_repo.get_by_id(message_id)
    if not message or not message.channel_msg_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="cannot edit")
    # доступ проверяем сразу, иначе правка чужого сообщения уйдёт в Kafka и упадёт в консьюмере
    await chat_service._assert_message_access(message, user_id=current_user.id)
    conv = await chat_service.conv_repo.get_by_id(message.conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    await kafka_producer.send_edit_message(
        channel_type=conv.channel_type,
        conversation_id=message.conversation_id,
        message_id=message.id,
        text=data.text,
        user_id=current_user.id,
        user_email=current_user.email,
        phone=conv.client_phone,
        line_id=str(conv.line_id),
    )
    log.info(f"_enqueue_edit channel_type={conv.channel_type} message_id={message.id}")
    return MessageResponse.model_validate(message)


async def _enqueue_delete(
    message_id: int,
    current_user: UserContext,
    chat_service: ChatService,
):
    message = await chat_service.msg_repo.get_by_id(message_id)
    if not message:
        log.error(
            "\n\n !!! --- ERROR --- !!! "
            "chat/app/api/v1/chat.py _enqueue_delete "
            f"kafka send_delete_message failed message_id={message_id}\n\n"
        )
        raise HTTPException(status_code=400, detail="cannot delete")
    # доступ проверяем сразу, иначе удаление чужого сообщения уйдёт в Kafka и упадёт в консьюмере
    await chat_service._assert_message_access(message, user_id=current_user.id)
    conv = await chat_service.conv_repo.get_by_id(message.conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    await kafka_producer.send_delete_message(
        channel_type=conv.channel_type,
        conversation_id=message.conversation_id,
        message_id=message.id,
        user_id=current_user.id,
        user_email=current_user.email,
        phone=conv.client_phone,
        line_id=str(conv.line_id),
    )
    log.info(f"_enqueue_delete channel_type={conv.channel_type} message_id={message.id}")
    return {"status": "queued"}


async def _crm_message_target(chat_service: ChatService, message_id: int):
    """line_id, phone и channel_type берём из чата сообщения, не из формы."""
    message = await chat_service.msg_repo.get_by_id(message_id)
    if not message:
        raise HTTPException(status_code=404, detail="message not found")
    conv = await chat_service.conv_repo.get_by_id(message.conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="conversation not found")
    channel_type = getattr(conv.channel_type, "value", conv.channel_type)
    return conv.line_id, conv.client_phone or "", str(channel_type)


@router.post(
    "/internal/crm-outgoing",
    dependencies=[
        Depends(require_crm_enabled),
        Depends(require_internal_token),
    ],
)
async def crm_outgoing(
    data: str = Form(...),
    channel_type: str = Form(...),
    user_id: uuid.UUID = Form(...),
    user_email: str = Form(...),
    files: list[UploadFile] = File(default=[]),
    chat_service: ChatService = Depends(get_chat_service),
):
    current_user = UserContext(id=user_id, email=user_email, role="user", member="owner")
    msg = MessageCreate.model_validate_json(data)
    return await _enqueue_outgoing(channel_type, msg, files, current_user, chat_service, check_access=False)


@router.post(
    "/internal/crm-delete",
    dependencies=[
        Depends(require_crm_enabled),
        Depends(require_internal_token),
    ],
)
async def crm_delete(
    message_id: int = Form(...),
    user_id: uuid.UUID = Form(...),
    user_email: str = Form(...),
    chat_service: ChatService = Depends(get_chat_service),
):
    current_user = UserContext(
        id=user_id, email=user_email, role="user", member="owner"
    )
    line_id, phone, channel_type = await _crm_message_target(
        chat_service, message_id
    )
    await chat_service.delete_message(
        message_id, current_user, phone, line_id, channel_type
    )
    return {"status": "ok", "message_id": message_id}


@router.post(
    "/internal/crm-edit",
    dependencies=[
        Depends(require_crm_enabled),
        Depends(require_internal_token),
    ],
)
async def crm_edit(
    message_id: int = Form(...),
    text: str = Form(...),
    user_id: uuid.UUID = Form(...),
    user_email: str = Form(...),
    chat_service: ChatService = Depends(get_chat_service),
):
    current_user = UserContext(
        id=user_id, email=user_email, role="user", member="owner"
    )
    line_id, phone, channel_type = await _crm_message_target(
        chat_service, message_id
    )
    await chat_service.edit_outgoing_message(
        message_id, text, current_user, phone, line_id, channel_type
    )
    return {"status": "ok", "message_id": message_id}


@router.post("/messages/tgbot", response_model=MessageResponse)
async def create_message_tgbot(
    data: str = Form(...),
    files: list[UploadFile] = File(default=[]),
    current_user: UserContext = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service),
):
    return await _enqueue_outgoing("telegram_bot", MessageCreate.model_validate_json(data), files, current_user, chat_service)


@router.post("/messages/tguser", response_model=MessageResponse)
async def create_message_tguser(
    data: str = Form(...),
    files: list[UploadFile] = File(default=[]),
    current_user: UserContext = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service),
):
    data_ = MessageCreate.model_validate_json(data)
    return await _enqueue_outgoing("telegram_user", data_, files, current_user, chat_service)


@router.post("/messages/whatsapp/green", response_model=MessageResponse)
async def create_message_whatsapp_green(
    data: str = Form(...),
    files: list[UploadFile] = File(default=[]),
    current_user: UserContext = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service),
):
    return await _enqueue_outgoing("whatsapp_green", MessageCreate.model_validate_json(data), files, current_user, chat_service)


@router.post("/messages/waba", response_model=MessageResponse)
async def create_message_waba(
    data: str = Form(...),
    files: list[UploadFile] = File(default=[]),
    current_user: UserContext = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service),
):
    return await _enqueue_outgoing("waba", MessageCreate.model_validate_json(data), files, current_user, chat_service)


@router.post("/messages/waba/template", response_model=MessageResponse)
async def create_message_waba_template(
    conversation_id: uuid.UUID = Form(...),
    template_name: str = Form(...),
    language: str = Form(default="ru"),
    components: str = Form(default="[]"),
    current_user: UserContext = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service),
):
    try:
        parsed_components = json.loads(components)
    except json.JSONDecodeError as e:
        log.error(
            "\n\n !!! --- ERROR --- !!! "
            "chat/app/api/v1/chat.py "
            "create_message_waba_template "
            f"bad components json e={e}\n\n"
        )
        raise HTTPException(status_code=422, detail="Invalid components")
    data = MessageCreate(
        conversation_id=conversation_id,
        direction=MessageDirection.outbound,
        text=f"[template:{template_name}]",
        phone="",
        template={
            "name": template_name,
            "language": language,
            "components": parsed_components,
        },
    )

    return await _enqueue_outgoing("waba", data, [], current_user, chat_service)


@router.post("/messages/vk", response_model=MessageResponse)
async def create_message_vk(
    data: str = Form(...),
    files: list[UploadFile] = File(default=[]),
    current_user: UserContext = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service),
):
    return await _enqueue_outgoing("vk", MessageCreate.model_validate_json(data), files, current_user, chat_service)


@router.patch("/conversations/{conversation_id}/status")
async def set_conversation_status(
    conversation_id: uuid.UUID,
    body: ConversationCloseRequest,
    current_user: UserContext = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service),
):
    return await chat_service.close_or_open_conversation(
        conversation_id=conversation_id,
        is_closed=body.is_closed,
        user_id=current_user.id,
    )


@router.get("/conversations", response_model=List[ConversationListResponse])
async def list_conversations(
    line_id: uuid.UUID | None = None,
    channel_type: str | None = None,
    is_closed: bool | None = None,
    current_user: UserContext = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service),
):
    try:
        return await chat_service.get_conversations_filtered(
            user_id=current_user.id,
            line_id=line_id,
            channel_type=channel_type,
            is_closed=is_closed,
        )
    except Exception as e:
        log.error(f"router.get /conversations failed: {e}", exc_info=True)
        raise


@router.get("/conversations/{conversation_id}", response_model=ConversationResponse)
async def get_conversation(
    conversation_id: uuid.UUID,
    current_user: UserContext = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
):
    """Get conversation details and messages"""
    conversation = await chat_service.get_conversation(
        conversation_id=conversation_id,
        tenant_id=current_user.tenant_id,
        user_id=current_user.id,
    )
    if not conversation:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return conversation


@router.get("/conversations/{conversation_id}/messages", response_model=List[MessageResponse])
async def get_messages(
    conversation_id: uuid.UUID,
    current_user: UserContext = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service),
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=1000)
):
    """Get messages for specific conversation"""
    res = await chat_service.get_messages(
        conversation_id=conversation_id,
        tenant_id=current_user.tenant_id,
        user_id=current_user.id,
        skip=skip,
        limit=limit
    )
    return res


@router.post("/conversations", response_model=ConversationResponse)
async def create_conversation(
    data: ConversationCreate,
    current_user: UserContext = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service),
):
    """Create new conversation"""
    return await chat_service.create_conversation(
        tenant_id=current_user.tenant_id,
        data=data,
        user_id=current_user.id,
    )


@router.post("/bot/webhook/{line_id}")
async def telegram_bot_webhook(
    line_id: uuid.UUID,
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    bot_service = BotService(db)
    data = await request.json()
    return await bot_service.handle_webhook(line_id, data, request)


@router.get("/metrics", dependencies=[Depends(require_internal_token)])
async def metrics(
    status_service: StatusService = Depends(get_status_service),
):
    await status_service.check_all()
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/lines", response_model=List[LineResponse])
async def get_my_lines(
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    line_service: LineService = Depends(get_line_service)
):
    return await line_service.get_user_lines(
        user_id=current_user.id,
        role=current_user.role,
        tenant_id=current_user.tenant_id
    )


@router.get("/lines/{line_id}/channels", response_model=List[LineResponse])
async def get_line_channels(
    line_id: uuid.UUID,
    current_user: UserContext = Depends(get_current_user),
    line_service: LineService = Depends(get_line_service)
):
    return await line_service.get_line_channels(
        line_id,
        user_id=current_user.id,
        role=current_user.role,
        tenant_id=current_user.tenant_id,
    )


@router.get("/lines-with-channels", response_model=List[LineWithChannelsResponse])
async def get_lines_with_channels(
    current_user: UserContext = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
):
    return await chat_service.get_lines_with_available_channels(
        user_id=current_user.id
    )


@router.post("/upload")
async def upload_file(
    media_id: uuid.UUID,
    file: UploadFile = File(...),
    current_user: UserContext = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
):
    """Upload file to S3"""
    if file.size is not None and file.size > MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail="File too large")
    log.info(f"Uploading file: {file.filename}")
    result = await chat_service.upload(media_id, file)
    return result


@router.post(
    "/internal/crm-message",
    dependencies=[
        Depends(require_crm_enabled),
        Depends(require_internal_token),
    ],
)
async def crm_incoming(
    data: CrmMessageIn,
    chat_service: ChatService = Depends(get_chat_service),
):
    return await chat_service.save_crm_incoming(data)


@router.post(
    "/internal/crm-robot-send",
    dependencies=[
        Depends(require_crm_enabled),
        Depends(require_internal_token),
    ],
)
async def crm_robot_send(
    request: Request, 
    data: CrmMessageOut,
    chat_service: ChatService = Depends(get_chat_service),
):
    return await chat_service.robot_send(data)


@router.patch("/messages/{message_id}", response_model=MessageResponse)
async def edit_message(
    message_id: int,
    data: MessageEditRequest,
    current_user: UserContext = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service),
):
    return await _enqueue_edit(message_id, data, current_user, chat_service)


@router.delete("/messages/{message_id}")
async def delete_message(
    message_id: int,
    request: Request,
    current_user: UserContext = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service),
):
    await _enqueue_delete(message_id, current_user, chat_service)
    return {"ok": True}
