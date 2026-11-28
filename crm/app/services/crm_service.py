"""CRM service: sync DoveChat conversations with external CRM systems.

Provider-specific HTTP details live in app.services.providers. This
module orchestrates repositories, token refresh and calls to the chat,
account and tg-connector services.
"""
import hmac
import json
import logging
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from urllib.parse import parse_qs, urlencode, urlparse

import httpx
import jwt
from fastapi import HTTPException
from fastapi.responses import RedirectResponse

from app.schemas.crm_schemas import AmoLineConnectRequest
from app.services.providers.amocrm.client import AmoCrmClient
from app.services.providers.amocrm.provider import AmoCrmProvider
from app.services.providers.base import CrmProvider
from app.services.providers.bitrix.client import BitrixClient
from app.services.providers.bitrix.provider import BitrixProvider
from core.config import settings
from infra.crypto import decrypt, encrypt
from models.crm import CrmAccount
from models.lines import ChannelType
from models.messages import MessageDirection
from schemas.chat_schemas import MessageCreate, MessageStatus
from services.storage_service import StorageService

log = logging.getLogger("uvicorn.error")

_LOC = "crm/app/services/crm_service.py:CrmService"

PROVIDERS: dict[str, type[CrmProvider]] = {
    "bitrix24": BitrixProvider,
    "amocrm": AmoCrmProvider,
}

BITRIX_WEBHOOK_PATH = "/crm/api/v1/webhooks/bitrix"
BITRIX_EVENT_ADD = "ONIMCONNECTORMESSAGEADD"
BITRIX_EVENT_DELETE = "ONIMCONNECTORMESSAGEDELETE"
BITRIX_EVENT_UPDATE = "ONIMCONNECTORMESSAGEUPDATE"
BITRIX_EVENTS = (BITRIX_EVENT_ADD, BITRIX_EVENT_DELETE, BITRIX_EVENT_UPDATE)

# Событие Bitrix -> ручка chat, куда пересылаем правку или удаление.
CHANGE_ENDPOINTS = {
    BITRIX_EVENT_DELETE: "crm-delete",
    BITRIX_EVENT_UPDATE: "crm-edit",
}

WABA_TEMPLATES = {
    "disclaimer": {
        "name": "disclaimer",
        "language": "ru",
        "components": [],
    },
    "first_welcome_messsage": {
        "name": "first_welcome_messsage",
        "language": "en",
        "components": [
            {
                "type": "body",
                "parameters": [{"type": "text", "text": "DoveChat"}],
            }
        ],
    },
}

_BB_TAGS = re.compile(r"\[/?(?:b|br)\]")
_TEMPLATE_RE = re.compile(r"^\[template:(\w+)\]$")
_EMBED_TOKEN_TTL = 8 * 3600
_EMBED_TOKEN_SUB = "970fb0f9-fbb4-46b3-8153-0c59f7ac883f"

# Поле сделки, куда робот пишет результат проверки аккаунта.
DEAL_STATUS_FIELD = "UF_CRM_DCHAT_WA_TG_STATUS"
# Пользовательское поле конкретного портала; вынести в настройки.
LEGACY_STATUS_FIELD = "UF_CRM_1781704941990"
INSTALL_PAGE_PATH = "/crm/api/v1/install/bitrix"
INSTALL_FINISH_URL = "/crm/html/install_finish.html"
ROBOT_HANDLER_PATH = "/crm/api/v1/robots"
ROBOT_SEND_TEXT = "dovechat_send_text"
ROBOT_SEND_FILE = "dovechat_send_file"
ROBOT_CHECK_ACCOUNT = "dovechat_check_account"
ROBOTS = (
    (ROBOT_SEND_TEXT, "DoveChat: Отправить текст"),
    (ROBOT_SEND_FILE, "DoveChat: Отправить файл"),
    (ROBOT_CHECK_ACCOUNT, "DoveChat: Проверить аккаунт"),
)
ROBOT_CHANNEL_OPTIONS = {
    "telegram_bot": "Telegram Bot",
    "telegram_user": "Telegram User",
    "whatsapp_green": "WhatsApp (Green)",
    "waba": "WhatsApp Business API",
    "vk": "VK",
}
ROBOT_DEAL_FILTER = {
    "INCLUDE": [
        ["crm", "CCrmDocumentDeal"],
        ["crm", "CCrmDocumentLead"],
        ["crm", "CCrmDocumentContact"],
    ]
}
WA_TG_STATUS_FIELD = {
    "ENTITY_ID": "CRM_DEAL",
    "FIELD_NAME": "DOVECHAT_WA_TG_STATUS",
    "USER_TYPE_ID": "string",
    "XML_ID": "DOVECHAT_WA_TG_STATUS",
    "EDIT_FORM_LABEL": "DoveChat: статус в мессенджере",
    "LIST_COLUMN_LABEL": "DoveChat: статус",
    "MANDATORY": "N",
}


def _parse_form(body: bytes) -> dict[str, str]:
    """Разбирает form-urlencoded тело в плоский словарь."""
    parsed = parse_qs(body.decode())
    return {key: values[0] for key, values in parsed.items()}


def _prop(
    name: str, type_: str = "string", required: str | None = None
) -> dict:
    """Описание свойства робота Bitrix."""
    prop = {"Name": name, "Type": type_, "Multiple": "N"}
    if required is not None:
        prop["Required"] = required
    return prop


def _robot_definition(code: str) -> tuple[dict, dict]:
    """Свойства и возвращаемые значения робота по его коду."""
    props = {
        "phone": _prop("Телефон клиента", required="Y"),
        "line_id": _prop("ID линии", required="Y"),
    }
    if code != ROBOT_CHECK_ACCOUNT:
        props["text"] = _prop(
            "Текст сообщения",
            "text",
            required="Y" if code == ROBOT_SEND_TEXT else "N",
        )
    props["channel_type"] = {
        **_prop("Канал", "select", required="Y"),
        "Options": ROBOT_CHANNEL_OPTIONS,
    }
    if code == ROBOT_SEND_FILE:
        props["file_url"] = _prop("Ссылка на файл", required="Y")
        props["file_name"] = _prop("Имя файла", required="N")

    if code == ROBOT_CHECK_ACCOUNT:
        returns = {
            "exists": _prop("Зарегистрирован", "bool"),
            "user_id": _prop("ID пользователя"),
            "error": _prop("Ошибка"),
        }
    else:
        returns = {
            "status": _prop("Статус"),
            "message_id": _prop("ID сообщения"),
            "error": _prop("Ошибка"),
        }
    return props, returns


def internal_headers() -> dict:
    """Заголовок межсервисной авторизации chat <-> crm."""
    return {"X-Internal-Token": settings.internal_api_token}


def get_provider(conn: CrmAccount) -> CrmProvider:
    cls = PROVIDERS.get(conn.crm_type)
    if not cls:
        raise ValueError(f"Unknown CRM type: {conn.crm_type}")
    return cls(conn)


def _msg_field(data: dict, path: str) -> str | None:
    """Поле первого сообщения из плоского form-тела вебхука Bitrix."""
    return data.get(f"data[MESSAGES][0]{path}")


def _strip_bb(text: str | None) -> str:
    return _BB_TAGS.sub("", text or "").strip()


def _bitrix_chat_id(conn, conversation_id: uuid.UUID) -> str:
    return f"{conn.connector_id}_conv{conversation_id}"


def _bitrix_session_chat_id(result: dict, conversation) -> str:
    """CHAT_ID сессии открытой линии из ответа Bitrix."""
    raw = result.get("result")
    if not isinstance(raw, dict):
        return conversation.crm_chat_id or ""
    rows = raw.get("DATA", {}).get("RESULT") or []
    if not rows:
        return conversation.crm_chat_id or ""
    return str(rows[0].get("session", {}).get("CHAT_ID", ""))


class CrmService:
    def __init__(self, uow) -> None:
        self._uow = uow

    # ------------------------------------------------------------------
    # Общие помощники
    # ------------------------------------------------------------------
    async def _with_token_refresh(
        self,
        conn: CrmAccount,
        call: Callable[[CrmProvider], Awaitable],
    ):
        """Вызывает call(provider); при 401 обновляет токены и повторяет."""
        provider = get_provider(conn)
        try:
            return await call(provider)
        except httpx.HTTPStatusError as e:
            if e.response.status_code != 401:
                raise
        new_tokens = await provider.refresh_tokens()
        await self._uow.crm_accounts.update(conn, **new_tokens)
        return await call(provider)

    async def _refresh_app_tokens(
        self, conn: CrmAccount, domain: str | None = None
    ) -> str:
        """Обновляет токены Bitrix ключами приложения.

        Возвращает действующий access_token; при ошибке обновления
        возвращает прежний.
        """
        try:
            refreshed = await BitrixClient.refresh_tokens(
                domain=domain or conn.domain,
                refresh_token=decrypt(conn.refresh_token_enc),
                client_id=settings.BITRIX_CLIENT_ID,
                client_secret=settings.BITRIX_CLIENT_SECRET,
            )
            await self._uow.crm_accounts.update(
                conn,
                access_token_enc=encrypt(refreshed["access_token"]),
                refresh_token_enc=encrypt(refreshed["refresh_token"]),
            )
            return refreshed["access_token"]
        except Exception as e:
            log.error("%s._refresh_app_tokens: %s", _LOC, e)
            return decrypt(conn.access_token_enc)

    async def _refresh_via_provider(self, account: CrmAccount) -> str:
        """Обновляет токены ключами самого аккаунта (через провайдер)."""
        try:
            provider = get_provider(account)
            refreshed = await provider.refresh_tokens()
            await self._uow.crm_accounts.update(account, **refreshed)
            return decrypt(refreshed["access_token_enc"])
        except Exception as e:
            log.error("%s._refresh_via_provider: %s", _LOC, e)
            return decrypt(account.access_token_enc)

    async def _post_chat(
        self, path: str, *, timeout: float = 10, **kwargs
    ) -> httpx.Response | None:
        """POST в /v1/internal/<path> сервиса chat. None при сбое сети."""
        url = f"{settings.chat_internal_url}/v1/internal/{path}"
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    url,
                    headers=internal_headers(),
                    timeout=timeout,
                    **kwargs,
                )
        except httpx.HTTPError as e:
            log.error("%s._post_chat %s: %s", _LOC, path, e)
            return None
        log.info("%s._post_chat %s status=%s", _LOC, path, resp.status_code)
        return resp

    async def _tenant_owner(self, tenant_id: uuid.UUID):
        """Владелец тенанта или None."""
        memberships = await self._uow.memberships.get_multi(
            filters={"tenant_id": tenant_id, "role": "owner"},
            limit=1,
        )
        if not memberships:
            log.warning("%s._tenant_owner: no owner tenant=%s",
                        _LOC, tenant_id)
            return None
        return await self._uow.users.get_by_id(memberships[0].user_id)

    async def _find_conversation(self, line_id: uuid.UUID, crm_chat_id: str):
        convs = await self._uow.conversations.get_multi(
            filters={"line_id": line_id, "crm_chat_id": crm_chat_id},
            limit=1,
        )
        return convs[0] if convs else None

    def _embed_token(self, tenant_id: uuid.UUID) -> str:
        return jwt.encode(
            {
                "sub": _EMBED_TOKEN_SUB,
                "tenant_id": str(tenant_id),
                "role": "owner",
                "type": "access",
                "exp": int(time.time()) + _EMBED_TOKEN_TTL,
            },
            settings.secret_key,
            algorithm=settings.algorithm,
        )

    # ------------------------------------------------------------------
    # chat -> CRM
    # ------------------------------------------------------------------
    async def send_to_crm(
        self,
        tenant_id: uuid.UUID,
        conversation_id: uuid.UUID,
        client_phone: str,
        client_name: str,
        text: str,
        message_id: str,
        channel_type: str,
        media: list[dict] | None = None,
    ) -> tuple[str | None, str | None]:
        """Отправляет сообщение в CRM.

        Возвращает (crm_chat_id, crm_message_id); любое из значений
        может быть None (нет подключения, сбой, Amo не отдаёт id).
        """
        conn = await self._uow.crm_accounts.get_by_tenant_and_channel(
            tenant_id, channel_type
        )
        if not conn:
            return None, None
        conversation = await self._uow.conversations.get_by_id(
            conversation_id
        )
        if conversation is None:
            log.error("%s.send_to_crm: no conversation id=%s",
                      _LOC, conversation_id)
            return None, None

        try:
            result = await self._with_token_refresh(
                conn,
                lambda provider: provider.send_message(
                    conversation_id=str(conversation_id),
                    client_phone=client_phone,
                    client_name=client_name,
                    text=text,
                    message_id=message_id,
                    crm_chat_id=conversation.crm_chat_id,
                    media=media or [],
                ),
            )
        except Exception as e:
            log.error("%s.send_to_crm: %s", _LOC, e, exc_info=True)
            return None, None

        if conn.crm_type == "amocrm":
            chat_id = result.get("crm_chat_id") or f"dc_{conversation_id}"
            await self._uow.conversations.update(
                conversation, crm_chat_id=chat_id
            )
            return chat_id, None

        session_chat_id = _bitrix_session_chat_id(result, conversation)
        if session_chat_id and session_chat_id != conversation.crm_chat_id:
            await self._uow.conversations.update(
                conversation, crm_chat_id=session_chat_id
            )

        real_message_id = None
        if session_chat_id:
            try:
                bx = BitrixClient(
                    conn.domain, decrypt(conn.access_token_enc)
                )
                real_message_id = await bx.get_real_message_id(
                    session_chat_id
                )
            except Exception as e:
                log.error("%s.send_to_crm real id: %s", _LOC, e)
        return session_chat_id or None, real_message_id

    async def _change_crm_message(
        self,
        op_name: str,
        tenant_id: uuid.UUID,
        conversation_id: uuid.UUID,
        channel_type: str,
        call: Callable[[CrmProvider, dict], Awaitable],
    ) -> None:
        """Общая часть edit_to_crm и delete_from_crm."""
        conn = await self._uow.crm_accounts.get_by_tenant_and_channel(
            tenant_id, channel_type
        )
        if not conn:
            return
        conversation = await self._uow.conversations.get_by_id(
            conversation_id
        )
        if conversation is None or not conversation.crm_chat_id:
            return
        target = {
            "connector_id": conn.connector_id,
            "line_id": str(conn.crm_line_id),
            "user_id": conversation.client_phone,
            "chat_id": _bitrix_chat_id(conn, conversation_id),
        }
        try:
            await self._with_token_refresh(
                conn, lambda provider: call(provider, target)
            )
        except Exception as e:
            log.error("%s.%s tenant=%s: %s", _LOC, op_name, tenant_id, e)

    async def delete_from_crm(
        self,
        tenant_id: uuid.UUID,
        conversation_id: uuid.UUID,
        client_phone: str,
        message_id: str,
        channel_type: str,
    ) -> None:
        await self._change_crm_message(
            "delete_from_crm",
            tenant_id,
            conversation_id,
            channel_type,
            lambda provider, target: provider.delete_message(
                message_id=message_id, **target
            ),
        )

    async def edit_to_crm(
        self,
        tenant_id: uuid.UUID,
        conversation_id: uuid.UUID,
        message_id: str,
        text: str,
        channel_type: str,
    ) -> None:
        await self._change_crm_message(
            "edit_to_crm",
            tenant_id,
            conversation_id,
            channel_type,
            lambda provider, target: provider.edit_message(
                message_id=message_id, text=text, **target
            ),
        )

    # ------------------------------------------------------------------
    # CRM -> chat: Bitrix
    # ------------------------------------------------------------------
    async def handle_webhook(
        self, tenant_id: uuid.UUID, crm_type: str, data: dict
    ) -> dict:
        conn = await self._uow.crm_accounts.get_by_tenant(
            tenant_id, crm_type
        )
        if not conn:
            return {}
        parsed = await get_provider(conn).handle_webhook(data)
        parsed["crm_connection_id"] = conn.id
        return parsed

    async def _collect_bitrix_files(
        self, data: dict
    ) -> list[tuple[bytes, str, str]]:
        """Скачивает вложения сообщения Bitrix: (байты, имя, mime)."""
        files: list[tuple[bytes, str, str]] = []
        i = 0
        async with httpx.AsyncClient(timeout=30) as client:
            while True:
                prefix = f"[message][files][{i}]"
                link = _msg_field(data, f"{prefix}[downloadLink]")
                if not link:
                    break
                name = _msg_field(data, f"{prefix}[name]") or f"file_{i}"
                mime = (
                    _msg_field(data, f"{prefix}[mime]")
                    or "application/octet-stream"
                )
                try:
                    resp = await client.get(link)
                    resp.raise_for_status()
                    files.append((resp.content, name, mime))
                except Exception as e:
                    log.error("%s._collect_bitrix_files i=%s: %s",
                              _LOC, i, e)
                i += 1
        return files

    def _verify_bitrix_app_token(
        self, account: CrmAccount, data: dict
    ) -> bool:
        """Сверяет application_token вебхука с сохранённым при установке."""
        if not account.bitrix_application_token_enc:
            log.warning("%s: no stored application_token account=%s",
                        _LOC, account.id)
            return True
        expected = decrypt(account.bitrix_application_token_enc)
        received = data.get("auth[application_token]") or ""
        return hmac.compare_digest(expected, received)

    async def handle_bitrix_incoming(self, data: dict) -> None:
        """Обрабатывает вебхук Bitrix: ADD, UPDATE, DELETE сообщений."""
        member_id = data.get("auth[member_id]")
        event = data.get("event")
        if not member_id or event not in BITRIX_EVENTS:
            return
        line_conn = await self._uow.crm_accounts.get_by_member_and_connector(
            member_id, data.get("data[CONNECTOR]")
        )
        if not line_conn or not line_conn.line_id:
            log.warning("%s.handle_bitrix_incoming: no line connection",
                        _LOC)
            return
        account = await self._uow.crm_accounts.get_by_id(
            line_conn.crm_account_id
        )
        if not account:
            log.warning("%s.handle_bitrix_incoming: no account id=%s",
                        _LOC, line_conn.crm_account_id)
            return
        if not self._verify_bitrix_app_token(account, data):
            log.error("%s.handle_bitrix_incoming: bad application_token "
                      "account=%s", _LOC, account.id)
            return
        owner = await self._tenant_owner(account.tenant_id)
        if owner is None:
            return

        if event in CHANGE_ENDPOINTS:
            await self._forward_bitrix_change(line_conn, owner, data, event)
        else:
            await self._forward_bitrix_outgoing(
                line_conn, account, owner, data
            )

    async def _forward_bitrix_change(
        self, line_conn, owner, data: dict, event: str
    ) -> None:
        """Правка или удаление сообщения в Bitrix -> ручка chat."""
        bx_message_id = str(_msg_field(data, "[im][message_id]") or "")
        crm_chat_id = _msg_field(data, "[im][chat_id]")
        conv = await self._find_conversation(line_conn.line_id, crm_chat_id)
        if not conv:
            log.warning("%s._forward_bitrix_change: no conversation "
                        "crm_chat_id=%s", _LOC, crm_chat_id)
            return
        target = await self._uow.msg_repo.get_message_by_crm_message_id(
            conv.id, bx_message_id
        )
        if not target:
            log.warning("%s._forward_bitrix_change: no message "
                        "crm_message_id=%s", _LOC, bx_message_id)
            return
        form = {
            "message_id": str(target.id),
            "channel_type": line_conn.channel_type,
            "phone": conv.client_phone,
            "line_id": str(line_conn.line_id),
            "user_id": str(owner.id),
            "user_email": owner.email,
        }
        if event == BITRIX_EVENT_UPDATE:
            form["text"] = _strip_bb(_msg_field(data, "[message][text]"))
        await self._post_chat(CHANGE_ENDPOINTS[event], data=form)

    async def _forward_bitrix_outgoing(
        self, line_conn, account: CrmAccount, owner, data: dict
    ) -> None:
        """Новое сообщение оператора из Bitrix -> chat -> клиенту."""
        access_token = await self._refresh_via_provider(account)
        crm_chat_id = _msg_field(data, "[im][chat_id]")
        conv = await self._find_conversation(line_conn.line_id, crm_chat_id)
        if not conv:
            log.warning("%s._forward_bitrix_outgoing: no conversation "
                        "crm_chat_id=%s", _LOC, crm_chat_id)
            return

        text = _strip_bb(_msg_field(data, "[message][text]"))
        match = _TEMPLATE_RE.match(text)
        template = WABA_TEMPLATES.get(match.group(1)) if match else None
        crm_message_id = str(_msg_field(data, "[im][message_id]") or "")

        msg_json = MessageCreate(
            conversation_id=conv.id,
            direction=MessageDirection.outbound,
            phone=conv.client_phone,
            crm_message_id=crm_message_id,
            text=f"[template:{template['name']}]" if template else text,
            status=MessageStatus.sent,
            template=template,
        ).model_dump_json()

        form = {
            "channel_type": line_conn.channel_type,
            "user_id": str(owner.id),
            "user_email": owner.email,
        }
        files = await self._collect_bitrix_files(data)
        if files:
            multipart = [("data", (None, msg_json, "application/json"))]
            multipart += [
                ("files", (name, content, mime))
                for content, name, mime in files
            ]
            await self._post_chat("crm-outgoing", files=multipart, data=form)
        else:
            form["data"] = msg_json
            await self._post_chat("crm-outgoing", data=form)

        await self._send_bitrix_delivery_status(
            line_conn, account, access_token, data
        )

    async def _send_bitrix_delivery_status(
        self,
        line_conn,
        account: CrmAccount,
        access_token: str,
        data: dict,
    ) -> None:
        crm_chat_id = _msg_field(data, "[im][chat_id]")
        im_message_id = _msg_field(data, "[im][message_id]")
        if not (crm_chat_id and im_message_id):
            return
        try:
            bx = BitrixClient(domain=account.domain,
                              access_token=access_token)
            await bx.call("imconnector.send.status.delivery", {
                "CONNECTOR": line_conn.connector_id,
                "LINE": line_conn.crm_line_id,
                "MESSAGES[0][im][chat_id]": crm_chat_id,
                "MESSAGES[0][im][message_id]": im_message_id,
                "MESSAGES[0][message][id][0]": im_message_id,
                "MESSAGES[0][chat][id]": _msg_field(data, "[chat][id]"),
            })
        except Exception as e:
            log.error("%s._send_bitrix_delivery_status: %s", _LOC, e)

    # ------------------------------------------------------------------
    # Установка приложения Bitrix
    # ------------------------------------------------------------------
    async def _bind_bitrix_events(self, bx: BitrixClient) -> None:
        handler = f"{settings.host_url}{BITRIX_WEBHOOK_PATH}"
        for event in BITRIX_EVENTS:
            try:
                await bx.call(
                    "event.unbind", {"event": event, "handler": handler}
                )
            except Exception as e:
                log.info("%s._bind_bitrix_events unbind %s: %s",
                         _LOC, event, e)
            await bx.call(
                "event.bind",
                {"event": event, "handler": handler, "auth_type": 1},
            )
            log.info("%s._bind_bitrix_events bound %s", _LOC, event)

    async def _register_robots(self, bx: BitrixClient) -> None:
        for code, name in ROBOTS:
            try:
                await bx.call("bizproc.robot.delete", {"CODE": code})
            except Exception as e:
                log.info("%s._register_robots delete %s: %s",
                         _LOC, code, e)
            props, returns = _robot_definition(code)
            try:
                await bx.call(
                    "bizproc.robot.add",
                    {
                        "CODE": code,
                        "HANDLER": (
                            f"{settings.host_url}{ROBOT_HANDLER_PATH}/{code}"
                        ),
                        "AUTH_USER_ID": 1,
                        "USE_SUBSCRIPTION": "Y",
                        "NAME": name,
                        "PROPERTIES": props,
                        "RETURN_PROPERTIES": returns,
                        "FILTER": ROBOT_DEAL_FILTER,
                    },
                    use_json=True,
                )
                log.info("%s._register_robots registered %s", _LOC, code)
            except Exception as e:
                log.warning("%s._register_robots add %s: %s",
                            _LOC, code, e)
        try:
            await bx.call(
                "crm.userfield.add", {"fields": WA_TG_STATUS_FIELD}
            )
        except Exception as e:
            log.info("%s._register_robots userfield (may exist): %s",
                     _LOC, e)

    async def save_install_tokens(
        self, data: dict, domain: str = ""
    ) -> tuple[bool, str] | None:
        """Сохраняет токены установки и настраивает портал Bitrix.

        data: разобранное тело ONAPPINSTALL. Возвращает
        (открыто ли из настроек коннектора, домен) или None, если в
        запросе нет обязательных полей.
        """
        app_token = data.get("APPLICATION_TOKEN")
        access_token = data.get("AUTH_ID")
        refresh_token = data.get("REFRESH_ID")
        member_id = data.get("member_id")
        domain = data.get("auth[domain]") or domain
        if not all([app_token, access_token, refresh_token, member_id,
                    domain]):
            log.warning("%s.save_install_tokens: missing auth fields",
                        _LOC)
            return None

        fields = {
            "member_id": member_id,
            "access_token_enc": encrypt(access_token),
            "refresh_token_enc": encrypt(refresh_token),
            "bitrix_application_token_enc": encrypt(app_token),
        }
        rows = await self._uow.crm_accounts.get_multi(
            filters={"domain": domain, "crm_type": "bitrix24"}, limit=1
        )
        if rows:
            await self._uow.crm_accounts.update(rows[0], **fields)
        else:
            await self._uow.crm_accounts.create(
                crm_type="bitrix24", domain=domain, is_active=True, **fields
            )

        bx = BitrixClient(domain=domain, access_token=access_token)
        for connector_id in settings.BITRIX_CONNECTOR_MAP.values():
            name = settings.BITRIX_CONNECTOR_NAMES.get(
                connector_id, connector_id
            )
            try:
                await bx.register_connector(connector_id, name=name)
            except Exception as e:
                log.warning("%s.save_install_tokens connector %s: %s",
                            _LOC, connector_id, e)
        await self._bind_bitrix_events(bx)
        await self._register_robots(bx)
        return data.get("PLACEMENT") == "SETTING_CONNECTOR", domain

    async def handle_onappinstall(self, body: bytes, domain: str = ""):
        data = _parse_form(body)
        saved = await self.save_install_tokens(data, domain=domain)
        if saved is None or not saved[0]:
            return RedirectResponse(url=INSTALL_FINISH_URL, status_code=303)

        connector, line = "", ""
        options = data.get("PLACEMENT_OPTIONS", "")
        if options:
            try:
                parsed = json.loads(options)
                connector = parsed.get("CONNECTOR", "")
                line = parsed.get("LINE", "")
            except (json.JSONDecodeError, AttributeError) as e:
                log.warning("%s.handle_onappinstall options: %s", _LOC, e)

        # Токены передаются во фрагменте (#), а не в query: фрагмент не
        # попадает в access-логи, Referer и историю прокси.
        params = urlencode({
            "DOMAIN": saved[1],
            "ACCESS_TOKEN": data.get("AUTH_ID", ""),
            "REFRESH_TOKEN": data.get("REFRESH_ID", ""),
            "MEMBER_ID": data.get("member_id", ""),
            "CONNECTOR": connector,
            "LINE": line,
        })
        return RedirectResponse(
            url=f"{settings.host_url}{INSTALL_PAGE_PATH}#{params}",
            status_code=303,
        )

    # ------------------------------------------------------------------
    # Подключение линии (страница выбора в Bitrix)
    # ------------------------------------------------------------------
    async def _account_request(
        self, method: str, path: str, **kwargs
    ) -> httpx.Response | None:
        """Запрос к сервису account. None при сбое сети."""
        url = f"{settings.account_internal_url}{path}"
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                return await client.request(method, url, **kwargs)
        except httpx.HTTPError as e:
            log.error("%s._account_request %s: %s", _LOC, path, e)
            return None

    async def get_lines_for_bitrix(
        self, email: str, password: str, bx_auth: dict
    ) -> dict:
        resp = await self._account_request(
            "POST",
            "/api/v1/auth/login",
            data={"username": email, "password": password},
        )
        if resp is None:
            return {"error": "Ошибка подключения к серверу"}
        if resp.status_code != 200:
            return {"error": "Неверный email или пароль"}
        jwt_token = resp.json().get("access_token")

        resp = await self._account_request(
            "GET",
            "/api/v1/tenant/",
            headers={"Authorization": f"Bearer {jwt_token}"},
        )
        ok = resp is not None and resp.status_code == 200
        return {
            "tenants": resp.json() if ok else [],
            "jwt_token": jwt_token,
            "bx_auth": bx_auth,
        }

    async def get_lines_for_tenant(
        self, jwt_token: str, tenant_id: str, bx_auth: dict
    ) -> dict:
        resp = await self._account_request(
            "POST",
            "/api/v1/auth/switch-tenant",
            params={"tenant_id": tenant_id},
            headers={"Authorization": f"Bearer {jwt_token}"},
        )
        if resp is None:
            return {"error": "Ошибка подключения к серверу"}
        if resp.status_code != 200:
            return {"error": "Ошибка переключения компании"}
        tenant_token = resp.json().get("access_token")

        resp = await self._account_request(
            "GET",
            f"/api/v1/tenants/{tenant_id}/lines/",
            headers={"Authorization": f"Bearer {tenant_token}"},
        )
        ok = resp is not None and resp.status_code == 200
        return {"lines": resp.json() if ok else [], "bx_auth": bx_auth}

    async def connect_line(self, line_id: uuid.UUID, bx_auth: dict) -> dict:
        domain = bx_auth.get("domain", "")
        member_id = bx_auth.get("member_id", "")
        access_token = bx_auth.get("access_token", "")
        refresh_token = bx_auth.get("refresh_token", "")
        connector_id = bx_auth.get("connector")

        line = await self._uow.lines.get_by_id(line_id)
        if not line:
            return {"error": "Линия не найдена"}
        channel_type = next(
            (k for k, v in settings.BITRIX_CONNECTOR_MAP.items()
             if v == connector_id),
            None,
        )
        if not channel_type:
            return {"error": f"Неподдерживаемый коннектор: {connector_id}"}

        rows = await self._uow.crm_accounts.get_multi(
            filters={"domain": domain}, limit=1
        )
        account = rows[0] if rows else None
        if (account and account.tenant_id
                and account.tenant_id != line.tenant_id):
            return {"error": "Этот портал Bitrix уже привязан к другому "
                             "тенанту DoveChat"}

        if (account and account.bitrix_client_id_enc
                and account.bitrix_client_secret_enc):
            try:
                refreshed = await BitrixClient.refresh_tokens(
                    domain=domain,
                    refresh_token=refresh_token,
                    client_id=decrypt(account.bitrix_client_id_enc),
                    client_secret=decrypt(account.bitrix_client_secret_enc),
                )
            except Exception as e:
                log.error("%s.connect_line token refresh: %s", _LOC, e)
                return {"error": "Ошибка обновления токена Битрикс"}
            access_token = refreshed["access_token"]
            refresh_token = refreshed["refresh_token"]

        account_fields = {
            "tenant_id": line.tenant_id,
            "access_token_enc": encrypt(access_token),
            "refresh_token_enc": encrypt(refresh_token),
            "member_id": member_id,
            "is_active": True,
        }
        if account:
            await self._uow.crm_accounts.update(account, **account_fields)
        else:
            account = await self._uow.crm_accounts.create(
                crm_type="bitrix24", domain=domain, **account_fields
            )

        bx = BitrixClient(domain=domain, access_token=access_token)
        await bx.register_connector(connector_id)

        rows = await self._uow.crm_line_connections.get_multi(
            filters={"line_id": line_id, "connector_id": connector_id},
            limit=1,
        )
        line_conn = rows[0] if rows else None
        crm_line_id = (
            bx_auth.get("bx_line_id")
            or (line_conn.crm_line_id if line_conn else None)
            or await bx.create_open_line(line.name)
        )

        conflicts = await self._uow.crm_line_connections.get_multi(
            filters={
                "crm_line_id": crm_line_id,
                "connector_id": connector_id,
                "is_active": True,
            }
        )
        conflict = next((r for r in conflicts if r.line_id != line_id), None)
        if conflict:
            return {"error": "Открытая линия Bitrix уже привязана к другой "
                             f"линии DoveChat (line_id={conflict.line_id})"}

        await bx.link_connector(crm_line_id, connector_id)
        await bx.activate_connector(crm_line_id, connector_id)

        conn_fields = {
            "crm_account_id": account.id,
            "channel_type": channel_type,
            "connector_id": connector_id,
            "crm_line_id": crm_line_id,
            "is_active": True,
        }
        if line_conn:
            await self._uow.crm_line_connections.update(
                line_conn, **conn_fields
            )
        else:
            await self._uow.crm_line_connections.create(
                line_id=line_id, **conn_fields
            )
        return {"line_name": line.name}

    async def get_lines_for_member(self, member_id: str) -> dict:
        conn = await self._uow.crm_accounts.get_by_member_id(member_id)
        if not conn:
            return {"lines": []}
        line_conns = await self._uow.crm_line_connections.get_by_account(
            conn.id
        )
        lines: dict[uuid.UUID, dict] = {}
        for line_conn in line_conns:
            if line_conn.line_id in lines:
                continue
            line = await self._uow.lines.get_by_id(line_conn.line_id)
            if not line:
                continue
            channels = await self._uow.channel_accounts.get_multi(
                filters={"line_id": line.id}
            )
            lines[line.id] = {
                "id": str(line.id),
                "name": line.name,
                "channels": [
                    {
                        "id": str(c.id),
                        "channel_type": getattr(
                            c.channel_type, "value", c.channel_type
                        ),
                    }
                    for c in channels
                ],
            }
        return {"lines": list(lines.values())}

    async def start_outgoing(
        self,
        member_id: str,
        contact_id: str,
        line_id: uuid.UUID,
        channel_account_id: uuid.UUID | None,
        text: str,
    ) -> dict:
        """Исходящее сообщение контакту Bitrix, начатое из CRM."""
        conn = await self._uow.crm_accounts.get_by_member_id(member_id)
        if not conn:
            return {"error": "Подключение не найдено"}
        line_conns = await self._uow.crm_line_connections.get_multi(
            filters={
                "crm_account_id": conn.id,
                "line_id": line_id,
                "is_active": True,
            },
            limit=1,
        )
        if not line_conns:
            return {"error": "Нет активной связки линии с CRM"}
        line_conn = line_conns[0]

        access_token = await self._refresh_app_tokens(conn)
        bx = BitrixClient(domain=conn.domain, access_token=access_token)
        phone = await bx.get_contact_phone(contact_id)
        if not phone:
            return {"error": "Телефон контакта не найден"}

        # Тело соответствует схеме CrmMessageOut ручки crm-robot-send.
        resp = await self._post_chat(
            "crm-robot-send",
            json={
                "line_id": str(line_id),
                "client_phone": phone,
                "text": text,
                "crm_connection_id": str(conn.id),
                "contact_id": contact_id,
                "crm_line_id": line_conn.crm_line_id,
                "connector_id": line_conn.connector_id,
                "channel_type": line_conn.channel_type,
                "channel_account_id": (
                    str(channel_account_id) if channel_account_id else None
                ),
            },
        )
        if resp is None:
            return {"error": "Ошибка соединения с чат-сервисом"}
        if resp.status_code != 200:
            return {"error": "Ошибка отправки сообщения"}
        return resp.json()

    # ------------------------------------------------------------------
    # Роботы Bitrix
    # ------------------------------------------------------------------
    @staticmethod
    def _robot_prop(data: dict, key: str) -> str | None:
        return data.get(f"properties[{key}]") or data.get(
            f"PROPERTIES[{key}]"
        )

    async def handle_robot(self, robot_code: str, data: dict) -> dict:
        event_token = data.get("event_token") or data.get("EVENT_TOKEN")
        member_id = data.get("auth[member_id]")
        phone = self._robot_prop(data, "phone")
        line_id_str = self._robot_prop(data, "line_id")
        channel_type = self._robot_prop(data, "channel_type")
        if not all([event_token, member_id, phone, line_id_str]):
            log.error("%s.handle_robot: missing required fields", _LOC)
            return {"status": "error"}

        conn = await self._uow.crm_accounts.get_by_member_id(member_id)
        if not conn:
            log.error("%s.handle_robot: no account member_id=%s",
                      _LOC, member_id)
            return {"status": "error"}
        if not self._verify_bitrix_app_token(conn, data):
            log.error("%s.handle_robot: bad application_token", _LOC)
            return {"status": "error"}
        try:
            line_id = uuid.UUID(line_id_str)
        except ValueError:
            log.error("%s.handle_robot: invalid line_id", _LOC)
            return {"status": "error"}

        await self._refresh_app_tokens(conn)
        bx = BitrixClient(
            domain=conn.domain, access_token=data.get("auth[access_token]")
        )
        if robot_code == ROBOT_CHECK_ACCOUNT:
            return_values = await self._robot_check_account(
                phone, line_id, channel_type
            )
            await self._save_deal_status(
                bx, data, return_values.get("exists")
            )
        else:
            return_values = await self._robot_send(
                robot_code, data, conn, line_id, phone, channel_type
            )

        try:
            await bx.call("bizproc.event.send", {
                "event_token": event_token,
                "RETURN_VALUES": return_values,
                "LOG_MESSAGE": (
                    return_values.get("error")
                    or "Получен ответ от приложения."
                ),
            })
        except Exception as e:
            log.error("%s.handle_robot event.send: %s", _LOC, e)
        return {"status": "ok"}

    async def _save_deal_status(
        self, bx: BitrixClient, data: dict, exists: str | None
    ) -> None:
        """Пишет результат проверки аккаунта в поле сделки."""
        doc_id = data.get("document_id[2]") or data.get("DOCUMENT_ID[2]")
        if not doc_id or not doc_id.startswith("DEAL_"):
            return
        try:
            await bx.call(
                "crm.deal.update",
                {
                    "id": doc_id.removeprefix("DEAL_"),
                    "fields": {DEAL_STATUS_FIELD: exists},
                },
                use_json=True,
            )
        except Exception as e:
            log.error("%s._save_deal_status: %r", _LOC, e)

    async def _store_remote_file(self, url: str, file_name: str) -> dict:
        """Скачивает файл по https-ссылке и кладёт его в хранилище."""
        if urlparse(url).scheme != "https":
            raise ValueError("file_url must use https")
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url)
            resp.raise_for_status()
        media_id = uuid.uuid4()
        mime = resp.headers.get(
            "content-type", "application/octet-stream"
        ).split(";")[0]
        await StorageService().save_file(
            resp.content, media_id, file_name, mime
        )
        return {
            "id": str(media_id),
            "file_name": file_name,
            "mime_type": mime,
        }

    async def _robot_send(
        self,
        robot_code: str,
        data: dict,
        conn: CrmAccount,
        line_id: uuid.UUID,
        phone: str,
        channel_type: str,
    ) -> dict:
        line_conns = await self._uow.crm_line_connections.get_multi(
            filters={
                "crm_account_id": conn.id,
                "line_id": line_id,
                "channel_type": channel_type,
                "is_active": True,
            },
            limit=1,
        )
        if not line_conns:
            return {
                "status": "error",
                "message_id": "",
                "error": "Нет активной связки линии с CRM",
            }
        line_conn = line_conns[0]
        file_url = self._robot_prop(data, "file_url")
        file_name = self._robot_prop(data, "file_name") or "file"
        payload = {
            "line_id": str(line_id),
            "client_phone": phone,
            "text": self._robot_prop(data, "text") or "",
            "crm_connection_id": str(conn.id),
            "contact_id": "",
            "crm_line_id": line_conn.crm_line_id,
            "connector_id": line_conn.connector_id,
            "channel_type": line_conn.channel_type,
            "channel_account_id": None,
            "media": [],
        }

        started_at = time.monotonic()
        status, message_id, error = "error", "", ""
        try:
            if robot_code == ROBOT_SEND_FILE and file_url:
                payload["media"] = [
                    await self._store_remote_file(file_url, file_name)
                ]
            resp = await self._post_chat(
                "crm-robot-send", json=payload, timeout=15
            )
            if resp is None:
                error = "Ошибка соединения с чат-сервисом"
            else:
                result = resp.json()
                if result.get("status") == "ok" or result.get(
                    "conversation_id"
                ):
                    status = "ok"
                    message_id = result.get(
                        "message_id", result.get("conversation_id", "")
                    )
                else:
                    error = result.get("error", "unknown error")
        except Exception as e:
            log.error("%s._robot_send: %s", _LOC, e)
            error = "Ошибка отправки сообщения"

        log.info("%s._robot_send code=%s status=%s duration=%sms", _LOC,
                 robot_code, status,
                 int((time.monotonic() - started_at) * 1000))
        return {"status": status, "message_id": message_id, "error": error}

    async def _check_telegram(self, phone: str, line_id: uuid.UUID) -> bool:
        accounts = await self._uow.channel_accounts.get_multi(
            filters={
                "line_id": line_id,
                "channel_type": ChannelType.telegram_user,
            },
            limit=1,
        )
        if not accounts or not accounts[0].session_string:
            return False
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{settings.tg_connector_url}/prod/check-phone",
                    headers=internal_headers(),
                    json={
                        "phone": phone,
                        "session_string": accounts[0].session_string,
                    },
                    timeout=15,
                )
            return bool(resp.json().get("exists"))
        except Exception as e:
            log.error("%s._check_telegram: %s", _LOC, e)
            return False

    async def _check_whatsapp(self, phone: str, line_id: uuid.UUID) -> bool:
        accounts = await self._uow.channel_accounts.get_multi(
            filters={
                "line_id": line_id,
                "channel_type": ChannelType.whatsapp_green,
            },
            limit=1,
        )
        if not accounts:
            return False
        try:
            creds = json.loads(
                decrypt(accounts[0].credentials_encrypted["encrypted_data"])
            )
            url = (
                f"{settings.proxy_green_url}/waInstance"
                f"{creds.get('id_instance')}/checkWhatsapp/"
                f"{creds.get('api_token')}"
            )
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    url, json={"phoneNumber": int(phone)}, timeout=15
                )
            return bool(resp.json().get("existsWhatsapp"))
        except Exception as e:
            log.error("%s._check_whatsapp: %s", _LOC, e)
            return False

    async def _robot_check_account(
        self, phone: str, line_id: uuid.UUID, channel_type: str | None
    ) -> dict:
        """Проверяет, есть ли номер в мессенджере: {"exists": "Y"|"N"}."""
        if channel_type == ChannelType.telegram_user.value:
            exists = await self._check_telegram(phone, line_id)
        elif channel_type == ChannelType.whatsapp_green.value:
            exists = await self._check_whatsapp(phone, line_id)
        else:
            log.error("%s._robot_check_account: unsupported channel %s",
                      _LOC, channel_type)
            return {"exists": "N", "error": "Неподдерживаемый канал"}
        return {"exists": "Y" if exists else "N"}

    async def check_wa_tg(self, data: dict) -> dict:
        """По событию сделки проверяет контакт и пишет статус в сделку."""
        member_id = data.get("auth[member_id]")
        deal_id = data.get("data[FIELDS][ID]")
        domain = data.get("auth[domain]")
        if not all([member_id, deal_id, domain]):
            log.error("%s.check_wa_tg: missing required fields", _LOC)
            return {"status": "error"}
        conn = await self._uow.crm_accounts.get_by_member_id(member_id)
        if not conn:
            log.error("%s.check_wa_tg: no account member_id=%s",
                      _LOC, member_id)
            return {"status": "error"}
        line_conns = await self._uow.crm_line_connections.get_by_account(
            conn.id
        )
        line_conn = next((lc for lc in line_conns if lc.is_active), None)
        if line_conn is None:
            return {"status": "no_line"}

        access_token = await self._refresh_app_tokens(conn, domain=domain)
        bx = BitrixClient(domain=domain, access_token=access_token)
        try:
            deal = await bx.call("crm.deal.get", {"id": deal_id})
            contact_id = deal.get("result", {}).get("CONTACT_ID")
            if not contact_id or contact_id == "0":
                return {"status": "no_contact"}
            contact = await bx.call("crm.contact.get", {"id": contact_id})
            phones = contact.get("result", {}).get("PHONE", [])
            phone = phones[0].get("VALUE") if phones else None
        except Exception as e:
            log.error("%s.check_wa_tg crm fetch: %s", _LOC, e)
            return {"status": "error"}
        if not phone:
            return {"status": "no_phone"}

        result = await self._robot_check_account(
            re.sub(r"\D", "", phone),
            line_conn.line_id,
            line_conn.channel_type,
        )
        exists = result.get("exists") == "Y"
        status_text = (
            "Есть в мессенджере" if exists else "Нет в мессенджере"
        )
        try:
            await bx.call(
                "crm.deal.update",
                {"id": deal_id, "fields": {LEGACY_STATUS_FIELD: status_text}},
                use_json=True,
            )
        except Exception as e:
            log.error("%s.check_wa_tg deal update: %s", _LOC, e)
        return {"status": "ok", "exists": exists}

    # ------------------------------------------------------------------
    # amoCRM
    # ------------------------------------------------------------------
    async def handle_amo_install(
        self, code: str, domain: str
    ) -> RedirectResponse:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"https://{domain}/oauth2/access_token",
                json={
                    "client_id": settings.AMO_CLIENT_ID,
                    "client_secret": settings.AMO_CLIENT_SECRET,
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": settings.AMO_REDIRECT_URI,
                },
            )
            resp.raise_for_status()
            tokens = resp.json()

            resp = await client.get(
                f"https://{domain}/api/v4/account",
                headers={
                    "Authorization": f"Bearer {tokens['access_token']}"
                },
            )
            resp.raise_for_status()
            account = resp.json()

        # tenant_id неизвестен: привязка к тенанту делается позже.
        await self._uow.crm_accounts.upsert_amo(
            tenant_id=None,
            domain=domain,
            account_id=str(account["id"]),
            amojo_id=account.get("amojo_id", ""),
            access_token_enc=encrypt(tokens["access_token"]),
            refresh_token_enc=encrypt(tokens["refresh_token"]),
        )
        log.info("%s.handle_amo_install saved domain=%s", _LOC, domain)
        return RedirectResponse(url=INSTALL_FINISH_URL, status_code=303)

    def _amo_client(self, conn: CrmAccount) -> AmoCrmClient:
        return AmoCrmClient(
            channel_id=settings.AMO_CHANNEL_ID,
            channel_secret=settings.AMO_CHANNEL_SECRET,
            account_id=conn.amojo_id,
        )

    async def handle_amo_incoming(
        self, body: bytes, amojo_id: str, signature: str
    ) -> None:
        conn = await self._uow.crm_accounts.get_by_amojo_id(amojo_id)
        if not conn:
            log.error("%s.handle_amo_incoming: no account amojo_id=%s",
                      _LOC, amojo_id)
            return
        if not self._amo_client(conn).verify_webhook(body, signature):
            log.error("%s.handle_amo_incoming: invalid signature "
                      "amojo_id=%s", _LOC, amojo_id)
            return
        line_conns = await self._uow.crm_line_connections.get_by_account(
            conn.id
        )
        if not line_conns:
            log.warning("%s.handle_amo_incoming: no line connection "
                        "account=%s", _LOC, conn.id)
            return

        parsed = await AmoCrmProvider(conn).handle_webhook(json.loads(body))
        conv = await self._amo_conversation(conn, line_conns[0], parsed)
        if conv is None:
            return
        owner = await self._tenant_owner(conn.tenant_id)
        if owner is None:
            return

        msg_json = MessageCreate(
            conversation_id=conv.id,
            direction=MessageDirection.outbound,
            phone=conv.client_phone,
            text=parsed["text"],
            status=MessageStatus.sent,
        ).model_dump_json()
        await self._post_chat(
            "crm-outgoing",
            data={
                "data": msg_json,
                "channel_type": line_conns[0].channel_type,
                "user_id": str(owner.id),
                "user_email": owner.email,
            },
        )

    async def _amo_conversation(self, conn: CrmAccount, line_conn, parsed):
        """Диалог для чата amoCRM; при первом сообщении создаёт связку."""
        crm_chat_id = parsed["crm_chat_id"]
        link = await self._uow.crm_dialog_links.get_by_crm_chat_id(
            crm_chat_id
        )
        if link is None:
            convs = await self._uow.conversations.get_multi(
                filters={
                    "client_phone": parsed["client_phone"],
                    "line_id": line_conn.line_id,
                },
                limit=1,
            )
            if not convs:
                log.warning("%s._amo_conversation: no conversation "
                            "line=%s", _LOC, line_conn.line_id)
                return None
            link = await self._uow.crm_dialog_links.create(
                conversation_id=convs[0].id,
                crm_account_id=conn.id,
                crm_chat_id=crm_chat_id,
                crm_user_id=parsed["client_phone"],
            )
        conv = await self._uow.conversations.get_by_id(link.conversation_id)
        if conv is None:
            log.error("%s._amo_conversation: no conversation id=%s",
                      _LOC, link.conversation_id)
        return conv

    async def amo_connect_channel(self, tenant_id: uuid.UUID) -> dict:
        conn = await self._uow.crm_accounts.get_by_tenant(
            tenant_id=tenant_id, crm_type="amocrm"
        )
        if not conn:
            raise ValueError(f"No amocrm connection tenant={tenant_id}")
        result = await self._amo_client(conn).connect_account()
        await self._uow.crm_accounts.update(
            conn, amojo_id=result.get("account_id")
        )
        return result

    async def disconnect_amo_line(self, line_id: uuid.UUID) -> None:
        conn = await self._uow.crm_line_connections.get_by_line(line_id)
        if conn:
            await self._uow.crm_line_connections.delete(conn)

    async def list_amo_accounts(self) -> list:
        return await self._uow.crm_accounts.get_multi(
            filters={"crm_type": "amocrm", "is_active": True},
        )

    async def connect_amo_line(self, body: AmoLineConnectRequest) -> dict:
        existing = await self._uow.crm_line_connections.get_multi(
            filters={"line_id": body.line_id, "is_active": True}, limit=1
        )
        if existing:
            await self._uow.crm_line_connections.update(
                existing[0],
                crm_account_id=body.crm_account_id,
                channel_type=body.channel_type,
            )
        else:
            await self._uow.crm_line_connections.create(
                crm_account_id=body.crm_account_id,
                line_id=body.line_id,
                channel_type=body.channel_type,
                is_active=True,
            )
        return {"ok": True}

    # ------------------------------------------------------------------
    # Диалог по сделке amoCRM
    # ------------------------------------------------------------------
    async def _conversation_by_lead_link(self, lead_id: str):
        links = await self._uow.crm_dialog_links.get_multi(
            filters={"amo_lead_id": lead_id}, limit=1
        )
        if not links:
            return None
        return await self._uow.conversations.get_by_id(
            links[0].conversation_id
        )

    async def _link_lead_to_conversation(self, lead_id: str):
        """Ищет диалог по телефону контакта сделки во всех аккаунтах Amo."""
        accounts = await self._uow.crm_accounts.get_multi(
            filters={"crm_type": "amocrm"}
        )
        for account in accounts:
            phone = await AmoCrmProvider(account).get_lead_contact_phone(
                domain=account.domain,
                access_token=decrypt(account.access_token_enc),
                lead_id=lead_id,
            )
            if not phone:
                continue
            convs = await self._uow.conversations.get_multi(
                filters={"client_phone": phone}, limit=1
            )
            if not convs:
                continue
            links = await self._uow.crm_dialog_links.get_multi(
                filters={"conversation_id": convs[0].id}, limit=1
            )
            if links:
                await self._uow.crm_dialog_links.update(
                    links[0], amo_lead_id=lead_id
                )
            return convs[0]
        return None

    async def get_conv_by_lead(self, lead_id: str) -> tuple[uuid.UUID, str]:
        """Возвращает (id диалога, embed-токен) для сделки amoCRM."""
        conversation = (
            await self._conversation_by_lead_link(lead_id)
            or await self._link_lead_to_conversation(lead_id)
        )
        if conversation is None:
            raise HTTPException(
                status_code=404, detail="conversation not found for lead"
            )
        line = await self._uow.lines.get_by_id(conversation.line_id)
        if line is None:
            raise HTTPException(status_code=404, detail="line not found")
        return conversation.id, self._embed_token(line.tenant_id)