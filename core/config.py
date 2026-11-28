import logging

from pydantic_settings import BaseSettings, SettingsConfigDict

log = logging.getLogger(__name__)


# Разрешённые CORS-origins по умолчанию: локальная разработка.
# В проде переопределяется переменной окружения BACKEND_CORS_ORIGINS.
DEFAULT_CORS_ORIGINS = (
    "http://localhost,http://127.0.0.1,"
    "http://localhost:3000,http://127.0.0.1:3000,"
    "http://localhost:3001,http://127.0.0.1:3001,"
    "http://localhost:5173,http://127.0.0.1:5173,"
    "http://localhost:8000,http://127.0.0.1:8000,"
    "http://localhost:8001,http://127.0.0.1:8001,"
    "http://localhost:8080,http://127.0.0.1:8080"
)


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    project_dir: str = "/opt/dovechat"

    # Database
    database_url: str

    # JWT
    secret_key: str
    algorithm: str = "HS256"

    # Service-to-service auth for /internal/* endpoints (chat <-> crm)
    internal_api_token: str = ""
    access_token_expire_minutes: int = 1440  # на проде задать через env ACCESS_TOKEN_EXPIRE_MINUTES
    invite_token_expire_hours: int = 72
    invite_url: str = "https://tg.dove-chat.pro/dev/lc/invite"
    refresh_token_expire_days: int = 7

    # API
    host_url: str = "http://localhost"
    proxy_url: str = "http://localhost"
    proxy_green_url: str = "https://tg.dove-chat.pro/green"  # api.green-api.com
    proxy_green_wh: str = ""
    proxy_green_login: str = ""
    proxy_green_password: str = ""
    api_v1_prefix: str = "/api/v1"
    debug: bool = False
    load_test_mode: bool = False

    # CORS
    backend_cors_origins: str = ""

    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str | None = None  # ← Optional
    smtp_password: str | None = None  # ← Optional  
    smtp_from: str | None = None
    otp_subject: str = "Authorization code"
    manager_subject: str = "Invitation to DChat"

    app_host: str = "localhost"
    app_port: int =  8070
    adm_app_port: int | str | None  = 8072
    account_port: int | str | None  = 8071
    chat_port: int | str | None  = 8001

    device_id: str = ""
    phone: str = ""
    apikey: str = ""
    kafka_bootstrap_servers: str = ""
    kafka_topic: str = ""
    kafka_group_id: str = ""
    redis_url: str = "redis://redis:6379"
    green_api_token: str = ""
    waba_token: str = ""
    waba_api_url: str = "https://waba-sandbox.360dialog.io/v1"
    tg_api_id: str = ""
    tg_api_hash: str = ""
    tg_webhook_url: str = ""
    tg_webhook_domain: str = ""
    tg_bot_host: str = ""
    tg_bot_send_url: str = ""
    tg_proxy_host: str = ""
    tg_proxy_port: str = ""
    tg_proxy_secret: str = ""
    vk_webhook_domain: str = ""
    notification_bot_token: str = ""
    low_balance_warn_days: tuple[int, ...]  = (5, 3)

    s3_endpoint: str = "minio:9000"
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_secure: bool = False
    s3_bucket: str = "dovechat-media"
    s3_bt_bucket: str = "storage"
    s3_external_url: str = "http://localhost:9000/"
    s3_internal_url: str = "http://minio:9000/"

    VK_OAUTH_CLIENT_ID: str = ""
    VK_OAUTH_CLIENT_SECRET: str = ""
    VK_OAUTH_REDIRECT_URI: str = "https://tg.dove-chat.pro/dev/crm/api/v1/vk/oauth/callback"
    VK_OAUTH_SCOPE: str = "groups,manage"
    VK_OAUTH_RESULT_URL: str = "https://tg.dove-chat.pro/dev/lc/vk-oauth-result"
    VK_TEMP_GROUP_ID: str = ""
    VK_TEMP_TOKEN: str = ""
    VK_TEMP_SECRET_KEY: str = ""
    VK_TEMP_CONFIRMATION_CODE: str = ""

    BITRIX_CLIENT_ID: str = ""
    BITRIX_CLIENT_SECRET: str = ""
    BITRIX_APPLICATION_TOKEN: str = ""
    
    CHANNELS: list = [
        "telegram_bot",
        "telegram_user",
        "whatsapp_green",
        "waba",
        "vk",
    ]

    BITRIX_CONNECTOR_NAMES: dict = {
        "dovechat_telegram_bot": "DoveChat TgBot",
        "dovechat_telegram_user": "DoveChat TgUser",
        "dovechat_vk": "DoveChat VK",
        "dovechat_whatsapp_green": "DoveChat WA",
        "dovechat_waba": "DoveChat WABA",
    }

    TOPICS: list = [k + "_outgoing"  for k in CHANNELS]
    BITRIX_CONNECTOR_MAP: dict = {k: "dovechat_" + k for k in CHANNELS}
    BITRIX_CONNECTOR_CHANNEL_MAP: dict = {"dovechat_" +  k: k for k in CHANNELS}
    CHANNEL_HANDLERS: dict = {k: "create_msg_" + k for k in CHANNELS}

    # AML
    channel_id: str  = ""
    channel_secret: str = ""
    AMO_CLIENT_ID: str = ""
    AMO_CLIENT_SECRET: str = ""
    AMO_REDIRECT_URI: str = ""
    AMO_CHANNEL_ID: str = ""
    AMO_CHANNEL_SECRET: str = ""
    AMO_OAUTH_URL: str = "https://www.kommo.com/oauth"
    AMO_AUTHORIZE_URL: str = (
        "https://new1782329019.amocrm.ru/oauth2/authorize"
    )

    TOPIC_INCOMING_MESSAGES: str = "telegram.incoming.messages"
    TOPIC_MESSAGE_STATUS: str = "telegram.message.status"
    TOPIC_CONTACT_SYNC: str = "telegram.contact.sync"
    HEALTH_CHECK_INTERVAL: int = 300

    @property
    def cors_origins_list(self) -> list[str]:
        """Разрешённые CORS-origins.

        Источник — окружение (BACKEND_CORS_ORIGINS, через запятую).
        Если переменная не задана — локальная разработка (localhost/127.0.0.1),
        чтобы сервис поднимался "из коробки" на ноутбуке.
        "*" — разрешить любые origins (явный выбор; credentials при этом выключаются).
        """
        raw = (self.backend_cors_origins or "").strip()
        if not raw:
            raw = DEFAULT_CORS_ORIGINS
        return [origin.strip() for origin in raw.split(",") if origin.strip()]

    @property
    def s3_public_base_url(self) -> str:
        """Публичный базовый URL для файлов: <external>/<prefix>/.

        Внешний nginx отдаёт /storage/* → бакет s3_bucket (см. nginx/nginx.conf).
        """
        return f"{self.s3_external_url.rstrip('/')}/{self.s3_bt_bucket.strip('/')}"

    '''
    @field_validator('app_port', mode='before')
    @classmethod
    def parse_port(cls, v):
        return int(v)
    '''

settings: Settings = Settings()

if settings.secret_key == "...":
    log.error(
        "SECRET_KEY is not set (default placeholder used). "
        "JWTs are signed with a publicly known key - set SECRET_KEY in the environment."
    )
if not settings.internal_api_token:
    log.error(
        "INTERNAL_API_TOKEN is not set - /internal/* endpoints will reject every request."
    )

    # Адреса сервисов внутри кластера (crm -> chat, account, tg-connector)
    chat_internal_url: str = "http://chat.dchat.svc.cluster.local:8001"
    account_internal_url: str = (
        "http://account.dchat.svc.cluster.local:8071"
    )
    tg_connector_url: str = "http://tg-connector.dchat.svc.cluster.local:8000"

    # CORS сервиса crm (amoCRM-виджет); на проде задать через env
    crm_cors_origins: list[str] = ["https://new1782329019.amocrm.ru"]