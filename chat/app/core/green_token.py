"""Токен вебхука Green для одного инстанса (HMAC от серверного секрета)."""
import hashlib
import hmac

from core.config import settings


def green_webhook_token(instance_id: str) -> str:
    key = settings.green_api_token or ""
    if not key or not instance_id:
        return ""
    return hmac.new(
        key.encode(),
        instance_id.encode(),
        hashlib.sha256,
    ).hexdigest()
