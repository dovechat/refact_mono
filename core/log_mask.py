"""Masking of secrets and personal data in log output.

Call install_log_masking() once at service start. Every log record is masked
before any handler formats it, including exception tracebacks.
"""
import logging
import re
import traceback

MASK = "***"

_KEYS = (
    "access_token|api_token|api_key|bot_token|secret_token|secret_key|"
    "session_string|confirmation_code|password|authorization|d360-api-key|"
    "token|secret"
)

_EMAIL = re.compile(r"[\w.+-]+@([\w-]+(?:\.[\w-]+)+)")
_PHONE = re.compile(r"(?<![\w.-])(\+?)\d{8,13}(\d{2})(?![\w.-])")
_BOT_TOKEN = re.compile(r"\b(bot)?\d{6,}:[A-Za-z0-9_-]{30,}")
_GREEN_PATH = re.compile(r"(/waInstance\d+/[A-Za-z]+/)[^/\s?#'\"]+")
_WABA_PATH = re.compile(r"(/webhooks/whatsapp/waba/[0-9a-fA-F-]{36}/)[^/\s?#'\"]+")
_BEARER = re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+")
_KEY_VALUE = re.compile(
    r"(?i)([\"']?(?:" + _KEYS + r")[\"']?\s*[:=]\s*)"
    r"(?:\"[^\"]*\"|'[^']*'|[^\s,&})\]]+)"
)


def mask(text: str) -> str:
    text = _EMAIL.sub(lambda m: f"{MASK}@{m.group(1)}", text)
    text = _BOT_TOKEN.sub(lambda m: f"{m.group(1) or ''}{MASK}", text)
    text = _GREEN_PATH.sub(rf"\1{MASK}", text)
    text = _WABA_PATH.sub(rf"\1{MASK}", text)
    text = _BEARER.sub(rf"\1{MASK}", text)
    text = _KEY_VALUE.sub(lambda m: f"{m.group(1)}{MASK}", text)
    text = _PHONE.sub(lambda m: f"{m.group(1)}{MASK}{m.group(2)}", text)
    return text


def install_log_masking() -> None:
    if getattr(logging, "_log_mask_installed", False):
        return
    logging._log_mask_installed = True
    old_factory = logging.getLogRecordFactory()

    def factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        try:
            message = record.getMessage()
            masked = mask(message)
            if masked != message:
                record.msg = masked
                record.args = ()
            if record.exc_info and not record.exc_text:
                exc_text = "".join(traceback.format_exception(*record.exc_info)).rstrip("\n")
                record.exc_text = mask(exc_text)
        except Exception:
            pass
        return record

    logging.setLogRecordFactory(factory)
