"""Маскирование чувствительных данных в логах."""
import logging
import re

_PATTERNS = [
    (re.compile(r'(token["\s:=]+)[^\s&"\']+', re.I), r'\1***'),
    (re.compile(r'(password["\s:=]+)[^\s&"\']+', re.I), r'\1***'),
    (re.compile(r'(secret["\s:=]+)[^\s&"\']+', re.I), r'\1***'),
    (re.compile(r'(api[_-]?key["\s:=]+)[^\s&"\']+', re.I), r'\1***'),
    (re.compile(r'(refresh_token["\s:=]+)[^\s&"\']+', re.I), r'\1***'),
    (re.compile(r'(/bot)[^/\s"\']+'), r'\1***'),
    (re.compile(r'(/waInstance\d+/\w+/)[^\s"\'?&]+'), r'\1***'),
]


class MaskingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        for pattern, replacement in _PATTERNS:
            msg = pattern.sub(replacement, msg)
        record.msg = msg
        record.args = ()
        return True


def install_log_masking() -> None:
    old_factory = logging.getLogRecordFactory()

    def factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        try:
            msg = record.getMessage()
            for pattern, replacement in _PATTERNS:
                msg = pattern.sub(replacement, msg)
            record.msg = msg
            record.args = ()
        except Exception:
            # логировать здесь нельзя: уйдём в рекурсию
            pass
        return record

    logging.setLogRecordFactory(factory)
