"""Единый контракт обмена chat <-> CRM.

`CrmService.send_to_crm` в зависимости от CRM возвращает разные типы:
  * AmoCRM  -> `str` (crm_chat_id) или `None`
  * Bitrix24 -> кортеж `(crm_chat_id, real_message_id)` или `None`

Нормализатор приводит любой из вариантов к паре `(crm_chat_id, crm_message_id)`.
"""

from typing import Any, Tuple


def normalize_crm_send_result(result: Any) -> Tuple[str | None, str | None]:
    """Привести результат `send_to_crm` к паре (crm_chat_id, crm_message_id)."""
    if result is None:
        return None, None

    # AmoCRM: строка — это crm_chat_id, message id CRM не возвращает
    if isinstance(result, str):
        return (result or None), None

    # Bitrix24: (crm_chat_id, real_message_id)
    if isinstance(result, (tuple, list)):
        chat_id = result[0] if len(result) > 0 else None
        message_id = result[1] if len(result) > 1 else None
        return (str(chat_id) if chat_id else None), (str(message_id) if message_id else None)

    if isinstance(result, dict):
        chat_id = result.get("crm_chat_id") or result.get("chat_id")
        message_id = result.get("crm_message_id") or result.get("message_id")
        return (str(chat_id) if chat_id else None), (str(message_id) if message_id else None)

    return None, None
