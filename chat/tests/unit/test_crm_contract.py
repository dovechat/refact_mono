"""Нормализация результата CRM (AmoCRM / Bitrix24)."""

from core.crm_contract import normalize_crm_send_result


def test_none_returns_pair_of_none():
    assert normalize_crm_send_result(None) == (None, None)


def test_amo_crm_string_is_chat_id():
    assert normalize_crm_send_result("12345") == ("12345", None)


def test_bitrix_tuple():
    assert normalize_crm_send_result(("12345", "67890")) == ("12345", "67890")


def test_list_is_supported():
    assert normalize_crm_send_result(["12345", "67890"]) == ("12345", "67890")


def test_dict_is_supported():
    assert normalize_crm_send_result({"chat_id": "1", "message_id": "2"}) == ("1", "2")


def test_empty_tuple():
    assert normalize_crm_send_result(()) == (None, None)


def test_unexpected_type():
    assert normalize_crm_send_result(42) == (None, None)
