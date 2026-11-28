"""Разбор form-urlencoded тел, которые присылает Bitrix24."""
from urllib.parse import parse_qs

from fastapi import Request


def parse_form(body: bytes) -> dict[str, str]:
    """Плоский словарь из form-urlencoded тела (первое значение ключа)."""
    parsed = parse_qs(body.decode())
    return {key: values[0] for key, values in parsed.items()}


async def get_form_data(request: Request) -> dict[str, str]:
    """FastAPI-зависимость: тело запроса как плоский словарь."""
    return parse_form(await request.body())