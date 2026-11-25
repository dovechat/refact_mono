import base64
import os
from functools import lru_cache

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


@lru_cache(maxsize=1)
def _aes() -> AESGCM:
    return AESGCM(base64.b64decode(os.environ["APP_ENCRYPTION_KEY"]))


def encrypt(text: str, aad: bytes | None = None) -> str:
    nonce = os.urandom(12)
    ct = _aes().encrypt(nonce, text.encode("utf-8"), aad)
    return base64.b64encode(nonce + ct).decode()


def decrypt(token: str, aad: bytes | None = None) -> str:
    raw = base64.b64decode(token)
    nonce, ct = raw[:12], raw[12:]
    return _aes().decrypt(nonce, ct, aad).decode("utf-8")
