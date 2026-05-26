from __future__ import annotations

import hashlib
import os


def generate_salt(size: int = 32) -> bytes:
    return os.urandom(size)


def hash_password(password: str, salt: bytes | None = None) -> tuple[bytes, bytes]:
    salt = salt or generate_salt()
    key = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000)
    return salt, key


def verify_password(password: str, salt: bytes, stored_key: bytes) -> bool:
    _, key = hash_password(password, salt)
    return key == stored_key


def sha256_hex(data: bytes | str) -> str:
    if isinstance(data, str):
        data = data.encode()
    return hashlib.sha256(data).hexdigest()
