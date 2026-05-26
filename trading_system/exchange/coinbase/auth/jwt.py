from __future__ import annotations

import hashlib
import json
import time
from base64 import b64decode, b64encode
from typing import Any

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePrivateKey


def _b64url(data: bytes) -> str:
    return b64encode(data).rstrip(b"=").decode()


def _load_private_key(key_bytes: bytes) -> EllipticCurvePrivateKey:
    key: Any = serialization.load_pem_private_key(key_bytes, password=None)
    if not isinstance(key, EllipticCurvePrivateKey):
        raise TypeError("key is not an EC private key")
    return key


def build_jwt_token(
    api_key: str,
    api_secret: str,
    request_method: str,
    request_path: str,
    body: str = "",
) -> str:
    raw_key = api_secret.encode("utf-8")
    try:
        private_key = _load_private_key(b64decode(raw_key))
    except Exception:
        private_key = _load_private_key(raw_key)
    public_key = private_key.public_key()
    raw_public = public_key.public_bytes(serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
    kid = _b64url(hashlib.sha256(raw_public).digest()[:16])

    now = int(time.time())
    header = {"alg": "ES256", "typ": "JWT", "kid": kid}
    payload: dict[str, Any] = {
        "iss": "cdp",
        "nbf": now - 30,
        "exp": now + 120,
        "sub": api_key,
        "uri": f"https://api.coinbase.com{request_path}",
    }
    if body:
        payload["body"] = hashlib.sha256(body.encode()).hexdigest()

    header_b64 = _b64url(json.dumps(header, separators=(",", ":")).encode())
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode())
    message = f"{header_b64}.{payload_b64}"
    signature = private_key.sign(message.encode(), ec.ECDSA(hashes.SHA256()))
    return f"{message}.{_b64url(signature)}"
