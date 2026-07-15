"""Tests for trading_system.core.security.utils."""

from trading_system.core.security import utils


def test_generate_salt_default():
    s = utils.generate_salt()
    assert isinstance(s, bytes)
    assert len(s) == 32


def test_generate_salt_size():
    s = utils.generate_salt(16)
    assert len(s) == 16


def test_hash_password_generates_salt():
    salt, key = utils.hash_password("pw")
    assert isinstance(salt, bytes)
    assert isinstance(key, bytes)
    assert len(key) == 32  # sha256 -> 32 bytes


def test_hash_password_with_salt():
    salt = b"x" * 16
    s1, k1 = utils.hash_password("pw", salt)
    s2, k2 = utils.hash_password("pw", salt)
    assert s1 == salt == s2
    assert k1 == k2


def test_verify_password_true():
    salt, key = utils.hash_password("secret")
    assert utils.verify_password("secret", salt, key) is True


def test_verify_password_false():
    salt, key = utils.hash_password("secret")
    assert utils.verify_password("wrong", salt, key) is False


def test_sha256_hex_bytes():
    h = utils.sha256_hex(b"abc")
    assert h == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_sha256_hex_str():
    h = utils.sha256_hex("abc")
    assert h == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
