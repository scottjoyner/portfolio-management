"""Coverage tests for event_markets.polymarket_relayer (network mocked)."""

import json
from unittest.mock import patch

import pytest

from event_markets import polymarket_relayer as pr
from event_markets.polymarket_relayer import (
    PolymarketRelayerClient, PolymarketRelayerCredentials,
    PolymarketBuilderCredentials, load_relayer_credentials, load_builder_credentials,
)
from em_helpers import FakeResp


def _creds_file(tmp_path, content):
    p = tmp_path / "creds.txt"
    p.write_text(content)
    return str(p)


def test_relayer_credentials_from_file():
    import tempfile, os
    content = "RELAYER_API_KEY: key123\nRELAYER_API_KEY_ADDRESS: addr456\n"
    fd = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
    fd.write(content)
    fd.close()
    try:
        c = PolymarketRelayerCredentials.from_file(fd.name)
        assert c.api_key == "key123"
        assert c.api_key_address == "addr456"
    finally:
        os.unlink(fd.name)


def test_relayer_credentials_signer_address_misspelling():
    import tempfile, os
    content = "RELAYER_API_KEY: k\nSIGNER_ADRESS: misspelled\n"
    fd = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
    fd.write(content)
    fd.close()
    try:
        c = PolymarketRelayerCredentials.from_file(fd.name)
        assert c.api_key_address == "misspelled"
    finally:
        os.unlink(fd.name)


def test_builder_credentials_from_file():
    import tempfile, os
    content = "APIKEY: a\nSECRET: s\nPASSPHRASE: p\n"
    fd = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
    fd.write(content)
    fd.close()
    try:
        c = PolymarketBuilderCredentials.from_file(fd.name)
        assert c.api_key == "a" and c.secret == "s" and c.passphrase == "p"
    finally:
        os.unlink(fd.name)


def test_relayer_client_init_direct():
    c = PolymarketRelayerClient(api_key="k", api_key_address="a", relayer_url="https://r.example.com/")
    assert c.api_key == "k" and c.api_key_address == "a"
    assert c.relayer_url == "https://r.example.com"


def test_relayer_client_init_from_env(monkeypatch):
    monkeypatch.setenv("RELAYER_API_KEY", "ek")
    monkeypatch.setenv("RELAYER_API_KEY_ADDRESS", "ea")
    monkeypatch.setenv("POLYMARKET_RELAYER_URL", "https://relayer.example.com")
    c = PolymarketRelayerClient()
    assert c.api_key == "ek"
    assert c.api_key_address == "ea"
    assert c.relayer_url == "https://relayer.example.com"


def test_relayer_client_init_from_credentials_file():
    import tempfile, os
    content = "RELAYER_API_KEY: fk\nRELAYER_API_KEY_ADDRESS: fa\n"
    fd = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
    fd.write(content)
    fd.close()
    try:
        c = PolymarketRelayerClient(credentials_path=fd.name)
        assert c.api_key == "fk"
        assert c.api_key_address == "fa"
    finally:
        os.unlink(fd.name)


def test_headers_require_creds():
    c = PolymarketRelayerClient()
    with pytest.raises(RuntimeError):
        c._headers()


def test_request_json_success():
    c = PolymarketRelayerClient(api_key="k", api_key_address="a")
    with patch("event_markets.polymarket_relayer.urlopen", return_value=FakeResp({"data": [{"id": 1}]})):
        out = c._request_json("/relayer/api/keys")
    assert out == {"data": [{"id": 1}]}


def test_request_json_http_error():
    from urllib.error import HTTPError
    c = PolymarketRelayerClient(api_key="k", api_key_address="a")
    fp = None
    with patch("event_markets.polymarket_relayer.urlopen", side_effect=HTTPError("u", 403, "forbidden", {}, fp)):
        with pytest.raises(RuntimeError):
            c._request_json("/x")


def test_request_json_url_error():
    from urllib.error import URLError
    c = PolymarketRelayerClient(api_key="k", api_key_address="a")
    with patch("event_markets.polymarket_relayer.urlopen", side_effect=URLError("conn")):
        with pytest.raises(RuntimeError):
            c._request_json("/x")


def test_list_api_keys_variants():
    c = PolymarketRelayerClient(api_key="k", api_key_address="a")
    with patch.object(c, "_request_json", return_value=[{"id": 1}]):
        assert c.list_api_keys() == [{"id": 1}]
    with patch.object(c, "_request_json", return_value={"data": [{"id": 2}]}):
        assert c.list_api_keys() == [{"id": 2}]
    with patch.object(c, "_request_json", return_value={"data": "notalist"}):
        assert c.list_api_keys() == []
    with patch.object(c, "_request_json", return_value=42):
        assert c.list_api_keys() == []


def test_ping_and_builder_auth():
    c = PolymarketRelayerClient(api_key="k", api_key_address="a")
    assert c.ping()["ok"] is True
    with pytest.raises(NotImplementedError):
        c.builder_auth_headers(PolymarketBuilderCredentials())


def test_module_loaders(tmp_path):
    import os
    p = tmp_path / "r.txt"
    p.write_text("RELAYER_API_KEY: x\nRELAYER_API_KEY_ADDRESS: y\n")
    c = load_relayer_credentials(str(p))
    assert c.api_key == "x"
    p2 = tmp_path / "b.txt"
    p2.write_text("APIKEY: a\nSECRET: s\nPASSPHRASE: p\n")
    b = load_builder_credentials(str(p2))
    assert b.api_key == "a"


def test_relayer_credentials_skips_non_colon_lines(tmp_path):
    content = "GARBAGE_LINE_NO_COLON\nRELAYER_API_KEY: k\nANOTHERBADLINE\nRELAYER_API_KEY_ADDRESS: a\n"
    p = tmp_path / "r.txt"
    p.write_text(content)
    c = load_relayer_credentials(str(p))
    assert c.api_key == "k" and c.api_key_address == "a"


def test_builder_credentials_skips_non_colon_lines(tmp_path):
    content = "NOPE\nAPIKEY: a\nSTILLNOPE\nSECRET: s\nPASSPHRASE: p\n"
    p = tmp_path / "b.txt"
    p.write_text(content)
    b = load_builder_credentials(str(p))
    assert b.api_key == "a" and b.secret == "s" and b.passphrase == "p"
