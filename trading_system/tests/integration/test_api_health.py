from apps.api.main import health, mode


def test_health():
    assert health()["status"] == "ok"


def test_mode_endpoint_shape():
    data = mode()
    assert "mode" in data
