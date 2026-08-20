import pytest

from core.config import load_config


def test_load_config_view_only(monkeypatch):
    monkeypatch.delenv("DASHBOARD_PORT", raising=False)
    monkeypatch.delenv("ACCOUNT_INDEX", raising=False)
    monkeypatch.delenv("DRY_RUN", raising=False)
    cfg = load_config(require_keys=False)
    assert cfg.dashboard_port == 8900
    assert cfg.account_index == 0


def test_banned_simulation_flag(monkeypatch):
    monkeypatch.setenv("DRY_RUN", "1")
    with pytest.raises(SystemExit, match="simulation"):
        load_config(require_keys=False)


def test_banned_testnet_url(monkeypatch):
    monkeypatch.setenv("SOME_URL", "https://api.testnet.arcus.xyz")
    with pytest.raises(SystemExit, match="non-production"):
        load_config(require_keys=False)


def test_wrong_api_override(monkeypatch):
    monkeypatch.setenv("ARCUS_API_URL", "https://example.com")
    with pytest.raises(SystemExit, match="production"):
        load_config(require_keys=False)
