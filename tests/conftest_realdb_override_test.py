from configparser import ConfigParser

import pytest

import CM.utils as utils_module
import conftest


def test_realdb_target_overrides_replace_db_and_offline_config(monkeypatch):
    isolated_config = ConfigParser()
    monkeypatch.setattr(utils_module, "config", isolated_config)
    monkeypatch.setenv("CM_REALDB_USER", "test-user")
    monkeypatch.setenv("CM_REALDB_PASSWORD", "test-password")
    monkeypatch.setenv("CM_REALDB_URI_SOCIOMAP", "bolt://127.0.0.1:39601")
    monkeypatch.setenv("CM_REALDB_URI_ARCHAMAP", "bolt://127.0.0.1:39602")
    monkeypatch.setenv("CM_REALDB_URI_GISDB", "bolt://127.0.0.1:39603")
    monkeypatch.setenv("CM_REALDB_URI_USERDB", "bolt://127.0.0.1:39604")

    conftest._configure_realdb_targets()

    for section in ("DB", "OFFLINE"):
        assert isolated_config[section]["user"] == "test-user"
        assert isolated_config[section]["pwd"] == "test-password"
        assert isolated_config[section]["sociomap"] == "bolt://127.0.0.1:39601"
        assert isolated_config[section]["archamap"] == "bolt://127.0.0.1:39602"
        assert isolated_config[section]["gisdb"] == "bolt://127.0.0.1:39603"
        assert isolated_config[section]["userdb"] == "bolt://127.0.0.1:39604"


def test_realdb_target_overrides_require_isolated_credentials(monkeypatch):
    monkeypatch.setenv("CM_REALDB_URI_ARCHAMAP", "bolt://127.0.0.1:39602")
    monkeypatch.delenv("CM_REALDB_USER", raising=False)
    monkeypatch.delenv("CM_REALDB_PASSWORD", raising=False)

    with pytest.raises(pytest.UsageError, match="CM_REALDB_USER"):
        conftest._configure_realdb_targets()
