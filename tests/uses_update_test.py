import pytest

import CM.USES as uses_module


def test_updateuses_runs_full_database_when_cmid_missing(monkeypatch):
    captured = {}

    def fake_process_uses(database, CMID=None, user="0", detailed=True):
        captured["database"] = database
        captured["CMID"] = CMID
        captured["user"] = user
        captured["detailed"] = detailed
        return "ok"

    monkeypatch.setattr(uses_module, "processUSES", fake_process_uses)

    result = uses_module.updateUSES(database="sociomap")

    assert result == "ok"
    assert captured == {
        "database": "sociomap",
        "CMID": None,
        "user": "0",
        "detailed": False,
    }


def test_updateuses_runs_single_cmid_when_provided(monkeypatch):
    captured = {}

    def fake_process_uses(database, CMID=None, user="0", detailed=True):
        captured["database"] = database
        captured["CMID"] = CMID
        captured["detailed"] = detailed
        return "ok"

    monkeypatch.setattr(uses_module, "processUSES", fake_process_uses)

    result = uses_module.updateUSES(database="archamap", CMID="am123")

    assert result == "ok"
    assert captured == {
        "database": "archamap",
        "CMID": "AM123",
        "detailed": False,
    }


def test_updateuses_rejects_invalid_cmid():
    with pytest.raises(ValueError, match="Invalid CMID"):
        uses_module.updateUSES(database="sociomap", CMID="ZZ99")
