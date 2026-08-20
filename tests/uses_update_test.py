import pytest
import pandas as pd

import CM.USES as uses_module


def test_updatelabels_replaces_stale_domain_labels(monkeypatch):
    queries = []

    monkeypatch.setattr(uses_module, "getDriver", lambda _database: object())

    def fake_get_query(query, driver, **kwargs):
        queries.append(query)
        if "return count(distinct c)" in query.lower():
            return [1]
        if "return count(distinct c) as count" in query.lower():
            return [1]
        return pd.DataFrame()

    monkeypatch.setattr(uses_module, "getQuery", fake_get_query)

    result = uses_module.updateLabels(database="sociomap", CMID="SM21903")

    assert any("Set labels" in log for log in result)
    label_query = next(query for query in queries if "staleLabels" in query)
    assert "REMOVE c:$(staleLabels)" in label_query
    assert "SET c:$(currentLabels)" in label_query
    assert "NOT label IN currentLabels" in label_query


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


def test_waitinguses_does_not_clear_markers_when_processing_fails(monkeypatch):
    queries = []

    def fake_get_query(query, driver, **kwargs):
        queries.append(query)
        return ["SM123"]

    monkeypatch.setattr(uses_module, "getDriver", lambda _database: object())
    monkeypatch.setattr(uses_module, "getQuery", fake_get_query)
    monkeypatch.setattr(
        uses_module,
        "processUSES",
        lambda **_kwargs: ("processing failed", 500),
    )

    result = uses_module.waitingUSES("SocioMap")

    assert result == (
        "Error in waitingUSES: processUSES failed for batch 1: processing failed",
        500,
    )
    assert len(queries) == 1
    assert "set r.status = NULL" not in queries[0]


def test_waitinguses_alerts_when_update_markers_remain(monkeypatch):
    responses = iter([["SM2", "SM1", "SM1"], [1]])
    processed_batches = []

    monkeypatch.setattr(uses_module, "getDriver", lambda _database: object())
    monkeypatch.setattr(
        uses_module,
        "getQuery",
        lambda query, driver, **kwargs: next(responses),
    )
    monkeypatch.setattr(
        uses_module,
        "processUSES",
        lambda **kwargs: processed_batches.append(kwargs["CMID"]) or "ok",
    )

    result = uses_module.waitingUSES("SocioMap", BATCH_SIZE=1)

    assert processed_batches == [["SM1"], ["SM2"]]
    assert result == (
        "Error in waitingUSES: 1 USES ties still have status = 'update' after processing",
        500,
    )


def test_waitinguses_succeeds_only_after_zero_marker_verification(monkeypatch):
    responses = iter([["SM123"], [0]])
    queries = []

    monkeypatch.setattr(uses_module, "getDriver", lambda _database: object())

    def fake_get_query(query, driver, **kwargs):
        queries.append(query)
        return next(responses)

    monkeypatch.setattr(uses_module, "getQuery", fake_get_query)
    monkeypatch.setattr(uses_module, "processUSES", lambda **_kwargs: "ok")

    result = uses_module.waitingUSES("SocioMap")

    assert result == "Successfully updated 1 CMIDs in batches of 1000."
    assert len(queries) == 2
    assert "return count(r) as count" in queries[1]
    assert all("set r.status = NULL" not in query for query in queries)
