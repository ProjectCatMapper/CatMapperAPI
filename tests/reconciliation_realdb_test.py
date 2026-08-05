import json

import pytest


@pytest.mark.realdb
def test_reconciliation_manifest_realdb_smoke(client, realdb_database):
    response = client.get(f"/reconcile/{realdb_database}")

    assert response.status_code == 200
    assert response.get_json()["versions"] == ["0.2"]


@pytest.mark.realdb
def test_reconciliation_suggest_type_realdb_smoke(client, realdb_database):
    response = client.get(f"/reconcile/{realdb_database}/suggest/type?prefix=&limit=1")

    assert response.status_code == 200
    assert "result" in response.get_json()


@pytest.mark.realdb
def test_reconciliation_cmid_query_realdb_smoke(client, realdb_database):
    cmid = "SM1" if str(realdb_database).lower() == "sociomap" else "AM1"
    payload = {"q0": {"properties": [{"pid": "CMID", "v": cmid}], "limit": 1}}

    response = client.get(
        f"/reconcile/{realdb_database}",
        query_string={"queries": json.dumps(payload)},
    )

    assert response.status_code == 200
    body = response.get_json()
    assert "q0" in body
    assert "result" in body["q0"]
