import json

import pandas as pd
import pytest

import CM.datasets as datasets_module
import CMroutes.explore_routes as explore_routes


def test_process_dataset_results_handles_property_collision_from_real_dataset_rows():
    """
    Regression for the production crash seen on:
    /dataset?cmid=AD354514&database=archamap&domain=SITE&children=false

    Real rows can include a USES property named 'CMID', which collides with
    pivot/reset_index columns if not filtered before reshaping.
    """
    rows = [
        {
            "datasetName": "Southwest NAA Database",
            "datasetID": "AD354514",
            "CMID": "AM453567",
            "CMName": "FB 9467",
            "relID": "4:7c029247-9c9f-4fe0-a9c3-af664b68f7b0:1636518",
            "property": "CMID",
            "value": "AM453567",
            "property_name": "",
        },
        {
            "datasetName": "Southwest NAA Database",
            "datasetID": "AD354514",
            "CMID": "AM453567",
            "CMName": "FB 9467",
            "relID": "4:7c029247-9c9f-4fe0-a9c3-af664b68f7b0:1636518",
            "property": "Name",
            "value": "FB 9467",
            "property_name": "",
        },
    ]

    payload = datasets_module._process_dataset_results(rows)
    parsed = json.loads(payload)

    assert isinstance(parsed, list)
    assert len(parsed) == 1
    assert parsed[0]["datasetID"] == "AD354514"
    assert parsed[0]["CMID"] == "AM453567"
    assert parsed[0]["Name"] == "FB 9467"


def test_get_dataset_data_normalizes_scalar_cmid_to_list(monkeypatch):
    captured = {}

    monkeypatch.setattr(datasets_module, "getDriver", lambda database: object())
    monkeypatch.setattr(
        datasets_module,
        "_get_label_mapping",
        lambda driver: pd.DataFrame([
            {"label": "SITE", "groupLabel": "SITE"},
            {"label": "CATEGORY", "groupLabel": "CATEGORY"},
        ]),
    )

    def fake_get_query(query, driver=None, params=None, type="dict", max_retries=3, **kwargs):
        captured["params"] = params
        return []

    monkeypatch.setattr(datasets_module, "getQuery", fake_get_query)

    payload = datasets_module.getDatasetData(
        database="archamap",
        cmid="AD354514",
        domain="SITE",
        children="false",
    )

    assert payload == "[]"
    assert captured["params"]["cmid"] == ["AD354514"]


def test_dataset_route_returns_json_error_payload(client, monkeypatch):
    monkeypatch.setattr(
        explore_routes,
        "getDatasetData",
        lambda database, cmid, domain, children: (_ for _ in ()).throw(RuntimeError("dataset boom")),
    )

    response = client.get(
        "/dataset",
        query_string={
            "cmid": "AD354514",
            "database": "archamap",
            "domain": "SITE",
            "children": "false",
        },
    )

    assert response.status_code == 500
    assert response.is_json
    assert response.get_json() == {"error": "dataset boom"}


@pytest.mark.realdb
def test_realdb_dataset_endpoint_handles_archamap_site_case(client):
    response = client.get(
        "/dataset",
        query_string={
            "cmid": "AD354514",
            "database": "archamap",
            "domain": "SITE",
            "children": "false",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert isinstance(payload, list)
