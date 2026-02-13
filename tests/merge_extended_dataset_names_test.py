import pandas as pd

import CM.merge as merge_mod


def test_extended_key_to_key_includes_dataset_cmnames(monkeypatch):
    monkeypatch.setattr(merge_mod, "getDriver", lambda _database: object())

    def fake_get_query(query, driver=None, params=None, type=None):
        if "RETURN d.CMID AS datasetID, d.CMName AS datasetName" in query:
            return [
                {"datasetID": "SD1", "datasetName": "Dataset One"},
                {"datasetID": "AD2", "datasetName": "Dataset Two"},
            ]
        if type == "df":
            return pd.DataFrame(
                [
                    {
                        "datasetID": "SD1",
                        "LCA_CMName": "Ancestor Name",
                        "LCA_CMID": "AM123",
                        "tie": 0,
                        "Key": "varA: value1",
                        "Name": "Term A",
                    },
                    {
                        "datasetID": "AD2",
                        "LCA_CMName": "Ancestor Name",
                        "LCA_CMID": "AM123",
                        "tie": 0,
                        "Key": "varB: value2",
                        "Name": "Term B",
                    },
                ]
            )
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(merge_mod, "getQuery", fake_get_query)

    result = merge_mod.proposeMerge(
        dataset_choices=["SD1", "AD2"],
        category_label="CATEGORY",
        criteria="extended",
        database="ArchaMap",
        intersection=True,
        selectedKeyvariables={},
        ncontains=2,
        resultFormat="key-to-key",
    )

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["datasetCMName_SD1"] == "Dataset One"
    assert result[0]["datasetCMName_AD2"] == "Dataset Two"


def test_extended_key_to_category_includes_dataset_cmname(monkeypatch):
    monkeypatch.setattr(merge_mod, "getDriver", lambda _database: object())

    def fake_get_query(query, driver=None, params=None, type=None):
        if "RETURN d.CMID AS datasetID, d.CMName AS datasetName" in query:
            return [
                {"datasetID": "SD1", "datasetName": "Dataset One"},
                {"datasetID": "AD2", "datasetName": "Dataset Two"},
            ]
        if type == "df":
            return pd.DataFrame(
                [
                    {
                        "datasetID": "SD1",
                        "LCA_CMName": "Ancestor Name",
                        "LCA_CMID": "AM123",
                        "tie": 0,
                        "Key": "varA: value1",
                        "Name": "Term A",
                    },
                    {
                        "datasetID": "AD2",
                        "LCA_CMName": "Ancestor Name",
                        "LCA_CMID": "AM123",
                        "tie": 0,
                        "Key": "varB: value2",
                        "Name": "Term B",
                    },
                ]
            )
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(merge_mod, "getQuery", fake_get_query)

    result = merge_mod.proposeMerge(
        dataset_choices=["SD1", "AD2"],
        category_label="CATEGORY",
        criteria="extended",
        database="ArchaMap",
        intersection=True,
        selectedKeyvariables={},
        ncontains=2,
        resultFormat="key-to-category",
    )

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["datasetCMName"] == "Dataset One"
    assert result[1]["datasetCMName"] == "Dataset Two"

