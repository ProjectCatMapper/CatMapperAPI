import pandas as pd

import CM.merge as merge_mod


def _setup_merge_domain_validation(monkeypatch):
    fake_driver = object()
    seen = {}
    monkeypatch.setattr(merge_mod, "getDriver", lambda _database: fake_driver)

    def fake_validate_domain_label(domain, driver=None, aliases=None, extra_allowed=None):
        seen["domain"] = domain
        seen["driver"] = driver
        return str(domain).upper()

    monkeypatch.setattr(merge_mod, "validate_domain_label", fake_validate_domain_label)
    return seen, fake_driver


def test_extended_key_to_key_includes_dataset_cmnames(monkeypatch):
    seen, fake_driver = _setup_merge_domain_validation(monkeypatch)

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
    assert seen["domain"] == "CATEGORY"
    assert seen["driver"] is fake_driver
    assert result[0]["datasetCMName_SD1"] == "Dataset One"
    assert result[0]["datasetCMName_AD2"] == "Dataset Two"


def test_extended_key_to_category_includes_dataset_cmname(monkeypatch):
    seen, fake_driver = _setup_merge_domain_validation(monkeypatch)

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
    assert seen["domain"] == "CATEGORY"
    assert seen["driver"] is fake_driver
    assert result[0]["datasetCMName"] == "Dataset One"
    assert result[1]["datasetCMName"] == "Dataset Two"


def test_extended_key_to_key_omits_empty_variable_value_columns(monkeypatch):
    seen, fake_driver = _setup_merge_domain_validation(monkeypatch)

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
                        "Key": "value1",
                        "Name": "Term A",
                    },
                    {
                        "datasetID": "AD2",
                        "LCA_CMName": "Ancestor Name",
                        "LCA_CMID": "AM123",
                        "tie": 0,
                        "Key": "value2",
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
    assert seen["domain"] == "CATEGORY"
    assert seen["driver"] is fake_driver
    row = result[0]
    assert "variable_Key_SD1" not in row
    assert "value_Key_SD1" not in row
    assert "variable_Key_AD2" not in row
    assert "value_Key_AD2" not in row


def test_standard_key_to_category_id_name_order(monkeypatch):
    seen, fake_driver = _setup_merge_domain_validation(monkeypatch)

    def fake_get_query(_query, driver=None, params=None, type=None):
        if type == "df":
            return pd.DataFrame(
                [
                    {
                        "datasetID": "SD1",
                        "CMName": "Category Name",
                        "CMID": "AM123",
                        "Key": "varA: value1",
                        "Name": "Term A",
                    }
                ]
            )
        raise AssertionError("Unexpected query type")

    monkeypatch.setattr(merge_mod, "getQuery", fake_get_query)

    result = merge_mod.proposeMerge(
        dataset_choices=["SD1"],
        category_label="CATEGORY",
        criteria="standard",
        database="ArchaMap",
        intersection=True,
        selectedKeyvariables={},
        ncontains=2,
        resultFormat="key-to-category",
    )

    assert isinstance(result, list)
    assert len(result) == 1
    assert seen["domain"] == "CATEGORY"
    assert seen["driver"] is fake_driver
    assert list(result[0].keys())[:2] == ["CMID", "CMName"]


def test_standard_category_to_category_id_name_order(monkeypatch):
    seen, fake_driver = _setup_merge_domain_validation(monkeypatch)

    def fake_get_query(_query, driver=None, params=None, type=None):
        if type == "df":
            return pd.DataFrame(
                [
                    {
                        "datasetID": "SD1",
                        "CMName": "Category Name",
                        "CMID": "AM123",
                        "Key": "varA: value1",
                        "Name": "Term A",
                    },
                    {
                        "datasetID": "AD2",
                        "CMName": "Category Name",
                        "CMID": "AM123",
                        "Key": "varB: value2",
                        "Name": "Term B",
                    },
                ]
            )
        raise AssertionError("Unexpected query type")

    monkeypatch.setattr(merge_mod, "getQuery", fake_get_query)

    result = merge_mod.proposeMerge(
        dataset_choices=["SD1", "AD2"],
        category_label="CATEGORY",
        criteria="standard",
        database="ArchaMap",
        intersection=False,
        selectedKeyvariables={},
        ncontains=2,
        resultFormat="category-to-category",
    )

    assert isinstance(result, list)
    assert len(result) == 1
    assert seen["domain"] == "CATEGORY"
    assert seen["driver"] is fake_driver
    assert list(result[0].keys())[:2] == ["CMID", "CMName"]


def test_extended_key_to_key_supports_three_datasets(monkeypatch):
    seen, fake_driver = _setup_merge_domain_validation(monkeypatch)

    def fake_get_query(query, driver=None, params=None, type=None):
        if "RETURN d.CMID AS datasetID, d.CMName AS datasetName" in query:
            return [
                {"datasetID": "SD1", "datasetName": "Dataset One"},
                {"datasetID": "AD2", "datasetName": "Dataset Two"},
                {"datasetID": "SD3", "datasetName": "Dataset Three"},
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
                        "tie": 1,
                        "Key": "varB: value2",
                        "Name": "Term B",
                    },
                    {
                        "datasetID": "SD3",
                        "LCA_CMName": "Ancestor Name",
                        "LCA_CMID": "AM123",
                        "tie": 1,
                        "Key": "varC: value3",
                        "Name": "Term C",
                    },
                ]
            )
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(merge_mod, "getQuery", fake_get_query)

    result = merge_mod.proposeMerge(
        dataset_choices=["SD1", "AD2", "SD3"],
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
    assert seen["domain"] == "CATEGORY"
    assert seen["driver"] is fake_driver
    assert result[0]["datasetCMName_SD1"] == "Dataset One"
    assert result[0]["datasetCMName_AD2"] == "Dataset Two"
    assert result[0]["datasetCMName_SD3"] == "Dataset Three"
