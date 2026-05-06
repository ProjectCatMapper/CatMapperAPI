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
                        "CMID": "AM123",
                        "tie": 0,
                        "Key": "varA: value1",
                        "Name": "Term A",
                    },
                    {
                        "datasetID": "AD2",
                        "LCA_CMName": "Ancestor Name",
                        "LCA_CMID": "AM123",
                        "CMID": "AM123",
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
    assert result[0]["matchedCMID_SD1"] == "AM123"
    assert result[0]["matchedCMID_AD2"] == "AM123"


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
                        "CMID": "AM123",
                        "tie": 0,
                        "Key": "varA: value1",
                        "Name": "Term A",
                    },
                    {
                        "datasetID": "AD2",
                        "LCA_CMName": "Ancestor Name",
                        "LCA_CMID": "AM123",
                        "CMID": "AM123",
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
    assert result[0]["CMID"] == "AM123"
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
                        "tie": 1,
                        "Key": "varA: value1",
                        "Name": "Term A",
                    },
                    {
                        "datasetID": "AD2",
                        "LCA_CMName": "Ancestor Name",
                        "LCA_CMID": "AM123",
                        "tie": 2,
                        "Key": "varB: value2",
                        "Name": "Term B",
                    },
                    {
                        "datasetID": "SD3",
                        "LCA_CMName": "Ancestor Name",
                        "LCA_CMID": "AM123",
                        "tie": 2,
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
        ncontains=4,
        resultFormat="key-to-key",
    )

    assert isinstance(result, list)
    assert len(result) == 1
    assert seen["domain"] == "CATEGORY"
    assert seen["driver"] is fake_driver
    assert result[0]["datasetCMName_SD1"] == "Dataset One"
    assert result[0]["datasetCMName_AD2"] == "Dataset Two"
    assert result[0]["datasetCMName_SD3"] == "Dataset Three"
    assert result[0]["maxPairwiseDistance"] == 4
    assert "nTie" not in result[0]


def test_extended_filters_by_max_pairwise_distance_not_individual_tie():
    result = pd.DataFrame(
        [
            {
                "LCA_CMID": "AM123",
                "LCA_CMName": "Ancestor Name",
                "Key_SD1": "varA: value1",
                "Name_SD1": "Term A",
                "tie_SD1": 2,
                "Key_AD2": "varB: value2",
                "Name_AD2": "Term B",
                "tie_AD2": 3,
            }
        ]
    )

    filtered = merge_mod._select_best_extended_rows(
        result=result,
        dataset_choices=["SD1", "AD2"],
        ncontains=4,
        intersection=True,
    )

    assert filtered.empty


def test_extended_ancestor_only_keeps_rows_where_lca_is_matched_node():
    result = pd.DataFrame(
        [
            {
                "LCA_CMID": "SM462198",
                "LCA_CMName": "Mid Eastern",
                "Key_SD1": "V024 == 6",
                "matchedCMID_SD1": "SM9227",
                "tie_SD1": 1,
                "Key_AD2": "V024 == 5",
                "matchedCMID_AD2": "SM462198",
                "tie_AD2": 0,
            },
            {
                "LCA_CMID": "SM462198",
                "LCA_CMName": "Mid Eastern",
                "Key_SD1": "V024 == 6",
                "matchedCMID_SD1": "SM9227",
                "tie_SD1": 1,
                "Key_AD2": "V024 == 8",
                "matchedCMID_AD2": "SM9999",
                "tie_AD2": 1,
            },
        ]
    )

    filtered = merge_mod._select_best_extended_rows(
        result=result,
        dataset_choices=["SD1", "AD2"],
        ncontains=2,
        intersection=True,
        ancestor_only=True,
    )

    assert len(filtered) == 1
    assert filtered.iloc[0]["matchedCMID_AD2"] == "SM462198"


def test_extended_pairwise_distance_deduplicates_shared_matched_nodes(monkeypatch):
    _setup_merge_domain_validation(monkeypatch)

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
                        "LCA_CMName": "Mid Eastern",
                        "LCA_CMID": "SM462198",
                        "CMID": "SM9227",
                        "tie": 1,
                        "Key": "V024 == 6",
                        "Name": "Bugisu",
                    },
                    {
                        "datasetID": "AD2",
                        "LCA_CMName": "Mid Eastern",
                        "LCA_CMID": "SM462198",
                        "CMID": "SM9227",
                        "tie": 1,
                        "Key": "V024 == 7",
                        "Name": "Bugishu",
                    },
                    {
                        "datasetID": "SD3",
                        "LCA_CMName": "Mid Eastern",
                        "LCA_CMID": "SM462198",
                        "CMID": "SM462198",
                        "tie": 0,
                        "Key": "V024 == 5",
                        "Name": "Mid Eastern",
                    },
                ]
            )
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(merge_mod, "getQuery", fake_get_query)

    result = merge_mod.proposeMerge(
        dataset_choices=["SD1", "AD2", "SD3"],
        category_label="CATEGORY",
        criteria="extended",
        database="SocioMap",
        intersection=True,
        selectedKeyvariables={},
        ncontains=1,
        resultFormat="key-to-key",
    )

    assert isinstance(result, list)
    assert len(result) == 1
    row = result[0]
    assert row["maxPairwiseDistance"] == 1
    assert row["matchedCMID_SD1"] == "SM9227"
    assert row["matchedCMID_AD2"] == "SM9227"
    assert row["matchedCMID_SD3"] == "SM462198"


def test_standard_key_to_key_returns_friendly_warning_when_dataset_has_no_rows(monkeypatch):
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

    payload, status = merge_mod.proposeMerge(
        dataset_choices=["SD1", "AD2"],
        category_label="CATEGORY",
        criteria="standard",
        database="ArchaMap",
        intersection=True,
        selectedKeyvariables={},
        ncontains=2,
        resultFormat="key-to-key",
    )

    assert seen["domain"] == "CATEGORY"
    assert seen["driver"] is fake_driver
    assert status == 400
    assert "could not find any matches" in payload["error"]
    assert "AD2" in payload["error"]
    assert "CATEGORY" in payload["error"]


def test_standard_key_to_key_filters_selected_key_variables(monkeypatch):
    _setup_merge_domain_validation(monkeypatch)

    def fake_get_query(_query, driver=None, params=None, type=None):
        if type == "df":
            return pd.DataFrame(
                [
                    {
                        "datasetID": "SD1",
                        "CMName": "Keep Category",
                        "CMID": "AM123",
                        "Key": "lang == english",
                        "Name": "English",
                    },
                    {
                        "datasetID": "SD1",
                        "CMName": "Drop Category",
                        "CMID": "AM456",
                        "Key": "region == north",
                        "Name": "North",
                    },
                    {
                        "datasetID": "AD2",
                        "CMName": "Keep Category",
                        "CMID": "AM123",
                        "Key": "period == early",
                        "Name": "Early",
                    },
                    {
                        "datasetID": "AD2",
                        "CMName": "Drop Category",
                        "CMID": "AM456",
                        "Key": "period == late",
                        "Name": "Late",
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
        selectedKeyvariables={"SD1": "lang"},
        ncontains=2,
        resultFormat="key-to-key",
    )

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["CMID"] == "AM123"
    assert result[0]["Key_SD1"] == "lang == english"
