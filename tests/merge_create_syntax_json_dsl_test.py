from pathlib import Path

import pandas as pd

import CM.merge as merge_mod


def test_create_syntax_uses_json_runtime_and_scoped_equivalence_rows(monkeypatch, tmp_path):
    monkeypatch.setattr(merge_mod, "getDriver", lambda _database: object())
    monkeypatch.setattr(
        merge_mod,
        "validate_domain_label",
        lambda label, driver=None, aliases=None, extra_allowed=None: str(label).upper(),
    )

    def fake_get_query(query, driver=None, params=None, type=None):
        if "MATCH (a:DATASET {CMID: cmid})" in query:
            return pd.DataFrame(
                [
                    {"CMID": "AD957", "CMName": "Merge Template"},
                    {"CMID": "AD958", "CMName": "Stack A"},
                    {"CMID": "AD959", "CMName": "Stack B"},
                    {"CMID": "AD354274", "CMName": "Dataset A"},
                    {"CMID": "AD354275", "CMName": "Dataset B"},
                ]
            )

        if "MATCH (m:DATASET {CMID: row.mergingID})-[:MERGING]->(s:DATASET {CMID: row.stackID})-[rsv:MERGING]->(v:VARIABLE)<-[rdv:MERGING]-(d:DATASET {CMID: row.datasetID})" in query:
            return pd.DataFrame(
                [
                    {
                        "mergingID": "AD957",
                        "mergingName": "Test Merge",
                        "stackID": "AD958",
                        "stackName": "Stack A",
                        "datasetID": "AD354274",
                        "datasetName": "Dataset A",
                        "varName": "easting",
                        "variableID": "AM1",
                        "stackTransform": '[{"stepOrder":1,"op":"as_numeric","target":"easting","sources":"easting","value":null,"options":{}}]',
                        "variableFilter": '[{"stepOrder":1,"op":"drop_na","target":"easting","sources":[],"value":null,"options":{}}]',
                        "summaryStatistic": "mean",
                        "summaryFilter": None,
                        "summaryWeight": None,
                        "datasetTransform": '[{"stepOrder":1,"op":"copy","target":"easting","sources":"UTM_E","value":null,"options":{}}]',
                        "variableKey": "Field == UTM_E",
                    },
                    {
                        "mergingID": "AD957",
                        "mergingName": "Test Merge",
                        "stackID": "AD959",
                        "stackName": "Stack B",
                        "datasetID": "AD354275",
                        "datasetName": "Dataset B",
                        "varName": "pithouse_depth_m",
                        "variableID": "AM2",
                        "stackTransform": '[{"stepOrder":1,"op":"as_numeric","target":"pithouse_depth_m","sources":"pithouse_depth_m","value":null,"options":{}}]',
                        "variableFilter": '[{"stepOrder":1,"op":"drop_na","target":"pithouse_depth_m","sources":[],"value":null,"options":{}}]',
                        "summaryStatistic": "mean",
                        "summaryFilter": None,
                        "summaryWeight": None,
                        "datasetTransform": '[{"stepOrder":1,"op":"copy","target":"pithouse_depth_m","sources":"Depth","value":null,"options":{}}]',
                        "variableKey": "Field == Depth",
                    },
                ]
            )

        if "MATCH (source:CATEGORY)-[e:EQUIVALENT {stack: row.stackID, dataset: row.datasetID}]->(target:CATEGORY)" in query:
            return pd.DataFrame(
                [
                    {
                        "datasetID": "AD354274",
                        "stackID": "AD958",
                        "Key": "site_num == AM1142",
                        "originalCMID": "AM1142",
                        "originalCMName": "Shared Site",
                        "equivalentCMID": "AM1142",
                        "equivalentCMName": "Shared Site",
                    },
                    {
                        "datasetID": "AD354275",
                        "stackID": "AD959",
                        "Key": "Site_Num == AM1142",
                        "originalCMID": "AM1142",
                        "originalCMName": "Shared Site",
                        "equivalentCMID": "AM1142",
                        "equivalentCMName": "Shared Site",
                    },
                ]
            )

        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(merge_mod, "getQuery", fake_get_query)

    template = [
        {
            "mergingID": "AD957",
            "stackID": "AD958",
            "datasetID": "AD354274",
            "filePath": "pseudo_data/TEST_A.csv",
        },
        {
            "mergingID": "AD957",
            "stackID": "AD959",
            "datasetID": "AD354275",
            "filePath": "pseudo_data/TEST_B.csv",
        },
    ]

    result = merge_mod.createSyntax(
        template=template,
        database="ArchaMap",
        syntax="R",
        dirpath=str(tmp_path),
        download=False,
    )

    assert Path(result["zip"]).exists()

    syntax_path = tmp_path / "syntax.R"
    syntax_text = syntax_path.read_text(encoding="utf-8")
    assert "qa_apply_transform_ast" in syntax_text
    assert 'file.path(bundle_dir, "data.xlsx")' in syntax_text
    assert '[{"stepOrder":1,"op":"copy"' not in syntax_text
    assert 'else if (op == "in")' in syntax_text
    assert "    in =" not in syntax_text

    categories = pd.read_excel(tmp_path / "categories.xlsx")
    assert set(
        ["datasetID", "stackID", "Key", "originalCMID", "equivalentCMID"]
    ).issubset(categories.columns)
    assert len(categories) == 2

    data = pd.read_excel(tmp_path / "data.xlsx")
    assert data.loc[0, "datasetTransform"].startswith('[{"stepOrder":1')
