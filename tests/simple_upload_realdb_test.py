import uuid

import pytest

from CM import getQuery, input_Nodes_Uses
from CMroutes.upload_routes import _prepare_upload_job


@pytest.fixture
def simple_upload_seed(realdb_driver, realdb_database):
    run_id = f"pytest_simple_upload_{uuid.uuid4().hex[:10]}"
    suffix = uuid.uuid4().hex[:8].upper()
    dataset_cmid = f"ADSU{suffix}"

    create_query = """
    CREATE (d:DATASET:TEST_TMP {
      CMID: $dataset_cmid,
      CMName: $dataset_cmid,
      names: [$dataset_cmid],
      testRunId: $run_id
    })
    RETURN d.CMID AS datasetCMID
    """
    getQuery(
        create_query,
        realdb_driver,
        params={"dataset_cmid": dataset_cmid, "run_id": run_id},
        type="dict",
    )

    payload = {
        "database": realdb_database,
        "run_id": run_id,
        "dataset_cmid": dataset_cmid,
    }

    try:
        yield payload
    finally:
        # Remove only this test's temporary category/ties, then TEST_TMP nodes.
        cleanup_uploaded_query = """
        MATCH (d:DATASET {CMID: $dataset_cmid})-[r:USES]->(c)
        WHERE c.CMName IN $cmnames
        DETACH DELETE c
        """
        cmnames = payload.get("upload_cmnames") or []
        if cmnames:
            getQuery(
                cleanup_uploaded_query,
                realdb_driver,
                params={"dataset_cmid": dataset_cmid, "cmnames": cmnames},
                type="dict",
            )

        cleanup_tmp_query = """
        MATCH (n:TEST_TMP {testRunId: $run_id})
        DETACH DELETE n
        RETURN count(n) AS deletedCount
        """
        getQuery(cleanup_tmp_query, realdb_driver, params={"run_id": run_id}, type="list")


@pytest.mark.realdb
def test_simple_upload_realdb_forces_add_uses_and_maps_columns(simple_upload_seed, realdb_driver):
    run_id = simple_upload_seed["run_id"]
    dataset_cmid = simple_upload_seed["dataset_cmid"]
    cmname = f"CMName_{run_id}"
    name = f"Name_{run_id}"
    key_raw = f"value_{run_id}"

    data = {
        "database": simple_upload_seed["database"],
        "so": "simple",
        # Intentionally not add_uses to assert simple path forces add_uses.
        "ao": "update_add",
        "addoptions": {"district": False, "recordyear": False},
        "allContext": [],
        "optionalProperties": [],
        "df": [
            {
                "col_cmname": cmname,
                "col_name": name,
                "col_key": key_raw,
                "alt_1": "Alt One",
                "alt_2": "Alt Two",
                "col_cmid": "",
            }
        ],
        "formData": {
            "domain": "DISTRICT",
            "subdomain": "DISTRICT",
            "datasetID": dataset_cmid,
            "cmNameColumn": "col_cmname",
            "categoryNamesColumn": "col_name",
            "alternateCategoryNamesColumns": ["alt_1", "alt_2"],
            "cmidColumn": "col_cmid",
            "keyColumn": "col_key",
        },
    }

    job_args, total_rows, _database, _warnings = _prepare_upload_job(data, acting_user="pytest")

    # Ensure simple preprocessing keeps expected mapping semantics stable.
    assert job_args["uploadOption"] == "add_uses"
    assert job_args["formatKey"] is False
    assert total_rows == 1
    assert job_args["dataset"][0]["CMName"] == cmname
    assert job_args["dataset"][0]["Name"] == name
    assert job_args["dataset"][0]["altNames"] == "Alt One;Alt Two"

    # Execute upload and verify key formatting reached Neo4j as "Key == value".
    response, _order = input_Nodes_Uses(**job_args)
    assert len(response) >= 1

    simple_upload_seed["upload_cmnames"] = [cmname]

    verify_query = """
    MATCH (d:DATASET {CMID: $dataset_cmid})-[r:USES]->(c:DISTRICT {CMName: $cmname})
    RETURN r.Key AS keyValue, c.CMID AS cmid
    """
    rows = getQuery(
        verify_query,
        realdb_driver,
        params={"dataset_cmid": dataset_cmid, "cmname": cmname},
        type="dict",
    )
    assert rows
    assert any(row.get("keyValue") == f"col_key == {key_raw}" for row in rows)


@pytest.mark.realdb
def test_simple_upload_realdb_multiple_key_columns_with_sparse_values(simple_upload_seed, realdb_driver):
    run_id = simple_upload_seed["run_id"]
    dataset_cmid = simple_upload_seed["dataset_cmid"]

    rows_in = [
        {
            "col_cmname": f"CMNameA_{run_id}",
            "col_name": f"NameA_{run_id}",
            "k_type": "TypeA",
            "k_period": "PeriodA",
            "col_cmid": "",
        },
        {
            "col_cmname": f"CMNameB_{run_id}",
            "col_name": f"NameB_{run_id}",
            "k_type": "TypeOnly",
            "k_period": "",
            "col_cmid": "",
        },
        {
            "col_cmname": f"CMNameC_{run_id}",
            "col_name": f"NameC_{run_id}",
            "k_type": "",
            "k_period": "PeriodOnly",
            "col_cmid": "",
        },
    ]

    data = {
        "database": simple_upload_seed["database"],
        "so": "simple",
        "ao": "add_node",
        "addoptions": {"district": False, "recordyear": False},
        "allContext": [],
        "optionalProperties": [],
        "df": rows_in,
        "formData": {
            "domain": "DISTRICT",
            "subdomain": "DISTRICT",
            "datasetID": dataset_cmid,
            "cmNameColumn": "col_cmname",
            "categoryNamesColumn": "col_name",
            "alternateCategoryNamesColumns": [],
            "cmidColumn": "col_cmid",
            "keyColumns": ["k_type", "k_period"],
            "keyColumn": "",
        },
    }

    job_args, total_rows, _database, _warnings = _prepare_upload_job(data, acting_user="pytest")

    assert job_args["uploadOption"] == "add_uses"
    assert job_args["formatKey"] is False
    assert total_rows == 3

    payload_rows = job_args["dataset"]
    assert payload_rows[0]["Key"] == "k_type == TypeA && k_period == PeriodA"
    assert payload_rows[1]["Key"] == "k_type == TypeOnly"
    assert payload_rows[2]["Key"] == "k_period == PeriodOnly"

    response, _order = input_Nodes_Uses(**job_args)
    assert len(response) >= 3

    cmnames = [row["col_cmname"] for row in rows_in]
    simple_upload_seed["upload_cmnames"] = cmnames

    verify_query = """
    MATCH (d:DATASET {CMID: $dataset_cmid})-[r:USES]->(c:DISTRICT)
    WHERE c.CMName IN $cmnames
    RETURN c.CMName AS cmname, r.Key AS keyValue
    """
    result_rows = getQuery(
        verify_query,
        realdb_driver,
        params={"dataset_cmid": dataset_cmid, "cmnames": cmnames},
        type="dict",
    )
    assert len(result_rows) == 3
    lookup = {row["cmname"]: row["keyValue"] for row in result_rows}
    assert lookup[f"CMNameA_{run_id}"] == "k_type == TypeA && k_period == PeriodA"
    assert lookup[f"CMNameB_{run_id}"] == "k_type == TypeOnly"
    assert lookup[f"CMNameC_{run_id}"] == "k_period == PeriodOnly"
