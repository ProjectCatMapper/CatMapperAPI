import csv
import json
import os
import shutil
import subprocess
import time
import uuid
from pathlib import Path

import pytest

from CM import getQuery
import CM.utils as cm_utils
from CM.utils import closeAllDrivers, getDriver
import CMroutes.upload_routes as upload_routes
from CMroutes.task_store import get_task_store


DATABASE = "ArchaMap"
USER_ID = "pytest-merge-template-e2e"


def _realdb_enabled(pytestconfig):
    return bool(
        pytestconfig.getoption("--run-realdb")
        or os.environ.get("CM_RUN_REALDB_TESTS", "").strip().lower() in {"1", "true", "yes"}
    )


@pytest.fixture
def archamap_driver(pytestconfig):
    if not _realdb_enabled(pytestconfig):
        pytest.skip("realdb tests are disabled")
    cm_utils.config.read(Path(__file__).resolve().parents[1] / "config.ini")
    driver = getDriver(DATABASE)
    try:
        yield driver
    finally:
        closeAllDrivers()


@pytest.fixture(autouse=True)
def _route_auth_and_task_state(monkeypatch):
    monkeypatch.setenv("CATMAPPER_USE_RQ", "0")
    monkeypatch.setattr(
        upload_routes,
        "verify_request_auth",
        lambda **_kwargs: {"userid": USER_ID, "role": "admin", "database": [DATABASE]},
    )

    store = get_task_store()
    if hasattr(store, "upload_tasks"):
        with store.lock:
            store.upload_tasks.clear()
            store.waiting_tasks.clear()
    yield
    if hasattr(store, "upload_tasks"):
        with store.lock:
            store.upload_tasks.clear()
            store.waiting_tasks.clear()


def _standard_payload(rows, upload_option, *, merging_type="0"):
    return {
        "database": DATABASE,
        "so": "standard",
        "ao": upload_option,
        "mergingType": merging_type,
        "addoptions": {"district": False, "recordyear": False},
        "allContext": [],
        "optionalProperties": [],
        "user": USER_ID,
        "df": rows,
        "formData": {
            "domain": "DATASET",
            "subdomain": "DATASET",
            "datasetID": "",
            "cmNameColumn": "",
            "categoryNamesColumn": "",
            "alternateCategoryNamesColumns": [],
            "cmidColumn": "",
            "keyColumn": "",
        },
    }


def _post_upload_and_wait(client, rows, upload_option, *, merging_type="0", timeout=120):
    response = client.post(
        "/uploadInputNodes",
        json=_standard_payload(rows, upload_option, merging_type=merging_type),
    )
    assert response.status_code == 202, response.get_data(as_text=True)
    task_id = (response.get_json() or {}).get("taskId")
    assert task_id

    deadline = time.time() + timeout
    last_payload = None
    while time.time() < deadline:
        status_response = client.post(
            "/uploadInputNodesStatus",
            json={"taskId": task_id, "user": USER_ID},
        )
        assert status_response.status_code == 200, status_response.get_data(as_text=True)
        last_payload = status_response.get_json() or {}
        status = str(last_payload.get("status") or "").lower()
        if status == "completed":
            return last_payload
        if status in {"failed", "canceled"}:
            pytest.fail(f"Upload task {task_id} ended as {status}: {last_payload}")
        time.sleep(0.5)

    pytest.fail(f"Upload task {task_id} did not complete before timeout: {last_payload}")


def _resolve_uploaded_dataset_cmid(driver, cmname):
    rows = getQuery(
        """
        MATCH (d:DATASET {CMName: $cmname})
        RETURN d.CMID AS CMID, labels(d) AS labels
        ORDER BY d.CMID DESC
        LIMIT 1
        """,
        driver,
        params={"cmname": cmname},
        type="dict",
    )
    assert rows, f"Could not resolve uploaded DATASET node with CMName={cmname}"
    return rows[0]["CMID"], rows[0]["labels"]


def _select_real_merge_inputs(driver):
    rows = getQuery(
        """
        MATCH (d:DATASET)
        WHERE NOT d:STACK AND NOT d:MERGING
        CALL {
          WITH d
          MATCH (d)-[uv:USES]->(v:VARIABLE)
          WHERE uv.Key IS NOT NULL
            AND trim(toString(uv.Key)) <> ''
            AND (
              uv.categoryType IS NULL
              OR trim(toString(uv.categoryType)) = ''
              OR toUpper(toString(uv.categoryType)) IN ['ORDINAL', 'CONTINUOUS', 'CATEGORICAL', 'TEXT']
            )
          RETURN v, uv
          ORDER BY v.CMID
          LIMIT 1
        }
        CALL {
          WITH d
          MATCH (d)-[uc:USES]->(c:CATEGORY)
          WHERE uc.Key IS NOT NULL
            AND trim(toString(uc.Key)) <> ''
          RETURN c, uc
          ORDER BY c.CMID
          LIMIT 1
        }
        RETURN
          d.CMID AS datasetID,
          d.CMName AS datasetCMName,
          v.CMID AS variableID,
          v.CMName AS variableCMName,
          uv.Key AS variableKey,
          c.CMID AS categoryID,
          c.CMName AS categoryCMName,
          uc.Key AS existingCategoryKey
        ORDER BY d.CMID
        LIMIT 2
        """,
        driver,
        type="dict",
    )
    if not rows:
        pytest.skip("ArchaMap does not have a real DATASET with both VARIABLE and CATEGORY USES ties")
    return rows


def _write_fake_csv(path, category_value, variable_value):
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["pytest_category_key", "pytest_source_value"])
        writer.writeheader()
        writer.writerow(
            {
                "pytest_category_key": category_value,
                "pytest_source_value": variable_value,
            }
        )


def _require_r_merge_runtime():
    if shutil.which("Rscript") is None:
        pytest.skip("Rscript is required to execute generated merge syntax")

    packages = ["rio", "dplyr", "tidyr", "purrr", "jsonlite"]
    expr = (
        "missing <- c("
        + ", ".join(json.dumps(package) for package in packages)
        + ")[!vapply(c("
        + ", ".join(json.dumps(package) for package in packages)
        + "), requireNamespace, logical(1), quietly=TRUE)]; "
        "if (length(missing)) { cat(paste(missing, collapse=',')); quit(status=1) }"
    )
    result = subprocess.run(
        ["Rscript", "-e", expr],
        text=True,
        capture_output=True,
        timeout=60,
    )
    if result.returncode != 0:
        missing = (result.stdout or result.stderr or "unknown").strip()
        pytest.skip(f"R merge runtime package(s) missing: {missing}")


@pytest.mark.realdb
def test_public_route_merge_template_e2e_persists_example_nodes(client, archamap_driver, tmp_path):
    _require_r_merge_runtime()

    run_id = f"TEST_MERGE_TEMPLATE_E2E_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    selected = _select_real_merge_inputs(archamap_driver)

    merging_name = f"{run_id}_MERGING"
    stack_name = f"{run_id}_STACK"

    _post_upload_and_wait(
        client,
        [
            {
                "CMName": merging_name,
                "label": "MERGING",
                "shortName": merging_name,
                "DatasetCitation": f"Persistent pytest merge template example {run_id}",
            }
        ],
        "add_node",
    )
    merging_id, merging_labels = _resolve_uploaded_dataset_cmid(archamap_driver, merging_name)
    assert "MERGING" in merging_labels
    assert "DATASET" in merging_labels

    _post_upload_and_wait(
        client,
        [
            {
                "CMName": stack_name,
                "label": "STACK",
                "shortName": stack_name,
                "DatasetCitation": f"Persistent pytest merge template example {run_id}",
            }
        ],
        "add_node",
    )
    stack_id, stack_labels = _resolve_uploaded_dataset_cmid(archamap_driver, stack_name)
    assert "STACK" in stack_labels
    assert "DATASET" in stack_labels

    dataset_rows = [
        {
            "mergingID": merging_id,
            "stackID": stack_id,
            "datasetID": row["datasetID"],
        }
        for row in selected
    ]
    _post_upload_and_wait(
        client,
        dataset_rows,
        "add_merging",
        merging_type="merging_ties_to_datasets",
    )

    bridge_rows = getQuery(
        """
        MATCH (:MERGING {CMID: $merging_id})-[:MERGING]->(:STACK {CMID: $stack_id})-[:MERGING]->(d:DATASET)
        WHERE d.CMID IN $dataset_ids
        RETURN count(DISTINCT d) AS count
        """,
        archamap_driver,
        params={
            "merging_id": merging_id,
            "stack_id": stack_id,
            "dataset_ids": [row["datasetID"] for row in selected],
        },
        type="dict",
    )
    assert bridge_rows[0]["count"] == len({row["datasetID"] for row in selected})

    var_name = "pytest_merge_value"
    transform = json.dumps(
        [
            {
                "stepOrder": 1,
                "op": "copy",
                "sources": ["pytest_source_value"],
                "target": var_name,
            }
        ]
    )
    variable_rows = [
        {
            "mergingID": merging_id,
            "stackID": stack_id,
            "datasetID": row["datasetID"],
            "variableID": row["variableID"],
            "Key": row["variableKey"],
            "varName": var_name,
            "datasetTransform": transform,
        }
        for row in selected
    ]
    _post_upload_and_wait(
        client,
        variable_rows,
        "add_merging",
        merging_type="merging_ties_to_variables",
    )

    variable_check = getQuery(
        """
        MATCH (:STACK {CMID: $stack_id})-[:MERGING]->(v:VARIABLE)<-[:MERGING {stack: $stack_id}]-(d:DATASET)
        WHERE d.CMID IN $dataset_ids
        RETURN count(DISTINCT d.CMID + ':' + v.CMID) AS count
        """,
        archamap_driver,
        params={
            "stack_id": stack_id,
            "dataset_ids": [row["datasetID"] for row in selected],
        },
        type="dict",
    )
    assert variable_check[0]["count"] == len(selected)

    category_values = {
        row["datasetID"]: f"category_{idx}_{uuid.uuid4().hex[:6]}"
        for idx, row in enumerate(selected, start=1)
    }
    category_rows = [
        {
            "stackID": stack_id,
            "datasetID": row["datasetID"],
            "categoryID": row["categoryID"],
            "Key": f"pytest_category_key == {category_values[row['datasetID']]}",
        }
        for row in selected
    ]
    _post_upload_and_wait(
        client,
        category_rows,
        "add_merging",
        merging_type="merging_ties_to_categories",
    )

    category_check = getQuery(
        """
        UNWIND $rows AS row
        MATCH (d:DATASET {CMID: row.datasetID})-[m:MERGING {stack: $stack_id, Key: row.Key}]->(c:CATEGORY {CMID: row.categoryID})
        OPTIONAL MATCH (d)-[u:USES {Key: row.Key}]->(c)
        RETURN
          count(DISTINCT m) AS categoryMergingTieCount,
          count(DISTINCT CASE WHEN u IS NULL THEN m END) AS keyReassignmentCount,
          count(DISTINCT CASE WHEN m.datasetID IS NOT NULL THEN m END) AS relationshipsWithDatasetProperty
        """,
        archamap_driver,
        params={"stack_id": stack_id, "rows": category_rows},
        type="dict",
    )[0]
    assert category_check["categoryMergingTieCount"] == len(category_rows)
    assert category_check["keyReassignmentCount"] == len(category_rows)
    assert category_check["relationshipsWithDatasetProperty"] == 0

    equivalent_check = getQuery(
        """
        UNWIND $rows AS row
        MATCH ()-[e:EQUIVALENT {Key: row.Key}]->(:CATEGORY {CMID: row.categoryID})
        RETURN count(e) AS count
        """,
        archamap_driver,
        params={"rows": category_rows},
        type="dict",
    )[0]
    assert equivalent_check["count"] == 0

    summary_response = client.get(f"/merge/template/summary/{DATABASE}/{merging_id}")
    assert summary_response.status_code == 200, summary_response.get_data(as_text=True)
    summary = summary_response.get_json() or {}
    assert summary["stackSummaryTotals"]["categoryMergingTieCount"] == len(category_rows)
    assert summary["stackSummaryTotals"]["keyReassignmentCount"] == len(category_rows)
    assert len(summary["categoryMergingTies"]) == len(category_rows)

    template_rows = []
    expected_values = []
    for idx, row in enumerate(selected, start=1):
        fake_path = tmp_path / f"{row['datasetID']}_{idx}.csv"
        value = f"value_{idx}_{uuid.uuid4().hex[:6]}"
        _write_fake_csv(fake_path, category_values[row["datasetID"]], value)
        expected_values.append(value)
        template_rows.append(
            {
                "mergingID": merging_id,
                "stackID": stack_id,
                "datasetID": row["datasetID"],
                "filePath": str(fake_path),
            }
        )

    syntax_response = client.post(
        f"/merge/syntax/{DATABASE}",
        json={"template": template_rows},
    )
    assert syntax_response.status_code == 200, syntax_response.get_data(as_text=True)
    syntax_payload = syntax_response.get_json() or {}
    download = syntax_payload.get("download") or {}
    zip_path = Path(download.get("zip", "")).resolve()
    assert zip_path.exists()

    bundle_dir = zip_path.parent
    syntax_path = bundle_dir / "syntax.R"
    assert syntax_path.exists()
    assert (bundle_dir / "data.xlsx").exists()
    assert (bundle_dir / "categories.xlsx").exists()

    env = os.environ.copy()
    env["CM_KEEP_TEMP"] = "1"
    run = subprocess.run(
        ["Rscript", str(syntax_path)],
        cwd=str(bundle_dir),
        env=env,
        text=True,
        capture_output=True,
        timeout=120,
    )
    assert run.returncode == 0, f"Rscript failed\nSTDOUT:\n{run.stdout}\nSTDERR:\n{run.stderr}"

    output_path = bundle_dir / f"{merging_id}-output.xlsx"
    assert output_path.exists()
    output_text = subprocess.run(
        [
            "Rscript",
            "-e",
            f"cat(paste(rio::import({json.dumps(str(output_path))})${var_name}, collapse='\\n'))",
        ],
        text=True,
        capture_output=True,
        timeout=60,
    )
    assert output_text.returncode == 0, output_text.stderr
    for value in expected_values:
        assert value in output_text.stdout
