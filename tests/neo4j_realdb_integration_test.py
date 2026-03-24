import uuid
import importlib

import pandas as pd
import pytest

import CM.explore as explore_module
import CM.USES as uses_module
from CM import getQuery

search_module = importlib.import_module("CM.search")
semantic_search_module = importlib.import_module("CM.semantic_search")


@pytest.fixture
def seeded_realdb_graph(realdb_driver, realdb_database):
    run_id = f"pytest_realdb_{uuid.uuid4().hex[:10]}"
    suffix = uuid.uuid4().hex[:8].upper()

    dataset_cmid = f"ADT{suffix}"
    parent_dataset_cmid = f"ADP{suffix}"
    child_dataset_cmid = f"ADC{suffix}"
    category_cmid = f"AMT{suffix}"
    child_category_cmid = f"AMC{suffix}"
    district_cmid = f"DXT{suffix}"

    create_query = """
    CREATE (d:DATASET:TEST_TMP {
      CMID: $dataset_cmid,
      CMName: $dataset_cmid,
      names: [$dataset_cmid],
      recordStart: ['2001'],
      recordEnd: ['2002'],
      ApplicableYears: '2001-2002',
      District: [$district_cmid],
      parent: [$parent_dataset_cmid],
      testRunId: $run_id
    })
    CREATE (p:DATASET:TEST_TMP {
      CMID: $parent_dataset_cmid,
      CMName: $parent_dataset_cmid,
      names: [$parent_dataset_cmid],
      recordStart: '2000',
      recordEnd: '2005',
      testRunId: $run_id
    })
    CREATE (dist:DISTRICT:TEST_TMP {
      CMID: $district_cmid,
      CMName: $district_cmid,
      names: [$district_cmid],
      testRunId: $run_id
    })
    CREATE (c:CATEGORY:TEST_TMP {
      CMID: $category_cmid,
      CMName: $category_cmid,
      names: [$category_cmid],
      testRunId: $run_id
    })
    CREATE (child_d:DATASET:TEST_TMP {
      CMID: $child_dataset_cmid,
      CMName: $child_dataset_cmid,
      names: [$child_dataset_cmid],
      recordStart: '2001',
      recordEnd: ['2002'],
      testRunId: $run_id
    })
    CREATE (child_c:CATEGORY:TEST_TMP {
      CMID: $child_category_cmid,
      CMName: $child_category_cmid,
      names: [$child_category_cmid],
      testRunId: $run_id
    })
    CREATE (d)-[:CONTAINS]->(child_d)
    CREATE (d)-[:USES {
      Key: 'Key == ' + $category_cmid,
      Name: ['Temp Name'],
      label: 'CATEGORY',
      recordStart: '2001',
      recordEnd: ['2002'],
      yearStart: ['2001'],
      yearEnd: '2002'
    }]->(c)
    CREATE (child_d)-[:USES {
      Key: 'Key == ' + $child_category_cmid,
      Name: ['Child Temp Name'],
      label: 'CATEGORY',
      recordStart: ['2001'],
      recordEnd: '2002',
      yearStart: '2001',
      yearEnd: ['2002']
    }]->(child_c)
    CREATE (d)-[:HAS_LOG]->(:LOG:TEST_TMP {
      user: 'pytest',
      action: 'seed',
      timestamp: datetime(),
      testRunId: $run_id
    })
    RETURN d.CMID AS datasetCMID
    """

    getQuery(
        create_query,
        realdb_driver,
        params={
            "run_id": run_id,
            "dataset_cmid": dataset_cmid,
            "parent_dataset_cmid": parent_dataset_cmid,
            "child_dataset_cmid": child_dataset_cmid,
            "category_cmid": category_cmid,
            "child_category_cmid": child_category_cmid,
            "district_cmid": district_cmid,
        },
        type="dict",
    )

    payload = {
        "database": realdb_database,
        "run_id": run_id,
        "dataset_cmid": dataset_cmid,
        "parent_dataset_cmid": parent_dataset_cmid,
        "child_dataset_cmid": child_dataset_cmid,
        "category_cmid": category_cmid,
        "child_category_cmid": child_category_cmid,
        "district_cmid": district_cmid,
    }

    try:
        yield payload
    finally:
        cleanup_query = """
        MATCH (n:TEST_TMP {testRunId: $run_id})
        DETACH DELETE n
        RETURN count(n) AS deletedCount
        """
        getQuery(cleanup_query, realdb_driver, params={"run_id": run_id}, type="list")


@pytest.mark.realdb
def test_realdb_explore_dataset_page_handles_scalar_and_list_labels(seeded_realdb_graph):
    result = explore_module.getCategoryPage(
        seeded_realdb_graph["database"],
        [seeded_realdb_graph["dataset_cmid"]],
    )

    assert isinstance(result, dict)
    assert "categories" in result
    assert "childcategories" in result
    # Regression target: label scalar/list mismatch should not throw and should emit a domain row.
    assert any(row.get("Domain") == "CATEGORY" for row in result["categories"])


@pytest.mark.realdb
def test_realdb_search_translate_executes_against_live_neo4j(seeded_realdb_graph):
    table = [{"term": "Temp Name", "yearStart": "2000", "yearEnd": "2025"}]
    output = search_module.translate(
        database=seeded_realdb_graph["database"],
        property="Name",
        domain="CATEGORY",
        key="false",
        term="term",
        country="false",
        context="false",
        dataset="false",
        yearStart="2000",
        yearEnd="2025",
        query="false",
        table=table,
        countsamename=False,
        uniqueRows=True,
    )

    assert isinstance(output, tuple)
    assert len(output) in {2, 3}
    translated_df = output[0]
    assert isinstance(translated_df, pd.DataFrame)


@pytest.mark.realdb
def test_realdb_semantic_translate_executes_against_live_neo4j(seeded_realdb_graph):
    table = [{"term": "Temp Name", "yearStart": "2000", "yearEnd": "2025"}]
    output = semantic_search_module.translate(
        database=seeded_realdb_graph["database"],
        property="Name",
        domain="CATEGORY",
        key="false",
        term="term",
        country="false",
        context="false",
        dataset="false",
        yearStart="2000",
        yearEnd="2025",
        query="false",
        table=table,
        uniqueRows=False,
    )

    # semantic translate currently returns the translated dataframe directly.
    assert isinstance(output, pd.DataFrame)


@pytest.mark.realdb
def test_realdb_processdatasets_runs_for_seeded_data(seeded_realdb_graph):
    result = uses_module.processDATASETs(
        seeded_realdb_graph["database"],
        CMID=[seeded_realdb_graph["dataset_cmid"]],
        user="0",
    )
    assert isinstance(result, str)
    assert "Completed processing DATASET nodes" in result
