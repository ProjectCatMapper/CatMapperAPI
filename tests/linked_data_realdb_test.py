import pytest
from rdflib import RDF

from CM.linked_data import CAT, assertion_iri, fetch_resource_projection, serialize_graph
from CM.utils import getDriver, getQuery


@pytest.mark.realdb
def test_live_representative_resources_project_without_private_fields(realdb_database):
    database = realdb_database.lower()
    examples = {
        "sociomap": ("SM1", "SD1"),
        "archamap": ("AM1", "AD1"),
    }
    category, dataset = examples[database]

    for cmid in (category, dataset):
        projection = fetch_resource_projection(database, cmid, assertion_limit=5)
        graph = projection["graph"]
        turtle = serialize_graph(graph, "text/turtle")
        assert f"https://catmapper.org/{database}/{cmid}" in turtle
        assert len(graph) > 0
        for forbidden in (
            "ownerUserId",
            "modifiedByOtherUser",
            "logID",
            "elementId",
            "embedding",
            "HAS_LOG",
        ):
            assert forbidden not in turtle


@pytest.mark.realdb
def test_live_assertion_page_has_one_resource_per_projected_tie(realdb_database):
    database = realdb_database.lower()
    dataset = "SD1" if database == "sociomap" else "AD1"
    projection = fetch_resource_projection(database, dataset, assertion_limit=20)
    assertions = set(projection["graph"].subjects(RDF.type, CAT.DatasetAssertion))
    assert len(assertions) == projection["assertionCount"]
    assert len(assertions) == len(set(map(str, assertions)))


@pytest.mark.realdb
def test_live_duplicate_assertion_groups_have_stable_distinct_discriminators(realdb_database):
    if realdb_database.lower() != "archamap":
        pytest.skip("The current collision fixture exists in ArchaMap")
    driver = getDriver("archamap")
    groups = getQuery(
        """
        MATCH (d:DATASET)-[r:USES]->(c:CATEGORY)
        WITH d.CMID AS datasetCmid, r.Key AS key, c.CMID AS conceptCmid, count(r) AS multiplicity
        WHERE multiplicity > 1
        RETURN datasetCmid, key, conceptCmid, multiplicity
        ORDER BY datasetCmid, key, conceptCmid
        LIMIT 1
        """,
        driver=driver,
        type="dict",
    )
    assert groups
    group = groups[0]
    rows = getQuery(
        """
        MATCH (d:DATASET {CMID: $datasetCmid})-[r:USES]->(c:CATEGORY {CMID: $conceptCmid})
        WHERE r.Key = $key
        RETURN d.CMID AS datasetCmid, r.Key AS key, c.CMID AS conceptCmid,
               r.logID AS stableDiscriminator
        """,
        driver=driver,
        params=group,
        type="dict",
    )
    iris = {
        assertion_iri({**row, "database": "archamap"}, group["multiplicity"])
        for row in rows
    }
    assert len(iris) == len(rows) == group["multiplicity"]
    assert all("4:" not in str(iri) for iri in iris)
