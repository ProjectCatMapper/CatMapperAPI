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
    from CM.assertion_identity import COLLISION_GROUP_QUERY, validate_collision_groups
    groups = getQuery(COLLISION_GROUP_QUERY, driver=getDriver(realdb_database),
                      params={"cmids": None}, type="dict")
    validate_collision_groups(groups)
    # Cover every group in either database. Testing only the first historical
    # duplicate missed the White Mountain collisions introduced by later moves.
    for group in groups:
        assert all(row["assertionId"] for row in group["identities"]), group["conceptCmid"]
        iris = {assertion_iri({**row, "database": realdb_database.lower(),
                              "datasetCmid": group["datasetCmid"], "key": group["key"],
                              "conceptCmid": group["conceptCmid"]}, group["multiplicity"])
                for row in group["identities"]}
        assert len(iris) == group["multiplicity"]
        assert all("4:" not in str(iri) for iri in iris)
