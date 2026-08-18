import gzip
import json

from rdflib import Graph

from CM.bulk_rdf import generate_snapshot
from CM.linked_data import project_assertion, project_hierarchy_link, project_resource


def test_snapshot_is_parseable_complete_and_atomic(tmp_path):
    def graph_iterator(database):
        assert database == "sociomap"
        yield "resource", {"CATEGORY", "ETHNICITY"}, project_resource(
            database,
            {"cmid": "SM1", "labels": ["CATEGORY", "ETHNICITY"], "name": "Aymara"},
        )
        yield "resource", {"DATASET"}, project_resource(
            database,
            {"cmid": "SD1", "labels": ["DATASET"], "name": "Dataset"},
        )
        yield "assertion", set(), project_assertion(
            database,
            {"datasetCmid": "SD1", "conceptCmid": "SM1", "key": "ethnicity"},
        )
        yield "hierarchy", set(), project_hierarchy_link(
            database,
            {
                "sourceCmid": "SM1",
                "targetCmid": "SM2",
                "sourceLabels": ["CATEGORY", "ETHNICITY"],
                "targetLabels": ["CATEGORY", "ETHNICITY"],
            },
        )

    snapshot, manifest_path, manifest = generate_snapshot(
        "sociomap",
        tmp_path,
        source_version="fixture-1",
        graph_iterator=graph_iterator,
    )

    with gzip.open(snapshot, "rt", encoding="utf-8") as stream:
        graph = Graph().parse(data=stream.read(), format="nt")
    stored_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert len(graph) > 0
    assert manifest["resourceCounts"]["resources"] == 2
    assert manifest["resourceCounts"]["datasetAssertions"] == 1
    assert manifest["resourceCounts"]["hierarchyLinks"] == 1
    assert stored_manifest["sha256"] == manifest["sha256"]
    assert not list(tmp_path.glob(".*"))


def test_failed_snapshot_removes_temporary_output(tmp_path):
    def graph_iterator(_database):
        yield "resource", {"CATEGORY"}, project_resource(
            "sociomap",
            {"cmid": "SM1", "labels": ["CATEGORY"], "name": "Aymara"},
        )
        raise KeyboardInterrupt

    try:
        generate_snapshot("sociomap", tmp_path, graph_iterator=graph_iterator)
    except KeyboardInterrupt:
        pass
    else:
        raise AssertionError("Expected the simulated interrupted export to fail")

    assert not list(tmp_path.glob(".*"))
    assert not list(tmp_path.glob("*.nt.gz"))
