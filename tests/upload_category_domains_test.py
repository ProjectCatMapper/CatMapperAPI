"""Uploads must not depend on background label reconciliation (CATMA-315)."""

from types import SimpleNamespace

import pandas as pd
import pytest

import CM.upload as upload


LABEL_DOMAINS = {"PPL": "AREA", "ADM0": "AREA", "AREA": "AREA", "DIALECT": "LANGUAGE"}


@pytest.fixture
def creation_graph(monkeypatch):
    """Capture the labels passed to node creation without writing to a database."""
    created = {}
    monkeypatch.setattr(upload, "getDriver", lambda _: object())
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "createLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        upload, "getAvailableID",
        lambda **kwargs: [f"SM{i + 1}" for i in range(kwargs["n"])],
    )

    def query(query, driver=None, params=None, **kwargs):
        if "m.groupLabel AS groupLabel" in query:
            return [{"label": label, "groupLabel": LABEL_DOMAINS[label]}
                    for label in params["labels"]]
        if "return p.CMName as property" in query:
            return ["CMName", "CMID", "shortName", "DatasetCitation"]
        if "call apoc.cypher.doIt" in query:
            results = []
            for row in params["rows"]:
                created[row["CMID"]] = set(row["label"].split(":"))
                results.append({k: v for k, v in row.items() if k != "label"}
                               | {"nodeID": f"node-{row['CMID']}"})
            return results
        if "WHERE n:STACK OR n:MERGING" in query:
            return []
        raise AssertionError(f"Unexpected query: {query}")

    def create(frame, is_dataset=False):
        with monkeypatch.context() as context:
            context.setattr(upload, "getQuery", query)
            return upload.createNodes(
                frame, "SocioMap", is_dataset, "tester", uniqueID="importID",
            )

    return SimpleNamespace(nodes=created, create=create)


def test_new_categories_have_domains_before_background_processing(creation_graph):
    frame = pd.DataFrame([
        {"CMName": "Gandourou", "label": "PPL", "importID": 1},
        {"CMName": "Other locality", "label": "ADM0", "importID": 2},
        {"CMName": "Dialect", "label": "DIALECT", "importID": 3},
        {"CMName": "Area", "label": "AREA", "importID": 4},
    ])

    creation_graph.create(frame)

    assert creation_graph.nodes == {
        "SM1": {"CATEGORY", "PPL", "AREA"},
        "SM2": {"CATEGORY", "ADM0", "AREA"},
        "SM3": {"CATEGORY", "DIALECT", "LANGUAGE"},
        "SM4": {"CATEGORY", "AREA"},
    }
    assert frame.loc[0, "label"] == "PPL"


@pytest.mark.parametrize("label", ["DATASET", "STACK", "MERGING"])
def test_dataset_creation_keeps_dataset_labels(creation_graph, label):
    creation_graph.create(pd.DataFrame([{
        "CMName": "Source", "shortName": "SRC", "DatasetCitation": "Citation",
        "label": label, "importID": 1,
    }]), is_dataset=True)

    assert creation_graph.nodes["SM1"] == {label}


class ValidationComplete(Exception):
    """Stop the real upload pipeline after label validation and before writes."""


@pytest.fixture
def validation_graph(monkeypatch):
    graph = {}
    monkeypatch.setattr(upload, "getDriver", lambda _: object())
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)

    def stop_after_validation(*args, **kwargs):
        raise ValidationComplete

    monkeypatch.setattr(upload, "_validate_restricted_node_property_domains", stop_after_validation)

    def query(query, driver=None, params=None, **kwargs):
        if "MATCH (a) WHERE a.importID IS NOT NULL" in query:
            return []
        if "MATCH (p:PROPERTY)" in query:
            return []
        if "MATCH (l:LABEL) return l.CMName as label" in query:
            return list(LABEL_DOMAINS)
        if "WITH DISTINCT row.datasetID AS datasetID, row.Key AS keyValue" in query:
            return []
        if "COUNT(n) AS count" in query:
            return [{"value": row["value"], "count": 1} for row in params["rows"]]
        if "RETURN labelValue, n.groupLabel AS groupLabel" in query:
            return [{"labelValue": label, "groupLabel": LABEL_DOMAINS[label]}
                    for label in params["labels"]]
        if "nodeLabels AS labels" in query:
            return [{"cmid": cmid, "labels": list(graph[cmid]),
                     "groupLabels": [LABEL_DOMAINS[label] for label in graph[cmid]
                                     if label in LABEL_DOMAINS]}
                    for cmid in params["cmids"]]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(upload, "getQuery", query)
    return graph


def upload_ties(labels, cmid="SM23618"):
    return upload.input_Nodes_Uses([
        {"CMID": cmid, "Name": "Gandourou", "label": label,
         "datasetID": "SD2269", "Key": f"Name == Gandourou && year == {1987 + i}"}
        for i, label in enumerate(labels)
    ], "SocioMap", "add_uses", user="tester")


@pytest.mark.parametrize("existing_labels", [
    {"CATEGORY", "PPL"},
    {"CATEGORY", "ADM0"},
    {"CATEGORY", "PPL", "AREA"},
    {"CATEGORY", "AREA"},
    {"CATEGORY", "PPL", "DIALECT"},
])
def test_add_uses_accepts_matching_domain_without_reconciliation(validation_graph, existing_labels):
    validation_graph["SM23618"] = existing_labels

    with pytest.raises(ValidationComplete):
        upload_ties(["PPL"])


@pytest.mark.parametrize("existing_labels", [
    {"CATEGORY", "DIALECT"},
    {"CATEGORY"},
    {"CATEGORY", "UNKNOWN"},
])
def test_add_uses_still_rejects_incompatible_or_unknown_domain(validation_graph, existing_labels):
    validation_graph["SM23618"] = existing_labels

    with pytest.raises(ValueError, match="doesnt match.*SM23618"):
        upload_ties(["PPL"])


def test_add_uses_rejects_conflicting_domains_within_file(validation_graph):
    validation_graph["SM23618"] = {"CATEGORY", "PPL", "DIALECT"}

    with pytest.raises(ValueError, match="conflicting labels.*SM23618"):
        upload_ties(["PPL", "DIALECT"])


def test_add_uses_allows_different_subtypes_within_same_domain(validation_graph):
    validation_graph["SM23618"] = {"CATEGORY", "PPL"}

    with pytest.raises(ValidationComplete):
        upload_ties(["PPL", "ADM0"])


def test_add_uses_still_allows_blank_cmid_for_new_category(validation_graph):
    with pytest.raises(ValidationComplete):
        upload_ties(["PPL"], cmid="")


def test_consecutive_upload_can_use_new_category_without_background_processing(
    creation_graph, validation_graph,
):
    result = creation_graph.create(pd.DataFrame([
        {"CMName": "Gandourou", "label": "PPL", "importID": 1},
    ]))
    validation_graph.update(creation_graph.nodes)

    with pytest.raises(ValidationComplete):
        upload_ties(["PPL"], cmid=result.iloc[0]["CMID"])
