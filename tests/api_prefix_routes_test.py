import pandas as pd

import CMroutes.admin_routes as admin_routes
import CMroutes.metadata_routes as metadata_routes


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def data(self):
        return self._rows


class FakeSession:
    def run(self, query, **kwargs):
        if "RETURN labels(n) AS labels" in query:
            return FakeCursor([{"labels": ["CATEGORY", "AREA"]}])
        if "return properties(n) AS props" in query:
            return FakeCursor([{"props": {"CMName": "Feature ID", "description": "Identifier"}}])
        if "p.type='node'" in query:
            return FakeCursor([{"property": "CMName"}, {"property": "description"}])
        raise AssertionError(f"Unexpected query: {query}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeDriver:
    def session(self):
        return FakeSession()


def test_api_prefix_serves_translate_domain_selector(client, monkeypatch):
    monkeypatch.setattr(
        metadata_routes,
        "get_metadata_groups",
        lambda database: [{"domain": "ANY DOMAIN", "labels": ["AREA"]}],
    )

    response = client.get("/api/getTranslatedomains", query_string={"database": "ArchaMap"})

    assert response.status_code == 200
    assert response.get_json() == [{"domain": "ANY DOMAIN", "labels": ["AREA"]}]


def test_api_prefix_serves_main_domain_selector(client, monkeypatch):
    monkeypatch.setattr(metadata_routes, "getDriver", lambda database: object())

    def fake_get_query(query, driver, type="df", **kwargs):
        if "g.displayOrder IS NOT NULL" in query:
            return pd.DataFrame([
                {"domain": "AREA", "display": "Area", "order": 1},
            ])
        return pd.DataFrame([
            {
                "domain": "AREA",
                "subdomain": "ADM0",
                "subdisplay": "Country",
                "description": "Country",
                "suborder": 1,
            },
        ])

    monkeypatch.setattr(metadata_routes, "getQuery", fake_get_query)

    response = client.get("/api/getDomains/archamap")

    assert response.status_code == 200
    assert response.get_json() == [
        {
            "domain": "AREA",
            "display": "Area",
            "order": 1,
            "subdomain": "ADM0",
            "subdisplay": "Country",
            "description": "Country",
            "suborder": 1,
        }
    ]


def test_api_prefix_serves_admin_edit_node_property_names(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: FakeDriver())
    monkeypatch.setattr(
        admin_routes,
        "verify_request_auth",
        lambda **kwargs: {"userid": "200", "role": "admin"},
    )

    response = client.get(
        "/api/admin_add_edit_delete_nodeproperties",
        headers={"Authorization": "Bearer test-token"},
        query_string={"CMID": "AM1", "database": "ArchaMap", "option": "edit"},
    )

    assert response.status_code == 200
    assert response.get_json() == {
        "r": {"CMName": "Feature ID", "description": "Identifier"},
        "r1": [],
        "error": "",
    }
