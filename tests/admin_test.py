import CMroutes.admin_routes as admin_routes


class FakeRelationship:
    def __init__(self, props, element_id):
        self._props = props
        self.element_id = element_id

    def items(self):
        return self._props.items()


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def data(self):
        return self._rows

    def __iter__(self):
        return iter(self._rows)


class FakeSession:
    def run(self, query, **kwargs):
        if "return properties(n) AS props" in query:
            return FakeCursor([{"props": {"Name": "Athens", "Key": "ATH", "ignore": "x"}}])
        if "p.type='node'" in query:
            return FakeCursor([{"property": "Name"}, {"property": "Key"}, {"property": "label"}])
        if "MATCH (n:CATEGORY)<-[r:USES]" in query:
            return FakeCursor(
                [
                    {
                        "n": {"CMName": "Domain", "CMID": "AM1", "elementId": "n1"},
                        "r": FakeRelationship({"Key": "A"}, "rel-1"),
                        "d": {"CMName": "Dataset A", "CMID": "D1"},
                    }
                ]
            )
        if "p.type='relationship'" in query:
            return FakeCursor([{"property": "Key"}, {"property": "year"}])
        if "MATCH (p:LABEL)" in query:
            return [{"p.CMName": "DISTRICT"}, {"p.CMName": "ALL NODES"}, {"p.CMName": "LANGUOID"}]
        return FakeCursor([])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeDriver:
    def session(self):
        return FakeSession()


def test_admin_nodeproperties_returns_filtered_fields(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: FakeDriver())

    response = client.get(
        "/admin_add_edit_delete_nodeproperties",
        query_string={"CMID": "AM256471", "database": "ArchaMap"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["error"] == ""
    assert payload["r"] == {"Key": "ATH", "Name": "Athens"}
    assert "label" in payload["r1"]


def test_admin_usesproperties_returns_records_and_allowed_props(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: FakeDriver())

    response = client.get(
        "/admin_add_edit_delete_usesproperties",
        query_string={"CMID": "AM256471", "database": "ArchaMap"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["error"] == ""
    assert len(payload["r"]) == 1
    assert set(payload["r1"]) == {"Key", "year"}


def test_create_label_helper_excludes_internal_labels(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: FakeDriver())

    response = client.get("/create_label_helper", query_string={"database": "ArchaMap"})

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["res"] == ["DISTRICT", "LANGUOID"]


def test_create_metadata_node_creates_in_both_databases(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "200", "role": "admin"})
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: f"driver-{database}")

    created_by_driver = []
    indexed_by_driver = []

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "WHERE n.CMID STARTS WITH $prefix RETURN n.CMID AS CMID" in query:
            if driver == "driver-sociomap":
                return [{"CMID": "CL200"}]
            if driver == "driver-archamap":
                return [{"CMID": "CL250"}]
            return []
        if "RETURN count(n) AS count" in query:
            return [{"count": 0}]
        if "CREATE (n:METADATA:LABEL)" in query:
            created_by_driver.append(driver)
            props = (params or {}).get("props", {})
            return [{
                "id": f"id-{driver}",
                "labels": ["METADATA", "LABEL"],
                "props": props,
            }]
        if "CREATE FULLTEXT INDEX New_Test_Label" in query:
            indexed_by_driver.append(driver)
            return []
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin_routes, "getQuery", fake_get_query)

    response = client.post(
        "/admin/metadata/create",
        headers={"Authorization": "Bearer test-token"},
        json={
            "CMName": "New_Test_Label",
            "nodeLabel": "LABEL",
            "properties": {
                "groupLabel": "FAMILY",
                "displayName": "Family Label",
            },
            "databaseTarget": "both",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["generatedCMID"] == "CL251"
    assert payload["createdIn"] == ["SocioMap", "ArchaMap"]
    assert set(created_by_driver) == {"driver-sociomap", "driver-archamap"}
    assert set(indexed_by_driver) == {"driver-sociomap", "driver-archamap"}
    assert payload["node"]["SocioMap"]["props"]["CMID"] == "CL251"
    assert payload["node"]["SocioMap"]["props"]["groupLabel"] == "FAMILY"
    assert payload["node"]["SocioMap"]["props"]["displayName"] == "Family Label"


def test_create_metadata_node_does_not_create_domain_index_for_non_label_nodes(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "200", "role": "admin"})
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: f"driver-{database}")

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "WHERE n.CMID STARTS WITH $prefix RETURN n.CMID AS CMID" in query:
            return [{"CMID": "CP10"}]
        if "RETURN count(n) AS count" in query:
            return [{"count": 0}]
        if "CREATE (n:METADATA:PROPERTY)" in query:
            return [{
                "id": f"id-{driver}",
                "labels": ["METADATA", "PROPERTY"],
                "props": (params or {}).get("props", {}),
            }]
        if "CREATE FULLTEXT INDEX" in query:
            raise AssertionError("Non-LABEL metadata creation should not create a full-text index")
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin_routes, "getQuery", fake_get_query)

    response = client.post(
        "/admin/metadata/create",
        headers={"Authorization": "Bearer test-token"},
        json={
            "CMName": "new_property",
            "nodeLabel": "PROPERTY",
            "databaseTarget": "archamap",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["generatedCMID"] == "CP11"
    assert payload["createdIn"] == ["ArchaMap"]


def test_create_metadata_node_rejects_invalid_node_label(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "200", "role": "admin"})
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: f"driver-{database}")

    response = client.post(
        "/admin/metadata/create",
        headers={"Authorization": "Bearer test-token"},
        json={
            "CMName": "Duplicate Label",
            "nodeLabel": "DOMAIN",
            "databaseTarget": "both",
        },
    )

    assert response.status_code == 400
    payload = response.get_json()
    assert "nodelabel must be one of" in payload["error"].lower()


def test_metadata_properties_by_label_returns_distinct_properties(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "200", "role": "admin"})
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: f"driver-{database}")

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        assert "MATCH (n:METADATA:LABEL)" in query
        if driver == "driver-sociomap":
            return [{"prop": "CMID"}, {"prop": "CMName"}, {"prop": "groupLabel"}, {"prop": "color"}]
        if driver == "driver-archamap":
            return [{"prop": "description"}, {"prop": "groupLabel"}]
        return []

    monkeypatch.setattr(admin_routes, "getQuery", fake_get_query)

    response = client.get(
        "/admin/metadata/properties/LABEL",
        headers={"Authorization": "Bearer test-token"},
        query_string={"databaseTarget": "both"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["nodeLabel"] == "LABEL"
    assert payload["properties"] == ["CMID", "CMName", "color", "description", "groupLabel"]
