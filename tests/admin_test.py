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
