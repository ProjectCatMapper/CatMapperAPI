import CMroutes.admin_routes as admin_routes
import CM.admin as admin_module


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
    def run(self, query, *args, **kwargs):
        if "return properties(n) AS props" in query:
            return FakeCursor([{"props": {"Name": "Athens", "Key": "ATH", "ignore": "x"}}])
        if "MATCH (n {CMID: $cmid}) RETURN labels(n) AS labels" in query:
            return FakeCursor([{"labels": ["CATEGORY"]}])
        if "OPTIONAL MATCH (m:LABEL {CMName: label})" in query:
            params = args[0] if args else kwargs
            label_rows = []
            for label in params.get("labels", []):
                group = {
                    "AREA": "AREA",
                    "ADM0": "AREA",
                    "LANGUOID": "LANGUOID",
                    "LANGUAGE": "LANGUOID",
                }.get(label)
                label_rows.append({"label": label, "groupLabel": group})
            return FakeCursor(label_rows)
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
            return [{"p.CMName": "AREA"}, {"p.CMName": "ALL NODES"}, {"p.CMName": "LANGUOID"}]
        return FakeCursor([])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeDriver:
    def session(self):
        return FakeSession()


class FakeNoUsesSession(FakeSession):
    def __init__(self, labels):
        self._labels = labels

    def run(self, query, **kwargs):
        if "MATCH (n:CATEGORY)<-[r:USES]" in query:
            return FakeCursor([])
        if "MATCH (n {CMID: $cmid}) RETURN n.CMID AS CMID" in query:
            return FakeCursor([
                {
                    "CMID": kwargs.get("cmid"),
                    "CMName": "Example",
                    "labels": self._labels,
                }
            ])
        return super().run(query, **kwargs)


class FakeNoUsesDriver:
    def __init__(self, labels):
        self._labels = labels

    def session(self):
        return FakeNoUsesSession(self._labels)


class FakeDeletedNodePropertiesSession(FakeSession):
    def run(self, query, *args, **kwargs):
        if "return properties(n) AS props" in query:
            return FakeCursor([{"props": {"CMName": "Deleted", "CMID": "SD2183"}}])
        if "MATCH (n {CMID: $cmid}) RETURN labels(n) AS labels" in query:
            return FakeCursor([{"labels": ["DELETED"]}])
        return super().run(query, **kwargs)


class FakeDeletedNodePropertiesDriver:
    def session(self):
        return FakeDeletedNodePropertiesSession()


class FakeUsesPropertyFilterSession(FakeSession):
    def run(self, query, *args, **kwargs):
        if "MATCH (n:CATEGORY)<-[r:USES]" in query:
            return FakeCursor(
                [
                    {
                        "n": {
                            "CMName": "Language Category",
                            "CMID": "SM-LANG",
                            "elementId": "n-lang",
                            "labels": ["CATEGORY", "LANGUOID", "LANGUAGE"],
                        },
                        "r": FakeRelationship({"Key": "L"}, "rel-lang"),
                        "d": {"CMName": "Dataset A", "CMID": "SD1"},
                    }
                ]
            )
        if "p.type='relationship'" in query:
            return FakeCursor([
                {"property": "language", "groupLabel": None, "relationship": "LANGUOID_OF"},
                {"property": "polity", "groupLabel": None, "relationship": "POLITY_OF"},
                {"property": "district", "groupLabel": None, "relationship": "DISTRICT_OF"},
                {"property": "glottocode", "groupLabel": None, "relationship": None},
                {"property": "FIPS", "groupLabel": None, "relationship": None},
                {"property": "ISO3", "groupLabel": None, "relationship": None},
                {"property": "eventDate", "groupLabel": None, "relationship": None},
                {"property": "eventType", "groupLabel": None, "relationship": None},
                {"property": "latitude", "groupLabel": None, "relationship": None},
                {"property": "longitude", "groupLabel": None, "relationship": None},
                {"property": "mergeOnly", "groupLabel": None, "relationship": None, "reltype": ["MERGING"]},
                {"property": "mergeDelimited", "groupLabel": None, "relationship": None, "reltype": "USES||MERGING"},
                {"property": "source", "groupLabel": None, "relationship": None},
            ])
        return super().run(query, *args, **kwargs)


class FakeUsesPropertyFilterDriver:
    def session(self):
        return FakeUsesPropertyFilterSession()


class FakeUsesPropertyEthnicityFilterSession(FakeSession):
    def run(self, query, *args, **kwargs):
        if "MATCH (n:CATEGORY)<-[r:USES]" in query:
            return FakeCursor(
                [
                    {
                        "n": {
                            "CMName": "Ethnicity Category",
                            "CMID": "SM-ETH",
                            "elementId": "n-eth",
                            "labels": ["CATEGORY", "ETHNICITY"],
                        },
                        "r": FakeRelationship({"Key": "E"}, "rel-eth"),
                        "d": {"CMName": "Dataset A", "CMID": "SD1"},
                    }
                ]
            )
        if "p.type='relationship'" in query:
            return FakeCursor([
                {"property": "glottocode", "groupLabel": None, "relationship": None},
                {"property": "FIPS", "groupLabel": None, "relationship": None},
                {"property": "ISO2", "groupLabel": None, "relationship": None},
                {"property": "ISO3", "groupLabel": None, "relationship": None},
                {"property": "ISONumeric", "groupLabel": None, "relationship": None},
                {"property": "source", "groupLabel": None, "relationship": None},
            ])
        return super().run(query, *args, **kwargs)


class FakeUsesPropertyEthnicityFilterDriver:
    def session(self):
        return FakeUsesPropertyEthnicityFilterSession()


class FakeRestrictedNodePropertySession(FakeSession):
    def run(self, query, *args, **kwargs):
        if "return properties(n) AS props" in query:
            return FakeCursor([{"props": {"CMID": "SM-LANG", "CMName": "Lang"}}])
        if "MATCH (n {CMID: $cmid}) RETURN labels(n) AS labels" in query:
            return FakeCursor([{"labels": ["CATEGORY", "LANGUOID", "LANGUAGE"]}])
        if "p.type='node'" in query:
            return FakeCursor([
                {"property": "CMName"},
                {"property": "glottocode"},
                {"property": "FIPS"},
                {"property": "ISO3"},
            ])
        return super().run(query, *args, **kwargs)


class FakeRestrictedNodePropertyDriver:
    def session(self):
        return FakeRestrictedNodePropertySession()


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


def test_admin_nodeproperties_filters_restricted_identifier_fields(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: FakeRestrictedNodePropertyDriver())

    response = client.get(
        "/admin_add_edit_delete_nodeproperties",
        query_string={"CMID": "SM-LANG", "database": "SocioMap", "option": "add"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["error"] == ""
    assert "glottocode" in payload["r1"]
    assert "ISO3" in payload["r1"]
    assert "FIPS" not in payload["r1"]


def test_admin_nodeproperties_rejects_deleted_node(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: FakeDeletedNodePropertiesDriver())

    response = client.get(
        "/admin_add_edit_delete_nodeproperties",
        query_string={"CMID": "SD2183", "database": "SocioMap", "option": "add"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert "deleted node" in payload["error"]


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


def test_admin_usesproperties_hides_same_domain_contextual_props_except_district(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: FakeUsesPropertyFilterDriver())
    group_label_calls = []

    def fake_get_group_labels(cmid, driver):
        group_label_calls.append(cmid)
        return "LANGUOID"

    monkeypatch.setattr(admin_module, "getGroupLabels", fake_get_group_labels)

    response = client.get(
        "/admin_add_edit_delete_usesproperties",
        query_string={"CMID": "SM-LANG", "database": "SocioMap"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["error"] == ""
    assert "language" not in payload["r1"]
    assert "district" in payload["r1"]
    assert "polity" in payload["r1"]
    assert "glottocode" in payload["r1"]
    assert "ISO3" in payload["r1"]
    assert "FIPS" not in payload["r1"]
    assert "eventDate" not in payload["r1"]
    assert "eventType" not in payload["r1"]
    assert "latitude" not in payload["r1"]
    assert "longitude" not in payload["r1"]
    assert "mergeOnly" not in payload["r1"]
    assert "mergeDelimited" not in payload["r1"]
    assert "source" in payload["r1"]
    assert group_label_calls == ["SM-LANG"]


def test_admin_usesproperties_hides_restricted_identifiers_for_unrelated_domain(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: FakeUsesPropertyEthnicityFilterDriver())
    monkeypatch.setattr(admin_module, "getGroupLabels", lambda cmid, driver: "ETHNICITY")

    response = client.get(
        "/admin_add_edit_delete_usesproperties",
        query_string={"CMID": "SM-ETH", "database": "SocioMap"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["error"] == ""
    assert "source" in payload["r1"]
    assert "glottocode" not in payload["r1"]
    assert "FIPS" not in payload["r1"]
    assert "ISO2" not in payload["r1"]
    assert "ISO3" not in payload["r1"]
    assert "ISONumeric" not in payload["r1"]


def test_admin_usesproperties_rejects_dataset_cmid(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: FakeNoUsesDriver(["DATASET", "STACK"]))

    response = client.get(
        "/admin_add_edit_delete_usesproperties",
        query_string={"CMID": "SD2182", "database": "SocioMap"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert "Use add/edit/delete node property" in payload["error"]
    assert payload["r"] == []
    assert payload["r1"] == []


def test_admin_usesproperties_rejects_deleted_cmid(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: FakeNoUsesDriver(["DELETED"]))

    response = client.get(
        "/admin_add_edit_delete_usesproperties",
        query_string={"CMID": "SD2183", "database": "SocioMap"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert "deleted node" in payload["error"]
    assert payload["r"] == []
    assert payload["r1"] == []


def test_create_label_helper_excludes_internal_labels(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: FakeDriver())

    response = client.get("/create_label_helper", query_string={"database": "ArchaMap"})

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["res"] == ["AREA", "LANGUOID"]


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


def test_merge_usesties_combines_lists_and_keeps_matching_scalars(monkeypatch):
    captured = {}

    monkeypatch.setattr(admin_module, "getDriver", lambda database: f"driver-{database}")

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "RETURN elementId(r) AS relID, properties(r) AS props" in query:
            return [
                {
                    "relID": "rel-1",
                    "props": {
                        "Key": "Name == Bolen Side Notch",
                        "recordStart": "2025",
                        "source": ["A", "B"],
                    },
                },
                {
                    "relID": "rel-2",
                    "props": {
                        "Key": "Name == Bolen Side Notch",
                        "recordStart": "2025",
                        "source": ["B", "C"],
                        "recordEnd": None,
                    },
                },
            ]
        if "SET keep = $mergedProps" in query:
            captured["params"] = params
            return [{"relID": "rel-1", "originalCount": 2, "mergedCount": 1}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin_module, "getQuery", fake_get_query)

    result = admin_module.mergeUSESties(
        "ArchaMap",
        "AM354486",
        "Name == Bolen Side Notch",
        "AD354481",
    )

    assert result["originalCount"] == 2
    merged_props = captured["params"]["mergedProps"]
    assert merged_props["Key"] == "Name == Bolen Side Notch"
    assert merged_props["recordStart"] == "2025"
    assert merged_props["source"] == ["A", "B", "C"]
    assert "recordEnd" not in merged_props


def test_merge_usesties_rejects_conflicting_scalar_values(monkeypatch):
    monkeypatch.setattr(admin_module, "getDriver", lambda database: f"driver-{database}")

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "RETURN elementId(r) AS relID, properties(r) AS props" in query:
            return [
                {
                    "relID": "rel-1",
                    "props": {
                        "Key": "Name == Bolen Side Notch",
                        "recordStart": "2025",
                    },
                },
                {
                    "relID": "rel-2",
                    "props": {
                        "Key": "Name == Bolen Side Notch",
                        "recordStart": "2024",
                    },
                },
            ]
        if "SET keep = $mergedProps" in query:
            raise AssertionError("Conflicting values should abort before merge query")
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin_module, "getQuery", fake_get_query)

    try:
        admin_module.mergeUSESties(
            "ArchaMap",
            "AM354486",
            "Name == Bolen Side Notch",
            "AD354481",
        )
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("Expected conflicting scalar values to raise ValueError")

    assert "recordStart" in message
    assert "rel-1: '2025'" in message
    assert "rel-2: '2024'" in message
    assert "AM354486" in message
    assert "AD354481" in message


def test_merge_usesties_route_accepts_selected_rows(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "200", "role": "admin"})

    calls = []

    def fake_merge(database, CMID, Key, datasetID):
        calls.append({"database": database, "CMID": CMID, "Key": Key, "datasetID": datasetID})
        return {"CMID": CMID, "Key": Key, "datasetID": datasetID, "mergedCount": 1}

    monkeypatch.setattr(admin_routes, "mergeUSESties", fake_merge)

    response = client.post(
        "/mergeUSESties",
        headers={"Authorization": "Bearer test-token"},
        json={
            "database": "ArchaMap",
            "rows": [
                {
                    "CMID": "AM354486",
                    "Key": "Name == Bolen Side Notch",
                    "datasetID": "AD354481",
                }
            ],
        },
    )

    assert response.status_code == 200
    assert response.get_json()["count"] == 1
    assert calls == [
        {
            "database": "ArchaMap",
            "CMID": "AM354486",
            "Key": "Name == Bolen Side Notch",
            "datasetID": "AD354481",
        }
    ]


def test_merge_usesties_route_returns_informative_row_failures(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "200", "role": "admin"})

    def fake_merge(database, CMID, Key, datasetID):
        error = ValueError(
            "Cannot merge duplicate USES ties for "
            f"CMID {CMID}, Key {Key}, datasetID {datasetID}. "
            "Conflicting scalar properties: property recordStart has values [rel-1: '2025'; rel-2: '2024']"
        )
        error.details = {
            "CMID": CMID,
            "Key": Key,
            "datasetID": datasetID,
            "conflicts": [
                {
                    "property": "recordStart",
                    "values": [
                        {"relID": "rel-1", "value": "2025"},
                        {"relID": "rel-2", "value": "2024"},
                    ],
                }
            ],
        }
        raise error

    monkeypatch.setattr(admin_routes, "mergeUSESties", fake_merge)

    response = client.post(
        "/mergeUSESties",
        headers={"Authorization": "Bearer test-token"},
        json={
            "database": "ArchaMap",
            "rows": [
                {
                    "CMID": "AM354486",
                    "Key": "Name == Bolen Side Notch",
                    "datasetID": "AD354481",
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is False
    assert payload["count"] == 0
    assert payload["merged"] == []
    assert payload["failed"][0]["CMID"] == "AM354486"
    assert payload["failed"][0]["Key"] == "Name == Bolen Side Notch"
    assert payload["failed"][0]["datasetID"] == "AD354481"
    assert payload["failed"][0]["details"]["conflicts"][0]["property"] == "recordStart"
    assert payload["failed"][0]["details"]["conflicts"][0]["values"] == [
        {"relID": "rel-1", "value": "2025"},
        {"relID": "rel-2", "value": "2024"},
    ]
