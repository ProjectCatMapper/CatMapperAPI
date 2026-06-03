import CMroutes.routine_routes as routine_routes


def test_routines_is_valid_json_wraps_boolean_result(client, monkeypatch):
    monkeypatch.setattr(
        routine_routes.routines_module,
        "is_valid_json",
        lambda value: True if value == '{"ok":1}' else False,
    )

    response = client.get(
        "/routines/is_valid_json/ArchaMap",
        query_string={"value": '{"ok":1}'},
    )

    assert response.status_code == 200
    assert response.is_json
    assert response.get_json() == {"result": True}


def test_routines_updateuses_runs_full_database_when_cmid_not_provided(client, monkeypatch):
    captured = {}

    def fake_update_uses(database, CMID=None, user="0", detailed=False):
        captured["database"] = database
        captured["CMID"] = CMID
        captured["detailed"] = detailed
        return "ok-full"

    monkeypatch.setattr(routine_routes.routines_module, "updateUSES", fake_update_uses)
    monkeypatch.setattr(routine_routes.uses_module, "updateUSES", fake_update_uses)

    response = client.get("/routines/updateUSES/sociomap")

    assert response.status_code == 200
    assert response.get_data(as_text=True) == "ok-full"
    assert captured == {"database": "sociomap", "CMID": None, "detailed": False}


def test_routines_updateuses_runs_single_cmid_when_provided(client, monkeypatch):
    captured = {}

    def fake_update_uses(database, CMID=None, user="0", detailed=False):
        captured["database"] = database
        captured["CMID"] = CMID
        return "ok-single"

    monkeypatch.setattr(routine_routes.routines_module, "updateUSES", fake_update_uses)
    monkeypatch.setattr(routine_routes.uses_module, "updateUSES", fake_update_uses)

    response = client.get(
        "/routines/updateUSES/archamap",
        query_string={"CMID": "AM123"},
    )

    assert response.status_code == 200
    assert response.get_data(as_text=True) == "ok-single"
    assert captured == {"database": "archamap", "CMID": "AM123"}


def test_routines_admin_endpoint_does_not_pass_mail_object(client, monkeypatch):
    captured = {}

    def fake_routine(database, mail=None, return_type="info", send_email=True):
        captured["database"] = database
        captured["mail"] = mail
        captured["return_type"] = return_type
        captured["send_email"] = send_email
        return "ok-no-mail"

    monkeypatch.setattr(routine_routes.routines_module, "fakeRoutine", fake_routine, raising=False)
    monkeypatch.setattr(routine_routes, "mail", object())

    response = client.get(
        "/routines/fakeRoutine/archamap",
        query_string={"mail": "force-mail", "return_type": "data"},
    )

    assert response.status_code == 200
    assert response.get_data(as_text=True) == "ok-no-mail"
    assert captured == {
        "database": "archamap",
        "mail": None,
        "return_type": "data",
        "send_email": False,
    }


def test_runRoutines_includes_duplicate_node_cmid_check(client, monkeypatch):
    called = []

    def fake_info(name):
        def _fake(*args, **kwargs):
            called.append(name)
            return {"info": f"{name}-ok", "filepath": None}
        return _fake

    routine_names = [
        "reportChanges",
        "checkDomains",
        "getBadDomains",
        "getBadCMID",
        "getDuplicateNodeCMIDs",
        "getMultipleLabels",
        "getBadComplexProperties",
        "getBadRelations",
        "CMNameNotInName",
        "missingCMName",
        "getBadContextual",
        "noUSES",
        "checkUSES",
        "get_duplicate_empty_USES",
        "get_duplicate_triplets",
        "processUSES",
        "getInappropriateprops_Nodes_Rels",
        "get_empty_nodeprops",
        "get_label_check",
        "getNumeric_Checks",
        "processDATASETs",
        "fixMetaTypes",
    ]

    for name in routine_names:
        monkeypatch.setattr(routine_routes.routines_module, name, fake_info(name))

    response = client.get("/runRoutines/TestDB")
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Duplicate CMIDs including deleted for TestDB" in body
    assert "getDuplicateNodeCMIDs-ok" in body
    assert "getDuplicateNodeCMIDs" in called
