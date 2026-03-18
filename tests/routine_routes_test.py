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
