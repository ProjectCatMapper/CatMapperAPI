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

