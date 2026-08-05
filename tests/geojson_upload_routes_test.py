import io

from CM.geojson_upload import GeoJSONUploadError, PreparedGeoJSON
from CMroutes import geojson_upload_routes


def test_polygon_preflight_accepts_multipart_and_returns_token(client, monkeypatch, tmp_path):
    claims = {"userid": "admin", "role": "admin"}
    monkeypatch.setattr(geojson_upload_routes, "verify_request_auth", lambda **_kwargs: claims)

    def fake_save(_storage):
        path = tmp_path / "incoming.geojson"
        path.write_text("{}", encoding="utf-8")
        return path

    monkeypatch.setattr(geojson_upload_routes, "_save_bounded_upload", fake_save)
    report = {"valid": True, "database": "archamap", "featureCount": 1, "byteCount": 2, "coordinateCount": 4, "existingPolygonCount": 0, "newGeometryCount": 1, "reusedGeometryCount": 0, "digest": "abc", "warnings": []}
    monkeypatch.setattr(geojson_upload_routes, "preflight_geojson", lambda *_args, **_kwargs: (PreparedGeoJSON([{"feature": 1}], "abc", 2, 4), report))
    monkeypatch.setattr(geojson_upload_routes, "create_preflight_token", lambda *_args, **_kwargs: "a" * 32)
    response = client.post("/api/uploads/geojson/polygons/preflight", data={"database": "archamap", "replaceExisting": "false", "file": (io.BytesIO(b"{}"), "polygons.geojson")}, content_type="multipart/form-data")
    assert response.status_code == 200
    assert response.get_json()["token"] == "a" * 32
    assert response.get_json()["featureCount"] == 1


def test_polygon_preflight_returns_structured_validation_errors(client, monkeypatch, tmp_path):
    monkeypatch.setattr(geojson_upload_routes, "verify_request_auth", lambda **_kwargs: {"userid": "admin", "role": "admin"})
    path = tmp_path / "incoming.geojson"
    path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(geojson_upload_routes, "_save_bounded_upload", lambda _storage: path)
    monkeypatch.setattr(geojson_upload_routes, "preflight_geojson", lambda *_args, **_kwargs: (_ for _ in ()).throw(GeoJSONUploadError("failed", [{"feature": 2, "field": "CMID", "code": "required", "message": "missing"}])))
    response = client.post("/api/uploads/geojson/polygons/preflight", data={"database": "archamap", "file": (io.BytesIO(b"{}"), "bad.geojson")}, content_type="multipart/form-data")
    assert response.status_code == 422
    assert response.get_json()["error_details"][0]["feature"] == 2
