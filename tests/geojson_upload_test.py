import json

import pytest

from CM import geojson_upload


def _feature(*, cmid="AM1", key="Code == 1", dataset_id="AD1", geometry=None):
    return {
        "type": "Feature",
        "properties": {"CMID": cmid, "Key": key, "datasetID": dataset_id},
        "geometry": geometry or {
            "type": "Polygon",
            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]],
        },
    }


def _write_geojson(tmp_path, features):
    path = tmp_path / "polygons.geojson"
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}), encoding="utf-8")
    return path


def test_prepare_geojson_builds_stable_content_addressed_records(tmp_path):
    path = _write_geojson(tmp_path, [_feature()])
    first = geojson_upload.prepare_geojson(path, "archamap")
    second = geojson_upload.prepare_geojson(path, "ArchaMap")
    assert first.digest == second.digest
    assert first.records[0]["geomID"] == second.records[0]["geomID"]
    assert first.records[0]["geomID"].startswith("geo_archamap_AD1_")
    assert first.records[0]["coordinateCount"] == 4
    assert json.loads(first.records[0]["geometry"])["type"] == "Polygon"


@pytest.mark.parametrize(
    "feature,code",
    [
        (_feature(key="broken"), "invalid_key"),
        (_feature(dataset_id="SD1"), "database_mismatch"),
        (_feature(geometry={"type": "Point", "coordinates": [0, 0]}), "unsupported_geometry"),
        (_feature(geometry={"type": "Polygon", "coordinates": [[[0, 0], [2, 2], [0, 2], [2, 0], [0, 0]]]}), "invalid_geometry"),
    ],
)
def test_prepare_geojson_returns_feature_specific_errors(tmp_path, feature, code):
    path = _write_geojson(tmp_path, [feature])
    with pytest.raises(geojson_upload.GeoJSONUploadError) as exc:
        geojson_upload.prepare_geojson(path, "archamap")
    assert code in {detail["code"] for detail in exc.value.details}
    assert exc.value.details[0]["feature"] == 1


def test_prepare_geojson_rejects_duplicate_triplets(tmp_path):
    path = _write_geojson(tmp_path, [_feature(), _feature()])
    with pytest.raises(geojson_upload.GeoJSONUploadError) as exc:
        geojson_upload.prepare_geojson(path, "archamap")
    assert any(detail["code"] == "duplicate_triplet" and detail["feature"] == 2 for detail in exc.value.details)


def test_preflight_requires_exact_uses_and_reuses_ownership_guard(tmp_path, monkeypatch):
    path = _write_geojson(tmp_path, [_feature()])
    ownership_rows = []
    monkeypatch.setattr(geojson_upload, "getDriver", lambda database: database)

    def fake_query(query, driver=None, params=None, type="dict", **_kwargs):
        if "count(p) AS count" in query:
            return [{"count": 1}]
        if "keyedRels" in query:
            return [{"feature": 1, "datasetCount": 1, "categoryCount": 1, "keyedRels": [{"targetCMID": "AM1", "relID": "rel-1", "geoPolygon": None}]}]
        if "geometryExists" in query:
            return [{"feature": 1, "geometryExists": False}]
        raise AssertionError(query)

    monkeypatch.setattr(geojson_upload, "getQuery", fake_query)
    monkeypatch.setattr(geojson_upload, "assert_owned_uses_by_triplets", lambda database, rows, claims: ownership_rows.extend(rows))
    prepared, report = geojson_upload.preflight_geojson(path, "archamap", {"userid": "admin", "role": "admin"})
    assert report["valid"] is True
    assert report["newGeometryCount"] == 1
    assert ownership_rows[0]["CMID"] == "AM1"
    assert prepared.records[0]["oldGeoPolygon"] is None


def test_preflight_blocks_existing_polygon_without_explicit_replace(tmp_path, monkeypatch):
    path = _write_geojson(tmp_path, [_feature()])
    monkeypatch.setattr(geojson_upload, "getDriver", lambda database: database)

    def fake_query(query, **_kwargs):
        if "count(p) AS count" in query:
            return [{"count": 1}]
        if "keyedRels" in query:
            return [{"feature": 1, "datasetCount": 1, "categoryCount": 1, "keyedRels": [{"targetCMID": "AM1", "relID": "rel-1", "geoPolygon": "old"}]}]
        if "geometryExists" in query:
            return [{"feature": 1, "geometryExists": False}]
        raise AssertionError(query)

    monkeypatch.setattr(geojson_upload, "getQuery", fake_query)
    monkeypatch.setattr(geojson_upload, "assert_owned_uses_by_triplets", lambda *_args, **_kwargs: True)
    with pytest.raises(geojson_upload.GeoJSONUploadError) as exc:
        geojson_upload.preflight_geojson(path, "archamap", {"userid": "1", "role": "admin"})
    assert any(detail["code"] == "existing_polygon" for detail in exc.value.details)


def test_apply_restores_uses_and_removes_pending_geometry_when_verify_fails(monkeypatch, tmp_path):
    path = _write_geojson(tmp_path, [_feature()])
    records = [{"feature": 1, "CMID": "AM1", "Key": "Code == 1", "datasetID": "AD1", "database": "archamap", "geomID": "geo-1", "geometry": "{}", "geometryChecksum": "checksum", "geometryExists": False, "oldGeoPolygon": "old-geo"}]
    prepared = geojson_upload.PreparedGeoJSON(records, "digest", 10, 4)
    calls = []
    monkeypatch.setattr(geojson_upload, "preflight_geojson", lambda *_args, **_kwargs: (prepared, {"featureCount": 1}))
    monkeypatch.setattr(geojson_upload, "_stage_geometry", lambda *_args: ["geo-1"])
    monkeypatch.setattr(geojson_upload, "_run_target_transaction", lambda *_args: calls.append("target"))
    monkeypatch.setattr(geojson_upload, "_verify", lambda *_args: (_ for _ in ()).throw(RuntimeError("verify failed")))
    monkeypatch.setattr(geojson_upload, "_restore_target", lambda *_args: calls.append("restore"))
    monkeypatch.setattr(geojson_upload, "_delete_upload_geometry", lambda *_args: calls.append("delete"))
    with pytest.raises(RuntimeError, match="verify failed"):
        geojson_upload.apply_geojson_upload(path, "archamap", {"userid": "1", "role": "admin"}, expected_digest="digest", replace_existing=True, upload_id="upload-1")
    assert calls == ["target", "restore", "delete"]
