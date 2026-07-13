"""Validated, compensating GeoJSON polygon uploads for existing USES ties."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from shapely.geometry import mapping, shape
from shapely.validation import explain_validity

from .keys import key_format_issue
from .ownership import assert_owned_uses_by_triplets, normalize_actor_claims
from .utils import getDriver, getQuery


REQUIRED_PROPERTIES = ("CMID", "Key", "datasetID")
ALLOWED_DATABASES = {"sociomap": "SD", "archamap": "AD"}
TOKEN_PATTERN = re.compile(r"^[a-f0-9]{32}$")
UPLOAD_ROOT = Path(
    os.getenv(
        "CATMAPPER_GEOJSON_UPLOAD_DIR",
        str(Path(__file__).resolve().parents[1] / "tmp" / "geojson_uploads"),
    )
)
MAX_BYTES = int(os.getenv("CATMAPPER_GEOJSON_MAX_BYTES", str(50 * 1024 * 1024)))
MAX_FEATURES = int(os.getenv("CATMAPPER_GEOJSON_MAX_FEATURES", "10000"))
MAX_COORDINATES = int(os.getenv("CATMAPPER_GEOJSON_MAX_COORDINATES", "2000000"))
TOKEN_TTL_SECONDS = int(os.getenv("CATMAPPER_GEOJSON_TOKEN_TTL_SECONDS", "3600"))


class GeoJSONUploadError(ValueError):
    def __init__(self, message: str, details: list[dict[str, Any]] | None = None):
        super().__init__(message)
        self.details = details or []


@dataclass(frozen=True)
class PreparedGeoJSON:
    records: list[dict[str, Any]]
    digest: str
    byte_count: int
    coordinate_count: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_database(database: Any) -> str:
    value = str(database or "").strip().lower()
    if value not in ALLOWED_DATABASES:
        raise GeoJSONUploadError("Database must be either sociomap or archamap.")
    return value


def _detail(feature: int | None, field: str, code: str, message: str) -> dict[str, Any]:
    return {"feature": feature, "field": field, "code": code, "message": message}


def _count_coordinates(value: Any) -> int:
    if not isinstance(value, list):
        return 0
    if value and all(isinstance(item, (int, float)) and not isinstance(item, bool) for item in value):
        return 1
    return sum(_count_coordinates(item) for item in value)


def _coordinates_are_finite_and_bounded(value: Any) -> bool:
    if not isinstance(value, list):
        return False
    if value and all(isinstance(item, (int, float)) and not isinstance(item, bool) for item in value):
        if len(value) < 2 or not all(math.isfinite(float(item)) for item in value):
            return False
        return -180 <= float(value[0]) <= 180 and -90 <= float(value[1]) <= 90
    return bool(value) and all(_coordinates_are_finite_and_bounded(item) for item in value)


def _canonical_geometry(geometry: dict[str, Any]) -> tuple[str, str]:
    text = json.dumps(geometry, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()


def _geometry_id(database: str, dataset_id: str, cmid: str, key: str, checksum: str) -> str:
    identity = "\0".join((cmid, key, checksum)).encode("utf-8")
    suffix = hashlib.sha256(identity).hexdigest()[:24]
    return f"geo_{database}_{dataset_id}_{suffix}"


def prepare_geojson(path: str | Path, database: str) -> PreparedGeoJSON:
    database = normalize_database(database)
    path = Path(path)
    raw = path.read_bytes()
    if not raw:
        raise GeoJSONUploadError("The uploaded GeoJSON file is empty.")
    if len(raw) > MAX_BYTES:
        raise GeoJSONUploadError(f"GeoJSON exceeds the {MAX_BYTES} byte upload limit.")

    digest = hashlib.sha256(raw).hexdigest()
    try:
        document = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GeoJSONUploadError(f"Invalid GeoJSON JSON: {exc}") from exc

    if not isinstance(document, dict) or document.get("type") != "FeatureCollection":
        raise GeoJSONUploadError("GeoJSON root must be a FeatureCollection.")
    if "crs" in document:
        raise GeoJSONUploadError(
            "GeoJSON must use RFC 7946 WGS84 longitude/latitude coordinates and must not include a crs member."
        )

    features = document.get("features")
    if not isinstance(features, list) or not features:
        raise GeoJSONUploadError("GeoJSON FeatureCollection must contain at least one feature.")
    if len(features) > MAX_FEATURES:
        raise GeoJSONUploadError(f"GeoJSON exceeds the {MAX_FEATURES} feature limit.")

    details: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    seen_triplets: dict[tuple[str, str, str], int] = {}
    total_coordinates = 0
    required_prefix = ALLOWED_DATABASES[database]

    for index, feature in enumerate(features, start=1):
        if not isinstance(feature, dict) or feature.get("type") != "Feature":
            details.append(_detail(index, "feature", "invalid_feature", "Item is not a GeoJSON Feature."))
            continue

        properties = feature.get("properties")
        if not isinstance(properties, dict):
            details.append(_detail(index, "properties", "missing_properties", "Feature properties must be an object."))
            continue

        values: dict[str, str] = {}
        for field in REQUIRED_PROPERTIES:
            raw_value = properties.get(field)
            if not isinstance(raw_value, str) or not raw_value.strip():
                details.append(
                    _detail(index, field, "required", f"Feature property {field} must be a nonblank string.")
                )
            else:
                values[field] = raw_value.strip()
        if len(values) != len(REQUIRED_PROPERTIES):
            continue

        key_issue = key_format_issue(values["Key"])
        if key_issue:
            details.append(_detail(index, "Key", "invalid_key", key_issue))
        if not values["datasetID"].upper().startswith(required_prefix):
            details.append(
                _detail(
                    index,
                    "datasetID",
                    "database_mismatch",
                    f"datasetID must begin with {required_prefix} for {database}.",
                )
            )

        triplet = (values["datasetID"], values["CMID"], values["Key"])
        if triplet in seen_triplets:
            details.append(
                _detail(
                    index,
                    "properties",
                    "duplicate_triplet",
                    f"Duplicate datasetID/CMID/Key triplet; first used by feature {seen_triplets[triplet]}. Use one MultiPolygon feature.",
                )
            )
        else:
            seen_triplets[triplet] = index

        geometry = feature.get("geometry")
        if not isinstance(geometry, dict) or geometry.get("type") not in {"Polygon", "MultiPolygon"}:
            details.append(
                _detail(index, "geometry", "unsupported_geometry", "Geometry must be Polygon or MultiPolygon.")
            )
            continue
        coordinates = geometry.get("coordinates")
        coordinate_count = _count_coordinates(coordinates)
        total_coordinates += coordinate_count
        if not _coordinates_are_finite_and_bounded(coordinates):
            details.append(
                _detail(
                    index,
                    "geometry",
                    "invalid_coordinates",
                    "Coordinates must be finite WGS84 longitude/latitude values within [-180, 180] and [-90, 90].",
                )
            )
            continue

        try:
            parsed_geometry = shape(geometry)
        except Exception as exc:
            details.append(_detail(index, "geometry", "invalid_geometry", str(exc)))
            continue
        if parsed_geometry.is_empty or not parsed_geometry.is_valid:
            details.append(
                _detail(index, "geometry", "invalid_geometry", explain_validity(parsed_geometry))
            )
            continue

        # Shapely normalizes numeric tuples without repairing invalid topology.
        normalized_geometry = mapping(parsed_geometry)
        geometry_text, checksum = _canonical_geometry(normalized_geometry)
        records.append(
            {
                "feature": index,
                "CMID": values["CMID"],
                "Key": values["Key"],
                "datasetID": values["datasetID"],
                "geomID": _geometry_id(
                    database, values["datasetID"], values["CMID"], values["Key"], checksum
                ),
                "geometry": geometry_text,
                "geometryChecksum": checksum,
                "coordinateCount": coordinate_count,
            }
        )

    if total_coordinates > MAX_COORDINATES:
        details.append(
            _detail(
                None,
                "geometry",
                "coordinate_limit",
                f"GeoJSON contains {total_coordinates} coordinates; limit is {MAX_COORDINATES}.",
            )
        )
    if details:
        raise GeoJSONUploadError("GeoJSON validation failed.", details)
    return PreparedGeoJSON(records, digest, len(raw), total_coordinates)


def _target_preflight(database: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    driver = getDriver(database)
    metadata = getQuery(
        "OPTIONAL MATCH (p:PROPERTY {CMName: 'geoPolygon'}) RETURN count(p) AS count",
        driver=driver,
        type="dict",
    )
    if not metadata or int(metadata[0].get("count") or 0) != 1:
        return [_detail(None, "geoPolygon", "metadata_missing", "Database must have exactly one geoPolygon PROPERTY definition.")]

    rows = getQuery(
        """
        UNWIND $rows AS row
        OPTIONAL MATCH (d:DATASET {CMID: row.datasetID})
        WITH row, collect(d) AS datasets
        OPTIONAL MATCH (c:CATEGORY {CMID: row.CMID})
        WITH row, datasets, collect(c) AS categories
        OPTIONAL MATCH (:DATASET {CMID: row.datasetID})-[r:USES {Key: row.Key}]->(target:CATEGORY)
        WITH row, datasets, categories,
             [x IN collect(CASE WHEN r IS NULL THEN NULL ELSE {
                 targetCMID: target.CMID,
                 relID: elementId(r),
                 geoPolygon: r.geoPolygon
             } END) WHERE x IS NOT NULL] AS keyedRels
        RETURN row.feature AS feature,
               size(datasets) AS datasetCount,
               size(categories) AS categoryCount,
               keyedRels
        ORDER BY feature
        """,
        driver=driver,
        params={"rows": records},
        type="dict",
    )
    by_feature = {int(row["feature"]): row for row in rows or []}
    errors: list[dict[str, Any]] = []
    for record in records:
        feature = record["feature"]
        row = by_feature.get(feature, {})
        if int(row.get("datasetCount") or 0) != 1:
            errors.append(_detail(feature, "datasetID", "dataset_not_unique", "datasetID must resolve to exactly one DATASET."))
        if int(row.get("categoryCount") or 0) != 1:
            errors.append(_detail(feature, "CMID", "category_not_unique", "CMID must resolve to exactly one CATEGORY."))
        keyed_rels = row.get("keyedRels") or []
        exact = [rel for rel in keyed_rels if rel.get("targetCMID") == record["CMID"]]
        wrong = [rel for rel in keyed_rels if rel.get("targetCMID") != record["CMID"]]
        if len(exact) != 1:
            errors.append(
                _detail(feature, "Key", "uses_not_unique", "The datasetID/CMID/Key triplet must resolve to exactly one USES tie.")
            )
        if wrong:
            errors.append(
                _detail(feature, "Key", "uses_wrong_target", "The same datasetID and Key target a different CMID.")
            )
        record["oldGeoPolygon"] = exact[0].get("geoPolygon") if len(exact) == 1 else None
    return errors


def _gis_preflight(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = getQuery(
        """
        UNWIND $rows AS row
        OPTIONAL MATCH (g:GEOMETRY {geomID: row.geomID})
        WITH row, g, properties(g) AS props
        RETURN row.feature AS feature,
               g IS NOT NULL AS geometryExists,
               props['geometryChecksum'] AS checksum,
               props['sourceDatabase'] AS sourceDatabase,
               props['sourceDatasetID'] AS sourceDatasetID,
               props['sourceCMID'] AS sourceCMID,
               props['sourceKey'] AS sourceKey
        """,
        driver=getDriver("gisdb"),
        params={"rows": records},
        type="dict",
    )
    by_feature = {int(row["feature"]): row for row in rows or []}
    errors: list[dict[str, Any]] = []
    for record in records:
        row = by_feature.get(record["feature"], {})
        exists = bool(row.get("geometryExists"))
        record["geometryExists"] = exists
        if not exists:
            continue
        expected = (
            record["geometryChecksum"],
            record["database"],
            record["datasetID"],
            record["CMID"],
            record["Key"],
        )
        actual = (
            row.get("checksum"),
            row.get("sourceDatabase"),
            row.get("sourceDatasetID"),
            row.get("sourceCMID"),
            row.get("sourceKey"),
        )
        if actual != expected:
            errors.append(
                _detail(record["feature"], "geomID", "geometry_collision", "Existing geomID has different checksum or source identity.")
            )
    return errors


def preflight_geojson(
    path: str | Path,
    database: str,
    actor_claims: dict[str, Any],
    *,
    replace_existing: bool = False,
) -> tuple[PreparedGeoJSON, dict[str, Any]]:
    database = normalize_database(database)
    actor = normalize_actor_claims(actor_claims)
    prepared = prepare_geojson(path, database)
    records = [dict(record, database=database) for record in prepared.records]

    errors = _target_preflight(database, records)
    if not errors:
        assert_owned_uses_by_triplets(database, records, actor)
    if not errors:
        errors.extend(_gis_preflight(records))

    existing = [record for record in records if record.get("oldGeoPolygon")]
    if existing and not replace_existing:
        for record in existing:
            errors.append(
                _detail(
                    record["feature"],
                    "geoPolygon",
                    "existing_polygon",
                    "USES tie already has geoPolygon; explicitly enable replacement to continue.",
                )
            )
    if errors:
        raise GeoJSONUploadError("GeoJSON database preflight failed.", errors)

    prepared_with_db = PreparedGeoJSON(records, prepared.digest, prepared.byte_count, prepared.coordinate_count)
    report = {
        "valid": True,
        "database": database,
        "featureCount": len(records),
        "byteCount": prepared.byte_count,
        "coordinateCount": prepared.coordinate_count,
        "existingPolygonCount": len(existing),
        "newGeometryCount": sum(1 for record in records if not record.get("geometryExists")),
        "reusedGeometryCount": sum(1 for record in records if record.get("geometryExists")),
        "digest": prepared.digest,
        "warnings": [
            f"{len(existing)} existing polygon reference(s) will be replaced."
        ] if existing else [],
    }
    return prepared_with_db, report


def ensure_upload_root() -> Path:
    UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    cleanup_expired_tokens()
    return UPLOAD_ROOT


def cleanup_expired_tokens() -> None:
    if not UPLOAD_ROOT.exists():
        return
    now = time.time()
    for metadata_path in UPLOAD_ROOT.glob("*.json"):
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if now - float(metadata.get("createdAtTs") or 0) <= TOKEN_TTL_SECONDS:
                continue
            token = metadata_path.stem
            for suffix in (".geojson", ".json", ".lock"):
                (UPLOAD_ROOT / f"{token}{suffix}").unlink(missing_ok=True)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue


def _token_path(token: str, suffix: str) -> Path:
    if not TOKEN_PATTERN.fullmatch(str(token or "")):
        raise GeoJSONUploadError("Invalid polygon upload token.")
    return ensure_upload_root() / f"{token}{suffix}"


def create_preflight_token(
    source_path: str | Path,
    database: str,
    actor_claims: dict[str, Any],
    report: dict[str, Any],
    replace_existing: bool,
) -> str:
    token = uuid.uuid4().hex
    source_path = Path(source_path)
    destination = _token_path(token, ".geojson")
    source_path.replace(destination)
    metadata = {
        "token": token,
        "database": normalize_database(database),
        "actor": normalize_actor_claims(actor_claims),
        "digest": report["digest"],
        "replaceExisting": bool(replace_existing),
        "createdAtTs": time.time(),
    }
    _token_path(token, ".json").write_text(json.dumps(metadata), encoding="utf-8")
    return token


def load_preflight_token(token: str, actor_claims: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    metadata_path = _token_path(token, ".json")
    geojson_path = _token_path(token, ".geojson")
    if not metadata_path.exists() or not geojson_path.exists():
        raise GeoJSONUploadError("Polygon upload token was not found or has expired.")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if time.time() - float(metadata.get("createdAtTs") or 0) > TOKEN_TTL_SECONDS:
        delete_preflight_token(token)
        raise GeoJSONUploadError("Polygon upload token has expired; run preflight again.")
    actor = normalize_actor_claims(actor_claims)
    if metadata.get("actor", {}).get("userid") != actor["userid"]:
        raise PermissionError("Polygon upload token belongs to a different user.")
    digest = hashlib.sha256(geojson_path.read_bytes()).hexdigest()
    if digest != metadata.get("digest"):
        raise GeoJSONUploadError("Staged GeoJSON digest changed; run preflight again.")
    return geojson_path, metadata


def claim_preflight_token(token: str) -> None:
    lock_path = _token_path(token, ".lock")
    try:
        with lock_path.open("x", encoding="utf-8") as handle:
            handle.write(_utc_now())
    except FileExistsError as exc:
        raise GeoJSONUploadError("Polygon upload token has already been applied.") from exc


def release_preflight_token_claim(token: str) -> None:
    _token_path(token, ".lock").unlink(missing_ok=True)


def delete_preflight_token(token: str) -> None:
    for suffix in (".geojson", ".json", ".lock"):
        try:
            _token_path(token, suffix).unlink(missing_ok=True)
        except OSError:
            pass


def _ensure_geometry_constraint(driver: Any) -> None:
    getQuery(
        "CREATE CONSTRAINT geometry_geom_id_unique IF NOT EXISTS FOR (g:GEOMETRY) REQUIRE g.geomID IS UNIQUE",
        driver=driver,
        type="dict",
    )


def _stage_geometry(records: list[dict[str, Any]], upload_id: str) -> list[str]:
    driver = getDriver("gisdb")
    _ensure_geometry_constraint(driver)
    new_records = [record for record in records if not record.get("geometryExists")]
    if new_records:
        getQuery(
            """
            UNWIND $rows AS row
            MERGE (g:GEOMETRY {geomID: row.geomID})
            ON CREATE SET g.createdAt = $now,
                          g.state = 'pending',
                          g.uploadID = $uploadID,
                          g.geometry = row.geometry,
                          g.geometryChecksum = row.geometryChecksum,
                          g.sourceDatabase = row.database,
                          g.sourceDatasetID = row.datasetID,
                          g.sourceCMID = row.CMID,
                          g.sourceKey = row.Key,
                          g.crs = 'EPSG:4326'
            RETURN count(g) AS count
            """,
            driver=driver,
            params={"rows": new_records, "uploadID": upload_id, "now": _utc_now()},
            type="dict",
        )
    return [record["geomID"] for record in new_records]


def _run_target_transaction(database: str, records: list[dict[str, Any]], upload_id: str) -> None:
    driver = getDriver(database)

    def _write(tx: Any) -> None:
        validation = list(
            tx.run(
                """
                UNWIND $rows AS row
                OPTIONAL MATCH (:DATASET {CMID: row.datasetID})-[r:USES {Key: row.Key}]->(:CATEGORY {CMID: row.CMID})
                RETURN row.feature AS feature, count(r) AS count
                """,
                rows=records,
            )
        )
        invalid = [dict(row) for row in validation if int(row.get("count") or 0) != 1]
        if invalid:
            raise GeoJSONUploadError("USES ties changed after preflight.", [
                _detail(int(row["feature"]), "Key", "uses_changed", "Exact USES tie no longer resolves uniquely.")
                for row in invalid
            ])
        result = tx.run(
            """
            UNWIND $rows AS row
            MATCH (:DATASET {CMID: row.datasetID})-[r:USES {Key: row.Key}]->(:CATEGORY {CMID: row.CMID})
            SET r.geoPolygon = row.geomID,
                r.status = coalesce(r.status, 'update'),
                r.geoPolygonUploadID = $uploadID,
                r.geoPolygonUpdatedAt = $now
            RETURN count(r) AS count
            """,
            rows=records,
            uploadID=upload_id,
            now=_utc_now(),
        ).single()
        if not result or int(result.get("count") or 0) != len(records):
            raise GeoJSONUploadError("Not all USES polygon references were updated.")

    with driver.session() as session:
        session.execute_write(_write)


def _verify(database: str, records: list[dict[str, Any]]) -> None:
    target = getQuery(
        """
        UNWIND $rows AS row
        MATCH (:DATASET {CMID: row.datasetID})-[r:USES {Key: row.Key}]->(:CATEGORY {CMID: row.CMID})
        WHERE r.geoPolygon = row.geomID
        RETURN count(r) AS count
        """,
        driver=getDriver(database),
        params={"rows": records},
        type="dict",
    )
    geometry = getQuery(
        """
        UNWIND $rows AS row
        MATCH (g:GEOMETRY {geomID: row.geomID})
        WHERE g.geometryChecksum = row.geometryChecksum AND g.geometry IS NOT NULL
        RETURN count(g) AS count
        """,
        driver=getDriver("gisdb"),
        params={"rows": records},
        type="dict",
    )
    target_count = int(target[0].get("count") or 0) if target else 0
    geometry_count = int(geometry[0].get("count") or 0) if geometry else 0
    if target_count != len(records) or geometry_count != len(records):
        raise GeoJSONUploadError(
            f"Post-upload verification failed: {target_count} USES references and {geometry_count} geometries for {len(records)} features."
        )


def _activate_geometry(new_geom_ids: list[str], upload_id: str) -> None:
    if not new_geom_ids:
        return
    result = getQuery(
        """
        UNWIND $geomIDs AS geomID
        MATCH (g:GEOMETRY {geomID: geomID, uploadID: $uploadID, state: 'pending'})
        SET g.state = 'active', g.updatedAt = $now
        RETURN count(g) AS count
        """,
        driver=getDriver("gisdb"),
        params={"geomIDs": new_geom_ids, "uploadID": upload_id, "now": _utc_now()},
        type="dict",
    )
    count = int(result[0].get("count") or 0) if result else 0
    if count != len(new_geom_ids):
        raise GeoJSONUploadError(f"Only {count} of {len(new_geom_ids)} staged geometries were activated.")


def _finalize_target(database: str, records: list[dict[str, Any]], upload_id: str) -> None:
    getQuery(
        """
        UNWIND $rows AS row
        MATCH (:DATASET {CMID: row.datasetID})-[r:USES {Key: row.Key}]->(:CATEGORY {CMID: row.CMID})
        WHERE r.geoPolygonUploadID = $uploadID AND r.geoPolygon = row.geomID
        REMOVE r.geoPolygonUploadID, r.geoPolygonUpdatedAt
        RETURN count(r) AS count
        """,
        driver=getDriver(database),
        params={"rows": records, "uploadID": upload_id},
        type="dict",
    )


def _restore_target(database: str, records: list[dict[str, Any]], upload_id: str) -> None:
    getQuery(
        """
        UNWIND $rows AS row
        MATCH (:DATASET {CMID: row.datasetID})-[r:USES {Key: row.Key}]->(:CATEGORY {CMID: row.CMID})
        WHERE r.geoPolygonUploadID = $uploadID
        FOREACH (_ IN CASE WHEN row.oldGeoPolygon IS NULL THEN [1] ELSE [] END | REMOVE r.geoPolygon)
        FOREACH (_ IN CASE WHEN row.oldGeoPolygon IS NOT NULL THEN [1] ELSE [] END | SET r.geoPolygon = row.oldGeoPolygon)
        REMOVE r.geoPolygonUploadID, r.geoPolygonUpdatedAt
        RETURN count(r) AS count
        """,
        driver=getDriver(database),
        params={"rows": records, "uploadID": upload_id},
        type="dict",
    )


def _delete_upload_geometry(new_geom_ids: list[str], upload_id: str) -> None:
    if not new_geom_ids:
        return
    getQuery(
        """
        UNWIND $geomIDs AS geomID
        MATCH (g:GEOMETRY {geomID: geomID, uploadID: $uploadID})
        DETACH DELETE g
        """,
        driver=getDriver("gisdb"),
        params={"geomIDs": new_geom_ids, "uploadID": upload_id},
        type="dict",
    )


def apply_geojson_upload(
    path: str | Path,
    database: str,
    actor_claims: dict[str, Any],
    *,
    expected_digest: str,
    replace_existing: bool,
    upload_id: str,
    cancelled: Callable[[], bool] | None = None,
    log: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    log = log or (lambda _message: None)
    cancelled = cancelled or (lambda: False)
    prepared, report = preflight_geojson(
        path, database, actor_claims, replace_existing=replace_existing
    )
    if prepared.digest != expected_digest:
        raise GeoJSONUploadError("GeoJSON digest differs from the preflight file.")
    records = prepared.records
    if cancelled():
        raise GeoJSONUploadError("Polygon upload cancelled before GIS staging.")

    new_geom_ids = [record["geomID"] for record in records if not record.get("geometryExists")]
    target_attempted = False
    try:
        log("Staging validated geometry in gisdb.")
        _stage_geometry(records, upload_id)
        if cancelled():
            raise GeoJSONUploadError("Polygon upload cancelled before USES update.")
        log("Updating exact USES polygon references in one transaction.")
        target_attempted = True
        _run_target_transaction(database, records, upload_id)
        log("Verifying geometry checksums and USES references.")
        _verify(database, records)
        _activate_geometry(new_geom_ids, upload_id)
        _finalize_target(database, records, upload_id)
    except Exception as exc:
        compensation_errors = []
        target_restored = True
        if target_attempted:
            try:
                _restore_target(database, records, upload_id)
            except Exception as restore_exc:  # pragma: no cover - exceptional external failure
                target_restored = False
                compensation_errors.append(f"USES restore failed: {restore_exc}")
        if target_restored:
            try:
                _delete_upload_geometry(new_geom_ids, upload_id)
            except Exception as cleanup_exc:  # pragma: no cover - exceptional external failure
                compensation_errors.append(f"GIS cleanup failed: {cleanup_exc}")
        if compensation_errors:
            raise RuntimeError(f"{exc}; compensation incomplete: {'; '.join(compensation_errors)}") from exc
        raise

    return {
        **report,
        "uploadID": upload_id,
        "geometryNodes": len(records),
        "usesLinks": len(records),
    }
