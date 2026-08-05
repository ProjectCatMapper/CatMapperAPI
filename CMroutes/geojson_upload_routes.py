"""REST endpoints for validated polygon GeoJSON uploads."""

from __future__ import annotations

import math
import threading
import uuid

from flask import Blueprint, jsonify, request

from CM.geojson_upload import (
    GeoJSONUploadError,
    MAX_BYTES,
    create_preflight_token,
    claim_preflight_token,
    delete_preflight_token,
    ensure_upload_root,
    load_preflight_token,
    normalize_database,
    preflight_geojson,
    release_preflight_token_claim,
)

from .auth_utils import classify_auth_error_status, verify_request_auth
from .task_queue import enqueue_geojson_upload_task, is_rq_enabled
from .task_store import DEFAULT_UPLOAD_BATCH_SIZE, get_task_store
from .upload_jobs import run_geojson_upload_task


geojson_upload_bp = Blueprint("geojson_upload", __name__)


def _error_response(exc, fallback=400):
    message = str(exc)
    status = classify_auth_error_status(message) or fallback
    payload = {"error": message}
    details = getattr(exc, "details", None)
    if details:
        payload["error_details"] = details
    return jsonify(payload), status


def _save_bounded_upload(file_storage):
    suffix = str(file_storage.filename or "").lower()
    if not suffix.endswith((".geojson", ".json")):
        raise GeoJSONUploadError("Polygon upload file must end in .geojson or .json.")
    path = ensure_upload_root() / f"incoming-{uuid.uuid4().hex}.geojson"
    total = 0
    try:
        with path.open("xb") as handle:
            while True:
                chunk = file_storage.stream.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_BYTES:
                    raise GeoJSONUploadError(f"GeoJSON exceeds the {MAX_BYTES} byte upload limit.")
                handle.write(chunk)
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return path


@geojson_upload_bp.route("/api/uploads/geojson/polygons/preflight", methods=["POST"])
def polygon_geojson_preflight():
    """Validate a polygon file and return a short-lived apply token."""
    incoming_path = None
    try:
        claims = verify_request_auth(required_role="admin", req=request)
        database = normalize_database(request.form.get("database"))
        replace_existing = str(request.form.get("replaceExisting", "false")).strip().lower() in {
            "1", "true", "yes", "on"
        }
        uploaded = request.files.get("file")
        if uploaded is None or not uploaded.filename:
            raise GeoJSONUploadError("A GeoJSON file is required.")
        incoming_path = _save_bounded_upload(uploaded)
        _prepared, report = preflight_geojson(
            incoming_path,
            database,
            claims,
            replace_existing=replace_existing,
        )
        token = create_preflight_token(
            incoming_path,
            database,
            claims,
            report,
            replace_existing,
        )
        incoming_path = None
        return jsonify({**report, "token": token}), 200
    except GeoJSONUploadError as exc:
        return _error_response(exc, fallback=422)
    except Exception as exc:
        return _error_response(exc, fallback=500)
    finally:
        if incoming_path is not None:
            incoming_path.unlink(missing_ok=True)


def _start_geojson_task(metadata, geojson_path, feature_count):
    store = get_task_store()
    task_id = store.create_upload_task(
        user=metadata["actor"]["userid"],
        database=metadata["database"],
        total_rows=feature_count,
        batch_size=DEFAULT_UPLOAD_BATCH_SIZE,
    )
    store.set_upload_job_payload(
        task_id,
        {
            "kind": "geojson_polygon",
            "token": metadata["token"],
            "path": str(geojson_path),
            "database": metadata["database"],
            "actorClaims": metadata["actor"],
            "expectedDigest": metadata["digest"],
            "replaceExisting": bool(metadata.get("replaceExisting")),
        },
    )
    try:
        if is_rq_enabled():
            job = enqueue_geojson_upload_task(task_id)
            if job is not None and getattr(job, "id", None):
                store.set_upload_rq_job_id(task_id, job.id)
        else:
            threading.Thread(
                target=run_geojson_upload_task,
                args=(task_id,),
                daemon=True,
                name=f"geojson-upload-{task_id[:8]}",
            ).start()
    except Exception:
        store.delete_upload_job_payload(task_id)
        store.fail_upload_task(task_id, "Unable to queue polygon upload.")
        raise
    return task_id


@geojson_upload_bp.route("/api/uploads/geojson/polygons/<token>/apply", methods=["POST"])
def polygon_geojson_apply(token):
    """Revalidate and queue an already-preflighted polygon file."""
    try:
        claims = verify_request_auth(required_role="admin", req=request)
        geojson_path, metadata = load_preflight_token(token, claims)
        # Repeat the preflight before queueing and again in the worker. This catches
        # changed ties immediately while preserving the worker's TOCTOU defense.
        prepared, _report = preflight_geojson(
            geojson_path,
            metadata["database"],
            claims,
            replace_existing=bool(metadata.get("replaceExisting")),
        )
        claim_preflight_token(token)
        try:
            task_id = _start_geojson_task(metadata, geojson_path, len(prepared.records))
        except Exception:
            release_preflight_token_claim(token)
            raise
        total_batches = math.ceil(len(prepared.records) / DEFAULT_UPLOAD_BATCH_SIZE)
        return jsonify(
            {
                "taskId": task_id,
                "status": "queued",
                "progress": {
                    "totalRows": len(prepared.records),
                    "totalBatches": total_batches,
                    "completedBatches": 0,
                    "percent": 0,
                },
                "events": ["Polygon upload queued."],
                "nextCursor": 0,
            }
        ), 202
    except GeoJSONUploadError as exc:
        return _error_response(exc, fallback=422)
    except Exception as exc:
        return _error_response(exc, fallback=500)


@geojson_upload_bp.route("/api/uploads/geojson/polygons/<token>", methods=["DELETE"])
def polygon_geojson_discard(token):
    try:
        claims = verify_request_auth(required_role="admin", req=request)
        load_preflight_token(token, claims)
        delete_preflight_token(token)
        return jsonify({"message": "Polygon upload discarded."}), 200
    except Exception as exc:
        return _error_response(exc, fallback=500)
