import json
from urllib.parse import urlparse

from flask import Blueprint, Response, jsonify, request
from flask_cors import cross_origin

from CM.reconciliation import (
    build_data_extension_response,
    build_manifest,
    build_preview_html,
    normalize_database,
    propose_properties,
    reconcile_query_batch,
    suggest_entities,
    suggest_properties,
    suggest_types,
)


reconciliation_bp = Blueprint("reconciliation", __name__)


def _canonical_base_url(database):
    database = normalize_database(database)
    root = request.url_root.rstrip("/")
    return f"{root}/reconcile/{database}"


def _frontend_base_url():
    parsed = urlparse(request.url_root)
    scheme = parsed.scheme or "https"
    host = (parsed.hostname or "").lower()
    port = f":{parsed.port}" if parsed.port else ""

    if host == "api.catmapper.org":
        return "https://catmapper.org"
    if host == "dev-api.catmapper.org":
        return "https://dev.catmapper.org"
    if host == "test-api.catmapper.org":
        return "https://test.catmapper.org"
    return f"{scheme}://{host}{port}".rstrip("/")


def _json_payload_field(field_name):
    if field_name in request.args:
        return request.args.get(field_name)
    if field_name in request.form:
        return request.form.get(field_name)

    payload = request.get_json(silent=True)
    if isinstance(payload, dict):
        value = payload.get(field_name)
        if isinstance(value, str):
            return value
        if value is not None:
            return json.dumps(value)

        if field_name == "queries" and _looks_like_query_batch(payload):
            return json.dumps(payload)
        if field_name == "extend" and {"ids", "properties"}.issubset(payload.keys()):
            return json.dumps(payload)

    return None


def _loads_json_object(raw, field_name):
    try:
        value = json.loads(raw)
    except Exception as err:
        raise ValueError(f"{field_name} must be valid JSON") from err
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a JSON object")
    return value


def _looks_like_query_batch(payload):
    if not payload:
        return False
    return all(isinstance(value, dict) for value in payload.values())


def _json_error(message, status):
    return jsonify({"error": str(message)}), status


@reconciliation_bp.route("/reconcile/<database>", methods=["GET", "POST"])
@reconciliation_bp.route("/api/reconcile/<database>", methods=["GET", "POST"])
@reconciliation_bp.route("/api/databases/<database>/reconcile", methods=["GET", "POST"])
@cross_origin(origins="*")
def reconcile_root(database):
    """OpenRefine reconciliation v0.2 root endpoint."""
    try:
        database = normalize_database(database)
        queries_raw = _json_payload_field("queries")
        extend_raw = _json_payload_field("extend")

        if queries_raw is not None:
            queries = _loads_json_object(queries_raw, "queries")
            return jsonify(reconcile_query_batch(database, queries))

        if extend_raw is not None:
            extension_query = _loads_json_object(extend_raw, "extend")
            return jsonify(build_data_extension_response(database, extension_query))

        if request.method == "GET" and not request.args:
            return jsonify(build_manifest(database, _canonical_base_url(database), _frontend_base_url()))

        return _json_error("Expected no parameters, queries, or extend.", 400)
    except OverflowError as err:
        return _json_error(err, 413)
    except ValueError as err:
        return _json_error(err, 400)
    except Exception as err:
        return _json_error(err, 500)


@reconciliation_bp.route("/reconcile/<database>/suggest/entity", methods=["GET"])
@reconciliation_bp.route("/api/reconcile/<database>/suggest/entity", methods=["GET"])
@reconciliation_bp.route("/api/databases/<database>/reconcile/suggest/entity", methods=["GET"])
@cross_origin(origins="*")
def reconcile_suggest_entity(database):
    try:
        return jsonify(suggest_entities(
            database,
            prefix=request.args.get("prefix", ""),
            cursor=request.args.get("cursor", 0),
            limit=request.args.get("limit", 20),
        ))
    except ValueError as err:
        return _json_error(err, 400)
    except Exception as err:
        return _json_error(err, 500)


@reconciliation_bp.route("/reconcile/<database>/suggest/type", methods=["GET"])
@reconciliation_bp.route("/api/reconcile/<database>/suggest/type", methods=["GET"])
@reconciliation_bp.route("/api/databases/<database>/reconcile/suggest/type", methods=["GET"])
@cross_origin(origins="*")
def reconcile_suggest_type(database):
    try:
        return jsonify(suggest_types(
            database,
            prefix=request.args.get("prefix", ""),
            cursor=request.args.get("cursor", 0),
            limit=request.args.get("limit", 20),
        ))
    except ValueError as err:
        return _json_error(err, 400)
    except Exception as err:
        return _json_error(err, 500)


@reconciliation_bp.route("/reconcile/<database>/suggest/property", methods=["GET"])
@reconciliation_bp.route("/api/reconcile/<database>/suggest/property", methods=["GET"])
@reconciliation_bp.route("/api/databases/<database>/reconcile/suggest/property", methods=["GET"])
@cross_origin(origins="*")
def reconcile_suggest_property(database):
    try:
        return jsonify(suggest_properties(
            database,
            prefix=request.args.get("prefix", ""),
            cursor=request.args.get("cursor", 0),
            limit=request.args.get("limit", 20),
        ))
    except ValueError as err:
        return _json_error(err, 400)
    except Exception as err:
        return _json_error(err, 500)


@reconciliation_bp.route("/reconcile/<database>/preview/<cmid>", methods=["GET"])
@reconciliation_bp.route("/api/reconcile/<database>/preview/<cmid>", methods=["GET"])
@reconciliation_bp.route("/api/databases/<database>/reconcile/preview/<cmid>", methods=["GET"])
@cross_origin(origins="*")
def reconcile_preview(database, cmid):
    try:
        return Response(build_preview_html(database, cmid, _frontend_base_url()), mimetype="text/html")
    except LookupError as err:
        return _json_error(err, 404)
    except ValueError as err:
        return _json_error(err, 400)
    except Exception as err:
        return _json_error(err, 500)


@reconciliation_bp.route("/reconcile/<database>/properties", methods=["GET"])
@reconciliation_bp.route("/api/reconcile/<database>/properties", methods=["GET"])
@reconciliation_bp.route("/api/databases/<database>/reconcile/properties", methods=["GET"])
@cross_origin(origins="*")
def reconcile_properties(database):
    try:
        return jsonify(propose_properties(
            database,
            type_id=request.args.get("type"),
            limit=request.args.get("limit"),
        ))
    except ValueError as err:
        return _json_error(err, 400)
    except Exception as err:
        return _json_error(err, 500)


@reconciliation_bp.route("/reconcile/<database>/extend", methods=["GET", "POST"])
@reconciliation_bp.route("/api/reconcile/<database>/extend", methods=["GET", "POST"])
@reconciliation_bp.route("/api/databases/<database>/reconcile/extend", methods=["GET", "POST"])
@cross_origin(origins="*")
def reconcile_extend(database):
    try:
        extend_raw = _json_payload_field("extend")
        if extend_raw is None:
            payload = request.get_json(silent=True)
            if isinstance(payload, dict):
                extend_raw = json.dumps(payload)
        if extend_raw is None:
            return _json_error("extend is required.", 400)
        extension_query = _loads_json_object(extend_raw, "extend")
        return jsonify(build_data_extension_response(database, extension_query))
    except ValueError as err:
        return _json_error(err, 400)
    except Exception as err:
        return _json_error(err, 500)
