"""HTTP representations for CatMapper ontology and public RDF resources."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path
from urllib.parse import urlencode

from flask import Blueprint, Response, request

from CM.linked_data import (
    CONTEXT_IRI,
    MalformedPublicResource,
    ResourceNotFound,
    canonical_resource_iri,
    domain_scheme_iri,
    fetch_domain_scheme_projection,
    fetch_resource_projection,
    serialize_graph,
)


linked_data_bp = Blueprint("linked_data", __name__)
_LOGGER = logging.getLogger("catmapper.linked_data")
_VERSION_PATTERN = re.compile(r"^[0-9]{4}\.[0-9]{2}\.[0-9]{2}(?:\.[1-9][0-9]*)?$")
_MEDIA_TYPES = ("application/ld+json", "text/turtle")


def _ontology_dir():
    configured = os.environ.get("CATMAPPER_ONTOLOGY_DIR")
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[2] / "ontology"


def _rdf_response(path, *, immutable=False, media_type="text/turtle"):
    if not path.is_file():
        return Response("Not found\n", status=404, content_type="text/plain; charset=utf-8")
    response = Response(path.read_bytes(), status=200, content_type=f"{media_type}; charset=utf-8")
    response.headers["Cache-Control"] = (
        "public, max-age=31536000, immutable" if immutable else "public, max-age=300, must-revalidate"
    )
    response.last_modified = path.stat().st_mtime
    response.add_etag()
    return response.make_conditional(request)


@linked_data_bp.route("/ontology/catmapper", methods=["GET", "HEAD"])
@linked_data_bp.route("/schema/catmapper", methods=["GET", "HEAD"])
def ontology_current():
    return _rdf_response(_ontology_dir() / "catmapper.ttl")


@linked_data_bp.route("/ontology/catmapper/<version>", methods=["GET", "HEAD"])
def ontology_version(version):
    if not _VERSION_PATTERN.fullmatch(version):
        return Response("Not found\n", status=404, content_type="text/plain; charset=utf-8")
    return _rdf_response(_ontology_dir() / "versions" / f"{version}.ttl", immutable=True)


@linked_data_bp.route("/contexts/catmapper", methods=["GET", "HEAD"])
def jsonld_context_current():
    return _rdf_response(_ontology_dir() / "context.jsonld", media_type="application/ld+json")


@linked_data_bp.route("/contexts/catmapper/<version>", methods=["GET", "HEAD"])
def jsonld_context_version(version):
    if not _VERSION_PATTERN.fullmatch(version):
        return Response("Not found\n", status=404, content_type="text/plain; charset=utf-8")
    return _rdf_response(
        _ontology_dir() / "versions" / f"{version}.context.jsonld",
        immutable=True,
        media_type="application/ld+json",
    )


def _requested_media_type():
    explicit = str(request.args.get("format") or "").strip().lower()
    if explicit:
        aliases = {
            "jsonld": "application/ld+json",
            "json-ld": "application/ld+json",
            "ttl": "text/turtle",
            "turtle": "text/turtle",
        }
        return aliases.get(explicit)
    return request.accept_mimetypes.best_match(_MEDIA_TYPES)


@linked_data_bp.route("/linked-data/<database>/<cmid>", methods=["GET", "HEAD"])
def linked_data_resource(database, cmid):
    started = time.monotonic()
    media_type = _requested_media_type()
    status = 200
    body = ""
    response = None
    try:
        if media_type is None:
            status = 406
            response = Response(
                json.dumps({"error": "Request application/ld+json or text/turtle."}),
                status=status,
                content_type="application/json",
            )
            return response

        offset = request.args.get("assertion_offset", 0, type=int)
        limit = request.args.get("assertion_limit", 100, type=int)
        if offset is None or limit is None or offset < 0 or limit < 0:
            raise ValueError("Assertion pagination values must be non-negative integers.")

        projection = fetch_resource_projection(
            database,
            cmid,
            assertion_offset=offset,
            assertion_limit=limit,
        )
        body = serialize_graph(projection["graph"], media_type)
        labels = set(projection["record"].get("labels") or [])
        if "DELETED" in labels and not projection["record"].get("replacementCmid"):
            status = 410
        response = Response(body, status=status, content_type=f"{media_type}; charset=utf-8")
        canonical = str(canonical_resource_iri(database, cmid))
        response.headers.add("Link", f'<{canonical}>; rel="canonical"')
        response.headers.add("Link", f'<{CONTEXT_IRI}>; rel="http://www.w3.org/ns/json-ld#context"')
        if projection["hasMoreAssertions"]:
            next_offset = projection["assertionOffset"] + projection["assertionCount"]
            next_url = (
                f"{request.host_url.rstrip('/')}/{database.lower()}/{cmid}?"
                + urlencode(
                    {
                        "assertion_offset": next_offset,
                        "assertion_limit": limit,
                    }
                )
            )
            response.headers.add("Link", f'<{next_url}>; rel="next"')
        response.headers["Cache-Control"] = "public, max-age=300, must-revalidate"
        response.headers["Vary"] = "Accept"
        return response
    except ResourceNotFound as exc:
        status = 404
        response = Response(json.dumps({"error": str(exc)}), status=status, content_type="application/json")
        return response
    except (MalformedPublicResource, ValueError) as exc:
        status = 422
        response = Response(json.dumps({"error": str(exc)}), status=status, content_type="application/json")
        return response
    except Exception:
        status = 500
        _LOGGER.exception("linked_data_projection_failed database=%s cmid=%s", database, cmid)
        response = Response(
            json.dumps({"error": "Linked-data representation failed."}),
            status=status,
            content_type="application/json",
        )
        return response
    finally:
        if response is not None:
            response.headers["Vary"] = "Accept"
        elapsed_ms = round((time.monotonic() - started) * 1000, 1)
        _LOGGER.info(
            "linked_data_response database=%s cmid=%s status=%s media_type=%s bytes=%s elapsed_ms=%s",
            database,
            cmid,
            status,
            media_type or "none",
            len(body.encode("utf-8")),
            elapsed_ms,
        )


@linked_data_bp.route("/linked-data/<database>/scheme/<slug>", methods=["GET", "HEAD"])
def linked_data_domain_scheme(database, slug):
    media_type = _requested_media_type()
    if media_type is None:
        response = Response(
            json.dumps({"error": "Request application/ld+json or text/turtle."}),
            status=406,
            content_type="application/json",
        )
        response.headers["Vary"] = "Accept"
        return response
    try:
        graph = fetch_domain_scheme_projection(database, slug)
        body = serialize_graph(graph, media_type)
        response = Response(body, status=200, content_type=f"{media_type}; charset=utf-8")
        response.headers.add("Link", f'<{domain_scheme_iri(database, slug)}>; rel="canonical"')
        response.headers["Cache-Control"] = "public, max-age=300, must-revalidate"
        response.headers["Vary"] = "Accept"
        return response
    except ResourceNotFound as exc:
        return Response(json.dumps({"error": str(exc)}), status=404, content_type="application/json")
    except (MalformedPublicResource, ValueError) as exc:
        return Response(json.dumps({"error": str(exc)}), status=422, content_type="application/json")
    except Exception:
        _LOGGER.exception("linked_data_scheme_failed database=%s slug=%s", database, slug)
        return Response(
            json.dumps({"error": "Linked-data representation failed."}),
            status=500,
            content_type="application/json",
        )
