"""Read-only release checks; never defaults to production."""
import json
import os
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

BASE = os.environ.get("CATMAPPER_RELEASE_API_URL", "")
pytestmark = pytest.mark.skipif(
    not BASE or not BASE.startswith("https://") or "dev-api.catmapper.org" not in BASE,
    reason="release smoke requires HTTPS dev API",
)


def _get(path):
    request = Request(f"{BASE.rstrip('/')}{path}", headers={"Accept": "application/json"})
    with urlopen(request, timeout=15) as response:
        return response.status, json.load(response)


@pytest.mark.release
def test_dev_health_contract():
    status, body = _get("/health")
    assert status == 200
    assert body["status"] == "healthy"
    assert isinstance(body["version"], str) and body["version"]
    assert isinstance(body.get("revision"), str) and body["revision"] != "unknown"


@pytest.mark.release
def test_public_endpoint_does_not_return_server_error():
    try:
        status, _ = _get("/api/homepage")
    except HTTPError as error:
        status = error.code
    assert status < 500
