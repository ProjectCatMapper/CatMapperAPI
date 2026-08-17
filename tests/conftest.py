import sys
import os
from pathlib import Path

import pytest
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import app as flask_app


def _configure_realdb_targets():
    """Point realdb tests at explicit disposable targets when supplied."""
    from CM import utils as utils_module

    user = os.environ.get("CM_REALDB_USER")
    password = os.environ.get("CM_REALDB_PASSWORD")
    uri_by_database = {
        database: os.environ.get(f"CM_REALDB_URI_{database.upper()}")
        for database in ("sociomap", "archamap", "gisdb", "userdb")
    }
    if not any(uri_by_database.values()):
        return
    if not user or not password:
        raise pytest.UsageError(
            "CM_REALDB_USER and CM_REALDB_PASSWORD are required when "
            "CM_REALDB_URI_* overrides are used"
        )

    for section in ("DB", "OFFLINE"):
        if not utils_module.config.has_section(section):
            utils_module.config.add_section(section)
        utils_module.config.set(section, "user", user)
        utils_module.config.set(section, "pwd", password)
        for database, uri in uri_by_database.items():
            if uri:
                utils_module.config.set(section, database, uri)


def _run_realdb_enabled(pytestconfig) -> bool:
    return bool(
        pytestconfig.getoption("--run-realdb")
        or os.environ.get("CM_RUN_REALDB_TESTS", "").strip().lower() in {"1", "true", "yes"}
    )


def _run_redis_enabled(pytestconfig) -> bool:
    return bool(
        pytestconfig.getoption("--run-redis")
        or os.environ.get("CM_RUN_REDIS_TESTS", "").strip().lower() in {"1", "true", "yes"}
    )


def _env_enabled(name):
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes"}


def pytest_addoption(parser):
    parser.addoption(
        "--run-realdb",
        action="store_true",
        default=False,
        help="Run tests marked with 'realdb' against a live Neo4j database.",
    )
    parser.addoption(
        "--run-redis",
        action="store_true",
        default=False,
        help="Run tests marked with 'redis' against an isolated Redis database.",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "realdb: executes integration tests against a live Neo4j instance",
    )
    config.addinivalue_line(
        "markers",
        "redis: executes integration tests against a disposable Redis database",
    )


def pytest_collection_modifyitems(config, items):
    run_realdb = _run_realdb_enabled(config)
    run_redis = _run_redis_enabled(config)
    skip_realdb = pytest.mark.skip(reason="need --run-realdb or CM_RUN_REALDB_TESTS=1")
    skip_redis = pytest.mark.skip(reason="need --run-redis or CM_RUN_REDIS_TESTS=1")
    for item in items:
        if "realdb" in item.keywords and not run_realdb:
            item.add_marker(skip_realdb)
        if "redis" in item.keywords and not run_redis:
            item.add_marker(skip_redis)


@pytest.fixture
def client():
    flask_app.config.update(TESTING=True)
    with flask_app.test_client() as test_client:
        yield test_client


@pytest.fixture(scope="session")
def realdb_database():
    return os.environ.get("CM_REALDB_DATABASE", "ArchaMap")


@pytest.fixture(scope="session")
def realdb_driver(pytestconfig, realdb_database):
    from CM.utils import closeAllDrivers, getDriver

    if not _run_realdb_enabled(pytestconfig):
        pytest.skip("realdb tests are disabled")

    if not _env_enabled("CM_REALDB_IS_DISPOSABLE") and not _env_enabled("CM_ALLOW_LIVE_REALDB_TESTS"):
        raise pytest.UsageError(
            "Real-DB tests require CM_REALDB_IS_DISPOSABLE=1. "
            "Set CM_ALLOW_LIVE_REALDB_TESTS=1 only for an intentional live-data run."
        )

    _configure_realdb_targets()
    # Let failures raise so CI/local runs fail loudly if connectivity breaks.
    driver = getDriver(realdb_database)
    try:
        yield driver
    finally:
        closeAllDrivers()


@pytest.fixture(scope="session")
def redis_client(pytestconfig):
    if not _run_redis_enabled(pytestconfig):
        pytest.skip("redis tests are disabled")

    redis_url = os.environ.get("CM_TEST_REDIS_URL", "").strip()
    parsed = urlparse(redis_url)
    try:
        database_number = int((parsed.path or "").lstrip("/"))
    except ValueError as exc:
        raise pytest.UsageError("CM_TEST_REDIS_URL must include a numeric database") from exc

    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"} or database_number < 1:
        raise pytest.UsageError(
            "Redis integration tests require a loopback URL and a nonzero test database"
        )

    import redis

    client = redis.from_url(redis_url, socket_timeout=2, socket_connect_timeout=2)
    client.ping()
    client.flushdb()
    try:
        yield client
    finally:
        client.flushdb()
        client.close()


@pytest.fixture(autouse=True)
def verify_realdb_test_cleanup(request):
    if request.node.get_closest_marker("realdb") is None or not _run_realdb_enabled(request.config):
        yield
        return

    driver = request.getfixturevalue("realdb_driver")
    yield

    from CM import getQuery

    remaining = getQuery(
        """
        MATCH (n:TEST_TMP)
        WHERE toString(n.testRunId) STARTS WITH 'pytest_'
        RETURN count(n) AS remaining
        """,
        driver,
        type="list",
    )
    assert remaining == [0], f"realdb test leaked TEST_TMP nodes: {remaining}"
