import sys
import os
from pathlib import Path

import pytest

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


def pytest_addoption(parser):
    parser.addoption(
        "--run-realdb",
        action="store_true",
        default=False,
        help="Run tests marked with 'realdb' against a live Neo4j database.",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "realdb: executes integration tests against a live Neo4j instance",
    )


def pytest_collection_modifyitems(config, items):
    if _run_realdb_enabled(config):
        return
    skip_realdb = pytest.mark.skip(
        reason="need --run-realdb or CM_RUN_REALDB_TESTS=1 to run realdb integration tests"
    )
    for item in items:
        if "realdb" in item.keywords:
            item.add_marker(skip_realdb)


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

    _configure_realdb_targets()
    # Let failures raise so CI/local runs fail loudly if connectivity breaks.
    driver = getDriver(realdb_database)
    try:
        yield driver
    finally:
        closeAllDrivers()
