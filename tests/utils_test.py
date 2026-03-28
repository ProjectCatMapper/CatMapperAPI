import CM.utils as utils


class FakeDriver:
    def __init__(self):
        self.closed = False
        self.verify_calls = 0

    def verify_connectivity(self):
        self.verify_calls += 1

    def close(self):
        self.closed = True


def test_getDriver_uses_cache(monkeypatch):
    utils._driver_cache.clear()
    utils._last_verified.clear()

    created = []

    def fake_create_driver(database):
        driver = FakeDriver()
        created.append((database, driver))
        return driver

    monkeypatch.setattr(utils, "_create_driver", fake_create_driver)

    first = utils.getDriver("ArchaMap")
    second = utils.getDriver("archamap")

    assert first is second
    assert len(created) == 1


def test_closeAllDrivers_closes_and_clears_cache():
    utils._driver_cache.clear()
    utils._last_verified.clear()

    driver = FakeDriver()
    utils._driver_cache["archamap"] = driver
    utils._last_verified["archamap"] = utils.datetime.now()

    utils.closeAllDrivers()

    assert driver.closed is True
    assert utils._driver_cache == {}
    assert utils._last_verified == {}


def test_getAvailableID_category_fills_lowest_missing_and_uses_category_deleted(monkeypatch):
    captured = {}

    monkeypatch.setattr(utils, "getDriver", lambda database: object())

    def fake_get_query(query, driver, params=None, type="dict", **kwargs):
        captured["query"] = query
        captured["params"] = params
        return [2, 4]

    monkeypatch.setattr(utils, "getQuery", fake_get_query)

    generated = utils.getAvailableID(
        new_id="CMID",
        label="CATEGORY",
        n=3,
        database="ArchaMap",
    )

    assert generated == ["AM1", "AM3", "AM5"]
    assert "n:CATEGORY OR n:DELETED" in captured["query"]
    assert captured["params"]["pattern"] == "^AM[0-9]+$"
    assert captured["params"]["prefix"] == "AM"


def test_getAvailableID_dataset_uses_dataset_scope(monkeypatch):
    captured = {}

    monkeypatch.setattr(utils, "getDriver", lambda database: object())

    def fake_get_query(query, driver, params=None, type="dict", **kwargs):
        captured["query"] = query
        captured["params"] = params
        return [1, 2, 3]

    monkeypatch.setattr(utils, "getQuery", fake_get_query)

    generated = utils.getAvailableID(
        new_id="CMID",
        label="DATASET",
        n=2,
        database="ArchaMap",
    )

    assert generated == ["AD4", "AD5"]
    assert "WHERE n:DATASET" in captured["query"]
    assert captured["params"]["pattern"] == "^AD[0-9]+$"
