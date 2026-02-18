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
