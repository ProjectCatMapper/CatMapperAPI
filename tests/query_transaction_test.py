from copy import deepcopy

import pytest

from CM import admin, utils


class Transaction:
    def __init__(self, graph):
        self.graph = graph
        self.working = deepcopy(graph)
        self.committed = False
        self.rolled_back = False
        self.calls = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def run(self, query, params):
        self.calls += 1
        if query == 'FAIL':
            raise RuntimeError('connection closed')
        if query == 'WRITE':
            self.working.update(params)
        return [dict(self.working)]

    def commit(self):
        self.graph.clear()
        self.graph.update(self.working)
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class Driver:
    def __init__(self):
        self.graph = {'keep': 'SM1', 'discard': 'SM2', 'parent': 'SM2', 'logs': 0}
        self.transaction = None

    def session(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def begin_transaction(self):
        self.transaction = Transaction(self.graph)
        return self.transaction


def test_helpers_share_uncommitted_writes_and_commit_once():
    driver = Driver()
    with utils.query_transaction('SocioMap', driver):
        assert utils.getDriver('sociomap') is driver
        utils.getQuery('WRITE', driver, params={'parent': 'SM1'})
        assert driver.graph['parent'] == 'SM2'
        assert utils.getQuery('READ', driver)[0]['parent'] == 'SM1'
    assert driver.graph['parent'] == 'SM1'
    assert driver.transaction.committed
    assert utils._QUERY_TRANSACTIONS.get() is None


def test_transaction_query_failure_is_not_retried_and_rolls_back():
    driver = Driver()
    before = deepcopy(driver.graph)
    with pytest.raises(RuntimeError, match='connection closed'):
        with utils.query_transaction('SocioMap', driver):
            utils.getQuery('WRITE', driver, params={'parent': 'SM1'})
            utils.getQuery('FAIL', driver)
    assert driver.transaction.calls == 2
    assert driver.transaction.rolled_back
    assert not driver.transaction.committed
    assert driver.graph == before
    assert utils._QUERY_TRANSACTIONS.get() is None


def test_late_merge_reconciliation_failure_rolls_back_node_sources_and_logs(monkeypatch):
    driver = Driver()
    before = deepcopy(driver.graph)
    monkeypatch.setattr(admin, 'getDriver', lambda database: driver)
    def execute(*args):
        utils.getQuery('WRITE', utils.getDriver('SocioMap'), params={
            'discard': None, 'parent': 'SM1', 'logs': 3})
        # Same failure convention used by processUSES and _merge_nodes.
        return 'processUSES failed after node merge', 500
    monkeypatch.setattr(admin, '_merge_nodes', execute)
    assert admin.mergeNodes('SM1', 'SM2', '7', 'SocioMap') == (
        'processUSES failed after node merge', 500)
    assert driver.graph == before
    assert driver.transaction.rolled_back
    assert not driver.transaction.committed
    assert utils._QUERY_TRANSACTIONS.get() is None
