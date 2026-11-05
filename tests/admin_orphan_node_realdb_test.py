"""Rename a temporary typed orphan and verify its audit in a rolled-back transaction."""
import uuid

import pytest

from CM.admin import add_edit_delete_Node
from CM.utils import getQuery, query_transaction

pytestmark = pytest.mark.realdb


def test_orphan_rename_logs_and_locks_without_creating_source_ties(realdb_driver, realdb_database):
    cmid = "AM" + str(uuid.uuid4().int)
    class RollbackFixture(Exception):
        pass

    with pytest.raises(RollbackFixture):
        with query_transaction(realdb_database, realdb_driver):
            getQuery("""
                CREATE (n:PERIOD {CMID:$cmid, CMName:'Original fixture name',
                    ownerUserId:'fixture-owner', modifiedByOtherUser:false})
            """, realdb_driver, params={"cmid": cmid})
            assert add_edit_delete_Node(realdb_database, "fixture-admin", {
                "s1_1": "edit", "s1_2": cmid, "s1_3": "Renamed fixture", "s1_7": "CMName",
            }) == "updated successfully"
            rows = getQuery("""
                MATCH (n {CMID:$cmid})
                OPTIONAL MATCH (n)-[:HAS_LOG]->(log:LOG)
                RETURN n.CMName AS name, labels(n) AS labels,
                    n.modifiedByOtherUser AS locked, log.action AS action,
                    log.user AS actor, COUNT { (n)-[:USES]-() } AS usesCount
            """, realdb_driver, params={"cmid": cmid})
            assert len(rows) == 1
            assert rows[0]["name"] == "Renamed fixture"
            assert rows[0]["labels"] == ["PERIOD"]
            assert rows[0]["usesCount"] == 0
            assert rows[0]["locked"] is True
            assert rows[0]["actor"] == "fixture-admin"
            assert "Original fixture name" in rows[0]["action"]
            assert "Renamed fixture" in rows[0]["action"]
            raise RollbackFixture()

    assert getQuery("MATCH (n {CMID:$cmid}) RETURN n", realdb_driver, params={"cmid": cmid}) == []
