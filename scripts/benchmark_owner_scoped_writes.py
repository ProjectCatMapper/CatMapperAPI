#!/usr/bin/env python
"""Benchmark owner-scoped Neo4j writes with cleanup-safe temporary fixtures.

The study compares:

1. an update without authorization;
2. a separate authorization query followed by an update; and
3. authorization and update in one atomic query.

It also compares node authorization by scanning incident USES ties with a
constant-time authorization summary stored on the node.

This script writes temporary graph data. It refuses to run unless ``--write``
is supplied and removes fixtures identified by its unique run ID in ``finally``.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
import math
from pathlib import Path
import random
import statistics
import sys
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from CM.utils import getDriver  # noqa: E402


TIE_UPDATE_QUERY = """
UNWIND $relIds AS relID
MATCH ()-[r:USES]->()
WHERE elementId(r) = relID
SET r[$property] = $value
RETURN count(r) AS updated
"""

TIE_VALIDATE_QUERY = """
UNWIND $relIds AS relID
MATCH ()-[r:USES]->()
WHERE elementId(r) = relID
WITH collect(r) AS rels
RETURN
  size(rels) AS matched,
  CASE
    WHEN size(rels) = size($relIds)
      AND all(
        r IN rels
        WHERE toString(coalesce(r.ownerUserId, '')) = $userid
        AND coalesce(r.modifiedByOtherUser, false) = false
      )
    THEN true
    ELSE false
  END AS eligible
"""

TIE_ATOMIC_UPDATE_QUERY = """
UNWIND $relIds AS relID
MATCH ()-[r:USES]->()
WHERE elementId(r) = relID
WITH collect(r) AS rels
WHERE size(rels) = size($relIds)
  AND all(
    r IN rels
    WHERE toString(coalesce(r.ownerUserId, '')) = $userid
    AND coalesce(r.modifiedByOtherUser, false) = false
  )
FOREACH (r IN rels | SET r[$property] = $value)
RETURN size(rels) AS updated
"""

NODE_SCAN_ATOMIC_UPDATE_QUERY = """
MATCH (n:OWNERSHIP_BENCHMARK {benchmarkRun: $runId, benchmarkNode: $nodeName})
OPTIONAL MATCH (n)-[r:USES]-()
WITH n, collect(r) AS rels
WHERE toString(coalesce(n.ownerUserId, '')) = $userid
  AND coalesce(n.modifiedByOtherUser, false) = false
  AND all(
    r IN rels
    WHERE toString(coalesce(r.ownerUserId, '')) = $userid
  )
SET n[$property] = $value
RETURN count(n) AS updated
"""

NODE_SUMMARY_ATOMIC_UPDATE_QUERY = """
MATCH (n:OWNERSHIP_BENCHMARK {benchmarkRun: $runId, benchmarkNode: $nodeName})
WHERE toString(coalesce(n.ownerUserId, '')) = $userid
  AND coalesce(n.modifiedByOtherUser, false) = false
  AND coalesce(n.unownedUsesCount, 0) = 0
SET n[$property] = $value
RETURN count(n) AS updated
"""


def percentile(values, percentile_value):
    """Return a linearly interpolated percentile for a non-empty sequence."""
    ordered = sorted(values)
    if not ordered:
        raise ValueError("Cannot calculate a percentile for an empty sequence")
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile_value
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def summarize(samples):
    return {
        "runs": len(samples),
        "medianMs": statistics.median(samples),
        "p95Ms": percentile(samples, 0.95),
        "meanMs": statistics.mean(samples),
        "minMs": min(samples),
        "maxMs": max(samples),
    }


def profile_db_hits(profile):
    if profile is None:
        return 0
    children = getattr(profile, "children", None)
    if children is None and isinstance(profile, dict):
        children = profile.get("children", [])
        own_hits = int(profile.get("dbHits") or profile.get("db_hits") or 0)
    else:
        arguments = getattr(profile, "arguments", {}) or {}
        own_hits = int(arguments.get("DbHits") or arguments.get("dbHits") or 0)
    return own_hits + sum(profile_db_hits(child) for child in children or [])


class Study:
    def __init__(self, args):
        self.args = args
        self.database = args.database
        self.dataset = args.dataset
        self.run_id = f"ownership_study_{uuid.uuid4().hex}"
        self.actor_id = f"benchmark_actor_{uuid.uuid4().hex}"
        self.other_actor_id = f"benchmark_other_{uuid.uuid4().hex}"
        self.driver = getDriver(self.database)
        self.random = random.Random(args.seed)
        self.created = False

    def execute(self, query, params=None, *, profile=False):
        statement = f"PROFILE {query}" if profile else query
        started = time.perf_counter_ns()
        with self.driver.session() as session:
            result = session.run(statement, params or {})
            rows = [dict(record) for record in result]
            summary = result.consume()
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
        return rows, summary, elapsed_ms

    def create_tie_fixtures(self):
        rows = []
        for cohort in range(3):
            for index in range(self.args.ties):
                rows.append(
                    {
                        "cohort": cohort,
                        "cmid": (
                            f"AM_BENCH_{self.run_id[-10:]}_"
                            f"{cohort}_{index:04d}"
                        ),
                        "cmname": f"Ownership study {cohort}-{index:04d}",
                        "key": (
                            f"BENCH_{self.run_id[-10:]}_"
                            f"{cohort}_{index:04d}"
                        ),
                    }
                )
        query = """
        MATCH (d:DATASET {CMID: $dataset})
        UNWIND $rows AS row
        CREATE (n:CATEGORY:OWNERSHIP_BENCHMARK {
          CMID: row.cmid,
          CMName: row.cmname,
          benchmarkRun: $runId,
          benchmarkCohort: row.cohort,
          ownerUserId: $actor,
          modifiedByOtherUser: false
        })
        CREATE (d)-[r:USES {
          Key: row.key,
          Name: row.cmname,
          label: 'BENCHMARK',
          benchmarkRun: $runId,
          benchmarkCohort: row.cohort,
          ownerUserId: $actor,
          modifiedByOtherUser: false
        }]->(n)
        RETURN row.cohort AS cohort, elementId(r) AS relID
        ORDER BY cohort, relID
        """
        fixture, _, _ = self.execute(
            query,
            {
                "dataset": self.dataset,
                "rows": rows,
                "runId": self.run_id,
                "actor": self.actor_id,
            },
        )
        self.created = True
        cohorts = defaultdict(list)
        for row in fixture:
            cohorts[int(row["cohort"])].append(row["relID"])
        expected = self.args.ties
        if set(cohorts) != {0, 1, 2} or any(
            len(rel_ids) != expected for rel_ids in cohorts.values()
        ):
            raise RuntimeError(
                f"Unexpected tie fixture counts: "
                f"{dict((key, len(value)) for key, value in cohorts.items())}"
            )
        return cohorts

    def create_node_degree_fixtures(self):
        targets = []
        ties = []
        for degree in self.args.node_degrees:
            for variant in ("scan", "summary"):
                node_name = f"degree-{degree}-{variant}"
                targets.append(
                    {
                        "nodeName": node_name,
                        "cmid": (
                            f"AM_BENCH_NODE_{self.run_id[-10:]}_"
                            f"{degree}_{variant}"
                        ),
                        "cmname": f"Ownership node study {degree} {variant}",
                        "degree": degree,
                        "variant": variant,
                    }
                )
                for index in range(degree):
                    ties.append(
                        {
                            "nodeName": node_name,
                            "key": (
                                f"BENCH_NODE_{self.run_id[-10:]}_"
                                f"{degree}_{variant}_{index:05d}"
                            ),
                        }
                    )
        create_nodes = """
        UNWIND $targets AS row
        CREATE (:CATEGORY:OWNERSHIP_BENCHMARK {
          CMID: row.cmid,
          CMName: row.cmname,
          benchmarkRun: $runId,
          benchmarkNode: row.nodeName,
          benchmarkDegree: row.degree,
          benchmarkVariant: row.variant,
          ownerUserId: $actor,
          modifiedByOtherUser: false,
          unownedUsesCount: 0
        })
        RETURN count(*) AS created
        """
        node_rows, _, _ = self.execute(
            create_nodes,
            {
                "targets": targets,
                "runId": self.run_id,
                "actor": self.actor_id,
            },
        )
        if int(node_rows[0]["created"]) != len(targets):
            raise RuntimeError("Failed to create all node-degree targets")
        if ties:
            create_ties = """
            MATCH (d:DATASET {CMID: $dataset})
            UNWIND $ties AS row
            MATCH (n:OWNERSHIP_BENCHMARK {
              benchmarkRun: $runId,
              benchmarkNode: row.nodeName
            })
            CREATE (d)-[:USES {
              Key: row.key,
              label: 'BENCHMARK',
              benchmarkRun: $runId,
              ownerUserId: $actor,
              modifiedByOtherUser: false
            }]->(n)
            RETURN count(*) AS created
            """
            tie_rows, _, _ = self.execute(
                create_ties,
                {
                    "dataset": self.dataset,
                    "ties": ties,
                    "runId": self.run_id,
                    "actor": self.actor_id,
                },
            )
            if int(tie_rows[0]["created"]) != len(ties):
                raise RuntimeError("Failed to create all node-degree ties")
        return {
            "targets": len(targets),
            "ties": len(ties),
        }

    def tie_operation(self, mode, rel_ids, value):
        common = {
            "relIds": rel_ids,
            "userid": self.actor_id,
            "property": f"benchmark_{mode}",
            "value": value,
        }
        started = time.perf_counter_ns()
        if mode == "unvalidated":
            rows, _, _ = self.execute(TIE_UPDATE_QUERY, common)
        elif mode == "separate":
            validation, _, _ = self.execute(TIE_VALIDATE_QUERY, common)
            if not validation or not validation[0]["eligible"]:
                raise RuntimeError("Separate authorization unexpectedly failed")
            rows, _, _ = self.execute(TIE_UPDATE_QUERY, common)
        elif mode == "atomic":
            rows, _, _ = self.execute(TIE_ATOMIC_UPDATE_QUERY, common)
        else:
            raise ValueError(f"Unknown mode: {mode}")
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
        updated = int(rows[0].get("updated") or 0) if rows else 0
        if updated != len(rel_ids):
            raise RuntimeError(
                f"{mode} updated {updated} ties; expected {len(rel_ids)}"
            )
        return elapsed_ms

    def run_tie_timings(self, cohorts):
        modes = ("unvalidated", "separate", "atomic")
        samples = {
            batch_size: {mode: [] for mode in modes}
            for batch_size in self.args.batch_sizes
        }
        total_rounds = self.args.warmups + self.args.repeats
        for batch_size in self.args.batch_sizes:
            if batch_size > self.args.ties:
                raise ValueError(
                    f"Batch size {batch_size} exceeds ties per cohort "
                    f"({self.args.ties})"
                )
            for round_index in range(total_rounds):
                ordered_modes = list(modes)
                self.random.shuffle(ordered_modes)
                mode_to_cohort = {
                    mode: (modes.index(mode) + round_index) % 3
                    for mode in modes
                }
                for mode in ordered_modes:
                    cohort = mode_to_cohort[mode]
                    rel_ids = self.random.sample(
                        cohorts[cohort],
                        batch_size,
                    )
                    elapsed_ms = self.tie_operation(
                        mode,
                        rel_ids,
                        f"{batch_size}-{round_index}-{mode}",
                    )
                    if round_index >= self.args.warmups:
                        samples[batch_size][mode].append(elapsed_ms)
        return {
            str(batch_size): {
                mode: summarize(mode_samples)
                for mode, mode_samples in mode_map.items()
            }
            for batch_size, mode_map in samples.items()
        }

    def run_node_degree_timings(self):
        output = {}
        total_rounds = self.args.warmups + self.args.repeats
        for degree in self.args.node_degrees:
            samples = {"scan": [], "summary": []}
            for round_index in range(total_rounds):
                variants = ["scan", "summary"]
                if round_index % 2:
                    variants.reverse()
                for variant in variants:
                    query = (
                        NODE_SCAN_ATOMIC_UPDATE_QUERY
                        if variant == "scan"
                        else NODE_SUMMARY_ATOMIC_UPDATE_QUERY
                    )
                    rows, _, elapsed_ms = self.execute(
                        query,
                        {
                            "runId": self.run_id,
                            "nodeName": f"degree-{degree}-{variant}",
                            "userid": self.actor_id,
                            "property": f"benchmark_{variant}",
                            "value": f"{degree}-{round_index}",
                        },
                    )
                    updated = int(rows[0].get("updated") or 0) if rows else 0
                    if updated != 1:
                        raise RuntimeError(
                            f"{variant} node update failed for degree {degree}"
                        )
                    if round_index >= self.args.warmups:
                        samples[variant].append(elapsed_ms)
            output[str(degree)] = {
                variant: summarize(values)
                for variant, values in samples.items()
            }
        return output

    def run_correctness_checks(self, cohorts):
        rel_id = cohorts[0][0]
        checks = {}

        wrong_owner_params = {
            "relIds": [rel_id],
            "userid": self.other_actor_id,
            "property": "benchmarkWrongOwner",
            "value": "must-not-write",
        }
        separate_rows, _, _ = self.execute(
            TIE_VALIDATE_QUERY,
            wrong_owner_params,
        )
        atomic_rows, _, _ = self.execute(
            TIE_ATOMIC_UPDATE_QUERY,
            wrong_owner_params,
        )
        checks["wrongOwnerRejected"] = (
            separate_rows[0]["eligible"] is False
            and not atomic_rows
        )

        self.execute(
            """
            MATCH ()-[r:USES]->()
            WHERE elementId(r) = $relID
            SET r.modifiedByOtherUser = true
            """,
            {"relID": rel_id},
        )
        foreign_params = {
            "relIds": [rel_id],
            "userid": self.actor_id,
            "property": "benchmarkForeignModified",
            "value": "must-not-write",
        }
        separate_rows, _, _ = self.execute(
            TIE_VALIDATE_QUERY,
            foreign_params,
        )
        atomic_rows, _, _ = self.execute(
            TIE_ATOMIC_UPDATE_QUERY,
            foreign_params,
        )
        checks["foreignModificationRejected"] = (
            separate_rows[0]["eligible"] is False
            and not atomic_rows
        )

        verify_rows, _, _ = self.execute(
            """
            MATCH ()-[r:USES]->()
            WHERE elementId(r) = $relID
            RETURN
              r[$wrongProperty] IS NULL AS wrongOwnerUntouched,
              r[$foreignProperty] IS NULL AS foreignModifiedUntouched
            """,
            {
                "relID": rel_id,
                "wrongProperty": "benchmarkWrongOwner",
                "foreignProperty": "benchmarkForeignModified",
            },
        )
        checks.update(verify_rows[0])

        positive_degrees = [
            degree for degree in self.args.node_degrees if degree > 0
        ]
        if positive_degrees:
            degree = max(positive_degrees)
            scan_node = f"degree-{degree}-scan"
            summary_node = f"degree-{degree}-summary"

            self.execute(
                """
                MATCH (n:OWNERSHIP_BENCHMARK {
                  benchmarkRun: $runId,
                  benchmarkNode: $nodeName
                })-[r:USES]-()
                WITH r
                LIMIT 1
                SET
                  r.ownerUserId = $otherActor
                """,
                {
                    "runId": self.run_id,
                    "nodeName": scan_node,
                    "otherActor": self.other_actor_id,
                },
            )
            scan_rows, _, _ = self.execute(
                NODE_SCAN_ATOMIC_UPDATE_QUERY,
                {
                    "runId": self.run_id,
                    "nodeName": scan_node,
                    "userid": self.actor_id,
                    "property": "benchmarkUnownedTie",
                    "value": "must-not-write",
                },
            )
            checks["nodeWithUnownedTieRejected"] = (
                int(scan_rows[0].get("updated") or 0)
                if scan_rows
                else 0
            ) == 0

            self.execute(
                """
                MATCH (n:OWNERSHIP_BENCHMARK {
                  benchmarkRun: $runId,
                  benchmarkNode: $nodeName
                })
                SET n.unownedUsesCount = 1
                """,
                {"runId": self.run_id, "nodeName": summary_node},
            )
            summary_rows, _, _ = self.execute(
                NODE_SUMMARY_ATOMIC_UPDATE_QUERY,
                {
                    "runId": self.run_id,
                    "nodeName": summary_node,
                    "userid": self.actor_id,
                    "property": "benchmarkUnownedTieCount",
                    "value": "must-not-write",
                },
            )
            checks["nodeWithUnownedTieCountRejected"] = (
                int(summary_rows[0].get("updated") or 0)
                if summary_rows
                else 0
            ) == 0

            self.execute(
                """
                MATCH (n:OWNERSHIP_BENCHMARK {
                  benchmarkRun: $runId,
                  benchmarkNode: $nodeName
                })
                SET n.unownedUsesCount = 0, n.modifiedByOtherUser = true
                """,
                {"runId": self.run_id, "nodeName": summary_node},
            )
            modified_rows, _, _ = self.execute(
                NODE_SUMMARY_ATOMIC_UPDATE_QUERY,
                {
                    "runId": self.run_id,
                    "nodeName": summary_node,
                    "userid": self.actor_id,
                    "property": "benchmarkNodeForeignModified",
                    "value": "must-not-write",
                },
            )
            checks["foreignModifiedNodeRejected"] = (
                int(modified_rows[0].get("updated") or 0)
                if modified_rows
                else 0
            ) == 0

            node_verify, _, _ = self.execute(
                """
                MATCH (n:OWNERSHIP_BENCHMARK {benchmarkRun: $runId})
                WHERE n.benchmarkNode IN $nodeNames
                RETURN
                  count(n[$scanProperty]) = 0 AS unownedTieUntouched,
                  count(n[$summaryProperty]) = 0 AS unownedTieCountUntouched,
                  count(n[$modifiedProperty]) = 0 AS modifiedNodeUntouched
                """,
                {
                    "runId": self.run_id,
                    "nodeNames": [scan_node, summary_node],
                    "scanProperty": "benchmarkUnownedTie",
                    "summaryProperty": "benchmarkUnownedTieCount",
                    "modifiedProperty": "benchmarkNodeForeignModified",
                },
            )
            checks.update(node_verify[0])

        if not all(checks.values()):
            raise RuntimeError(f"Correctness checks failed: {checks}")
        return checks

    def run_profiles(self, cohorts):
        batch_size = max(self.args.batch_sizes)
        rel_ids = cohorts[1][:batch_size]
        params = {
            "relIds": rel_ids,
            "userid": self.actor_id,
            "property": "benchmarkProfile",
            "value": "profile",
        }
        output = {}
        for mode in ("unvalidated", "atomic"):
            query = (
                TIE_UPDATE_QUERY
                if mode == "unvalidated"
                else TIE_ATOMIC_UPDATE_QUERY
            )
            rows, summary, _ = self.execute(query, params, profile=True)
            output[mode] = {
                "updated": int(rows[0].get("updated") or 0) if rows else 0,
                "dbHits": profile_db_hits(summary.profile),
            }

        validation_rows, validation_summary, _ = self.execute(
            TIE_VALIDATE_QUERY,
            params,
            profile=True,
        )
        update_rows, update_summary, _ = self.execute(
            TIE_UPDATE_QUERY,
            params,
            profile=True,
        )
        output["separate"] = {
            "eligible": bool(validation_rows[0]["eligible"]),
            "updated": (
                int(update_rows[0].get("updated") or 0)
                if update_rows
                else 0
            ),
            "dbHits": (
                profile_db_hits(validation_summary.profile)
                + profile_db_hits(update_summary.profile)
            ),
        }
        return output

    def cleanup(self):
        rows, _, _ = self.execute(
            """
            MATCH (n:OWNERSHIP_BENCHMARK {benchmarkRun: $runId})
            WITH n
            DETACH DELETE n
            RETURN count(*) AS deletedNodes
            """,
            {"runId": self.run_id},
        )
        remaining, _, _ = self.execute(
            """
            OPTIONAL MATCH (n:OWNERSHIP_BENCHMARK {benchmarkRun: $runId})
            WITH count(n) AS nodes
            OPTIONAL MATCH ()-[r:USES {benchmarkRun: $runId}]->()
            RETURN nodes, count(r) AS ties
            """,
            {"runId": self.run_id},
        )
        return {
            "deletedNodes": int(rows[0]["deletedNodes"]) if rows else 0,
            "remaining": remaining[0],
        }

    def run(self):
        result = {
            "configuration": {
                "database": self.database,
                "dataset": self.dataset,
                "runId": self.run_id,
                "tiesPerCohort": self.args.ties,
                "tieCohorts": 3,
                "batchSizes": self.args.batch_sizes,
                "nodeDegrees": self.args.node_degrees,
                "warmups": self.args.warmups,
                "measuredRepeats": self.args.repeats,
                "seed": self.args.seed,
            }
        }
        try:
            dataset_rows, _, _ = self.execute(
                """
                MATCH (d:DATASET {CMID: $dataset})
                RETURN count(d) AS count
                """,
                {"dataset": self.dataset},
            )
            if (
                not dataset_rows
                or int(dataset_rows[0].get("count") or 0) != 1
            ):
                raise RuntimeError(
                    f"Expected exactly one DATASET {self.dataset}"
                )

            cohorts = self.create_tie_fixtures()
            node_fixture_counts = self.create_node_degree_fixtures()
            result["fixtures"] = {
                "tieNodes": self.args.ties * 3,
                "tieRelationships": self.args.ties * 3,
                "nodeDegreeTargets": node_fixture_counts["targets"],
                "nodeDegreeRelationships": node_fixture_counts["ties"],
            }
            result["tieTimings"] = self.run_tie_timings(cohorts)
            result["nodeDegreeTimings"] = self.run_node_degree_timings()
            result["correctness"] = self.run_correctness_checks(cohorts)
            result["profilesAtLargestBatch"] = self.run_profiles(cohorts)
        finally:
            if self.created:
                result["cleanup"] = self.cleanup()
        return result


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write",
        action="store_true",
        help="Required acknowledgement that temporary graph writes are allowed.",
    )
    parser.add_argument("--database", default="ArchaMap")
    parser.add_argument("--dataset", default="AD941")
    parser.add_argument("--ties", type=int, default=100)
    parser.add_argument(
        "--batch-sizes",
        type=int,
        nargs="+",
        default=[1, 10, 100],
    )
    parser.add_argument(
        "--node-degrees",
        type=int,
        nargs="+",
        default=[0, 10, 100, 1000],
    )
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=30)
    parser.add_argument("--seed", type=int, default=941)
    args = parser.parse_args()
    if not args.write:
        parser.error("--write is required because this study creates temporary graph data")
    if args.ties < 1 or args.repeats < 1 or args.warmups < 0:
        parser.error("ties and repeats must be positive; warmups cannot be negative")
    if any(value < 1 for value in args.batch_sizes):
        parser.error("batch sizes must be positive")
    if any(value < 0 for value in args.node_degrees):
        parser.error("node degrees cannot be negative")
    return args


def main():
    args = parse_args()
    result = Study(args).run()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
