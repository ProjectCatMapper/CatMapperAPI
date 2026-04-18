#!/usr/bin/env python3
"""
Preview or backfill dataset->variable MERGING tie metadata from USES ties.

This script mirrors:
  - Key
  - categoryType

from (DATASET)-[:USES]->(VARIABLE) onto existing
(DATASET)-[:MERGING]->(VARIABLE) ties.

Default behavior is a dry run. Pass --apply to write changes.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from CM import closeAllDrivers, getDriver, getQuery  # noqa: E402
from CM.upload import sync_dataset_variable_merging_metadata  # noqa: E402


PREVIEW_QUERY = """
MATCH (d:DATASET)-[r:MERGING]->(v:VARIABLE)
OPTIONAL MATCH (d)-[u:USES]->(v)
WITH d, v, r, collect(DISTINCT {Key: u.Key, categoryType: u.categoryType}) AS rawMetadata
RETURN
  elementId(r) AS relID,
  d.CMID AS datasetID,
  d.CMName AS datasetCMName,
  v.CMID AS variableID,
  v.CMName AS variableCMName,
  r.stack AS stackID,
  r.Key AS existingKey,
  r.categoryType AS existingCategoryType,
  rawMetadata AS rawMetadata
ORDER BY datasetID, variableID, stackID
"""


def _normalized_metadata(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    metadata = []
    for item in row.get("rawMetadata") or []:
        key = str(item.get("Key") or "").strip()
        if not key:
            continue
        metadata.append(
            {
                "Key": key,
                "categoryType": item.get("categoryType"),
            }
        )

    existing_key = str(row.get("existingKey") or "").strip()
    if existing_key:
        metadata = [item for item in metadata if item["Key"] == existing_key]

    deduped = []
    seen = set()
    for item in metadata:
        pair = (item["Key"], item.get("categoryType"))
        if pair in seen:
            continue
        seen.add(pair)
        deduped.append(item)
    return deduped


def preview_database(database: str, sample_limit: int) -> Dict[str, Any]:
    driver = getDriver(database)
    rows = getQuery(PREVIEW_QUERY, driver=driver, type="dict") or []

    resolvable = []
    unresolved = []

    for row in rows:
        metadata = _normalized_metadata(row)
        if len(metadata) == 1:
            resolved = metadata[0]
            resolvable.append(
                {
                    "relID": row.get("relID"),
                    "datasetID": row.get("datasetID"),
                    "datasetCMName": row.get("datasetCMName"),
                    "variableID": row.get("variableID"),
                    "variableCMName": row.get("variableCMName"),
                    "stackID": row.get("stackID"),
                    "existingKey": row.get("existingKey"),
                    "existingCategoryType": row.get("existingCategoryType"),
                    "resolvedKey": resolved.get("Key"),
                    "resolvedCategoryType": resolved.get("categoryType"),
                }
            )
        else:
            unresolved.append(
                {
                    "relID": row.get("relID"),
                    "datasetID": row.get("datasetID"),
                    "datasetCMName": row.get("datasetCMName"),
                    "variableID": row.get("variableID"),
                    "variableCMName": row.get("variableCMName"),
                    "stackID": row.get("stackID"),
                    "existingKey": row.get("existingKey"),
                    "existingCategoryType": row.get("existingCategoryType"),
                    "candidateCount": len(metadata),
                    "candidates": metadata,
                }
            )

    return {
        "database": database,
        "totalTies": len(rows),
        "resolvableCount": len(resolvable),
        "unresolvedCount": len(unresolved),
        "resolvableSample": resolvable[:sample_limit],
        "unresolvedSample": unresolved[:sample_limit],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Preview or backfill Key/categoryType on dataset->variable MERGING ties "
            "from matching USES ties."
        )
    )
    parser.add_argument(
        "--database",
        choices=["archamap", "sociomap", "both"],
        default="archamap",
        help="Target database. Defaults to archamap.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply updates. Without this flag the script only previews changes.",
    )
    parser.add_argument(
        "--user",
        default="server-script",
        help="User identifier written to the upload log when applying updates.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=20,
        help="How many preview rows to print for resolvable and unresolved samples.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of human-readable text.",
    )
    return parser.parse_args()


def _targets(database_arg: str) -> List[str]:
    if database_arg == "both":
        return ["archamap", "sociomap"]
    return [database_arg]


def _print_human_preview(summary: Dict[str, Any]) -> None:
    print(f"Database: {summary['database']}")
    print(f"  Total dataset->variable MERGING ties: {summary['totalTies']}")
    print(f"  Resolvable from USES metadata: {summary['resolvableCount']}")
    print(f"  Unresolved ties: {summary['unresolvedCount']}")

    if summary["resolvableSample"]:
        print("  Resolvable sample:")
        for row in summary["resolvableSample"]:
            print(
                "    "
                f"{row['datasetID']} -> {row['variableID']} "
                f"(stack={row['stackID']}, key={row['resolvedKey']}, "
                f"categoryType={row['resolvedCategoryType']})"
            )

    if summary["unresolvedSample"]:
        print("  Unresolved sample:")
        for row in summary["unresolvedSample"]:
            print(
                "    "
                f"{row['datasetID']} -> {row['variableID']} "
                f"(stack={row['stackID']}, existingKey={row['existingKey']}, "
                f"candidateCount={row['candidateCount']})"
            )


def main() -> int:
    args = parse_args()
    exit_code = 0
    results = []

    try:
        for database in _targets(args.database):
            preview = preview_database(database=database, sample_limit=args.sample_limit)

            apply_result = None
            if args.apply:
                apply_result = sync_dataset_variable_merging_metadata(database, user=args.user)

            result = {
                "database": database,
                "preview": preview,
                "apply": apply_result,
            }
            results.append(result)

            if not args.json:
                _print_human_preview(preview)
                if args.apply:
                    print(f"  Updated ties: {apply_result['updated']}")
                    print(f"  Remaining unresolved after apply: {len(apply_result['unresolved'])}")
                print()
    except Exception as exc:
        exit_code = 1
        if args.json:
            print(json.dumps({"error": str(exc)}, indent=2))
        else:
            print(f"Error: {exc}", file=sys.stderr)
    finally:
        closeAllDrivers()

    if args.json:
        print(json.dumps(results, indent=2))

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
