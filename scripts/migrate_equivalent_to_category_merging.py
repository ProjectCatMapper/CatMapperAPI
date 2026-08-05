#!/usr/bin/env python3
"""Normalize category merge-template ties into DATASET->CATEGORY MERGING ties."""

import argparse
import os
import re
import sys
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from CM import getDriver, getQuery  # noqa: E402


def load_renviron(path):
    if not path.exists():
        return
    pattern = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$")
    for line in path.read_text().splitlines():
        match = pattern.match(line)
        if not match:
            continue
        key, value = match.groups()
        os.environ.setdefault(key, value.strip().strip('"').strip("'"))


def split_stack_values(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_parts = []
        for item in value:
            raw_parts.extend(str(item).split(";"))
    else:
        raw_parts = str(value).split(";")
    seen = set()
    output = []
    for part in raw_parts:
        stack_id = part.strip()
        if stack_id and stack_id not in seen:
            seen.add(stack_id)
            output.append(stack_id)
    return output


def _relationship_props(row):
    props = dict(row.get("properties") or {})
    props.pop("dataset", None)
    return props


def _candidate_rows(rows, source):
    candidates = []
    skipped = []
    for row in rows or []:
        dataset_id = row.get("datasetID")
        category_id = row.get("categoryID")
        key = row.get("Key")
        stack_ids = split_stack_values(row.get("stack"))
        if not dataset_id or not category_id or key is None or not stack_ids:
            skipped.append(
                {
                    "source": source,
                    "relID": row.get("relID"),
                    "datasetID": dataset_id,
                    "categoryID": category_id,
                    "Key": key,
                    "stack": row.get("stack"),
                    "reason": "missing datasetID, categoryID, Key, or stack",
                }
            )
            continue
        for stack_id in stack_ids:
            props = _relationship_props(row)
            props["stack"] = stack_id
            props["Key"] = key
            candidates.append(
                {
                    "source": source,
                    "relID": row.get("relID"),
                    "datasetID": dataset_id,
                    "categoryID": category_id,
                    "Key": key,
                    "stackID": stack_id,
                    "properties": props,
                }
            )
    return candidates, skipped


def _dedupe_normalized_rows(rows):
    deduped = {}
    for row in rows:
        key = (row["datasetID"], row["stackID"], row["Key"], row["categoryID"])
        deduped.setdefault(key, row)
    return list(deduped.values())


def _find_conflicts(rows):
    grouped = defaultdict(set)
    for row in rows:
        grouped[(row["datasetID"], row["stackID"], row["Key"])].add(row["categoryID"])
    conflicts = [
        {
            "datasetID": dataset_id,
            "stackID": stack_id,
            "Key": key,
            "categoryIDs": sorted(category_ids),
        }
        for (dataset_id, stack_id, key), category_ids in grouped.items()
        if len(category_ids) > 1
    ]
    return sorted(conflicts, key=lambda item: (item["datasetID"], item["stackID"], item["Key"]))


def _group_key(row):
    return (row["datasetID"], row["stackID"], row["Key"])


def _fetch_uses_targets(driver, rows):
    if not rows:
        return {}
    lookup_rows = [
        {"datasetID": row["datasetID"], "Key": row["Key"]}
        for row in rows
    ]
    query = """
    UNWIND $rows AS row
    WITH DISTINCT row.datasetID AS datasetID, row.Key AS keyValue
    MATCH (:DATASET {CMID: datasetID})-[u:USES {Key: keyValue}]->(c:CATEGORY)
    RETURN datasetID, keyValue AS `Key`, collect(DISTINCT c.CMID) AS categoryIDs
    """
    uses_rows = getQuery(query, driver, params={"rows": lookup_rows}, type="dict") or []
    return {
        (row.get("datasetID"), row.get("Key")): set(row.get("categoryIDs") or [])
        for row in uses_rows
    }


def _resolve_source_conflicts(driver, existing_rows, source_rows):
    conflicts = _find_conflicts(existing_rows + source_rows)
    if not conflicts:
        return source_rows, [], []

    conflict_keys = {
        (conflict["datasetID"], conflict["stackID"], conflict["Key"])
        for conflict in conflicts
    }
    source_by_group = defaultdict(list)
    existing_by_group = defaultdict(set)
    for row in source_rows:
        source_by_group[_group_key(row)].append(row)
    for row in existing_rows:
        existing_by_group[_group_key(row)].add(row["categoryID"])

    uses_targets = _fetch_uses_targets(
        driver,
        [row for row in source_rows if _group_key(row) in conflict_keys],
    )

    keep_rows = []
    discard_rows = []
    unresolved = []

    for row in source_rows:
        group = _group_key(row)
        if group not in conflict_keys:
            keep_rows.append(row)

    for conflict in conflicts:
        group = (conflict["datasetID"], conflict["stackID"], conflict["Key"])
        group_source_rows = source_by_group.get(group, [])
        group_category_ids = {row["categoryID"] for row in group_source_rows}
        existing_category_ids = existing_by_group.get(group, set())

        if len(existing_category_ids) == 1:
            chosen = set(existing_category_ids)
        else:
            uses_category_ids = uses_targets.get((conflict["datasetID"], conflict["Key"]), set())
            matching_uses_targets = group_category_ids.intersection(uses_category_ids)
            chosen = matching_uses_targets if len(matching_uses_targets) == 1 else set()

        if not chosen:
            unresolved.append(conflict)
            continue

        for row in group_source_rows:
            if row["categoryID"] in chosen:
                keep_rows.append(row)
            else:
                discard_rows.append(row)

    return keep_rows, discard_rows, unresolved


def _fully_migrated_relationship_ids(valid_rows, skipped_rows, source):
    valid_ids = {row["relID"] for row in valid_rows if row.get("source") == source and row.get("relID")}
    skipped_ids = {row["relID"] for row in skipped_rows if row.get("source") == source and row.get("relID")}
    return sorted(valid_ids - skipped_ids)


def _fetch_legacy_rows(driver):
    query = """
    MATCH (:CATEGORY)-[e:EQUIVALENT]->(to:CATEGORY)
    WHERE NOT to:VARIABLE
    RETURN
      elementId(e) AS relID,
      e.dataset AS datasetID,
      e.Key AS `Key`,
      e.stack AS stack,
      to.CMID AS categoryID,
      properties(e) AS properties
    """
    return getQuery(query, driver, type="dict") or []


def _fetch_category_merging_rows(driver):
    query = """
    MATCH (d:DATASET)-[m:MERGING]->(c:CATEGORY)
    WHERE NOT c:VARIABLE
    RETURN
      elementId(m) AS relID,
      d.CMID AS datasetID,
      m.Key AS `Key`,
      m.stack AS stack,
      c.CMID AS categoryID,
      properties(m) AS properties
    """
    return getQuery(query, driver, type="dict") or []


def _split_category_merging_rows(rows):
    exact_rows = []
    malformed_rows = []
    for row in rows or []:
        stack_ids = split_stack_values(row.get("stack"))
        stack_text = "" if row.get("stack") is None else str(row.get("stack")).strip()
        if len(stack_ids) == 1 and stack_ids[0] == stack_text:
            exact_rows.append(row)
        else:
            malformed_rows.append(row)
    return exact_rows, malformed_rows


def _filter_valid_bridges(driver, rows):
    if not rows:
        return [], []
    lookup_rows = [
        {"datasetID": row["datasetID"], "stackID": row["stackID"]}
        for row in rows
    ]
    query = """
    UNWIND $rows AS row
    WITH DISTINCT row.datasetID AS datasetID, row.stackID AS stackID
    OPTIONAL MATCH (s:STACK {CMID: stackID})-[:MERGING]->(d:DATASET {CMID: datasetID})
    RETURN datasetID, stackID, count(s) AS bridgeCount
    """
    bridge_rows = getQuery(query, driver, params={"rows": lookup_rows}, type="dict") or []
    valid_pairs = {
        (row.get("datasetID"), row.get("stackID"))
        for row in bridge_rows
        if (row.get("bridgeCount") or 0) > 0
    }
    valid = []
    skipped = []
    for row in rows:
        if (row["datasetID"], row["stackID"]) in valid_pairs:
            valid.append(row)
        else:
            skipped.append(
                {
                    "source": row.get("source"),
                    "relID": row.get("relID"),
                    "datasetID": row["datasetID"],
                    "categoryID": row["categoryID"],
                    "Key": row["Key"],
                    "stackID": row["stackID"],
                    "reason": "missing STACK->DATASET bridge",
                }
            )
    return valid, skipped


def _create_normalized_ties(driver, rows):
    if not rows:
        return 0
    query = """
    UNWIND $rows AS row
    MATCH (d:DATASET {CMID: row.datasetID})
    MATCH (c:CATEGORY {CMID: row.categoryID})
    MERGE (d)-[m:MERGING {stack: row.stackID, Key: row.Key}]->(c)
    SET m += row.properties
    SET m.stack = row.stackID, m.Key = row.Key
    RETURN count(DISTINCT m) AS count
    """
    result = getQuery(query, driver, params={"rows": rows}, type="dict") or []
    return result[0].get("count", 0) if result else 0


def _delete_relationships(driver, rel_type, rel_ids):
    if not rel_ids:
        return 0
    query = f"""
    MATCH ()-[r:{rel_type}]->()
    WHERE elementId(r) IN $rel_ids
    DELETE r
    RETURN count(r) AS count
    """
    result = getQuery(query, driver, params={"rel_ids": rel_ids}, type="dict") or []
    return result[0].get("count", 0) if result else 0


def normalize(database, apply=False):
    driver = getDriver(database)

    legacy_rows = _fetch_legacy_rows(driver)
    category_merging_rows = _fetch_category_merging_rows(driver)
    exact_rows, malformed_rows = _split_category_merging_rows(category_merging_rows)

    legacy_candidates, skipped_missing_legacy = _candidate_rows(legacy_rows, "legacy_equivalent")
    malformed_candidates, skipped_missing_malformed = _candidate_rows(malformed_rows, "malformed_merging")
    exact_candidates, skipped_missing_exact = _candidate_rows(exact_rows, "existing_merging")

    source_candidates = legacy_candidates + malformed_candidates
    valid_sources, skipped_bridge_sources = _filter_valid_bridges(driver, source_candidates)
    valid_existing, skipped_bridge_existing = _filter_valid_bridges(driver, exact_candidates)

    normalized_to_create = _dedupe_normalized_rows(valid_sources)
    normalized_to_create, discarded_conflict_rows, unresolved_conflicts = _resolve_source_conflicts(
        driver,
        valid_existing,
        normalized_to_create,
    )
    conflicts = _find_conflicts(valid_existing + normalized_to_create)
    if conflicts:
        return {
            "database": database,
            "dryRun": not apply,
            "aborted": True,
            "reason": "conflicting datasetID + stackID + Key mappings",
            "conflictCount": len(conflicts),
            "conflictSamples": conflicts[:20],
            "legacyEquivalentCount": len(legacy_rows),
            "malformedMergingCount": len(malformed_rows),
        }

    skipped = (
        skipped_missing_legacy
        + skipped_missing_malformed
        + skipped_missing_exact
        + skipped_bridge_sources
        + skipped_bridge_existing
    )
    conflict_skips = [
        {
            "source": "conflicting_category_targets",
            "datasetID": conflict["datasetID"],
            "stackID": conflict["stackID"],
            "Key": conflict["Key"],
            "categoryIDs": conflict["categoryIDs"],
            "reason": "no existing MERGING target or unique USES target to prefer",
        }
        for conflict in unresolved_conflicts
    ]
    skipped = skipped + conflict_skips
    delete_candidate_rows = normalized_to_create + discarded_conflict_rows
    legacy_delete_ids = _fully_migrated_relationship_ids(delete_candidate_rows, skipped, "legacy_equivalent")
    malformed_delete_ids = _fully_migrated_relationship_ids(delete_candidate_rows, skipped, "malformed_merging")

    result = {
        "database": database,
        "dryRun": not apply,
        "aborted": False,
        "legacyEquivalentCount": len(legacy_rows),
        "existingCategoryMergingCount": len(category_merging_rows),
        "malformedMergingCount": len(malformed_rows),
        "normalizedTieCount": len(normalized_to_create),
        "legacyRelationshipsToDelete": len(legacy_delete_ids),
        "malformedRelationshipsToDelete": len(malformed_delete_ids),
        "resolvedConflictRelationshipCount": len(discarded_conflict_rows),
        "unresolvedConflictCount": len(unresolved_conflicts),
        "unresolvedConflictSamples": unresolved_conflicts[:20],
        "skippedCount": len(skipped),
        "skippedSamples": skipped[:20],
        "createdOrMatchedCount": 0,
        "deletedLegacyEquivalentCount": 0,
        "deletedMalformedMergingCount": 0,
    }

    if not apply:
        return result

    created_count = _create_normalized_ties(driver, normalized_to_create)
    deleted_legacy_count = _delete_relationships(driver, "EQUIVALENT", legacy_delete_ids)
    deleted_malformed_count = _delete_relationships(driver, "MERGING", malformed_delete_ids)

    result.update(
        {
            "dryRun": False,
            "createdOrMatchedCount": created_count,
            "deletedLegacyEquivalentCount": deleted_legacy_count,
            "deletedMalformedMergingCount": deleted_malformed_count,
        }
    )
    return result


def migrate(database, dry_run=True):
    return normalize(database, apply=not dry_run)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("database", help="Neo4j database name, such as SocioMap or ArchaMap")
    parser.add_argument("--apply", action="store_true", help="Write normalized MERGING ties and delete migrated source ties")
    parser.add_argument("--dry-run", action="store_true", help="Preview only; retained for backwards compatibility")
    args = parser.parse_args()

    load_renviron(Path.home() / ".Renviron")
    result = normalize(args.database, apply=args.apply)
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
