#!/usr/bin/env python3
"""
Push :LABEL color updates using CatMapper API utilities only.

Uses:
  - CM.getDriver("sociomap")
  - CM.getDriver("archamap")
  - CM.getQuery(query, driver, params=...)
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from CM import getDriver, getQuery  # noqa: E402


COLOR_ROWS: List[Dict[str, str]] = [
    {"CMName": "PROJECTILE_POINT_TYPE", "color": "#e6194b"},
    {"CMName": "PROJECTILE_POINT_CLUSTER", "color": "#3cb44b"},
    {"CMName": "PROJECTILE_POINT", "color": "#ffe119"},
    {"CMName": "CERAMIC_TYPE", "color": "#0082c8"},
    {"CMName": "CERAMIC_WARE", "color": "#f58231"},
    {"CMName": "CERAMIC", "color": "#911eb4"},
    {"CMName": "PHYTOLITH", "color": "#46f0f0"},
    {"CMName": "BOTANICAL", "color": "#f032e6"},
    {"CMName": "FAUNA", "color": "#d2f53c"},
    {"CMName": "SUBSPECIES", "color": "#fabebe"},
    {"CMName": "SPECIES", "color": "#008080"},
    {"CMName": "SUBGENUS", "color": "#e6beff"},
    {"CMName": "GENUS", "color": "#aa6e28"},
    {"CMName": "FAMILY", "color": "#fffac8"},
    {"CMName": "ORDER", "color": "#800000"},
    {"CMName": "CLASS", "color": "#aaffc3"},
    {"CMName": "PHYLUM", "color": "#808000"},
    {"CMName": "KINGDOM", "color": "#ffd8b1"},
    {"CMName": "BIOTA", "color": "#000080"},
    {"CMName": "FEATURE", "color": "#808080"},
    {"CMName": "SITE", "color": "#7b4173"},
    {"CMName": "ADM0", "color": "#d62728"},
    {"CMName": "ADM1", "color": "#2ca02c"},
    {"CMName": "ADM2", "color": "#ff7f0e"},
    {"CMName": "ADM3", "color": "#1f77b4"},
    {"CMName": "ADM4", "color": "#a9a9a9"},
    {"CMName": "ADMD", "color": "#9467bd"},
    {"CMName": "ADME", "color": "#8c564b"},
    {"CMName": "ADML", "color": "#e377c2"},
    {"CMName": "ADMX", "color": "#7f7f7f"},
    {"CMName": "REGION", "color": "#bcbd22"},
    {"CMName": "DISTRICT", "color": "#17becf"},
    {"CMName": "PERIOD", "color": "#393b79"},
    {"CMName": "DIALECT", "color": "#637939"},
    {"CMName": "LANGUAGE", "color": "#8c6d31"},
    {"CMName": "LANGUOID", "color": "#843c39"},
    {"CMName": "ETHNICITY", "color": "#7b4173"},
    {"CMName": "RELIGION", "color": "#3182bd"},
    {"CMName": "OCCUPATION", "color": "#fdd0a2"},
    {"CMName": "POLITY", "color": "#a1d99b"},
    {"CMName": "CULTURE", "color": "#9e9ac8"},
    {"CMName": "STONE", "color": "#f768a1"},
    {"CMName": "DATASET", "color": "#41ab5d"},
    {"CMName": "GENERIC", "color": "#6baed6"},
    {"CMName": "VARIABLE", "color": "#d6616b"},
]


UPDATE_QUERY = """
UNWIND $rows AS row
MATCH (l:LABEL {CMName: row.CMName})
SET l.color = row.color
RETURN count(l) AS updated
"""

MISSING_QUERY = """
UNWIND $rows AS row
OPTIONAL MATCH (l:LABEL {CMName: row.CMName})
WITH row, l
WHERE l IS NULL
RETURN collect(row.CMName) AS missing
"""


def apply_colors(database: str, dry_run: bool) -> tuple[int, List[str]]:
    driver = getDriver(database)

    if dry_run:
        missing_rows = getQuery(MISSING_QUERY, driver, params={"rows": COLOR_ROWS}, type="dict")
        missing = (missing_rows[0].get("missing") if missing_rows else []) or []
        return 0, sorted(missing)

    updated_rows = getQuery(UPDATE_QUERY, driver, params={"rows": COLOR_ROWS}, type="dict")
    updated = int((updated_rows[0].get("updated") if updated_rows else 0) or 0)

    missing_rows = getQuery(MISSING_QUERY, driver, params={"rows": COLOR_ROWS}, type="dict")
    missing = (missing_rows[0].get("missing") if missing_rows else []) or []

    return updated, sorted(missing)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Push LABEL color values to sociomap and archamap using CM.getDriver/getQuery."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate connectivity and report missing labels without writing colors.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    targets = ["sociomap", "archamap"]
    print(f"Applying {len(COLOR_ROWS)} LABEL color mappings via CM utilities.")
    if args.dry_run:
        print("Dry run: no writes will be made.")

    exit_code = 0
    for database in targets:
        try:
            updated, missing = apply_colors(database=database, dry_run=args.dry_run)
            print(f"\nDatabase: {database}")
            print(f"  Updated labels: {updated}")
            if missing:
                print(f"  Missing LABEL CMNames ({len(missing)}): {', '.join(missing)}")
            else:
                print("  Missing LABEL CMNames: none")
        except Exception as exc:
            exit_code = 1
            print(f"\nDatabase: {database}", file=sys.stderr)
            print(f"  Error: {exc}", file=sys.stderr)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
