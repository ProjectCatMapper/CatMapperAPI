#!/usr/bin/env python3
"""Generate one validated, atomically published CatMapper RDF snapshot."""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from CM.bulk_rdf import generate_snapshot


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("database", choices=("sociomap", "archamap"))
    parser.add_argument("output_directory")
    parser.add_argument("--source-version")
    args = parser.parse_args()
    _, _, manifest = generate_snapshot(
        args.database,
        args.output_directory,
        source_version=args.source_version,
    )
    print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()
