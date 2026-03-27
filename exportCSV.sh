#!/bin/bash

set -euo pipefail

mkdir -p \
    /mnt/storage/app/CatMapperAPI/log \
    /mnt/storage/app/db/sociomap1/backups/download \
    /mnt/storage/app/db/archamap1/backups/download

normalize_download_files() {
    local prefix="$1"
    local directory="$2"

    shopt -s nullglob
    for filepath in "$directory"/*.csv; do
        local filename target
        filename="$(basename "$filepath")"
        if [[ "$filename" == "${prefix}_"* ]]; then
            continue
        fi

        target="$directory/${prefix}_${filename}"
        if [[ -e "$target" ]]; then
            rm -f "$filepath"
        else
            mv "$filepath" "$target"
        fi
    done
    shopt -u nullglob
}

echo "Starting CSV export and pivoting process"

# Ensure parent backup directories are traversable for the sync user.
chmod 775 /mnt/storage/app/db/sociomap1/backups /mnt/storage/app/db/archamap1/backups >/dev/null 2>&1 || true
chmod 777 /mnt/storage/app/db/sociomap1/backups/download /mnt/storage/app/db/archamap1/backups/download >/dev/null 2>&1 || true
today="$(date +%Y-%m-%d)"
find /mnt/storage/app/db/sociomap1/backups/download -maxdepth 1 -type f -name "*_${today}.csv" -exec chmod 666 {} + 2>/dev/null || true
find /mnt/storage/app/db/archamap1/backups/download -maxdepth 1 -type f -name "*_${today}.csv" -exec chmod 666 {} + 2>/dev/null || true
rm -f /mnt/storage/app/db/sociomap1/backups/download/*_"$today".csv
rm -f /mnt/storage/app/db/archamap1/backups/download/*_"$today".csv

response=$(docker exec -i api python - <<'PY'
from CM.routines import backup2CSV
print(backup2CSV('ArchaMap'))
PY
)

echo "API Response for backup CSV in ArchaMap:"
echo "$response"
if [[ "$response" != *"backup2CSV completed for ArchaMap"* ]]; then
    echo "ArchaMap export did not report success. Aborting."
    exit 1
fi

response=$(docker exec -i api python - <<'PY'
from CM.routines import backup2CSV
print(backup2CSV('SocioMap'))
PY
)

echo "API Response for backup CSV in SocioMap:"
echo "$response"
if [[ "$response" != *"backup2CSV completed for SocioMap"* ]]; then
    echo "SocioMap export did not report success. Aborting."
    exit 1
fi

echo "Normalizing historical CSV filenames to prefixed form..."
normalize_download_files "ArchaMap" "/mnt/storage/app/db/archamap1/backups/download"
normalize_download_files "SocioMap" "/mnt/storage/app/db/sociomap1/backups/download"

echo "CSV files are exported already pivoted by Neo4j/APOC."

echo "Syncing CSV files"
sudo -u rjbischo aws s3 sync /mnt/storage/app/db/sociomap1/backups/download s3://sociomap-backups/sociomap1-backups/download/ --acl public-read;
sudo -u rjbischo aws s3 sync /mnt/storage/app/db/archamap1/backups/download s3://sociomap-backups/archamap-backups/download/ --acl public-read;

echo "Renaming S3 CSV files so database name is first..."
sudo -u rjbischo python3 - <<'PY'
import boto3

BUCKET = "sociomap-backups"
TARGETS = [
    ("SocioMap", "sociomap1-backups/download/"),
    ("ArchaMap", "archamap-backups/download/"),
]

s3 = boto3.client("s3")
moved = 0

for prefix_name, key_prefix in TARGETS:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.rsplit("/", 1)[-1]
            if not filename.endswith(".csv"):
                continue
            if filename.startswith("SocioMap_") or filename.startswith("ArchaMap_"):
                continue

            new_key = f"{key_prefix}{prefix_name}_{filename}"
            s3.copy_object(
                Bucket=BUCKET,
                CopySource={"Bucket": BUCKET, "Key": key},
                Key=new_key,
                ACL="public-read",
            )
            s3.delete_object(Bucket=BUCKET, Key=key)
            moved += 1

print(f"S3 rename operations completed: {moved} file(s) moved.")
PY

echo "CSV files exported and synced to S3"
