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

rename_s3_prefixed_files() {
    local db_prefix="$1"
    local key_prefix="$2"
    local moved=0
    local filename target

    while IFS= read -r filename; do
        [[ -z "$filename" ]] && continue
        [[ "$filename" != *.csv ]] && continue
        [[ "$filename" == SocioMap_* || "$filename" == ArchaMap_* ]] && continue

        target="${db_prefix}_${filename}"
        sudo -u rjbischo aws s3 mv \
            "s3://sociomap-backups/${key_prefix}${filename}" \
            "s3://sociomap-backups/${key_prefix}${target}" \
            --acl public-read >/dev/null
        moved=$((moved + 1))
    done < <(sudo -u rjbischo aws s3 ls "s3://sociomap-backups/${key_prefix}" | awk '{print $4}')

    echo "$db_prefix: moved $moved file(s)."
}

echo "Renaming S3 CSV files so database name is first..."
rename_s3_prefixed_files "SocioMap" "sociomap1-backups/download/"
rename_s3_prefixed_files "ArchaMap" "archamap-backups/download/"

echo "CSV files exported and synced to S3"
