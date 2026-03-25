#!/bin/bash

set -euo pipefail

mkdir -p \
    /mnt/storage/app/CatMapperAPI/log \
    /mnt/storage/app/db/sociomap1/backups/download \
    /mnt/storage/app/db/archamap1/backups/download

echo "Starting CSV export and pivoting process"

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

echo "CSV files are exported already pivoted by Neo4j/APOC."

echo "Syncing CSV files"
sudo -u rjbischo aws s3 sync /mnt/storage/app/db/sociomap1/backups/download s3://sociomap-backups/sociomap1-backups/download/ --acl public-read;
sudo -u rjbischo aws s3 sync /mnt/storage/app/db/archamap1/backups/download s3://sociomap-backups/archamap-backups/download/ --acl public-read;

echo "CSV files exported and synced to S3"
