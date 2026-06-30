#!/bin/bash

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

if [ -f "$repo_root/.brevo.env" ]; then
    set -a
    . "$repo_root/.brevo.env"
    set +a
fi

log_file="${1:-}"
if [ -n "$log_file" ]; then
    mkdir -p "$(dirname "$log_file")"
    exec >>"$log_file" 2>&1
fi

mkdir -p /mnt/storage/app/CatMapperAPI/log

python_cmd=()
if [ -n "${CATMAPPER_API_PYTHON:-}" ]; then
    python_cmd=("$CATMAPPER_API_PYTHON")
elif [ -x "/home/rjbischo/.conda/envs/api_env/bin/python" ]; then
    python_cmd=("/home/rjbischo/.conda/envs/api_env/bin/python")
elif [ -x "/opt/conda/bin/conda" ]; then
    python_cmd=("/opt/conda/bin/conda" "run" "-n" "api_env" "python")
elif command -v python3 >/dev/null 2>&1; then
    python_cmd=("python3")
fi

# File paths for backups
archamap_backup="/mnt/storage/app/db/archamap1/backups/neo4j-backup.tar.zst"
sociomap_backup="/mnt/storage/app/db/sociomap1/backups/neo4j-backup.tar.zst"
gisdb_backup="/mnt/storage/app/db/gisdb/backups/neo4j-backup.tar.zst"
userdb_backup="/mnt/storage/app/db/userdb/backups/neo4j-backup.tar.zst"

# Today's date for comparison (formatted as YYYY-MM-DD)
today=$(date "+%Y-%m-%d")

# Get the last modified dates
archamap_last_modified=$(date -r "$archamap_backup" "+%Y-%m-%d %H:%M:%S")
sociomap_last_modified=$(date -r "$sociomap_backup" "+%Y-%m-%d %H:%M:%S")
gis_last_modified=$(date -r "$gisdb_backup" "+%Y-%m-%d %H:%M:%S")
user_last_modified=$(date -r "$userdb_backup" "+%Y-%m-%d %H:%M:%S")

# Extract just the date portion
archamap_date=${archamap_last_modified:0:10}
sociomap_date=${sociomap_last_modified:0:10}
gis_date=${gis_last_modified:0:10}
user_date=${user_last_modified:0:10}

# Determine backup status based on modification dates
if [ "$archamap_date" == "$today" ] && \
   [ "$sociomap_date" == "$today" ] && \
   [ "$gis_date" == "$today" ] && \
   [ "$user_date" == "$today" ]; then
    subject="CatMapper Backup Successful"
else
    subject="CatMapper Backup Failed"
fi

# Email body
body="Archamap last backed up: $archamap_last_modified<br>Sociomap last backed up: $sociomap_last_modified<br>GIS database last backed up: $gis_last_modified<br>User database last backed up: $user_last_modified<br>Please check the logs for more details."

if [ "${#python_cmd[@]}" -gt 0 ]; then
    "${python_cmd[@]}" "$script_dir/send_configured_email.py" \
        --subject "$subject" \
        --html "$body" \
        --text "Please review the CatMapper backup timestamps."
else
    echo -e "$body" | mail -a "Content-Type: text/html" -s "$subject" admin@catmapper.org
fi
