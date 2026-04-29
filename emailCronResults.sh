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
body="Archamap last backed up: $archamap_last_modified<br>Sociomap last backed up: $sociomap_last_modified<br>GIS database last backed up: $gis_last_modified<br>User database last backed up: $user_last_modified<br>\nPlease check the logs for more details."

if [ -n "${BREVO_API_KEY:-${API_KEY:-}}" ] && command -v python3 >/dev/null 2>&1; then
    python3 "$script_dir/send_transactional_email.py" \
        --to "admin@catmapper.org" \
        --sender "admin@catmapper.org" \
        --sender-name "${MAIL_SENDER_NAME:-CatMapper}" \
        --subject "$subject" \
        --html "$body" \
        --text "Please review the CatMapper backup timestamps." \
        --tag "catmapper" \
        --tag "backup-status"
else
    echo -e "$body" | mail -a "Content-Type: text/html" -s "$subject" admin@catmapper.org
fi
