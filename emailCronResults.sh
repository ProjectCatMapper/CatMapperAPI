#!/bin/bash

# File paths for backups
archamap_backup="/mnt/storage/app/db/archamap1/backups/neo4j-backup.tar.zst"
sociomap_backup="/mnt/storage/app/db/sociomap1/backups/neo4j-backup.tar.zst"
gisdb_backup="/mnt/storage/app/GISdb/backups/neo4j-backup.tar.zst"
userdb_backup="/mnt/storage/app/userdb/backups/neo4j-backup.tar.zst"

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
body="Archamap last backed up: $archamap_last_modified\nSociomap last backed up: $sociomap_last_modified\nGIS database last backed up: $gis_last_modified\nUser database last backed up: $user_last_modified\n\nPlease check the logs for more details."

# Send the email
echo -e "$body" | mail -s "$subject" admin@catmapper.org
