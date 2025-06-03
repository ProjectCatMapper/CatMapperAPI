#!/bin/bash

file_path="/mnt/storage/cron.out"

# Determine the backup status
if grep -qi "successfully" "$file_path"; then
    subject="CatMapper Backup Successful"
else
    subject="CatMapper Backup Failed"
fi

# File paths for Archamap and Sociomap backups
archamap_backup="/mnt/storage/app/db/archamap1/backups/neo4j.dump"
sociomap_backup="/mnt/storage/app/db/sociomap1/backups/neo4j.dump"
gisdb_backup="/mnt/storage/app/GISdb/backups/neo4j.dump"

# Get the last modified date for Archamap backup
archamap_last_modified=$(date -r "$archamap_backup" "+%Y-%m-%d %H:%M:%S")

# Get the last modified date for Sociomap backup
sociomap_last_modified=$(date -r "$sociomap_backup" "+%Y-%m-%d %H:%M:%S")

# Get the last modified date for GIS backup
gis_last_modified=$(date -r "$gisdb_backup" "+%Y-%m-%d %H:%M:%S")

# Email body
body="Archamap last backed up: $archamap_last_modified\nSociomap last backed up: $sociomap_last_modified\nGIS database last backed up: $gis_last_modified"

# Send the email with attachment
echo -e "$body" | mail -s "$subject" admin@catmapper.org
