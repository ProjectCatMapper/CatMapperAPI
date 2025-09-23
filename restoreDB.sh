#!/bin/bash

# Check if the argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <database_name>"
    exit 1
fi

# Check if the second argument is provided and is 'true' (case-insensitive)
if [[ -n "$2" && "${2,,}" == "true" ]]; then
    pullAWS=true
else
    pullAWS=false
fi

# print the result for debugging
echo "pullAWS is set to: $pullAWS"

# Assign the first command line argument to the variable
database_name="$1"

chmod -R 777 /mnt/storage/app/db/"$database_name"/backups;

# Pull the backup from AWS S3 if pullAWS is true
if [ "$pullAWS" = true ]; then
    echo "Pulling backup from AWS S3..."
    sudo -u rjbischo aws s3 cp s3://catmapper/backups/"$database_name"/neo4j-backup.tar.zst /mnt/storage/app/db/"$database_name"/backups/
else
    echo "Skipping AWS S3 pull."
fi

echo "Stopping the Docker container..."
docker stop "$database_name"

echo "Deleting the existing data directory..."
rm -rf /mnt/storage/app/db/"$database_name"/data;

echo "Extracting the backup..."
tar -I zstd -xvf /mnt/storage/app/db/"$database_name"/backups/neo4j-backup.tar.zst -C /mnt/storage/app/db/"$database_name"/;

echo "Starting the Docker container..."
docker start "$database_name"
