#!/bin/bash

# Check if the argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <database_name>"
    exit 1
fi

# Assign the first command line argument to the variable
database_name="$1"

# Stop the Docker container
docker stop "$database_name"

# Run the database restoration command
docker run --interactive --tty --rm \
    --volume=/mnt/storage/app/db/"$database_name"/data:/var/lib/neo4j/data  \
    --volume=/mnt/storage/app/db/"$database_name"/backups:/var/lib/neo4j/backups \
    --privileged \
    neo4j/neo4j-admin:2025-community \
    neo4j-admin database load --from-path=/var/lib/neo4j/backups --overwrite-destination=true neo4j

# Start the Docker container
docker start "$database_name"
