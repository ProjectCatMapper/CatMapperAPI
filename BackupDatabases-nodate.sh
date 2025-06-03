#!/bin/bash

# backup databases

# Check if a parameter was provided
if [ "$#" -eq 0 ]; then
    # No parameter provided, run all sections
    run_sociomap=1
    run_archamap=1
    run_gisdb=1
else
    # Parameter provided, check which section to run
    case "$1" in
        SocioMap)
            run_sociomap=1
            ;;
        ArchaMap)
            run_archamap=1
            ;;
        gisdb)
            run_gisdb=1
            ;;
        *)
            echo "Invalid parameter. Valid options are: SocioMap, ArchaMap, gisdb."
            exit 1
            ;;
    esac
fi

# SocioMap
if [ "$run_sociomap" -eq 1 ]; then
    docker stop sociomap1;

    docker run --interactive --rm \
        -v /mnt/storage/app/db/sociomap1/data:/var/lib/neo4j/data  \
        -v /mnt/storage/app/db/sociomap1/backups:/var/lib/neo4j/backups \
    neo4j/neo4j-admin:5-community \
        neo4j-admin database dump neo4j --to-path=backups --overwrite-destination=true;

    docker start sociomap1;
fi
# ArchaMap
if [ "$run_archamap" -eq 1 ]; then
    docker stop archamap1;

    docker run --interactive --rm \
        -v /mnt/storage/app/db/archamap1/data:/var/lib/neo4j/data  \
        -v /mnt/storage/app/db/archamap1/backups:/var/lib/neo4j/backups \
    neo4j/neo4j-admin:5-community \
    neo4j-admin database dump neo4j --to-path=backups --overwrite-destination=true;

    docker start archamap1;
fi

# gisdb
if [ "$run_gisdb" -eq 1 ]; then
    docker stop gisdb;

    docker run --interactive --rm \
        -v /mnt/storage/app/GISdb/data:/var/lib/neo4j/data  \
        -v /mnt/storage/app/GISdb/backups:/var/lib/neo4j/backups \
    neo4j/neo4j-admin:5-community \
    neo4j-admin database dump neo4j --to-path=backups --overwrite-destination=true;

    docker start gisdb;
fi

sleep 10
chmod -R 777 /mnt/storage/app/db/archamap1/backups;

chmod -R 777 /mnt/storage/app/db/sociomap1/backups;

chmod -R 777 /mnt/storage/app/GISdb/backups;
