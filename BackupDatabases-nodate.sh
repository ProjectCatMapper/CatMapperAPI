#!/bin/bash

# backup databases

mkdir -p \
    /mnt/storage/app/CatMapperAPI/log \
    /mnt/storage/app/db/sociomap1/backups \
    /mnt/storage/app/db/archamap1/backups \
    /mnt/storage/app/db/gisdb/backups \
    /mnt/storage/app/db/userdb/backups

# Check if a parameter was provided
if [ "$#" -eq 0 ]; then
    # No parameter provided, run all sections
    run_sociomap=1
    run_archamap=1
    run_gisdb=1
    run_userdb=1
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
        userdb)
            run_userdb=1
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
        -v /mnt/storage/app/db/gisdb/data:/var/lib/neo4j/data  \
        -v /mnt/storage/app/db/gisdb/backups:/var/lib/neo4j/backups \
    neo4j/neo4j-admin:5-community \
    neo4j-admin database dump neo4j --to-path=backups --overwrite-destination=true;

    docker start gisdb;
fi

# gisdb
if [ "$run_userdb" -eq 1 ]; then
    docker stop userdb;

    docker run --interactive --rm \
        -v /mnt/storage/app/db/userdb/data:/var/lib/neo4j/data  \
        -v /mnt/storage/app/db/userdb/backups:/var/lib/neo4j/backups \
    neo4j/neo4j-admin:5-community \
    neo4j-admin database dump neo4j --to-path=backups --overwrite-destination=true;

    docker start userdb;
fi


sleep 10
chmod -R 777 /mnt/storage/app/db/archamap1/backups;

chmod -R 777 /mnt/storage/app/db/sociomap1/backups;

chmod -R 777 /mnt/storage/app/db/gisdb/backups;

chmod -R 777 /mnt/storage/app/db/userdb/backups;
