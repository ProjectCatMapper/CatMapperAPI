#!/bin/bash

# backup databases

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
        *)
            echo "Invalid parameter. Valid options are: SocioMap, ArchaMap, gisdb."
            exit 1
            ;;
    esac
fi

# SocioMap
if [ "$run_sociomap" -eq 1 ]; then
    docker stop sociomap1;

    tar -I 'zstd -T0 --fast' -cvf /mnt/storage/app/db/sociomap1/backups/neo4j-backup.tar.zst /mnt/storage/app/db/sociomap1/data

    docker start sociomap1;
fi

# ArchaMap
if [ "$run_archamap" -eq 1 ]; then
    docker stop archamap1;

    tar -I 'zstd -T0 --fast' -cvf /mnt/storage/app/db/archamap1/backups/neo4j-backup.tar.zst /mnt/storage/app/db/archamap1/data

    docker start archamap1;
fi

# gisdb
if [ "$run_gisdb" -eq 1 ]; then
    docker stop gisdb;

    tar -I 'zstd -T0 --fast' -cvf /mnt/storage/app/GISdb/backups/neo4j-backup.tar.zst /mnt/storage/app/GISdb/data

    docker start gisdb;
fi

# userdb
if [ "$run_gisdb" -eq 1 ]; then
    docker stop userdb;

    tar -I 'zstd -T0 --fast' -cvf /mnt/storage/app/userdb/backups/neo4j-backup.tar.zst /mnt/storage/app/userdb/data

    docker start userdb;
fi


sleep 10
chmod -R 777 /mnt/storage/app/db/archamap1/backups;

chmod -R 777 /mnt/storage/app/db/sociomap1/backups;

chmod -R 777 /mnt/storage/app/GISdb/backups;

chmod -R 777 /mnt/storage/app/userdb/backups;

sudo -u rjbischo /mnt/storage/app/CatMapperAPI/syncBackups.sh;