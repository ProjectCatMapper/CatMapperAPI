#!/bin/bash

chmod -R 777 /mnt/storage/app/db/sociomap1/backups;

rm -rf /mnt/storage/app/db/sociomap1/data;

sudo -u ubuntu aws s3 cp s3://catmapper/backups/sociomap1/neo4j-backup.tar.zst /mnt/storage/app/db/sociomap1/backups/;

