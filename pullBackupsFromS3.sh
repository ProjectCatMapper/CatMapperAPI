#!/bin/bash

chmod -R 777 /mnt/storage/app/db/sociomap1/backups;
chmod -R 777 /mnt/storage/app/db/archamap1/backups;
chmod -R 777 /mnt/storage/app/GISdb/backups;
chmod -R 777 /mnt/storage/app/userdb/backups;

rm -rf /mnt/storage/app/db/sociomap1/data;
rm -rf /mnt/storage/app/db/archamap1/data;
rm -rf /mnt/storage/app/GISdb/data;
rm -rf /mnt/storage/app/userdb/data;

sudo -u rjbischo aws s3 cp s3://catmapper/backups/sociomap1/neo4j-backup.tar.zst /mnt/storage/app/db/sociomap1/backups/;

sudo -u rjbischo aws s3 cp s3://catmapper/backups/archamap1/neo4j-backup.tar.zst /mnt/storage/app/db/archamap1/backups/;

sudo -u rjbischo aws s3 cp s3://catmapper/backups/gisdb/neo4j-backup.tar.zst /mnt/storage/app/GISdb/backups/;

sudo -u rjbischo aws s3 cp s3://catmapper/backups/userdb/neo4j-backup.tar.zst /mnt/storage/app/userdb/backups/;
