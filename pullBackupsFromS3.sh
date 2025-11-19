#!/bin/bash

docker stop sociomap1 archamap1 gisdb userdb

chmod -R 777 /mnt/storage/app/db/sociomap1/backups;
chmod -R 777 /mnt/storage/app/db/archamap1/backups;
chmod -R 777 /mnt/storage/app/db/gisdb/backups;
chmod -R 777 /mnt/storage/app/db/userdb/backups;

rm -rf /mnt/storage/app/db/sociomap1/data;
rm -rf /mnt/storage/app/db/archamap1/data;
rm -rf /mnt/storage/app/db/gisdb/data;
rm -rf /mnt/storage/app/db/userdb/data;

sudo -u rjbischo aws s3 cp s3://catmapper/backups/sociomap1/neo4j-backup.tar.zst /mnt/storage/app/db/sociomap1/backups/;

sudo -u rjbischo aws s3 cp s3://catmapper/backups/archamap1/neo4j-backup.tar.zst /mnt/storage/app/db/archamap1/backups/;

sudo -u rjbischo aws s3 cp s3://catmapper/backups/gisdb/neo4j-backup.tar.zst /mnt/storage/app/db/gisdb/backups/;

sudo -u rjbischo aws s3 cp s3://catmapper/backups/userdb/neo4j-backup.tar.zst /mnt/storage/app/db/userdb/backups/;

docker start sociomap1 archamap1 gisdb userdb