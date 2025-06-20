#!/bin/bash

# sync backups

aws s3 sync /mnt/storage/app/db/sociomap1/backups/ s3://catmapper/backups/sociomap1/;

aws s3 sync /mnt/storage/app/db/archamap1/backups/ s3://catmapper/backups/archamap1/;

aws s3 sync /mnt/storage/app/GISdb/backups/ s3://catmapper/backups/gisdb/;

aws s3 sync /mnt/storage/app/userdb/backups/ s3://catmapper/backups/userdb/;

