#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/aws_config_credentials.sh"
trap cleanup_aws_credentials_file EXIT

mkdir -p \
    /mnt/storage/app/db/sociomap1/backups \
    /mnt/storage/app/db/archamap1/backups \
    /mnt/storage/app/db/gisdb/backups \
    /mnt/storage/app/db/userdb/backups \
    /mnt/storage/app/CatMapperAPI/log

docker stop sociomap1 archamap1 gisdb userdb

chmod -R 777 /mnt/storage/app/db/sociomap1/backups;
chmod -R 777 /mnt/storage/app/db/archamap1/backups;
chmod -R 777 /mnt/storage/app/db/gisdb/backups;
chmod -R 777 /mnt/storage/app/db/userdb/backups;

rm -rf /mnt/storage/app/db/sociomap1/data;
rm -rf /mnt/storage/app/db/archamap1/data;
rm -rf /mnt/storage/app/db/gisdb/data;
rm -rf /mnt/storage/app/db/userdb/data;

configure_aws_cli_credentials

aws s3 cp s3://catmapper/backups/sociomap1/neo4j-backup.tar.zst /mnt/storage/app/db/sociomap1/backups/;

aws s3 cp s3://catmapper/backups/archamap1/neo4j-backup.tar.zst /mnt/storage/app/db/archamap1/backups/;

aws s3 cp s3://catmapper/backups/gisdb/neo4j-backup.tar.zst /mnt/storage/app/db/gisdb/backups/;

aws s3 cp s3://catmapper/backups/userdb/neo4j-backup.tar.zst /mnt/storage/app/db/userdb/backups/;

docker start sociomap1 archamap1 gisdb userdb
