#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/aws_config_credentials.sh"
trap cleanup_aws_credentials_file EXIT

# sync backups
mkdir -p \
    /mnt/storage/app/db/sociomap1/backups \
    /mnt/storage/app/db/archamap1/backups \
    /mnt/storage/app/db/gisdb/backups \
    /mnt/storage/app/db/userdb/backups

configure_aws_cli_credentials
aws s3 sync /mnt/storage/app/db/sociomap1/backups/ s3://catmapper/backups/sociomap1/;

aws s3 sync /mnt/storage/app/db/archamap1/backups/ s3://catmapper/backups/archamap1/;

aws s3 sync /mnt/storage/app/db/gisdb/backups/ s3://catmapper/backups/gisdb/;

aws s3 sync /mnt/storage/app/db/userdb/backups/ s3://catmapper/backups/userdb/;
