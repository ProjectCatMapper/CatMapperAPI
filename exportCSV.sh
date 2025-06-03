
#!/bin/bash

api_url="https://catmapper.org/api/routines/backup2CSV/ArchaMap"

response=$(curl -ks "Content-Type: application/json" "$api_url")

echo "API Response for backup CSV in ArchaMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/backup2CSV/SocioMap"

response=$(curl -ks "Content-Type: application/json" "$api_url")

echo "API Response for backup CSV in ArchaMap:"
echo "$response"

chmod -R 777 /mnt/storage/app/db/sociomap1/backups;
chmod -R 777 /mnt/storage/app/db/archamap1/backups;
sudo -u rjbischo aws s3 sync /mnt/storage/app/db/sociomap1/backups/download s3://sociomap-backups/sociomap1-backups/download/ --acl public-read;
sudo -u rjbischo aws s3 sync /mnt/storage/app/db/archamap1/backups/download s3://sociomap-backups/archamap-backups/download/ --acl public-read;
