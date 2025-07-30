# !/bin/bash

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

fp1="/mnt/storage/app/db/sociomap1/backups/download/datasetNodes_$(date +%Y-%m-%d).csv"
fp2="/mnt/storage/app/db/archamap1/backups/download/datasetNodes_$(date +%Y-%m-%d).csv"
fp3="/mnt/storage/app/db/sociomap1/backups/download/categoryNodes_$(date +%Y-%m-%d).csv"
fp4="/mnt/storage/app/db/archamap1/backups/download/categoryNodes_$(date +%Y-%m-%d).csv"
fp5="/mnt/storage/app/db/sociomap1/backups/download/USESties_$(date +%Y-%m-%d).csv"
fp6="/mnt/storage/app/db/archamap1/backups/download/USESties_$(date +%Y-%m-%d).csv"

echo "Pivoting CSV files"
source /opt/conda/etc/profile.d/conda.sh
which conda
conda info --envs
for fp in "$fp1" "$fp2" "$fp3" "$fp4" "$fp5" "$fp6"; do
    conda run -n global_api_env python /mnt/storage/app/CatMapperAPI/pivotCSVs.py "$fp"
done

echo "Syncing CSV files"
sudo -u rjbischo aws s3 sync /mnt/storage/app/db/sociomap1/backups/download s3://sociomap-backups/sociomap1-backups/download/ --acl public-read;
sudo -u rjbischo aws s3 sync /mnt/storage/app/db/archamap1/backups/download s3://sociomap-backups/archamap-backups/download/ --acl public-read;
