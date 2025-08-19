#!/bin/bash

# Set the target directory
target_dir="/mnt/storage/app/db/archamap1/backups/download"

# Loop over all files in the directory
for fp in "$target_dir"/*; do
    if [ -f "$fp" ]; then
        /opt/conda/bin/conda run -n global_api_env python /mnt/storage/app/CatMapperAPI/pivotCSVs.py "$fp"
    fi
done
