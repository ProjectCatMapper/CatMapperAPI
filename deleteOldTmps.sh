#!/bin/bash
# Script to delete files older than 7 days in /mnt/storage/app/CatMapperAPI/tmp

TARGET_DIR="/mnt/storage/app/CatMapperAPI/tmp"
TARGET_DIR2="/tmp"
# Delete files older than 7 days
find "$TARGET_DIR" -type f -mtime +7 -exec rm -f {} \;

find "$TARGET_DIR2" -type f -mtime +7 -exec rm -f {} \;