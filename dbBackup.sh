#!/bin/bash
set -Eeuo pipefail

app_root="${CATMAPPER_APP_ROOT:-/mnt/storage/app}"
runtime_env="${CATMAPPER_RUNTIME_ENV:-/home/rjbischo/.Renviron}"
if [[ -r "$runtime_env" ]]; then
    set -a
    source "$runtime_env" >/dev/null 2>&1
    set +a
fi

mkdir -p "$app_root/CatMapperAPI/log"
exec 8>"$app_root/CatMapperAPI/log/dbBackup.lock"
flock -n 8 || { echo "Another database backup is already running." >&2; exit 75; }

case "${1:-all}" in
    all) containers=(sociomap1 archamap1 gisdb userdb) ;;
    SocioMap) containers=(sociomap1) ;;
    ArchaMap) containers=(archamap1) ;;
    gisdb|userdb) containers=("$1") ;;
    *) echo "Valid parameters: SocioMap, ArchaMap, gisdb, userdb." >&2; exit 2 ;;
esac
if (( $# > 1 )); then
    echo "Only one database parameter is supported." >&2
    exit 2
fi

stopped_container=""
partial_archive=""
cleanup() {
    local backup_status=$?
    trap - EXIT
    if [[ -n "$stopped_container" ]]; then
        echo "Restarting $stopped_container after an interrupted backup." >&2
        if ! docker start "$stopped_container"; then
            echo "Failed to restart $stopped_container." >&2
            backup_status=1
        fi
    fi
    if [[ -n "$partial_archive" && -e "$partial_archive" ]]; then
        rm -- "$partial_archive"
    fi
    exit "$backup_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "Database backup started at $(date --iso-8601=seconds)"
for container in "${containers[@]}"; do
    database_dir="$app_root/db/$container"
    mkdir -p "$database_dir/backups"
    archive="$database_dir/backups/neo4j-backup.tar.zst"
    partial_archive="$database_dir/backups/.neo4j-backup.tar.zst.partial.$$"

    # Track the container before stopping it so interruption also restarts it.
    stopped_container="$container"
    docker stop "$container"
    tar -I 'zstd -T0 --fast' -cf "$partial_archive" -C "$database_dir" data
    chmod 0644 "$partial_archive"
    mv -- "$partial_archive" "$archive"
    partial_archive=""
    docker start "$container"
    stopped_container=""
done

# A failed archive must not replace the previous copy or reach S3. Failed
# uploads propagate to the caller, which must not start routines afterward.
if [[ "$(id -un)" == "rjbischo" ]]; then
    "$app_root/CatMapperAPI/syncBackups.sh"
else
    /usr/sbin/runuser -u rjbischo -- "$app_root/CatMapperAPI/syncBackups.sh"
fi
echo "Database backup and AWS synchronization completed at $(date --iso-8601=seconds)"
