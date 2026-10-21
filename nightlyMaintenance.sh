#!/bin/bash
set -Eeuo pipefail

app_root="${CATMAPPER_APP_ROOT:-/mnt/storage/app}"
api_dir="$app_root/CatMapperAPI"
log_dir="$api_dir/log"
runtime_env="${CATMAPPER_RUNTIME_ENV:-/home/rjbischo/.Renviron}"
if [[ -r "$runtime_env" ]]; then
    set -a
    source "$runtime_env" >/dev/null 2>&1
    set +a
fi
mkdir -p "$log_dir"
exec 9>"$log_dir/nightlyMaintenance.lock"
flock -n 9 || { echo "Nightly maintenance is already running." >&2; exit 75; }

stage="backup or AWS upload"
failure_notice() {
    local status=$?
    trap - EXIT
    if (( status != 0 )); then
        echo "Nightly maintenance failed during $stage (exit $status)." >&2
        if ! "$api_dir/notifyMaintenanceFailure.sh" "$stage" "$status"; then
            echo "FAILURE NOTIFICATION COULD NOT BE SUBMITTED." >&2
        fi
    fi
    exit "$status"
}
trap failure_notice EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "Nightly backup-before-routines started at $(date --iso-8601=seconds)"
if ! "$api_dir/dbBackup.sh" >"$log_dir/dbBackup.out" 2>&1; then
    echo "Backup or AWS upload failed; routines were not started. See $log_dir/dbBackup.out" >&2
    exit 1
fi

# A successful docker start does not mean Bolt is ready. Wait for all four
# databases before the API is allowed to perform nightly maintenance.
stage="database readiness"
ready_timeout="${CATMAPPER_DATABASE_READY_TIMEOUT:-120}"
[[ "$ready_timeout" =~ ^[0-9]+$ ]] || { echo "Invalid database readiness timeout." >&2; exit 2; }
deadline=$((SECONDS + ready_timeout))
ready=false
while (( SECONDS < deadline )); do
    if timeout 20 docker exec api python -c '
from CM.utils import getDriver
for database in ("sociomap", "archamap", "gisdb", "userdb"):
    getDriver(database).verify_connectivity()
' >/dev/null 2>&1; then
        ready=true
        break
    fi
    sleep 5
done
if [[ "$ready" != true ]]; then
    echo "Database readiness checks failed; routines were not started." >&2
    exit 1
fi

echo "Backup and AWS upload succeeded; starting routines at $(date --iso-8601=seconds)"
stage="routines"
if ! "$api_dir/routines.sh" >"$log_dir/cronRoutines.out" 2>&1; then
    echo "Routines failed. See $log_dir/cronRoutines.out" >&2
    exit 1
fi
echo "Nightly backup-before-routines completed at $(date --iso-8601=seconds)"
