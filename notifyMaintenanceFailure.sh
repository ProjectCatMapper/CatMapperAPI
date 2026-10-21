#!/bin/bash
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
app_root="${CATMAPPER_APP_ROOT:-$(dirname "$script_dir")}"
if [[ -r "$app_root/.brevo.env" ]]; then
    set -a
    source "$app_root/.brevo.env" >/dev/null 2>&1
    set +a
fi
recipient="${CATMAPPER_MAINTENANCE_EMAIL:-${CATMAPPER_ROUTINES_EMAIL:-admin@catmapper.org}}"
subject="CatMapper Nightly Maintenance Failed"
body="Nightly maintenance failed at $(date --iso-8601=seconds).
Stage: ${1:?stage required}
Exit status: ${2:?exit status required}
Routines require successful backups, AWS upload, and database readiness.
Check $app_root/CatMapperAPI/log/nightlyMaintenance.out, dbBackup.out, and cronRoutines.out.
The routines log may be from an earlier run if routines were skipped."
if [[ -n "${BREVO_API_KEY:-${API_KEY:-}}" ]]; then
    if python3 "$script_dir/send_transactional_email.py" --to "$recipient" \
        --subject "$subject" --text "$body" --tag maintenance-failure; then
        exit 0
    fi
    echo "Brevo submission failed; attempting local mail relay." >&2
fi
{
    printf 'From: admin@catmapper.org\nTo: %s\nSubject: %s\nContent-Type: text/plain; charset=UTF-8\n\n%s\n' "$recipient" "$subject" "$body"
} | /usr/sbin/sendmail -f admin@catmapper.org -t
