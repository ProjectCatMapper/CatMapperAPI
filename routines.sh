#!/bin/bash
set -uo pipefail

mkdir -p /mnt/storage/app/CatMapperAPI/log

api_url="${CATMAPPER_ROUTINES_URL:-https://127.0.0.1/runRoutines/all}"
api_host_header="${CATMAPPER_ROUTINES_HOST:-api.catmapper.org}"
recipient="${CATMAPPER_ROUTINES_EMAIL:-admin@catmapper.org}"
max_time="${CATMAPPER_ROUTINES_MAX_TIME:-600000}"
heartbeat_sec="${CATMAPPER_ROUTINES_HEARTBEAT_SEC:-60}"

start_ts="$(date '+%Y-%m-%d %H:%M:%S %Z')"
tmp_output="$(mktemp /tmp/catmapper_routines_output.XXXXXX)"
tmp_error="$(mktemp /tmp/catmapper_routines_error.XXXXXX)"

echo "routines.sh started at: $start_ts"
echo "Calling: $api_url (Host: $api_host_header)"

curl_exit=0
echo "API Response for runRoutines (streaming):"
curl_cmd_status=0

(
    while true; do
        sleep "$heartbeat_sec"
        echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] waiting for stream output..." >&2
    done
) &
heartbeat_pid=$!

curl -sS -k --no-buffer --max-time "$max_time" \
    -H "Content-Type: application/json" \
    -H "Host: $api_host_header" \
    "$api_url" \
    2> >(tee "$tmp_error" >&2) \
    | tee "$tmp_output" || curl_cmd_status=$?
kill "$heartbeat_pid" 2>/dev/null || true
wait "$heartbeat_pid" 2>/dev/null || true
curl_exit=$curl_cmd_status

run_status="success"
failure_reason=""
mail_status_line=""
mail_status=""

if [[ $curl_exit -ne 0 ]]; then
    run_status="failed"
    failure_reason="curl exited with status $curl_exit"
fi

mail_status_line="$(grep -o 'Mail sent with status:[^<]*' "$tmp_output" | tail -n 1 || true)"
mail_status="${mail_status_line#Mail sent with status: }"
mail_status="$(echo "$mail_status" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"

if [[ -z "$mail_status_line" ]]; then
    run_status="failed"
    if [[ -n "$failure_reason" ]]; then
        failure_reason="$failure_reason; completion marker was not found"
    else
        failure_reason="completion marker was not found"
    fi
fi

if grep -q "Mail sent with status: Error sending email" "$tmp_output"; then
    run_status="failed"
    if [[ -n "$failure_reason" ]]; then
        failure_reason="$failure_reason; endpoint email send failed"
    else
        failure_reason="endpoint email send failed"
    fi
fi

if [[ -n "$mail_status_line" ]] && [[ "$mail_status" != "Email sent successfully" ]]; then
    run_status="failed"
    if [[ -n "$failure_reason" ]]; then
        failure_reason="$failure_reason; endpoint mail status was '$mail_status'"
    else
        failure_reason="endpoint mail status was '$mail_status'"
    fi
fi

# Treat API-reported runtime/query errors as failures, even if the stream completed.
if grep -Eq "Query execution error|Internal Server Error|Exception:|Error in .*|\\(.*500\\)" "$tmp_output"; then
    run_status="failed"
    if [[ -n "$failure_reason" ]]; then
        failure_reason="$failure_reason; routine output contains API errors"
    else
        failure_reason="routine output contains API errors"
    fi
fi

end_ts="$(date '+%Y-%m-%d %H:%M:%S %Z')"
if [[ "$run_status" == "success" ]]; then
    echo "Nightly routines completed successfully. Summary table email is sent by the runRoutines endpoint."
    echo "Mail status: $mail_status"
    echo "routines.sh ended at: $end_ts"
    rm -f "$tmp_output" "$tmp_error"
    exit 0
fi

output_tail="$(tail -n 80 "$tmp_output" | sed 's/</\&lt;/g' | sed 's/>/\&gt;/g')"
error_tail="$(tail -n 40 "$tmp_error" | sed 's/</\&lt;/g' | sed 's/>/\&gt;/g')"

subject="CatMapper Nightly Routines Failed"
body="Nightly routines wrapper detected a failure.<br>\
Started: ${start_ts}<br>\
Ended: ${end_ts}<br>\
Endpoint: ${api_url}<br>\
Host header: ${api_host_header}<br>\
curl exit status: ${curl_exit}<br>"

if [[ -n "$failure_reason" ]]; then
    body="${body}Failure reason: ${failure_reason}<br>"
fi

body="${body}<br>Last routine output lines:<br><pre>${output_tail}</pre><br>\
Last curl stderr lines:<br><pre>${error_tail}</pre>"

if command -v sendmail >/dev/null 2>&1; then
    {
        echo "To: $recipient"
        echo "Subject: $subject"
        echo "MIME-Version: 1.0"
        echo "Content-Type: text/html; charset=UTF-8"
        echo
        echo -e "$body"
    } | sendmail -t
else
    echo -e "$body" | mail -a "MIME-Version: 1.0" -a "Content-Type: text/html; charset=UTF-8" -s "$subject" "$recipient"
fi

rm -f "$tmp_output" "$tmp_error"
echo "routines.sh ended at: $end_ts"
exit 1
