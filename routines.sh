#!/bin/bash
set -u

api_url="${CATMAPPER_ROUTINES_URL:-https://127.0.0.1/runRoutines/all}"
api_host_header="${CATMAPPER_ROUTINES_HOST:-api.catmapper.org}"
recipient="${CATMAPPER_ROUTINES_EMAIL:-admin@catmapper.org}"
max_time="${CATMAPPER_ROUTINES_MAX_TIME:-600000}"

start_ts="$(date '+%Y-%m-%d %H:%M:%S %Z')"
tmp_output="$(mktemp /tmp/catmapper_routines_output.XXXXXX)"
tmp_error="$(mktemp /tmp/catmapper_routines_error.XXXXXX)"

curl_exit=0
curl -sS -k --max-time "$max_time" \
    -H "Content-Type: application/json" \
    -H "Host: $api_host_header" \
    "$api_url" >"$tmp_output" 2>"$tmp_error" || curl_exit=$?

echo "API Response for runRoutines:"
cat "$tmp_output"

run_status="success"
failure_reason=""

if [[ $curl_exit -ne 0 ]]; then
    run_status="failed"
    failure_reason="curl exited with status $curl_exit"
fi

if ! grep -q "Mail sent with status:" "$tmp_output"; then
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

# Treat API-reported runtime/query errors as failures, even if the stream completed.
if grep -Eq "Query execution error|Internal Server Error|Exception:|Error in .*|\\(.*500\\)" "$tmp_output"; then
    run_status="failed"
    if [[ -n "$failure_reason" ]]; then
        failure_reason="$failure_reason; routine output contains API errors"
    else
        failure_reason="routine output contains API errors"
    fi
fi

if [[ "$run_status" == "success" ]]; then
    subject="CatMapper Nightly Routines Successful"
else
    subject="CatMapper Nightly Routines Failed"
fi

end_ts="$(date '+%Y-%m-%d %H:%M:%S %Z')"
output_tail="$(tail -n 80 "$tmp_output" | sed 's/</\&lt;/g' | sed 's/>/\&gt;/g')"
error_tail="$(tail -n 40 "$tmp_error" | sed 's/</\&lt;/g' | sed 's/>/\&gt;/g')"

body="Nightly routines status: ${run_status}<br>\
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

if [[ "$run_status" != "success" ]]; then
    exit 1
fi
