#!/bin/bash

AWS_CREDENTIALS_FILE=""

cleanup_aws_credentials_file() {
    if [[ -n "$AWS_CREDENTIALS_FILE" && -f "$AWS_CREDENTIALS_FILE" ]]; then
        rm -f "$AWS_CREDENTIALS_FILE"
    fi
}

configure_aws_cli_credentials() {
    local script_dir config_path
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    config_path="${AWS_CONFIG_INI:-${script_dir}/config.ini}"

    AWS_CREDENTIALS_FILE="$(mktemp)"
    chmod 600 "$AWS_CREDENTIALS_FILE"
    CONFIG_PATH="$config_path" OUTPUT_PATH="$AWS_CREDENTIALS_FILE" python3 - <<'PY'
import os
from configparser import ConfigParser

config_path = os.environ["CONFIG_PATH"]
output_path = os.environ["OUTPUT_PATH"]

parser = ConfigParser()
parser.read(config_path)
if not parser.has_section("AWS"):
    raise SystemExit(f"Missing [AWS] section in {config_path}")

access_key = parser.get("AWS", "AccessKeyId", fallback="").strip()
secret_key = parser.get("AWS", "SecretAccessKey", fallback="").strip()
session_token = parser.get("AWS", "SessionToken", fallback="").strip()

if not access_key or not secret_key:
    raise SystemExit(f"Missing AccessKeyId or SecretAccessKey in {config_path}")

with open(output_path, "w", encoding="utf-8") as handle:
    handle.write("[default]\n")
    handle.write(f"aws_access_key_id = {access_key}\n")
    handle.write(f"aws_secret_access_key = {secret_key}\n")
    if session_token:
        handle.write(f"aws_session_token = {session_token}\n")
PY
    export AWS_SHARED_CREDENTIALS_FILE="$AWS_CREDENTIALS_FILE"
}
