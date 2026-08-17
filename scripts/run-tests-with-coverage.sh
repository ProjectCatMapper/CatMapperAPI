#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

conda run -n api_env python -m pytest -q -m "not realdb and not redis" \
  --cov=CM \
  --cov=CMroutes \
  --cov=app \
  --cov-branch \
  --cov-report=term-missing \
  --cov-report=xml
