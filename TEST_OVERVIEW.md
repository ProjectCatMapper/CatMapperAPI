# CatMapperAPI Test Overview

Last reviewed: February 18, 2026

This folder currently has 15 automated API test suites with 54 total checks.  
The goal of these tests is to confirm that core account, admin, upload/download, merge, and documentation features behave safely and predictably.

## Current Test Suites

- `tests/user_profile_test.py` (14 checks): Confirms profile viewing/updating, password changes, bookmarks/history, and API key creation all work correctly with proper safety rules.
- `tests/upload_routes_test.py` (4 checks): Verifies upload naming behavior (including alternate names) and blocks uploads when the signed-in user does not match the request.
- `tests/download_test.py` (3 checks): Confirms download links are returned correctly and advanced download requires the right inputs before returning data.
- `tests/translate_test.py` (1 check): Confirms the translate endpoint returns expected output files and ordering.
- `tests/merge_template_summary_test.py` (2 checks): Verifies merge templates produce the right summary details for different merge modes.
- `tests/merge_routes_validation_test.py` (3 checks): Checks merge request guardrails so invalid combinations are rejected and valid ones are accepted.
- `tests/merge_extended_dataset_names_test.py` (5 checks): Verifies merged outputs include expected dataset naming and clean column behavior.
- `tests/admin_test.py` (3 checks): Confirms admin endpoints return the correct editable data and hide internal-only labels.
- `tests/admin_auth_test.py` (3 checks): Verifies admin edit routes enforce admin permissions and user identity checks.
- `tests/auth_utils_api_key_test.py` (4 checks): Confirms API key authentication accepts valid keys and rejects invalid or mismatched credentials.
- `tests/forgot_password_test.py` (3 checks): Verifies password reset request/confirm flow works and avoids exposing whether an account exists.
- `tests/docs_test.py` (2 checks): Confirms API home and documentation pages load.
- `tests/cors_auth_header_test.py` (1 check): Verifies browsers are allowed to send authorization headers during cross-origin preflight checks.
- `tests/explore_routes_color_logic_test.py` (4 checks): Confirms map color-assignment rules are consistent and deterministic.
- `tests/utils_test.py` (2 checks): Verifies shared driver utilities reuse connections and clean up correctly.

## Notes

- This summary reflects the automated suites in `tests/*_test.py`.
- Non-suite files like `test.py`, `test.sh`, notebooks, and output artifacts are not counted as automated test suites.
