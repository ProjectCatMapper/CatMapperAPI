# ArchaMap Explore Built Features Download CMID Error 2026 03 28

The Explore-page advanced download for ArchaMap could fail with the opaque error `CMID`.

Cause:
- `CM/download.py:getAdvancedDownload` concatenated two pandas dataframes and always grouped by `CMID`.
- When both queries returned no rows, pandas raised `KeyError('CMID')` because the empty concatenated dataframe had no `CMID` column.

Fix:
- Validate that `CMIDs` are present and non-empty before querying.
- Normalize and validate requested `properties`.
- Raise a clear `LookupError` when the advanced download finds no rows for the requested CMIDs.
- Treat `ValueError` and `LookupError` as `400` responses in `CMroutes/download_routes.py`.

Files:
- `[[../CM/download.py]]`
- `[[../CMroutes/download_routes.py]]`
- `[[../tests/download_test.py]]`
