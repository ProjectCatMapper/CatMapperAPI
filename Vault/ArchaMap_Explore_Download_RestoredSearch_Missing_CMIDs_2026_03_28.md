# ArchaMap Explore Download Restored Search Missing CMIDs 2026 03 28

The Explore page could show cached search rows while losing the parallel CMID list needed for advanced download.

Cause:
- `CatMapperJS/src/components/ExploreSearch.js` restored `users` from `sessionStorage`.
- It did not restore `qcount` or `cmid_download`.
- The table could therefore show old results, such as 207 rows, while the download dialog posted `CMIDs: null`.

Fix:
- Persist and restore `qcount` and `cmid_download` with the search state.
- Derive fallback CMIDs from visible `users` rows in `CatMapperJS/src/components/EditAdvanced.js` if the saved list is missing.

Validation:
- `vite build` succeeded in `CatMapperJS`.
