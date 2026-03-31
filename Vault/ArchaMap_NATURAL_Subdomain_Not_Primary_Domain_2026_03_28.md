# ArchaMap NATURAL Subdomain Not Primary Domain 2026 03 28

Issue:
- `NATURAL` appeared as a top-level Explore domain in ArchaMap after the label metadata copy.
- It should only appear as a subdomain under `DISTRICT` / Areas.

Cause:
- ArchaMap already had a separate top-level label node:
  - `CL99`
  - `CMName = NATURAL`
  - `groupLabel = NATURAL`
  - `public = TRUE`
- The copied subdomain node was correct:
  - `CL50`
  - `CMName = NATURAL`
  - `groupLabel = DISTRICT`

Fix:
- Set `CL99.public = 'FALSE'` in ArchaMap.
- Restarted the `api` container to clear cached domain metadata.

Verification:
- `/api/metadata/domains/ArchaMap` returns `DISTRICT` but not `NATURAL`
- `/api/metadata/subdomains/ArchaMap` returns `NATURAL` under `DISTRICT`
